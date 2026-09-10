#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
run_read_cnindex.py — INDEX 全量运行 (批量预载 + 多进程): 调用 read_cnindex_L2, 解析 INDEX_ROOT 全部日期。

与逐天版 (直接让 4 个子进程各自随机读 H 盘) 的区别:
  每轮主进程先把一批日期的源 parquet 以"压缩字节"形式一次性顺序读入内存
  (单线程顺序读, HDD 磁头不来回摆), 再把 (date, [(stem, bytes), ...]) 分发给子进程;
  子进程从内存 BytesIO 解 parquet -> 调 CNIndexL2._decode -> 落盘, 全程零随机读盘。
  分批按压缩字节预算控制内存: 解压后在内存膨胀 2~6 倍, 预算 6GB/批 (机器 32GB),
  V1 大天 (~180MB/天) 自动缩到 ~30 天/批, 2023-05 后小天 (~33MB/天) 可到 200 天/批。

write_together=True 落盘 <md_root>\full\<date>.feather;
以 <date>_INDEX.csv (每天最后写出) 为完成标记, 已存在则跳过 —— 可断点续跑。
"""
import gc
import io
import os
import sys
import time
from multiprocessing import Pool

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))   # 项目根, 供 import file_io/read_cnindex_L2

import pandas as pd

from file_io import create_directory, exist_file, join_path
from read_cnindex_L2 import CNIndexL2

INDEX_ROOT = r'H:\index\CNIndex'   # 源: 1127 个日期目录 (2022~2026), 每目录 ~550 个 parquet
MD_ROOT = r'H:\index'              # 输出: <MD_ROOT>\full\<date>.feather
N_WORKERS = 4                      # 并行进程数 (只做 CPU 解码 + feather 顺序写, 不再随机读盘)
MEM_BUDGET_GB = 6.0                # 一批源 parquet 压缩字节上限; 解压后膨胀 2~6 倍仍安全 (机器 32GB)
MAX_BATCH_DAYS = 200               # 单批天数上限


def run_day(task):
    """子进程任务: date + 该日全部源 parquet 字节 (已在内存) -> 解码落盘。
    复刻 parse 的薄胶水 (不改动 read_cnindex_L2): 解码直接调用其静态方法 _decode。
    单日失败不拖垮全局: 返回 (date, n_symbols, n_records, 错误信息)。"""
    date, items = task
    try:
        frames = []
        for stem, buf in items:
            src = pd.read_parquet(io.BytesIO(buf))
            frames.append(CNIndexL2._decode(src, stem, date))
        df = pd.concat(frames, ignore_index=True)
        out_dir = join_path(MD_ROOT, 'full')
        create_directory(out_dir, last_as_directory=True)
        df.to_feather(join_path(out_dir, f'{date}.feather'))
        symbols = df.groupby('symbol', sort=False).size().rename('day_records').reset_index()
        symbols.to_csv(join_path(out_dir, f'{date}_INDEX.csv'), index=False)
        return date, len(symbols), int(symbols['day_records'].sum()), None
    except Exception as e:                             # 源文件损坏/缺列等, 记录后继续
        return date, 0, 0, f'{type(e).__name__}: {e}'


def day_size(date: str) -> int:
    """一天源 parquet 总字节 (压缩), 用于分批预算。"""
    n = 0
    with os.scandir(join_path(INDEX_ROOT, date)) as it:
        for e in it:
            if e.name.endswith('.parquet'):
                n += e.stat().st_size
    return n


def build_batches(todo: list, sizes: dict) -> list:
    """按压缩字节预算 + 天数上限切批 (顺序不乱, V1 大天自动缩批)。"""
    budget = MEM_BUDGET_GB * 1024 ** 3
    batches, cur, acc = [], [], 0
    for d in todo:
        if cur and (acc + sizes[d] > budget or len(cur) >= MAX_BATCH_DAYS):
            batches.append(cur)
            cur, acc = [], 0
        cur.append(d)
        acc += sizes[d]
    if cur:
        batches.append(cur)
    return batches


def preload(batch: list) -> list:
    """主进程顺序读入一批日期的全部源 parquet 字节 (零随机写, 磁头单向走)。"""
    payload = []
    for d in batch:
        items = []
        for fn in sorted(os.listdir(join_path(INDEX_ROOT, d))):
            if fn.endswith('.parquet'):
                with open(join_path(INDEX_ROOT, d, fn), 'rb') as f:
                    items.append((fn[:-len('.parquet')], f.read()))
        payload.append((d, items))
    return payload


def main() -> None:
    days = sorted(d for d in os.listdir(INDEX_ROOT) if d.isdigit())
    todo = [d for d in days if not exist_file(join_path(MD_ROOT, 'full', f'{d}_INDEX.csv'))]
    print(f'--- [INDEX 全量] 共 {len(days)} 天, 待解析 {len(todo)}, 已完成跳过 {len(days) - len(todo)}', flush=True)
    t_all = time.perf_counter()
    print('统计待解析天源文件大小 (一次性, 用于分批)...', flush=True)
    sizes = {d: day_size(d) for d in todo}
    batches = build_batches(todo, sizes)
    print(f'分 {len(batches)} 批 (每批 <= {MEM_BUDGET_GB} GB 压缩字节 / {MAX_BATCH_DAYS} 天), {N_WORKERS} 进程', flush=True)
    n_done, failed = 0, []
    with Pool(N_WORKERS) as pool:
        for bi, batch in enumerate(batches, 1):
            t0 = time.perf_counter()
            payload = preload(batch)
            gb = sum(sizes[d] for d in batch) / 1024 ** 3
            print(f'  [批 {bi}/{len(batches)}] 预载 {len(batch)} 天 / {gb:.1f} GB, {time.perf_counter() - t0:.0f}s', flush=True)
            for date, n_sym, n_rec, err in pool.imap_unordered(run_day, payload):
                n_done += 1
                if err is None:
                    print(f'  [{n_done}/{len(todo)}] INDEX {date}: {n_sym} symbols, {n_rec} records, '
                          f'累计 {time.perf_counter() - t_all:.0f}s', flush=True)
                else:
                    failed.append(date)
                    print(f'  [{n_done}/{len(todo)}] !! {date} 失败: {err}', flush=True)
            del payload                                     # 释放本批内存再进下一批
            gc.collect()
    print(f'--- [INDEX 全量] {len(days)} 天: 成功 {n_done - len(failed)}, 失败 {len(failed)} {failed}, '
          f'跳过 {len(days) - len(todo)}, 总耗时 {time.perf_counter() - t_all:.0f}s', flush=True)


if __name__ == '__main__':
    main()
