#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
run_read_cnindex.py — INDEX 全量运行 (共享内存块池版): 调用 read_cnindex_L2。

架构 (v8: 单读 + 少量 worker + 大批写; 读是瓶颈, 参数按瓶颈收敛):
  主进程     : 预创建 N_IN+N_OUT 块命名共享内存 (各 BLK_SIZE), join 后统一回收 —— 块全程存活无竞态
  读进程 x1  : 一天源 parquet 字节 pickle 后写输入块, 队列只发 (date, 块名, 长度) 小消息
  worker x N : attach 输入块读出解码 -> 拼全天 df -> zstd 压缩成 feather 字节写输出块,
               队列只发 (date, 块名, 长度, csv字节) 小消息; 用完的输入块归还池子
  写进程 x1  : attach 输出块读出压缩字节, 收到一天即写一天 (读写异盘 H/D); 读完的块归还池子

读是硬瓶颈 (HDD 每天 ~550 个小文件随机读 ~12-20s/天), worker 解码 ~4.5s/天 —— 4 个 worker
足以跟上; writer 收到一天即写一天 (单路大文件顺序写, 不与读小文件抢头)。

为何不用 Queue 直接传: multiprocessing.Queue 底层单管道, 大 payload 全串行挤一根管;
块池方案下管道只传 KB 级消息, 数据走共享内存零拷贝竞争。

内存上界: (N_IN + N_OUT) x BLK_SIZE + N_WORKERS x ~1.5GB ≈ 3.2 + 6 = 9.2GB (32GB 机器安全)。
输入: SRC_ROOT\<date>\ 逐天取日期 (源 parquet 小文件); 输出: 统一平铺在 OUT_ROOT\CNIndex\ 下
(与库 read_cnindex_L2 的新 out 布局一致): <date>.feather (zstd) + <date>.csv 完成标记,
可断点续跑。2026-09-10 全量 1127 天已完成, 产物在 H:\processed\CNIndex; 源 parquet 已删除。
"""
import io
import os
import pickle
import sys
import time
from multiprocessing import Process, Queue, get_context
from multiprocessing.shared_memory import SharedMemory

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))   # 项目根, 供 import file_io/read_cnindex_L2

import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather

from file_io import exist_file, join_path
from read_cnindex_L2 import CNIndexL2

SRC_ROOT = r'H:\raw\CNIndex'       # 输入: 日期目录, 每目录 ~550 个 parquet (与库 _test 一致; 源已于 2026-09-10 删除)
OUT_ROOT = r'H:\processed'         # 输出: <OUT_ROOT>\CNIndex\<date>.feather 平铺 (与库 out 布局一致)
N_WORKERS = 4                      # 解码+压缩 worker 数 (读是瓶颈 ~12-20s/天, 4 个已跟得上)
WRITE_BATCH = 1                    # 写进程攒 N 天一批落盘; 1 = 收到即写 (读写异盘 H 读/D 写, 不混跑)
BLK_SIZE = 320 * 1024 * 1024       # 共享内存块大小: V1 大天源 ~190MB, zstd 输出 ~100MB, 留余量
N_IN_BLK = 4                       # 输入块数 (reader 向前缓冲)
N_OUT_BLK = 6                      # 输出块数 (worker 已完成待写盘缓冲)
N_READERS = 1                      # 读进程数: 单读顺序喂流水线 (读为瓶颈, 多读只会在 HDD 上互相抢头)


# ============================================================== 子进程函数 (模块级, 便于 spawn pickling)
def reader(todo: list, in_blk_q: Queue, in_msg_q: Queue, n_sentinel: int) -> None:
    """单线程顺序读源: 一天全部 parquet 字节 pickle 后写输入块, 队列发小消息; 块用完后由 worker 归还池子。
    N_READERS 路并发时各自领 todo 的一个分片, 哨兵共发 N_WORKERS 个 (每路 n_sentinel 个)。"""
    for date in todo:
        items = []
        day_dir = join_path(SRC_ROOT, date)
        for fn in sorted(os.listdir(day_dir)):
            if fn.endswith('.parquet'):
                with open(join_path(day_dir, fn), 'rb') as f:
                    items.append((fn[:-len('.parquet')], f.read()))
        data = pickle.dumps(items)
        assert len(data) <= BLK_SIZE, f'{date} 源 {len(data)/1024**2:.0f}MB 超过块大小, 需调大 BLK_SIZE'
        name = in_blk_q.get()                                     # 拿空闲块 (没有就等 worker 归还)
        shm = SharedMemory(name=name)
        shm.buf[:len(data)] = data
        shm.close()                                               # 主进程持有块句柄, 块不会消失
        in_msg_q.put((date, name, len(data)))
        print(f'  [reader] {date} 读入完成 {len(data)/1024**2:.0f}MB', flush=True)
    for _ in range(n_sentinel):
        in_msg_q.put(None)                                        # 各路哨兵合计 N_WORKERS 个


def worker(in_msg_q: Queue, in_blk_q: Queue, out_blk_q: Queue, out_msg_q: Queue) -> None:
    """解码 worker: attach 输入块取一天字节 -> 解码拼表 -> zstd 压缩写输出块 -> 发小消息。 纯 CPU 不碰盘。"""
    while True:
        msg = in_msg_q.get()
        if msg is None:                                           # reader 发完后的哨兵: 转发给 writer 再退出
            out_msg_q.put(None)
            return
        date, in_name, n = msg
        shm = SharedMemory(name=in_name)
        items = pickle.loads(bytes(shm.buf[:n]))
        shm.close()
        in_blk_q.put(in_name)                                     # 输入块用完立即归还池子
        try:
            frames = []
            for stem, buf in items:
                src = pd.read_parquet(io.BytesIO(buf))
                frames.append(CNIndexL2._decode(src, stem, date))
            df = pd.concat(frames, ignore_index=True)
            del frames
            symbols = df.groupby('symbol', sort=False).size().rename('day_records').reset_index()
            fbuf = io.BytesIO()
            feather.write_feather(pa.Table.from_pandas(df, preserve_index=False), fbuf, compression='zstd')
            cbuf = io.BytesIO()
            symbols.to_csv(cbuf, index=False)
            n_sym, n_rec = len(symbols), int(symbols['day_records'].sum())
            fdata = fbuf.getvalue()
            del df, fbuf
            out_name = out_blk_q.get()                            # 拿空闲输出块 (满则等 writer 归还)
            osm = SharedMemory(name=out_name)
            osm.buf[:len(fdata)] = fdata
            osm.close()
            out_msg_q.put((date, out_name, len(fdata), cbuf.getvalue(), n_sym, n_rec))
            del fdata
        except Exception as e:                                    # 源文件损坏等, 记录后继续
            out_msg_q.put((date, None, 0, b'', 0, f'{type(e).__name__}: {e}'))


def writer(out_msg_q: Queue, out_blk_q: Queue, n_total: int, t0: float) -> None:
    """单线程批量写: attach 输出块取压缩字节 (零解析), 攒满 WRITE_BATCH 天一批落盘, 读完块归还池子。
    收满 N_WORKERS 个哨兵后写完剩余批次、打印失败汇总再退出。"""
    out_dir = join_path(OUT_ROOT, 'CNIndex')
    os.makedirs(out_dir, exist_ok=True)
    batch, done, failed, n_stop = [], 0, [], 0
    while True:
        msg = out_msg_q.get()
        if msg is None:                                           # worker 退出哨兵
            n_stop += 1
            if n_stop < N_WORKERS:
                continue
            break                                                 # 全部 worker 退出 -> 写完剩余批次后收尾
        date, out_name, n, cdata, n_sym, n_rec = msg
        if out_name is None:                                      # 该天解码失败: 不落盘, 记录
            failed.append((date, n_rec))
            done += 1
            print(f'  [{done}/{n_total}] !! {date} 失败: {n_rec}', flush=True)
            continue
        shm = SharedMemory(name=out_name)
        fdata = bytes(shm.buf[:n])
        shm.close()
        out_blk_q.put(out_name)                                   # 输出块读完立即归还池子
        batch.append((date, fdata, cdata, n_sym, n_rec))
        if len(batch) >= WRITE_BATCH:                             # 攒满一批, 一次性落盘
            _flush(out_dir, batch, n_total, t0)
            done += len(batch)
            batch = []
    if batch:                                                     # 收尾: 剩余不足一批的
        _flush(out_dir, batch, n_total, t0)
        done += len(batch)
    print(f'--- [writer] 完成 {done}/{n_total}, 失败 {len(failed)} {failed}', flush=True)


def _flush(out_dir: str, batch: list, n_total: int, t0: float) -> None:
    """一批落盘: 顺序 dump feather 字节 + csv 字节 (纯 IO, 不占 CPU)。 csv 最后写出 = 完成标记。"""
    for date, fdata, cdata, n_sym, n_rec in batch:
        with open(join_path(out_dir, f'{date}.feather'), 'wb') as f:
            f.write(fdata)
        with open(join_path(out_dir, f'{date}.csv'), 'wb') as f:
            f.write(cdata)
    dates = ', '.join(b[0] for b in batch)
    print(f'  落盘 {len(batch)} 天 [{dates}] 最后 {batch[-1][3]} symbols, {batch[-1][4]} records, '
          f'累计 {time.perf_counter() - t0:.0f}s', flush=True)


def main() -> None:
    days = sorted(d for d in os.listdir(SRC_ROOT) if d.isdigit())
    # 跳过检查: H:\processed\CNIndex\<date>.csv (唯一产出位置)
    def _done(d: str) -> bool:
        return exist_file(join_path(OUT_ROOT, 'CNIndex', f'{d}.csv'))
    todo = [d for d in days if not _done(d)]
    print(f'--- [INDEX 全量] 共 {len(days)} 天, 待解析 {len(todo)}, 已完成跳过 {len(days) - len(todo)}, '
          f'{N_READERS} 读 + {N_WORKERS} 解码压缩 + 1 批量写(批 {WRITE_BATCH}), 共享内存 {N_IN_BLK}+{N_OUT_BLK} 块 x '
          f'{BLK_SIZE // 1024 // 1024}MB', flush=True)
    if not todo:
        return
    ctx = get_context('spawn')                                   # Windows 必须显式
    # 块名带 PID: 进程被强杀时旧块残留系统且 unlink 无效 (Windows), 每次换名绕开, 残块随系统重启释放
    in_names = [f'idx_in_{os.getpid()}_{i}' for i in range(N_IN_BLK)]
    out_names = [f'idx_out_{os.getpid()}_{i}' for i in range(N_OUT_BLK)]
    in_shms = [SharedMemory(name=n, create=True, size=BLK_SIZE) for n in in_names]
    out_shms = [SharedMemory(name=n, create=True, size=BLK_SIZE) for n in out_names]
    in_blk_q = ctx.Queue()
    out_blk_q = ctx.Queue()
    for n in in_names:
        in_blk_q.put(n)
    for n in out_names:
        out_blk_q.put(n)
    in_msg_q = ctx.Queue()
    out_msg_q = ctx.Queue()
    t0 = time.perf_counter()
    n_sent = N_WORKERS // N_READERS
    reader_procs = [ctx.Process(target=reader, args=(todo[i::N_READERS], in_blk_q, in_msg_q, n_sent),
                                name=f'reader{i}') for i in range(N_READERS)]
    writer_proc = ctx.Process(target=writer, args=(out_msg_q, out_blk_q, len(todo), t0), name='writer')
    writer_proc.start()
    for r in reader_procs:
        r.start()
    workers = [ctx.Process(target=worker, args=(in_msg_q, in_blk_q, out_blk_q, out_msg_q), name=f'w{i}')
               for i in range(N_WORKERS)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()                                                 # 全部 worker 退出 = 解码压缩全完
    writer_proc.join()                                           # writer 收满哨兵后写完剩余批次退出
    for r in reader_procs:
        r.join()
    for shm in in_shms + out_shms:                               # 回收共享内存块
        shm.close()
        shm.unlink()
    print(f'--- [INDEX 全量] {len(days)} 天: 跳过 {len(days) - len(todo)}, '
          f'总耗时 {time.perf_counter() - t0:.0f}s', flush=True)


if __name__ == '__main__':
    main()
