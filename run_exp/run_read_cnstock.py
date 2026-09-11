#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
run_read_cnstock.py — CNStock 流水线 (worker / writer): 调用 read_cnstock_L2, 不改库逻辑。

场景: 2026 年归档已全部在 H:\raw\SSE\ (H 盘只剩读, 无下载写入/无输出写入), 无需盯盘也无需中转缓存。
盘序: H 盘 HDD 只被 worker 的 tar 流式读触碰 (解压限速后每路 ~4.5MB/s, 6 路合计 ~27MB/s, 远低于
HDD 顺序读能力, 磁盘 90% 时间空闲); D 盘 SSD 写产物 (feather/csv), 两盘彻底错开。

  主进程 : 启动时一次性扫描 H:\raw\SSE 建任务清单 (跳过 succsse.txt 账本已有的天, 按日期升序),
           全部任务立刻入队 —— 4 个 worker 马上各自开流进入流水线 (预读即任务前置, 无空窗)。
  worker x N : 取任务 -> 直接调库 CNStockL2.parse (src_root=H:\raw, 库内流式:
           tar 解压 -> 逐块解码 -> Arrow RecordBatch 追加写 feather (zstd) -> csv 到 D:\processed)。
           bz2 解压是单核 CPU 密集 (~6-7 分钟/档), N 个 worker = N 路并行解压。
           任何异常 -> 失败消息, 不中断。
  writer x1 : 成功 -> 记 SUCC_TXT; 失败 -> 记 FAIL_TXT (H 盘原始归档一律不删, 由用户处置)。

跳过口径: 仅以 SUCC_TXT (succsse.txt) 为已完成账本 —— 启动时加载历史成功记录 (交易所+日期),
账本里有的天不重跑。 全部任务跑完自然结束; Ctrl+C 可随时提前停。
worker 失败的天不写 csv, 重启自动重试 (仍不在账本); 中途强停可能留无 footer 的残废 feather
(无 csv), 重跑覆盖, 无脏数据风险。

内存上界: N_WORKERS x ~1.5GB 峰值 (单块 424MB + 过滤拷贝 + DataFrame + RecordBatch) ≈ 9GB, 32GB 机器宽裕。

当前只跑 SSE (EXCHANGES 只含 SSE; SZSE 的归档下不动, 待后续指令)。
"""
import os
import re
import sys
import time
from multiprocessing import Process, Queue, get_context

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))   # 项目根, 供 import file_io/read_cnstock_L2

import pandas as pd

from file_io import join_path
from read_cnstock_L2 import CNStockL2

RAW_ROOT = r'H:\raw'               # 源: H 盘 HDD, <RAW_ROOT>\SSE\*_3s_tick_quote_<date>.tar.bz2 (只读不删)
OUT_ROOT = r'D:\processed'         # 输出: <OUT_ROOT>\SSE\<date>.feather + <date>.csv (D 盘写)
FAIL_TXT = r'H:\raw\fail.txt'      # 失败记录 (追加: 时间 \t 交易所 \t 文件名 \t 错误; 源文件保留不删)
SUCC_TXT = r'H:\raw\succsse.txt'   # 成功记录 (追加: 时间 \t 交易所 \t 日期 \t symbols \t records)
EXCHANGES = ('SSE',)               # 先只跑 SSE; SZSE 归档暂不下动, 待后续指令
N_WORKERS = 6                      # worker 数 = 并行解压路数 (bz2 单核密集; 无下载后 H 盘只读,
                                   # 6 路 ~27MB/s 仍在 HDD 能力内, 16 核富余; 用户白天轻用机器)


# ============================================================== 任务清单
def _load_done() -> set:
    """从 SUCC_TXT 加载已完成 (exchange, date) 集合 —— succsse.txt 即完成账本。
    文件不存在/坏行容错跳过。"""
    done = set()
    if not os.path.isfile(SUCC_TXT):
        return done
    with open(SUCC_TXT, encoding='utf-8') as f:
        for line in f:
            parts = line.rstrip('\n').split('\t')
            if len(parts) >= 3 and len(parts[2]) == 8 and parts[2].isdigit():
                done.add((parts[1], parts[2]))
    return done


def scan_todo() -> list:
    r"""一次性扫描 RAW_ROOT 下各交易所目录 -> 待处理任务 [(exchange, date, fn)], 按日期升序。
    跳过: 仅 succsse.txt 账本已有的天; 文件名无日期的忽略。"""
    done = _load_done()
    if done:
        print(f'  [scan] succsse.txt 已有 {len(done)} 条完成记录, 对应天将跳过', flush=True)
    tasks = []
    for exchange in EXCHANGES:
        day_dir = join_path(RAW_ROOT, exchange)
        if not os.path.isdir(day_dir):
            continue
        for fn in os.listdir(day_dir):
            if not fn.endswith('.tar.bz2'):                        # 只认归档名; fail.txt/succsse.txt 天然排除
                continue
            m = re.search(r'(\d{8})', fn)
            if not m:
                print(f'  [scan] !! {exchange}\\{fn} 文件名无日期, 忽略', flush=True)
                continue
            date = m.group(1)
            if (exchange, date) in done:                           # 账本里已有 = 完成过
                continue
            tasks.append((exchange, date, fn))
    tasks.sort(key=lambda t: t[1])                                 # 按日期升序
    return tasks


# ============================================================== worker (子进程)
def worker(task_q: Queue, msg_q: Queue) -> None:
    r"""处理 worker: 取任务 -> 调库 CNStockL2.parse (src_root=H:\raw, 直接流式读 H 盘归档;
    库内流式写盘: feather + csv 都写到 D:\processed) -> 读 csv 汇总 symbol/records 数发小消息。
    异常发失败消息, 不中断。"""
    r = CNStockL2(src_root=RAW_ROOT, out_root=OUT_ROOT)
    while True:
        msg = task_q.get()
        if msg is None:                                             # 主进程退出前的哨兵
            return
        exchange, date, fn = msg
        t0 = time.perf_counter()
        try:
            r.parse(exchange, date)                                 # H 盘流式读 -> 解压解码 -> D 盘落盘
            sym = pd.read_csv(join_path(OUT_ROOT, exchange, f'{date}.csv'), dtype={'symbol': str})
            n_sym, n_rec = len(sym), int(sym['day_records'].sum())
            msg_q.put((exchange, date, fn, n_sym, n_rec, None))
            print(f'  [worker] {exchange} {date} 完成: {n_sym} symbols, {n_rec:,} records, '
                  f'{time.perf_counter() - t0:.0f}s', flush=True)
        except Exception as e:                                      # 失败: 不中断, 发失败消息给 writer 记 fail.txt
            msg_q.put((exchange, date, fn, 0, 0, f'{type(e).__name__}: {e}'))
            print(f'  [worker] !! {exchange} {date} 失败: {type(e).__name__}: {e}', flush=True)


# ============================================================== writer (子进程)
def writer(msg_q: Queue) -> None:
    """日志进程 (串行化写两个日志文件, 避免多进程并发 append 交错):
    成功 -> 记 SUCC_TXT; 失败 -> 记 FAIL_TXT。 H 盘原始归档一律不删。 收到 None 哨兵退出。"""
    n_done, n_fail = 0, 0
    while True:
        msg = msg_q.get()
        if msg is None:                                             # 主进程退出前的哨兵
            return
        exchange, date, fn, n_sym, n_rec, err = msg
        if err is not None:                                         # worker 失败: 记 fail.txt (源文件保留)
            n_fail += 1
            with open(FAIL_TXT, 'a', encoding='utf-8') as f:
                f.write(f'{time.strftime("%Y-%m-%d %H:%M:%S")}\t{exchange}\t{fn}\t{err}\n')
            print(f'  [writer] !! 累计 {n_done} 成功 / {n_fail} 失败, 最新失败: {exchange} {date} {err}',
                  flush=True)
            continue
        with open(SUCC_TXT, 'a', encoding='utf-8') as f:             # 成功日志 (与 fail.txt 对称)
            f.write(f'{time.strftime("%Y-%m-%d %H:%M:%S")}\t{exchange}\t{date}\t{n_sym}\t{n_rec}\n')
        n_done += 1
        print(f'  [writer] 落盘 {exchange} {date}: {n_sym} symbols, {n_rec:,} records, '
              f'累计 {n_done} 成功 / {n_fail} 失败', flush=True)


# ============================================================== 主流程
def main() -> None:
    tasks = scan_todo()                                             # 一次性扫描建清单
    if not tasks:
        print('--- [CNStock 流水线] 无待处理任务, 退出', flush=True)
        return
    print(f'--- [CNStock 流水线] 待处理 {len(tasks)} 档 ({tasks[0][1]} ~ {tasks[-1][1]}): '
          f'{N_WORKERS} worker (H 盘流式读 -> D 盘写 {OUT_ROOT}) + 1 writer (记日志), '
          f'成功记 {SUCC_TXT}, 失败记 {FAIL_TXT}', flush=True)
    ctx = get_context('spawn')                                      # Windows 必须显式
    task_q = ctx.Queue()                                            # 主进程 -> worker (小消息)
    msg_q = ctx.Queue()                                             # worker -> writer (小消息)
    writer_proc = ctx.Process(target=writer, args=(msg_q,), name='writer', daemon=True)
    writer_proc.start()
    workers = [ctx.Process(target=worker, args=(task_q, msg_q), name=f'w{i}', daemon=True)
               for i in range(N_WORKERS)]
    for w in workers:
        w.start()
    t0 = time.perf_counter()
    try:
        for t in tasks:                                             # 全部任务立刻入队: worker 即刻进入流水线
            task_q.put(t)
    finally:
        # 哨兵排在队列尾部: worker 把全部任务处理完才退出 —— join 不设超时
        for _ in workers:
            task_q.put(None)
        for w in workers:
            w.join()
        msg_q.put(None)                                             # 通知 writer 退出 (写完当前消息后)
        writer_proc.join()
        print(f'--- [CNStock 流水线] 结束, 总耗时 {(time.perf_counter() - t0) / 60:.0f} 分钟, '
              f'结果见 succsse.txt / fail.txt', flush=True)


if __name__ == '__main__':
    main()
