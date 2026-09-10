#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
scan_index_parquet.py — 扫描 INDEX_ROOT 下全部 parquet, 按 footer magic (PAR1) 找损坏文件。
结果写 .workbuddy/bad_parquet.txt (每行: 状态 + 文件路径)。
"""
import os
import sys
import time
from multiprocessing import Pool

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

INDEX_ROOT = r'H:\index\CNIndex'
OUT_TXT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.workbuddy', 'bad_parquet.txt')


def scan_day(day: str):
    """扫描单日目录, 返回 (day, 坏文件列表, 文件总数)。"""
    bad, total = [], 0
    d = os.path.join(INDEX_ROOT, day)
    for name in os.listdir(d):
        if not name.endswith('.parquet'):
            continue
        total += 1
        p = os.path.join(d, name)
        try:
            with open(p, 'rb') as f:
                f.seek(0)
                head = f.read(4)
                f.seek(-4, os.SEEK_END)
                tail = f.read(4)
            if head != b'PAR1' or tail != b'PAR1':
                bad.append(p)
        except OSError as e:
            bad.append(f'{p} (IO错误: {e})')
    return day, bad, total


def main() -> None:
    days = sorted(d for d in os.listdir(INDEX_ROOT) if d.isdigit())
    t0 = time.perf_counter()
    n_bad_files, n_bad_days = 0, 0
    with open(OUT_TXT, 'w', encoding='utf-8') as out:
        with Pool(16) as pool:
            for i, (day, bad, total) in enumerate(pool.imap_unordered(scan_day, days), 1):
                if bad:
                    n_bad_days += 1
                    n_bad_files += len(bad)
                    out.write(f'# {day}: {len(bad)}/{total} 个坏文件\n')
                    out.write('\n'.join(bad) + '\n')
                if i % 200 == 0:
                    print(f'  已扫 {i}/{len(days)} 天, 坏文件 {n_bad_files}', flush=True)
    print(f'--- [扫描] {len(days)} 天完成: 坏文件 {n_bad_files} 个 (分布在 {n_bad_days} 天), '
          f'清单 -> {OUT_TXT}, 耗时 {time.perf_counter() - t0:.0f}s', flush=True)


if __name__ == '__main__':
    main()
