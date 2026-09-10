#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
compress_index_years.py — 把 H:\index\CNIndex 按年份多进程压缩成 5 个 zip

源:   H:\index\CNIndex\<YYYYMMDD>\<sh|sz><code>.parquet   (2022~2026 共 1127 个交易日, ~62 万文件)
输出: H:\index\<year>.zip                                  (zip 根为日期目录, 如 20220104/sh000001.parquet)

多进程: 每年一个进程 (共 5 个), deflate level=1 (parquet 已压缩过, 快速档即可)。
用法:  python compress_index_years.py
"""
import os
import time
import zipfile
from multiprocessing import Pool

SRC = r'H:\index\CNIndex'
DST = r'H:\index'
YEARS = ['2022', '2023', '2024', '2025', '2026']


def zip_year(year: str) -> None:
    """把一年的全部日期目录压成一个 <year>.zip, 返回 (天数, 文件数)。"""
    days = sorted(d for d in os.listdir(SRC) if d.startswith(year))
    out = os.path.join(DST, f'{year}.zip')
    t0 = time.perf_counter()
    n = 0
    with zipfile.ZipFile(out, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=1,
                         allowZip64=True) as zf:
        for i, day in enumerate(days, 1):
            day_dir = os.path.join(SRC, day)
            for f in sorted(os.listdir(day_dir)):
                zf.write(os.path.join(day_dir, f), arcname=f'{day}/{f}')
                n += 1
            if i % 20 == 0 or i == len(days):
                print(f'  [{year}] {i}/{len(days)} 天, {n:,} 文件, {time.perf_counter() - t0:.0f}s', flush=True)
    print(f'[{year}] 完成: {len(days)} 天 {n:,} 文件 -> {out} ({time.perf_counter() - t0:.0f}s)', flush=True)


if __name__ == '__main__':
    t0 = time.perf_counter()
    with Pool(len(YEARS)) as pool:
        pool.map(zip_year, YEARS)
    print(f'全部完成, 总耗时 {time.perf_counter() - t0:.0f}s', flush=True)
