#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
read_cnstock_L2.py — SSE/SZSE 3 秒十档快照 (OB3S) 解析器

数据布局 (每个交易所一个数据源; 归档名后缀 .tar.bz2 但约 1/3 实为 xz, 按压缩 magic 自动选 tar 参数):
  <binary_root>\SSE\dzsse_3s_tick_quote_YYYYMMDD.tar.bz2    上交所 (DZ 源)
  <binary_root>\SZSE\gj2szse_3s_tick_quote_YYYYMMDD.tar.bz2 深交所 (GJ2 源)

记录: OB3S 424B, 小端, C 8 字节对齐, 无文件头 offset=0, 每条首 8 字节 = ReceiveTime (epoch 微秒 UTC)。

用法:
  单天冒烟: python read_cnstock_L2.py (内置 _test, 跑 20251229 两个交易所各一遍)
  或 import (src_root/out_root 必传, 不在库内硬编码路径):
  from read_cnstock_L2 import CNStockL2
  r = CNStockL2(src_root=SRC_ROOT, out_root=OUT_ROOT)
  r.parse('SSE', '20251229')                        # 全天一张表落盘
  # 输入: src_root\SSE\*.tar.bz2 / src_root\SZSE\*.tar.bz2 (每所一个源目录);
  # 输出: 按交易所分两个目录, 各自平铺:
  #   out_root\SSE\20251229.feather                 全天汇总一张表 (流式逐块写入)
  #   out_root\SSE\20251229.csv                     当日符号清单 (最后写出 = 完成标记; SZSE 同理)
  # feather 为 zstd 压缩 (Arrow IPC 文件格式, 逐 RecordBatch 追加写);
  # 之后取数直接读 feather, 不再碰归档。
  # 流式: 每块 (~100 万条) 解码后立即写盘, 全天 DataFrame 不再常驻 (~1GB 内存/进程);
  # 中途失败留无 footer 的残废 feather (读不了也无 csv 完成标记), 重跑自动覆盖。

字段口径 (2026-09-10 实测核实):
  - qty = 开盘至今累计成交量, SSE(DZ 源)/SZSE(GJ2 源) 统一为"股" (债券为张/回购为手), 无需缩放
    (判据: qty x last_price / turnover 中位 ≈ 1.0, 多只抽样一致);
  - 十档 bid_vol/ask_vol 与 total_bid_vol/total_ask_vol 同为"股" (total 含十档外隐藏深度,
    sum(十档)/total 占比 1%~87% 属挂单分布差异, 非单位差异);
  - 注意: 仅 GJ/GJ2 的 SSE 源 (当前未使用) qty 与十档量为 x10 虚高 (判据 turnover/(qty*last) 中位 0.10),
    若换源需 /10;
  - turnover = 累计成交额 (元);
  - pre_close = 交易所官方昨收盘价 (含除权除息调整), 算涨跌幅直接用它, 勿拼昨日 close;
  - avg_weight_* = 全量挂单 (含十档外隐藏深度) 加权均价; 可见十档 VWAP 需自算 sum(P*V)/sum(V);
  - 原始 exchange 字段 (DZ 源恒 0) 输出为 exchange_id (1=SSE, 2=SZSE) + exchange ('SSE'/'SZSE' str);
  - 时间戳统一: receive_timestamp = 落地时间 epoch 微秒; send_timestamp = 交易所时间 epoch 微秒
    (由 data_time HHMMSSmmm + 归档日期合成); receive_timestr/send_timestr = 北京时间可读字符串 (微秒);
  - SZSE 09:15 集合竞价前十档全 0 但 tot_bid_vol 已非零, 盘口统计避开开盘前窗口。
"""
import subprocess

import os
import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc

from file_io import create_directory, join_path
from config_schema import STOCK_COLUMN


# ============================================================== OB3S 字段定义 (424 B)
OB3S = np.dtype([
    ('ReceiveTime', '<i8'),           # 0   本地落地时间, epoch 微秒 (UTC), 单调不减
    ('symbol', 'S16'),                # 8   证券代码, NUL 截断 (后有垃圾字节)
    ('exchange', '<i4'),              # 24  SSE/DZ 恒 0 (解析时补); SZSE/GJ2 填 2
    ('last_price', '<f8'),            # 32  最新价 (元); 债券开盘前为 0
    ('qty', '<i8'),                   # 40  累计成交量 (股/张); 回购单位=手(1000 元面值)
    ('turnover', '<f8'),              # 48  累计成交额 (元); 回购=面值总额
    ('pre_close_price', '<f8'),       # 56  交易所官方昨收盘价 (含除权除息调整)
    *[('BPrice%d' % i, '<f8') for i in range(1, 11)],   # 64  十档买价 (元)
    *[('SPrice%d' % i, '<f8') for i in range(1, 11)],   # 144 十档卖价 (元)
    *[('BVol%d' % i, '<i8') for i in range(1, 11)],    # 224 十档买量 (股/张)
    *[('SVol%d' % i, '<i8') for i in range(1, 11)],    # 304 十档卖量 (股/张)
    ('data_time', '<i8'),             # 384 交易所时间 HHMMSSmmm (SSE 带毫秒, SZSE 整秒 000)
    ('tot_bid_vol', '<i4'),           # 392 委买总量 (真实股数)
    ('tot_ask_vol', '<i4'),           # 396 委卖总量 (真实股数)
    ('avg_weight_bid_price', '<f8'),  # 400 全量委买(含隐藏深度)加权均价
    ('avg_weight_ask_price', '<f8'),  # 408 全量委卖加权均价
    ('rdtsc', '<u8'),                 # 416 接收机 CPU TSC 周期计数 (÷3000 = µs)
], align=True)
assert OB3S.itemsize == 424


CHUNK_SIZE = 1_000_000                                    # 流式单块记录数 (424B/条 ~424MB/块)
DICT_EXCHANGE = {'SSE': ('SH', 1), 'SZSE': ('SZ', 2)}   # 交易所 -> (symbol 前缀, exchange_id)
ZIP_PREFIX = {'SSE': 'dzsse', 'SZSE': 'gj2szse'}        # 归档名前缀


def _clean_symbol(raw: bytes) -> str:
    """S16 字段 -> 干净代码: b'600519\\x00垃圾' -> '600519' (按第一个 NUL 截断, 不能 strip)."""
    return raw.split(b'\0', 1)[0].decode('ascii', 'replace')

# ============================================================== 解析器
class CNStockL2:
    """SSE/SZSE 3 秒十档快照解析器: 整档解压 (tar 子进程流式) -> 逐块解码流式落盘。"""

    def __init__(self, src_root: str, out_root: str):
        self.src_root = src_root     # 输入根: <src_root>\{SSE,SZSE}\*.tar.bz2, 由调用方传入
        self.out_root = out_root     # 输出根: 按交易所平铺在 <out_root>\{SSE,SZSE}\ 下

    # ---------- 解压 ----------
    @staticmethod
    def _cmd_untar(file_path: str) -> list:
        """按压缩 magic 选 tar 参数 (约 1/3 的 .tar.bz2 实为 xz; Python bz2/tarfile 极慢, 必须走系统管道)."""
        with open(file_path, 'rb') as fh:
            magic = fh.read(8)
        if magic.startswith(b'BZh'):
            return ['tar', '-xjOf', file_path]
        if magic.startswith(b'\xfd7zXZ\x00'):
            return ['tar', '-xJOf', file_path]
        if magic.startswith(b'\x1f\x8b'):
            return ['tar', '-xzOf', file_path]
        return ['tar', '-xOf', file_path]

    # ---------- 解析 ----------
    def parse(self, exchange: str, date: str) -> None:
        r"""整档解压 -> 分块解码 -> 流式落盘到 <out_root>\<exchange>\, 无返回值。
        当日符号清单 (symbol + day_records) 落盘为 <exchange>\<date>.csv (最后写出 = 完成标记)。

        流式: 每块 (~100 万条) 解码后立即转 Arrow RecordBatch 追加写进 feather
        (Arrow IPC 文件格式 + zstd, footer 在收尾时写出), 全天 DataFrame 不再常驻;
        symbol 计数逐块累加 (口径与整表 groupby 完全一致, 首次出现顺序)。

        内存: ~1.5GB/进程峰值 (单块 424MB 原始字节 + 过滤拷贝 + DataFrame + RecordBatch)。
        中途失败: 留无 footer 的残废 feather (无 csv 完成标记), 重跑自动覆盖。"""
        exchange = exchange.upper()
        in_fname = join_path(self.src_root, exchange, f'{ZIP_PREFIX[exchange]}_3s_tick_quote_{date}.tar.bz2')
        out_dir = join_path(self.out_root, exchange)
        create_directory(out_dir, last_as_directory=True)
        #
        proc = subprocess.Popen(self._cmd_untar(in_fname), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        chunk_bytes = OB3S.itemsize * CHUNK_SIZE     # 每次读 CHUNK_SIZE 条 (~424 MB/块)
        #
        sym_counts = {}                              # symbol -> 累计条数 (dict 保首次出现顺序)
        n_read = 0                                   # 全天有效记录总数
        writer, schema, fout = None, None, None      # IPC 文件写入器 (首块定 schema 懒建)
        feather_path = join_path(out_dir, f'{date}.feather')
        try:
            while True:
                chunk = proc.stdout.read(chunk_bytes)
                if not chunk:
                    break                            # 读到尾
                n = len(chunk) // OB3S.itemsize      # 尾部不足一条的残片丢弃
                if n == 0:
                    continue
                arr = np.frombuffer(chunk[:n * OB3S.itemsize], dtype=OB3S)
                arr = arr[arr['ReceiveTime'] > 0]    # 滤全零/残缺记录
                if not len(arr):                     # 全部过滤掉的就直接继续下一步读取
                    continue
                n_read += len(arr)
                df = self._decode(arr, exchange, date)
                for s, c in df['symbol'].value_counts().items():   # 逐块累加, 替代整表 groupby
                    sym_counts[s] = sym_counts.get(s, 0) + c
                batch = pa.RecordBatch.from_pandas(df, schema=schema, preserve_index=False)
                if writer is None:                   # 首块: 建 IPC 文件写入器 (feather v2 底层格式)
                    schema = batch.schema
                    fout = open(feather_path, 'wb')
                    writer = ipc.new_file(fout, schema, options=ipc.IpcWriteOptions(compression='zstd'))
                writer.write_batch(batch)            # 本块立即写盘, 内存随即可回收
                print(f'  {n_read:>12,} 条已解码', flush=True)
        finally:
            if writer is not None:
                writer.close()                       # footer 在此写出, 文件才可读
            if fout is not None:
                fout.close()
            proc.stdout.close()
            proc.wait()
            if proc.returncode != 0:                 # parse 总是读尽全流, 直接检查退出码
                raise RuntimeError(f'解压失败 {in_fname}')
        if n_read == 0:
            raise RuntimeError(f'{exchange} {date} 全天无有效记录')

        # 符号清单 (口径与整表 groupby('symbol', sort=False) 一致: 首次出现顺序 + day_records)
        symbols = pd.DataFrame({'symbol': list(sym_counts.keys()),
                                'day_records': [sym_counts[s] for s in sym_counts]})
        symbols.to_csv(join_path(out_dir, f'{date}.csv'), index=False)
        print(f'{exchange} {date}: {len(symbols)} symbols, {symbols["day_records"].sum()} records -> {out_dir}', flush=True)

    # ---------- 解码 ----------
    # profile (200k/批): 慢版 3.4s, 大头是 tz-aware strftime (1.7s) + to_datetime zfill 路径 (0.33s)。
    # 向量化后: strftime -> np.datetime_as_string (C 实现, 73ms); send 时间 -> 纯整数拆位合成 (6ms)。
    # 与指数侧 _decode 的向量化格式化完全同构。
    _RENAME = {'ReceiveTime': 'receive_timestamp', 'exchange': 'exchange_id', 'data_time': 'send_timestamp',
               'tot_bid_vol': 'total_bid_vol', 'tot_ask_vol': 'total_ask_vol',
               **{f'BPrice{i}': f'bid_price{i}' for i in range(1, 11)},
               **{f'SPrice{i}': f'ask_price{i}' for i in range(1, 11)},
               **{f'BVol{i}': f'bid_vol{i}' for i in range(1, 11)},
               **{f'SVol{i}': f'ask_vol{i}' for i in range(1, 11)}}

    @staticmethod
    def _decode(arr: np.ndarray, exchange: str, date: str) -> pd.DataFrame:
        """结构化 ndarray -> 解码 DataFrame: 一次 DataFrame 转换 + 向量化时间列, 按 STOCK_COLUMN 顺序输出。"""
        d = pd.DataFrame(arr).rename(columns=CNStockL2._RENAME)
        #
        prefix, exchange_id = DICT_EXCHANGE[exchange]
        d['symbol'] = [prefix + _clean_symbol(t) for t in arr['symbol']]
        d['exchange'] = exchange                                      # 交易所 (str)
        d['exchange_id'] = exchange_id                                # (DZ 源原始恒 0, 按目录补齐)
        #
        # receive: epoch 微秒 + 8h = 北京墙钟, as_string (C 实现) 输出 'YYYY-MM-DDTHH:MM:SS.ffffff', 再把 'T' 换 ' '
        d['receive_timestr'] = np.char.replace(
            np.datetime_as_string((arr['ReceiveTime'] + 8 * 3600 * 1_000_000).astype('datetime64[us]'), unit='us'), 'T',
            ' ')
        #
        # send: HHMMSSmmm 纯整数拆位 -> 当日微秒; 墙钟零点 naive, epoch = 墙钟 - 8h
        # (整数本身就是完整数值, 无字符串前导零问题)
        t = arr['data_time']
        day_us = (t // 10_000_000) * 3_600_000_000 + ((t // 100_000) % 100) * 60_000_000 \
            + ((t // 1000) % 100) * 1_000_000 + (t % 1000) * 1000
        zero_us = pd.Timestamp(date).value // 1000                    # 当日 00:00 墙钟 (naive epoch µs)
        d['send_timestamp'] = zero_us - 8 * 3600 * 1_000_000 + day_us  # 北京墙钟 -> epoch 微秒
        d['send_timestr'] = np.char.replace(
            np.datetime_as_string((zero_us + day_us).astype('datetime64[us]'), unit='us'), 'T', ' ')
        return d[STOCK_COLUMN]                                        # 按 schema 约定顺序输出


# ============================================================== 实测: python read_cnstock_L2.py
def _test() -> None:
    """实测: 跑 20251229 两个交易所各一遍, 逐次计时。"""
    base = os.path.dirname(os.path.abspath(__file__))
    r = CNStockL2(src_root=join_path(base, 'data/raw'), out_root=join_path(base, 'data/processed'))
    date = '20251229'
    for exchange in ('SSE', 'SZSE'):
        t0 = time.perf_counter()
        r.parse(exchange, date)
        print(f'--- [_test]  {exchange} 耗时 {time.perf_counter() - t0:.1f}s', flush=True)
    print(f'--- [_test] {exchange} {date} OK', flush=True)


if __name__ == '__main__':
    _test()
