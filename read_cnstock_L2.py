#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
read_cnstock_L2.py — SSE/SZSE 3 秒十档快照 (OB3S) 解析器

数据布局 (每个交易所一个数据源):
  data/raw_data/SSE/dzsse_3s_tick_quote_YYYYMMDD.tar.bz2    上交所 (DZ 源)
  data/raw_data/SZSE/gj2szse_3s_tick_quote_YYYYMMDD.tar.bz2 深交所 (GJ2 源)

记录: OB3S 424B, 小端, C 8 字节对齐, 无文件头, 每条首 8 字节 = ReceiveTime (epoch 微秒 UTC)。
归档名后缀 .tar.bz2 但约 1/3 实为 xz, 按压缩 magic 自动选 tar 参数。

用法:
  直接跑内置自检 (合成数据, 不碰真实归档):  python read_cnstock_L2.py
  或 import 使用:
  from read_cnstock_L2 import CNStockL2
  r = CNStockL2()
  r.parse('SSE', '20251229')                        # 逐符号落盘 (默认, write_together=False)
  r.parse('SSE', '20251229', write_together=True)   # 全天一张表落盘
  # 输出:
  #   data/md/single/20251229/SH600519.feather            每个符号全天 3s 快照 (write_together=False)
  #   data/md/single/20251229/20251229_full_symbol_SSE.csv  当日符号清单
  #   data/md/full/20251229.feather                       全天一张表 (write_together=True)
  #   data/md/full/20251229_full_symbol_SSE.csv             当日符号清单
  # 之后取数直接读 feather, 不再碰归档:
  #   df = pd.read_feather('data/md/single/20251229/SH600519.feather')

字段口径:
  - qty 累计成交量(股/张);
  - pre_close = 交易所官方昨收盘价(含除权除息调整), 算涨跌幅直接用它, 勿拼昨日 close
  - avg_weight_* = 全量挂单(含十档外隐藏深度)加权均价; 可见十档 VWAP 需自算 sum(P*V)/sum(V)
  - 原始 exchange 字段 (DZ 源恒 0) 输出为 exchange_id (1=SSE, 2=SZSE) + exchange ('SSE'/'SZSE' str)
  - 时间戳统一: receive_timestamp = 落地时间 epoch 微秒; send_timestamp = 交易所时间 epoch 微秒
    (由 data_time HHMMSSmmm + 归档日期合成); receive_timestr/send_timestr = 北京时间可读字符串 (微秒)
  - SZSE 09:15 集合竞价前十档全 0 但 tot_bid_vol 已非零, 盘口统计避开开盘前窗口
"""
import subprocess

import os
import numpy as np
import pandas as pd

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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CHUNK_SIZE = 200_000  # 流式单块记录数
DICT_EXCHANGE = {'SSE': ('SH', 1), 'SZSE': ('SZ', 2)}   # 交易所 -> (symbol 前缀, exchange_id)
ZIP_PREFIX = {'SSE': 'dzsse', 'SZSE': 'gj2szse'}
BINARY_ROOT = join_path(BASE_DIR, 'data/raw_data')
MD_ROOT = join_path(BASE_DIR, 'data/md')


def _clean_symbol(raw: bytes) -> str:
    """S16 字段 -> 干净代码: b'600519\\x00垃圾' -> '600519' (按第一个 NUL 截断)."""
    return raw.split(b'\0', 1)[0].decode('ascii', 'replace')

# ============================================================== 解析器
class CNStockL2:
    """SSE/SZSE 3 秒十档快照解析器: 整档解压, 按 symbol 分组, 每个 symbol 落盘一个 feather。

    参数:
      exchange: 'SSE' | 'SZSE' (大小写不敏感)
      date:     'YYYYMMDD'
    归档路径直接拼接 (每个交易所一个数据源):
      SSE : data/raw_data/SSE/dzsse_3s_tick_quote_<date>.tar.bz2    (DZ 源)
      SZSE: data/raw_data/SZSE/gj2szse_3s_tick_quote_<date>.tar.bz2 (GJ2 源)
    """

    def __init__(self, **kwargs):
        pass

    # ---------- 解压 ----------
    @staticmethod
    def _cmd_untar(file_path: str) -> list:
        """按压缩 magic 选 tar 参数 (约 1/3 的 .tar.bz2 实为 xz)."""
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
    def parse(self, exchange: str, date: str, write_together: bool = False) -> pd.DataFrame:
        """整档解压 -> 按 symbol 分组 -> 落盘。

        参数:
          write_together: True  -> 全天一张表存 <md_root>/full/<date>.feather
                           False -> 逐符号存 <md_root>/single/<date>/<symbol>.feather (默认)
        输出 (csv 与 feather 同目录):
          <md_root>/full/<date>.feather                      (write_together=True)
          <md_root>/full/<date>_full_symbol_<exchange>.csv
          <md_root>/single/<date>/<symbol>.feather           (write_together=False)
          <md_root>/single/<date>/<date>_full_symbol_<exchange>.csv
        返回符号清单 DataFrame。

        内存: 全天解码后的 DataFrame 常驻内存 (SSE/SZSE 各约 10GB)。
        """
        exchange = exchange.upper()
        full_name = join_path(BINARY_ROOT, exchange, f'{ZIP_PREFIX[exchange]}_3s_tick_quote_{date}.tar.bz2')
        out_dir = join_path(MD_ROOT, 'full') if write_together else join_path(MD_ROOT, 'single', date)
        create_directory(out_dir, last_as_directory=True)
        #
        proc = subprocess.Popen(self._cmd_untar(full_name), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        chunk_bytes = OB3S.itemsize * CHUNK_SIZE     # 每次读 CHUNK_SIZE 条 (~85 MB)
        #
        frames = []                                  # 每批解码后的 DataFrame
        n_read = 0
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
                if not len(arr): #全部过滤掉的就直接继续下一步读取
                    continue
                n_read += len(arr)
                frames.append(self._decode_fast(arr, exchange, date))
                print(f'  {n_read:>12,} 条已解码', flush=True)
        finally:
            proc.stdout.close()
            proc.wait()
            if proc.returncode != 0:                 # parse 总是读尽全流, 直接检查退出码
                raise RuntimeError(f'解压失败 {full_name}')

        # 全天拼成一张表, 按 symbol 归拢 (组内保持原时间顺序), 落盘
        df = pd.concat(frames, ignore_index=True)
        if write_together:                              # 全天一张表
            df.to_feather(join_path(out_dir, f'{date}.feather'))
            symbols = df.groupby('symbol', sort=False).size().rename('day_records').reset_index()
        else:                                           # 逐符号一个文件
            rows = []
            for s, g in df.groupby('symbol', sort=False):
                g.reset_index(drop=True).to_feather(join_path(out_dir, f'{s}.feather'))
                rows.append({'symbol': s, 'day_records': len(g)})
            symbols = pd.DataFrame(rows)
        symbols.to_csv(join_path(out_dir, f'{date}_full_symbol_{exchange}.csv'), index=False)
        print(f'{exchange} {date}: {len(symbols)} symbols, {symbols["day_records"].sum()} records -> {out_dir}', flush=True)
        return symbols

    # ---------- 解码 ----------
    @staticmethod
    def _decode(arr: np.ndarray, exchange: str, date: str) -> pd.DataFrame:
        """结构化 ndarray -> 解码 DataFrame: 按 STOCK_COLUMN 声明空表, 逐列从 arr 填充。
        可读性优先; 慢 (每批 200k 约 3.4s, 大头是 tz-aware strftime) — 生产用 _decode_fast。"""
        d = pd.DataFrame(index=np.arange(arr.shape[0]), columns=STOCK_COLUMN)
        #
        prefix, exchange_id = DICT_EXCHANGE[exchange]
        d['symbol'] = [prefix + _clean_symbol(t) for t in arr['symbol']]
        d['exchange'] = exchange                                      # 交易所 (str)
        d['exchange_id'] = exchange_id                                # (DZ 源原始恒 0, 按目录补齐)
        #
        # receive: epoch 微秒 -> 北京时间字符串
        d['receive_timestamp'] = arr['ReceiveTime']
        recv_dt = pd.to_datetime(d['receive_timestamp'].values, unit='us', utc=True).tz_convert('Asia/Shanghai')
        d['receive_timestr'] = recv_dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        #
        # send: HHMMSSmmm 整数 -> 补零9位 -> 拼上日期一步解析成 datetime (北京时间墙钟)
        send_dt = pd.to_datetime(date + pd.Series(arr['data_time']).astype(str).str.zfill(9),
                                 format='%Y%m%d%H%M%S%f').astype('datetime64[us]')
        d['send_timestamp'] = send_dt.astype('int64') - 8 * 3600 * 1_000_000   # 北京墙钟 -> epoch 微秒
        d['send_timestr'] = send_dt.dt.strftime('%Y-%m-%d %H:%M:%S.%f')
        #
        d['last_price'] = arr['last_price']
        d['qty'] = arr['qty']
        d['turnover'] = arr['turnover']
        d['pre_close_price'] = arr['pre_close_price']
        for i in range(1, 11):
            d[f'bid_price{i}'] = arr[f'BPrice{i}']   # 十档买价
            d[f'ask_price{i}'] = arr[f'SPrice{i}']   # 十档卖价
            d[f'bid_vol{i}'] = arr[f'BVol{i}']       # 十档买量
            d[f'ask_vol{i}'] = arr[f'SVol{i}']       # 十档卖量
        d['total_bid_vol'] = arr['tot_bid_vol']
        d['total_ask_vol'] = arr['tot_ask_vol']
        d['avg_weight_bid_price'] = arr['avg_weight_bid_price']
        d['avg_weight_ask_price'] = arr['avg_weight_ask_price']
        d['rdtsc'] = arr['rdtsc']
        return d

    # ---------- 解码 (快) ----------
    # profile (200k/批): _decode 3.4s, 其中 tz-aware strftime 1.7s + to_datetime(zfill 路径) 0.33s。
    # fast 版: strftime -> np.datetime_as_string (C 实现, 73ms); send 时间 -> 纯整数拆位合成 (6ms)。
    _RENAME = {'ReceiveTime': 'receive_timestamp', 'exchange': 'exchange_id', 'data_time': 'send_timestamp',
               'tot_bid_vol': 'total_bid_vol', 'tot_ask_vol': 'total_ask_vol',
               **{f'BPrice{i}': f'bid_price{i}' for i in range(1, 11)},
               **{f'SPrice{i}': f'ask_price{i}' for i in range(1, 11)},
               **{f'BVol{i}': f'bid_vol{i}' for i in range(1, 11)},
               **{f'SVol{i}': f'ask_vol{i}' for i in range(1, 11)}}

    @staticmethod
    def _decode_fast(arr: np.ndarray, exchange: str, date: str) -> pd.DataFrame:
        """同 _decode 的快速版: 一次 DataFrame 转换 + 向量化时间列, 输出列/值与 _decode 完全一致。"""
        d = pd.DataFrame(arr).rename(columns=CNStockL2._RENAME)
        #
        prefix, exchange_id = DICT_EXCHANGE[exchange]
        d['symbol'] = [prefix + _clean_symbol(t) for t in arr['symbol']]
        d['exchange'] = exchange                                      # 交易所 (str)
        d['exchange_id'] = exchange_id                                # (DZ 源原始恒 0, 按目录补齐)
        #
        # receive: epoch 微秒 + 8h = 北京墙钟, as_string (C 实现) 替换 'T' -> ' '
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
def _test():
    """实测: 跑一个真实归档, 统计耗时 / 符号数 / 记录数 / 输出文件数。"""
    exchange = "SSE"
    date = "20251229"
    import time
    t0 = time.perf_counter()
    symbols = CNStockL2().parse(exchange, date)
    sec = time.perf_counter() - t0
    out_dir = join_path(MD_ROOT, 'single', date)
    files = os.listdir(out_dir)
    n_feather = sum(1 for f in files if f.endswith('.feather'))
    print(f'--- [{exchange} {date}] 耗时 {sec:.0f}s | symbols {len(symbols)} | '
          f'records {symbols["day_records"].sum():,} | feather {n_feather} | 目录共 {len(files)} 文件')


if __name__ == '__main__':
    _test()
