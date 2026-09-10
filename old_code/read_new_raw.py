# -*- coding: utf-8 -*-
"""
读取 /mnt/data_center/New_Raw/{SSE,SZSE}/<SRC>/<LEAF>/tick_quote_YYYYMMDD.dat 的最小读取器.

结论(2026-09 实测, 见 TASK B 报告):
  * 3s   (LEAF=3s/sh3s, 2023-12 之后): 每条 424B = ReceiveTime(i8, unix us) + quote_3s(416B), 无文件头
  * tick (LEAF=tick/cbtick/stocktick/stockorder/stocktrade): 每条 128B = ReceiveTime(i8) + tick_quote(120B), 无文件头
  * index(LEAF=index, 2023-10 之后): 每条 128B = ReceiveTime(i8) + index(104B) + 16B 垃圾(下一条的 len+rdtsc)
  * 旧格式(2022-06 ~ 2023-11): SSE 3s=456B(dz_sh_dtype_utc), SZSE 3s=392B(dz_sz_dtype_utc), index=112B(dz_index_t)
  * dump/all 目录里的 shm_dump 文件: 每条 = len(u64) + payload, 没有本地时间, 用 offset=8 + 带尾部 8B 保留字段的 dtype 读
文件大小是 4096 的倍数(页对齐), 末尾可能有残缺/全零记录, 读取时按 ReceiveTime>0 过滤.
"""
import os
import numpy as np
import pandas as pd

# ---------- 3s 快照 (424B) ----------
QUOTE_3S_DT = np.dtype([
    ('ReceiveTime', 'i8'),           # 本地落地时间 unix 微秒 (gettimeofday)
    ('ticker', 'S16'),               # 代码, '\0' 结尾, GJ 源 '\0' 后有 'L2' 垃圾
    ('exchange', 'i4'),              # GJ/GJ2: 1=SSE 2=SZSE; DZ/HX: 恒为 0 (不可靠)
    ('last_px', 'f8'),
    ('qty', 'i8'),                   # 累计成交量(股/张)
    ('turnover', 'f8'),              # 累计成交额(元)
    ('pre_close_px', 'f8'),          # 实测为昨收(f8); 代码里此位叫 trades_count/seq(i8), 已过时
    *[('BPrice%d' % i, 'f8') for i in range(1, 11)],
    *[('SPrice%d' % i, 'f8') for i in range(1, 11)],
    *[('BVol%d' % i, 'i8') for i in range(1, 11)],
    *[('SVol%d' % i, 'i8') for i in range(1, 11)],
    ('data_time', 'i8'),             # 交易所时间 HHMMSSmmm (SZSE 毫秒恒 000)
    ('tot_bid_vol', 'i4'),
    ('tot_ask_vol', 'i4'),
    ('avg_weight_bid_price', 'f8'),
    ('avg_weight_ask_price', 'f8'),
    ('rdtsc', 'u8'),                 # 落地机 TSC, 3GHz 计数
], align=True)
assert QUOTE_3S_DT.itemsize == 424

# ---------- 逐笔 (128B) ----------
_TICK_HEAD = [
    ('ReceiveTime', 'i8'),
    ('data_type', 'i4'),             # 1=委托 2=成交 3=SSE 产品状态/其它 (ticker 为空)
    ('exchange', 'i4'),              # 1=SSE 2=SZSE
    ('channel_no', 'i4'),            # SSE: 1-6 股票/基金, 20, 801=债券/可转债; SZSE: 2011-2015, 2021-2025, 2031-2035, 2061, 2071
    ('seq', 'i8'),                   # 频道内连续序号
    ('ticker', 'S16'),
    ('int_time', 'i8'),              # SSE: HHMMSSmmm(9位, 首 0 省略); SZSE: YYYYMMDDHHMMSSmmm
]
_TICK_TAIL = [
    ('pkg_loss', 'i4'),              # cbscript 里叫 ori_seq(i8) 的位置: 低 4B=pkg_loss, 高 4B=is_old(补包=1)
    ('is_old', 'i4'),
    ('biz_idx', 'i8'),
    ('tsc0', 'i8'),                  # cbscript: ori_no_idx
    ('rdtsc', 'u8'),
]
TICK_RAW_DT = np.dtype(_TICK_HEAD + [('m', (np.void, 40))] + _TICK_TAIL, align=True)
TICK_ENTRUST_DT = np.dtype(_TICK_HEAD + [
    ('side', 'S1'),                  # '1'买 '2'卖 'G'借入 'F'出借
    ('ord_type', 'S1'),              # '1'市价 '2'/'0'限价 'U'本方最优 'C'撤单
    ('order_no', 'i8'),
    ('price', 'i8'),                 # x10000
    ('qty', 'i8'),                   # SSE x1000, SZSE x100
    ('r1', 'i8'),
] + _TICK_TAIL, align=True)
TICK_TRADE_DT = np.dtype(_TICK_HEAD + [
    ('trade_flag', 'S1'),            # SSE: 'B'/'S'/'N' 主动方向(部分源为空); SZSE: 无意义
    ('price', 'i8'),                 # x10000 (注意: cbscript dz_tick_quote_trade_all_utc 错标为 f8)
    ('qty', 'i8'),
    ('bid_no', 'i8'),
    ('ask_no', 'i8'),
] + _TICK_TAIL, align=True)
assert TICK_RAW_DT.itemsize == TICK_ENTRUST_DT.itemsize == TICK_TRADE_DT.itemsize == 128

# ---------- 指数 (128B, 2023-10 之后) ----------
INDEX_DT = np.dtype([
    ('ReceiveTime', 'i8'),
    ('rdtsc', 'i8'),
    ('symbol', 'S16'),
    ('exchange', 'i4'),              # 1=SSE 2=SZSE (同一文件里两所都有)
    ('update_time', 'u8'),           # SSE: HHMMSS; SZSE: YYYYMMDDHHMMSSmmm
    ('pre_close_px', 'f8'), ('open_px', 'f8'), ('turnover', 'f8'), ('volume', 'u8'),
    ('high_px', 'f8'), ('low_px', 'f8'), ('last_px', 'f8'), ('close_px', 'f8'),
    ('junk_len', 'i8'),              # 下一条 shm 记录的 len(=112), 无意义
    ('junk_rdtsc', 'i8'),            # 下一条的 rdtsc, 无意义
], align=True)
assert INDEX_DT.itemsize == 128

LEAF_DT = {'3s': QUOTE_3S_DT, 'sh3s': QUOTE_3S_DT, 'tick': TICK_RAW_DT, 'cbtick': TICK_RAW_DT,
           'stocktick': TICK_RAW_DT, 'stockorder': TICK_RAW_DT, 'stocktrade': TICK_RAW_DT, 'index': INDEX_DT}


def _strip(b):
    return b.split(b'\0')[0].decode(errors='ignore')


def read_chunk(path, dtype, start_rec=0, count=200000):
    """按记录号读取一段 (大文件 1-15GB, 切勿整文件读)."""
    with open(path, 'rb') as fp:
        fp.seek(start_rec * dtype.itemsize)
        arr = np.fromfile(fp, dtype=dtype, count=count)
    return arr[arr['ReceiveTime'] > 0]


def n_records(path, dtype):
    return os.path.getsize(path) // dtype.itemsize


def to_frame(arr, leaf):
    """转 DataFrame 并做单位换算; tick 目录按 data_type 拆成 entrust/trade 两张表."""
    if leaf in ('3s', 'sh3s'):
        df = pd.DataFrame(arr)
        df['ticker'] = df['ticker'].apply(_strip)
        df['Time'] = pd.to_datetime(df['ReceiveTime'], unit='us', utc=True).dt.tz_convert('Asia/Shanghai')
        return df
    if leaf == 'index':
        df = pd.DataFrame(arr[[n for n in arr.dtype.names if not n.startswith('junk')]])
        df['symbol'] = df['symbol'].apply(_strip)
        df['Time'] = pd.to_datetime(df['ReceiveTime'], unit='us', utc=True).dt.tz_convert('Asia/Shanghai')
        return df
    out = {}
    for name, dt, typ in (('entrust', TICK_ENTRUST_DT, 1), ('trade', TICK_TRADE_DT, 2)):
        sub = arr[arr['data_type'] == typ].view(dt)
        df = pd.DataFrame(sub)
        df['ticker'] = df['ticker'].apply(_strip)
        df['price'] = df['price'] / 10000.0
        sse = df['exchange'] == 1
        df['qty'] = np.where(sse, df['qty'] / 1000.0, df['qty'] / 100.0)
        df['Time'] = pd.to_datetime(df['ReceiveTime'], unit='us', utc=True).dt.tz_convert('Asia/Shanghai')
        out[name] = df
    return out


if __name__ == '__main__':
    base = '/mnt/data_center/New_Raw'
    for rel in ('SZSE/GJ2/3s/tick_quote_20260902.dat', 'SSE/GJ2/cbtick/tick_quote_20260902.dat',
                'SZSE/DZ/index/tick_quote_20260429.dat'):
        leaf = rel.split('/')[2]
        dt = LEAF_DT[leaf]
        p = os.path.join(base, rel)
        n = n_records(p, dt)
        arr = read_chunk(p, dt, start_rec=n // 2, count=50000)
        res = to_frame(arr, leaf)
        print('==', rel, 'records', n)
        if isinstance(res, dict):
            for k, v in res.items():
                print(k, len(v)); print(v.head(3).to_string())
        else:
            print(len(res)); print(res.iloc[:3, :12].to_string())
