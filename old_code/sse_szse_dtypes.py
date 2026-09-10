# -*- coding: utf-8 -*-
"""
SSE/SZSE 原始行情 .dat 读取工具（基于 /home/colin/git/cbscript/utils/dtype.py 与实盘文件逐字节核对整理）

三类文件（/mnt/data_center/New_Raw/{SSE,SZSE}/{GJ,GJ2,DZ,HX}/...）:
  1) 逐笔  tick / cbtick / stocktick : 128 B/记录, dtype = TICK128  (data_type 1=委托 2=成交 3=空/心跳)
  2) 快照  3s                        : 424 B/记录, dtype = OB3S     (自建十档订单簿快照, 带 ReceiveTime)
  3) 指数  index                     : 128 B/记录, dtype = INDEX128 (沪深指数混合)
所有文件无文件头(offset=0), 文件尾可能有不足一条记录的残片(np.fromfile 自动忽略).
大文件 1~40 GB: 只能 count= 分块 / fp.seek 读取, 不要整文件 np.fromfile.
"""
import os
import numpy as np
import pandas as pd

# ---------------------------------------------------------------- 逐笔 128B
TICK128 = np.dtype([
    ('ReceiveTime', 'i8'),   # 本地接收时间, epoch 微秒(UTC), 对应 C struct 的 len 槽位
    ('data_type', 'i4'),     # 1 委托(含撤单) 2 成交 3 空记录/心跳(ticker 为空, 跳过)
    ('exchange', 'i4'),      # 1 SSE 2 SZSE
    ('channel_no', 'i4'),    # 交易所频道: SSE 1-6(股票) 20 801(债券/可转债); SZSE 2011-2015 202x 203x 2061 2071
    ('seq', 'i8'),           # 频道内序号, 从 1 连续
    ('ticker', 'S16'),       # 证券代码, '\0' 结尾, 之后为垃圾字节, 必须按 NUL 截断
    ('int_time', 'i8'),      # 交易所时间: GJ/GJ2/HX = HHMMSSmmm; DZ-SZSE = YYYYMMDDHHMMSSmmm; DZ-SSE = HHMMSSss 或 HHMMSSmmm
    ('m', (np.void, 40)),    # union{entrust, trade}, 见 ENTRUST40 / TRADE40
    ('ori_seq', 'i8'),       # 一般 0; DZ-SSE 回放数据高 32 位=1 (pkg_loss:i4, is_old:i4 的合并槽位)
    ('biz_idx', 'i8'),       # 一般 0 (SSE 业务序号预留)
    ('ori_no_idx', 'i8'),    # 一般 0
    ('rdtsc', 'u8'),         # 接收机 TSC 计数
], align=True)
assert TICK128.itemsize == 128

ENTRUST40 = np.dtype([
    ('side', 'S1'),          # '1' 买 '2' 卖 ('G' 借入 'F' 出借, 未见)
    ('ord_type', 'S1'),      # '2' 限价(DZ-SSE 为 '0') '1' 市价 'U' 本方最优 'C' 撤单
    ('order_no', 'i8'),      # 委托号; 撤单时为被撤原委托号
    ('price', 'i8'),         # 价格 ×10000 ; SZSE 撤单=0, SSE 撤单=原委托价 (HX-SSE 撤单=0)
    ('qty', 'i8'),           # 数量: SSE ×1000 (股/手), SZSE ×100 (股/张)
], align=True)
TRADE40 = np.dtype([
    ('trade_flag', 'S1'),    # SSE-GJ2: 'B' 主动买 'S' 主动卖 ; SZSE/DZ/HX: 空
    ('price', 'i8'),         # 成交价 ×10000 (注意 cbscript 的 *_utc dtype 误标为 f8)
    ('qty', 'i8'),           # 成交量: SSE ×1000, SZSE ×100
    ('bid_no', 'i8'),        # 买方委托号
    ('ask_no', 'i8'),        # 卖方委托号
], align=True)
assert ENTRUST40.itemsize == 32 and TRADE40.itemsize == 40

def _flat(prefix_fields, tail_fields):
    return np.dtype(prefix_fields + tail_fields, align=True)

_HEAD = [('ReceiveTime','i8'),('data_type','i4'),('exchange','i4'),('channel_no','i4'),('seq','i8'),('ticker','S16'),('int_time','i8')]
_TAIL = [('ori_seq','i8'),('biz_idx','i8'),('ori_no_idx','i8'),('rdtsc','u8')]
TICK_ENTRUST = np.dtype(_HEAD + [('side','S1'),('ord_type','S1'),('order_no','i8'),('price','i8'),('qty','i8'),('r1','i8')] + _TAIL, align=True)
TICK_TRADE   = np.dtype(_HEAD + [('trade_flag','S1'),('price','i8'),('qty','i8'),('bid_no','i8'),('ask_no','i8')] + _TAIL, align=True)
assert TICK_ENTRUST.itemsize == 128 and TICK_TRADE.itemsize == 128

# ---------------------------------------------------------------- 快照 424B
OB3S = np.dtype([
    ('ReceiveTime', 'i8'),          # 本地接收时间 epoch 微秒
    ('ticker', 'S16'),              # '\0' 截断
    ('exchange', 'i4'),             # GJ/GJ2: 1 SSE 2 SZSE ; DZ/HX: 0(未填)
    ('last_price', 'f8'),           # 最新价(元)
    ('qty', 'i8'),                  # 总成交量(股/张, 未缩放)
    ('turnover', 'f8'),             # 总成交额(元)
    ('pre_close_price', 'f8'),      # [推断] 昨收; cbscript dz_myob_quote 此槽位叫 trades_count(i8), 实盘为 double
    *[('BPrice%d' % i, 'f8') for i in range(1, 11)],
    *[('SPrice%d' % i, 'f8') for i in range(1, 11)],
    *[('BVol%d' % i, 'i8') for i in range(1, 11)],
    *[('SVol%d' % i, 'i8') for i in range(1, 11)],
    ('data_time', 'i8'),            # 交易所时间 HHMMSSmmm
    ('tot_bid_vol', 'i4'),          # 委买总量
    ('tot_ask_vol', 'i4'),          # 委卖总量
    ('avg_weight_bid_price', 'f8'), # 加权平均委买价
    ('avg_weight_ask_price', 'f8'), # 加权平均委卖价
    ('rdtsc', 'u8'),
], align=True)
assert OB3S.itemsize == 424

# ---------------------------------------------------------------- 指数 128B
INDEX128 = np.dtype([
    ('ReceiveTime', 'i8'),   # epoch 微秒
    ('rdtsc', 'u8'),
    ('symbol', 'S16'),       # '\0' 截断
    ('exchange', 'i4'),      # 1 SSE 2 SZSE (同一文件混合)
    ('update_time', 'u8'),   # SSE: HHMMSS ; SZSE: YYYYMMDDHHMMSSmmm
    ('pre_close_px', 'f8'), ('open_px', 'f8'), ('turnover', 'f8'), ('volume', 'u8'),
    ('high_px', 'f8'), ('low_px', 'f8'), ('last_px', 'f8'), ('close_px', 'f8'),
    ('len', 'i8'),           # 112 或 0
    ('rdtsc2', 'u8'),
], align=True)
assert INDEX128.itemsize == 128

# ---------------------------------------------------------------- 代码表
EXCHANGE = {1: 'SSE', 2: 'SZSE', 0: 'NA'}
DATA_TYPE = {1: 'entrust', 2: 'trade', 3: 'empty'}
SIDE = {b'1': 'buy', b'2': 'sell', b'G': 'borrow', b'F': 'lend'}
ORD_TYPE = {b'2': 'limit', b'0': 'limit(DZ-SSE)', b'1': 'market', b'U': 'best_own', b'C': 'cancel'}
TRADE_FLAG = {b'B': 'active_buy', b'S': 'active_sell', b'N': 'unknown', b'F': 'fill(SZ)', b'4': 'cancel(SZ)', b'': 'n/a'}
PRICE_DIV = 10000.0
QTY_DIV = {1: 1000.0, 2: 100.0}   # SSE ×1000, SZSE ×100

def nrec(path, dtype):
    return os.path.getsize(path) // dtype.itemsize

def read_block(path, dtype, start=0, count=200000):
    """从第 start 条记录起读 count 条 (count<=200000 建议)."""
    with open(path, 'rb') as fp:
        fp.seek(start * dtype.itemsize)
        return np.fromfile(fp, dtype=dtype, count=count)

def iter_blocks(path, dtype, count=200000):
    n = nrec(path, dtype)
    for s in range(0, n, count):
        yield s, read_block(path, dtype, s, min(count, n - s))

def _ticker(a):
    # 按第一个 NUL 截断(NUL 之后是垃圾字节), 等价于 cbscript utils.common.symbol_decode
    return pd.Series([x.split(b'\0', 1)[0].decode('ascii', 'replace') for x in a])

def tick_to_df(arr, scale=True):
    """TICK128 数组 -> (entrust_df, trade_df); 自动跳过 data_type==3; 价格/数量按交易所缩放."""
    arr = arr[(arr['data_type'] == 1) | (arr['data_type'] == 2)]
    e = arr[arr['data_type'] == 1].view(TICK_ENTRUST)
    t = arr[arr['data_type'] == 2].view(TICK_TRADE)
    de = pd.DataFrame(e[['ReceiveTime','exchange','channel_no','seq','ticker','int_time','side','ord_type','order_no','price','qty']])
    dt = pd.DataFrame(t[['ReceiveTime','exchange','channel_no','seq','ticker','int_time','trade_flag','price','qty','bid_no','ask_no']])
    for d in (de, dt):
        d['ticker'] = _ticker(d['ticker'].values)
        d['recv'] = pd.to_datetime(d['ReceiveTime'], unit='us', utc=True).dt.tz_convert('Asia/Shanghai')
        if scale:
            d['price'] = d['price'] / PRICE_DIV
            d['qty'] = d['qty'] / d['exchange'].map(QTY_DIV)
    return de, dt

def ob3s_to_df(arr):
    d = pd.DataFrame(arr)
    d['ticker'] = _ticker(d['ticker'].values)
    d['recv'] = pd.to_datetime(d['ReceiveTime'], unit='us', utc=True).dt.tz_convert('Asia/Shanghai')
    return d

def index_to_df(arr):
    d = pd.DataFrame(arr)
    d['symbol'] = _ticker(d['symbol'].values)
    d['recv'] = pd.to_datetime(d['ReceiveTime'], unit='us', utc=True).dt.tz_convert('Asia/Shanghai')
    return d

def guess_dtype(path):
    p = path.replace('\\', '/')
    if '/3s/' in p or '/sh3s/' in p:
        return OB3S
    if '/index/' in p:
        return INDEX128
    return TICK128

if __name__ == '__main__':
    import sys
    pd.set_option('display.width', 250); pd.set_option('display.max_columns', 40)
    f = sys.argv[1]; n = int(sys.argv[2]) if len(sys.argv) > 2 else 20000
    dt = guess_dtype(f)
    print(f, 'dtype=', dt.itemsize, 'B', 'nrec=', nrec(f, dt))
    mid = nrec(f, dt) // 2
    a = read_block(f, dt, mid, n)
    if dt is TICK128:
        e, t = tick_to_df(a); print(e.head(5).to_string()); print(t.head(5).to_string())
    elif dt is OB3S:
        print(ob3s_to_df(a).head(5).to_string())
    else:
        print(index_to_df(a).head(5).to_string())
