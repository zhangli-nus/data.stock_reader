#!/root/anaconda3/bin/python3
# -*- coding: utf-8 -*-
"""
read_sse_szse.py — SSE/SZSE 原始行情 .dat 自包含读取器 (不依赖 /home/colin 下任何代码)

适用文件: /mnt/data_center/New_Raw/{SSE,SZSE}/{DZ,DZ2,GJ,GJ2,HX,CFFEX}/<leaf>/tick_quote_YYYYMMDD.dat
          (tar.bz2/xz 归档解包后得到的同名 .dat; 2023-12 之后的当前格式)

三种记录 (全部小端 '<'、C 8 字节对齐、**无文件头 offset=0**、每条首 8 字节 = 本地接收时间 epoch 微秒):
  TICK   128 B  逐笔委托/成交       leaf = tick / cbtick / stocktick / stockorder / stocktrade
  OB3S   424 B  3 秒十档快照        leaf = 3s / sh3s
  INDEX  128 B  指数快照            leaf = index
dtype 与实盘文件逐字节核对 (见 README.md §3/§4), 与 cbscript utils/dtype.py 的对应:
  TICK  = dz_tick_quote_utc (dtype.py:339) ; 委托视图 = dz_tick_quote_entrust_all_utc (L146),
          成交视图 = dz_tick_quote_trade_all_ext_utc (L283, price i8; 不要用 L250 的 f8 版本)
  OB3S  = ReceiveTime(i8) + dz_myob_quote (dtype.py:362) 去掉尾部 reserve2 ; trades_count 槽实为 f8 昨收
  INDEX = dz_index_t (dtype.py:640, 112 B) + len(i8) + rdtsc2(u8)

用法:
  python read_sse_szse.py peek  <file>          # 自动识别 dtype + 基本统计
  python read_sse_szse.py head  <file> [n] [start|mid]  # 打印从 start 起的 n 条 (默认 5, 文件头), 已解码; tick 自动跳过 data_type=3
  python read_sse_szse.py summary <file>        # 记录数 / 时间范围 / ticker 数 (抽样 head+mid+tail)
  python read_sse_szse.py summary <file> --full # 全文件分块扫描 (慢, 25 GB 约数分钟)

  import sys; sys.path.insert(0, '/home/leo/sse_szse_data'); from read_sse_szse import *
  a = read_dat(f, 'TICK', start=1_000_000, count=200_000)   # 结构化 ndarray (未解码)
  ent, trd = read_tick(f, start=..., count=...)                # 两张 DataFrame, 已缩放/解码
  ob = read_snapshot(f, start=0, count=100_000)               # SSE-GJ/GJ2 源股票/基金/B股的 qty+十档量 自动 //10
  ix = read_index(f)
  for start, blk in iter_dat(f, 'TICK', count=200_000): ...    # 全天分块遍历
所有函数默认只读一块 (count 上限 DEFAULT_COUNT), 不会整文件加载多 GB 文件。
"""
import os
import re
import sys
import numpy as np
import pandas as pd

DEFAULT_COUNT = 200_000      # 单次读取默认记录数上限
PRICE_DIV = 10000.0          # 逐笔价格 = 整数 / 10000
QTY_DIV = {1: 1000.0, 2: 100.0}   # 逐笔数量: SSE 整数/1000 = 股(张); SZSE 整数/100 = 股(张)

# ============================================================== dtype 定义 (verified)
# ---- 逐笔 128 B: 头部 56 B + union 40 B + 尾部 32 B ----
_TICK_HEAD = [
    ('ReceiveTime', '<i8'),   # 本地落地时间 epoch 微秒 (save_tbt_quote/main.c:112-119 write_quote: gettimeofday)
    ('data_type', '<i4'),     # 1 委托(含撤单) 2 成交 3 空/占位记录(仅 SSE, ticker 空, 需过滤)
    ('exchange', '<i4'),      # 1 SSE 2 SZSE
    ('channel_no', '<i4'),    # SSE 1-6 股票/基金, 20, 801 债券/可转债; SZSE 2011-2015/2021-2025/2031-2035/2061/2071
    ('seq', '<i8'),           # 频道内序号, 从 1 严格连续 (type=3 也占号)
    ('ticker', 'S16'),       # 证券代码 '\0' 结尾, NUL 之后是垃圾字节, 必须截断
    ('int_time', '<i8'),      # 交易所时间: GJ/GJ2/HX HHMMSSmmm; DZ-SZSE YYYYMMDDHHMMSSmmm; DZ-SSE stocktick HHMMSScc
]
_TICK_TAIL = [
    ('ori_seq', '<i8'),       # 实际是 (pkg_loss:i4, is_old:i4) 两个 int (save_tbt_quote/main.c:61-62); 一般 0
    ('biz_idx', '<i8'),       # 0
    ('ori_no_idx', '<i8'),    # 实际 tsc0 (main.c:64); 一般 0
    ('rdtsc', '<u8'),         # 接收机 TSC
]
TICK = np.dtype(_TICK_HEAD + [('m', (np.void, 40))] + _TICK_TAIL, align=True)
TICK_ENTRUST = np.dtype(_TICK_HEAD + [
    ('side', 'S1'),          # '1' 买 '2' 卖 ('G' 借入 'F' 出借 未见)
    ('ord_type', 'S1'),      # '2' 限价(GJ/HX/DZ-SZSE) '0' 限价(DZ-SSE) '1' 市价 'U' 本方最优 'C' 撤单
    ('order_no', '<i8'),      # 委托号; 撤单时 = 被撤原委托号 (SZSE 新单 order_no == seq)
    ('price', '<i8'),         # ×10000 ; SZSE/HX-SSE 撤单为 0, GJ2/DZ-SSE 撤单为原委托价
    ('qty', '<i8'),           # SSE ×1000, SZSE ×100
    ('r1', '<i8'),            # 填充, 恒 0
] + _TICK_TAIL, align=True)
TICK_TRADE = np.dtype(_TICK_HEAD + [
    ('trade_flag', 'S1'),    # SSE-GJ2 'B' 主动买 'S' 主动卖 'N' 未知; SZSE 及 SSE-DZ/HX 为空
    ('price', '<i8'),         # ×10000
    ('qty', '<i8'),           # SSE ×1000, SZSE ×100
    ('bid_no', '<i8'),        # 买方委托号
    ('ask_no', '<i8'),        # 卖方委托号
] + _TICK_TAIL, align=True)
assert TICK.itemsize == TICK_ENTRUST.itemsize == TICK_TRADE.itemsize == 128

# ---- 3s 快照 424 B ----
OB3S = np.dtype([
    ('ReceiveTime', '<i8'),          # epoch 微秒
    ('ticker', 'S16'),              # NUL 截断
    ('exchange', '<i4'),             # GJ/GJ2: 1 SSE 2 SZSE; DZ/HX: 0 (未填, 按目录判断)
    ('last_price', '<f8'),           # 最新价 (元); 债券开盘前为 0, 昨收显示 100
    ('qty', '<i8'),                  # 累计成交量 (股/张); **SSE-GJ/GJ2 源对 5/6/9 开头(基金/股票/B股)为 ×10, 0/1/2 债券为 ×1** (README 坑#3)
    ('turnover', '<f8'),             # 累计成交额 (元)
    ('pre_close_price', '<f8'),      # 昨收 (推断; cbscript dz_myob_quote 此槽叫 trades_count i8)
    *[('BPrice%d' % i, '<f8') for i in range(1, 11)],
    *[('SPrice%d' % i, '<f8') for i in range(1, 11)],
    *[('BVol%d' % i, '<i8') for i in range(1, 11)],   # 十档买量 (股/张); SSE-GJ/GJ2 源股票/基金/B股同样 ×10 (README 坑#3)
    *[('SVol%d' % i, '<i8') for i in range(1, 11)],   # 十档卖量, 同上
    ('data_time', '<i8'),            # 交易所时间 HHMMSSmmm (SZSE 毫秒恒 000)
    ('tot_bid_vol', '<i4'),          # 委买总量 (股, 真实股数, SSE-GJ 源亦不 ×10); SSE 股票/基金/B股 ≈100% 有值, 1/2/7 开头债券为 0; SZSE 有值
    ('tot_ask_vol', '<i4'),          # 委卖总量 (同上)
    ('avg_weight_bid_price', '<f8'), # 加权平均委买价 (元; 有值范围同 tot_bid_vol, ≈0.98×BPrice1)
    ('avg_weight_ask_price', '<f8'), # 加权平均委卖价
    ('rdtsc', '<u8'),                # 接收机 TSC (cbscript dtype.py 标 i8, 等价)
], align=True)
assert OB3S.itemsize == 424

# ---- 指数 128 B ----
INDEX = np.dtype([
    ('ReceiveTime', '<i8'),   # epoch 微秒
    ('rdtsc', '<u8'),
    ('symbol', 'S16'),       # NUL 截断
    ('exchange', '<i4'),      # 1 SSE 2 SZSE (同一文件混合)
    ('update_time', '<u8'),   # SSE: HHMMSS ; SZSE: YYYYMMDDHHMMSSmmm
    ('pre_close_px', '<f8'), ('open_px', '<f8'), ('turnover', '<f8'), ('volume', '<u8'),
    ('high_px', '<f8'), ('low_px', '<f8'), ('last_px', '<f8'), ('close_px', '<f8'),
    ('len', '<i8'),           # 下一条 shm 记录的 len (=112 或 0), 无意义
    ('rdtsc2', '<u8'),        # 下一条的 rdtsc, 无意义
], align=True)
assert INDEX.itemsize == 128

DTYPES = {'TICK': TICK, 'TICK_ENTRUST': TICK_ENTRUST, 'TICK_TRADE': TICK_TRADE, 'OB3S': OB3S, 'INDEX': INDEX}
LEAF_DTYPE = {'tick': 'TICK', 'cbtick': 'TICK', 'stocktick': 'TICK', 'stockorder': 'TICK', 'stocktrade': 'TICK',
              '3s': 'OB3S', 'sh3s': 'OB3S', 'index': 'INDEX'}

# ---- 代码表 ----
EXCHANGE = {1: 'SSE', 2: 'SZSE', 0: 'NA'}
DATA_TYPE = {1: 'entrust', 2: 'trade', 3: 'empty'}
SIDE = {b'1': 'buy', b'2': 'sell', b'G': 'borrow', b'F': 'lend'}
ORD_TYPE = {b'2': 'limit', b'0': 'limit', b'1': 'market', b'U': 'best_own', b'C': 'cancel'}
TRADE_FLAG = {b'B': 'active_buy', b'S': 'active_sell', b'N': 'unknown', b'F': 'fill', b'4': 'cancel', b'': ''}


# ============================================================== 底层读取
def _dt(dtype_name):
    return DTYPES[dtype_name] if isinstance(dtype_name, str) else dtype_name


def nrec(path, dtype_name='TICK', offset=0):
    """文件中完整记录条数 (尾部残片自动舍弃)."""
    return (os.path.getsize(path) - offset) // _dt(dtype_name).itemsize


def read_dat(path, dtype_name, offset=0, count=None, start=0, memmap=False):
    """
    从第 start 条记录起读 count 条, 返回结构化 ndarray (未解码).
    count=None -> DEFAULT_COUNT (绝不整文件读入); memmap=True 时返回 np.memmap 惰性视图 (整文件, 不占内存).
    offset: 文件头字节数, 当前所有 tick_quote_*.dat 为 0; shm_dump 老文件用 8.
    """
    dt = _dt(dtype_name)
    n = nrec(path, dt, offset)
    if memmap:
        return np.memmap(path, dtype=dt, mode='r', offset=offset, shape=(n,))
    if count is None:
        count = DEFAULT_COUNT
    start = max(0, min(start, n))
    count = max(0, min(count, n - start))
    with open(path, 'rb') as fp:
        fp.seek(offset + start * dt.itemsize)
        return np.fromfile(fp, dtype=dt, count=count)


def iter_dat(path, dtype_name, count=DEFAULT_COUNT, offset=0):
    """分块遍历整个文件: yield (start_record, ndarray)."""
    dt = _dt(dtype_name)
    n = nrec(path, dt, offset)
    for s in range(0, n, count):
        yield s, read_dat(path, dt, offset, min(count, n - s), s)


# ============================================================== 解码工具
def decode_ticker(a):
    """S16 bytes 数组 -> str 数组, 按第一个 NUL 截断 (NUL 后为脏字节)."""
    return np.array([x.split(b'\0', 1)[0].decode('ascii', 'replace') for x in np.asarray(a)], dtype=object)


def recv_to_datetime(rt):
    """ReceiveTime epoch 微秒 (UTC) -> tz-aware Asia/Shanghai."""
    return pd.to_datetime(np.asarray(rt, dtype='int64'), unit='us', utc=True).tz_convert('Asia/Shanghai')


def int_time_to_ms(t, hundredths=False):
    """
    交易所整数时间 -> 当日毫秒数 (int64).
    支持 HHMMSSmmm (9 位/前导零省略), YYYYMMDDHHMMSSmmm (17 位, 取后 9 位), HHMMSS (<=6 位, 指数 SSE);
    hundredths=True 时把 8 位 HHMMSScc 视为百分秒 (DZ-SSE stocktick).
    """
    t = np.asarray(t, dtype='int64')
    t = np.where(t >= 10**9, t % 10**9, t)          # 17 位 -> HHMMSSmmm
    if hundredths:
        t = t * 10                                   # HHMMSScc -> HHMMSSmmm
    else:
        t = np.where(t < 10**6, t * 1000, t)         # HHMMSS -> HHMMSSmmm
    hh, rem = t // 10**7, t % 10**7
    mm, rem = rem // 10**5, rem % 10**5
    ss, ms = rem // 1000, rem % 1000
    return ((hh * 60 + mm) * 60 + ss) * 1000 + ms


def int_time_to_datetime(t, date, hundredths=False):
    """date: 'YYYYMMDD' 或 int; 返回 naive datetime (交易所本地时间)."""
    base = pd.Timestamp(str(date))
    return base + pd.to_timedelta(int_time_to_ms(t, hundredths), unit='ms')


def date_from_path(path):
    m = re.search(r'(\d{8})', os.path.basename(path))
    return m.group(1) if m else None


def source_from_path(path):
    """从路径推断 (exchange_name, source, leaf), 例如 ('SSE','GJ2','tick')."""
    parts = os.path.abspath(path).split(os.sep)
    ex = src = leaf = None
    for i, p in enumerate(parts):
        if p in ('SSE', 'SZSE') and i + 2 < len(parts):
            ex, src, leaf = p, parts[i + 1], parts[i + 2]
    if leaf == 'sh3s':          # SZSE/DZ2/sh3s 里存的是 DZ2 机器抓的上交所 3s (README §1.3)
        ex = 'SSE'
    return ex, src, leaf


# ============================================================== 高层读取
def _tick_frame(sub, view, cols, exchange, scale, hundredths):
    d = pd.DataFrame(sub.view(view)[cols])
    d['ticker'] = decode_ticker(d['ticker'].values)
    d['recv'] = recv_to_datetime(d['ReceiveTime'].values)
    ex = d['exchange'].where(d['exchange'] > 0, exchange if exchange else 1)
    d['ex_time_ms'] = int_time_to_ms(d['int_time'].values, hundredths)
    if scale:
        d['price'] = d['price'] / PRICE_DIV
        d['qty'] = d['qty'] / ex.map(QTY_DIV).astype(float)
    return d


def read_tick(path, exchange=None, source=None, start=0, count=None, scale=True):
    """
    逐笔文件 -> (entrust_df, trade_df). 自动丢弃 data_type=3 空记录; ticker 截断; 价格/10000;
    数量按交易所缩放 (SSE /1000, SZSE /100); recv=本地接收时间(上海时区); ex_time_ms=交易所时间当日毫秒.
    exchange: 1/2 或 'SSE'/'SZSE' (记录里 exchange 字段为 0 时的兜底); source: 'DZ'/'GJ2'/... (决定 int_time 口径).
    """
    ex_p, src_p, leaf_p = source_from_path(path)
    if exchange is None:
        exchange = ex_p
    if isinstance(exchange, str):
        exchange = {'SSE': 1, 'SZSE': 2}.get(exchange.upper(), 0)
    source = (source or src_p or '').upper()
    a = read_dat(path, TICK, 0, count, start)
    a = a[(a['data_type'] == 1) | (a['data_type'] == 2)]
    # DZ-SSE stocktick/stockorder/stocktrade 的 int_time 是百分秒 HHMMSScc: 10:00 前 7 位 (9300570=09:30:05.70),
    # 之后 8 位 (10535678=10:53:56.78). 判定: 块内最大值 < 1e8 且去掉后 6 位后为 9..15 (HH).
    # DZ-SSE cbtick 是 HHMMSSmmm: 9 点档 8 位时前两位 91..95 (>15), 10 点后 9 位 (>=1e8), 不会误判.
    hundredths = False
    if source == 'DZ' and exchange == 1 and len(a):
        mx = int(a['int_time'].max())
        hundredths = (mx < 10**8 and 9 <= (mx // 10**6) <= 15) or \
                     (leaf_p in ('stocktick', 'stockorder', 'stocktrade') and mx < 10**8)
    ent_cols = ['ReceiveTime', 'data_type', 'exchange', 'channel_no', 'seq', 'ticker', 'int_time',
                'side', 'ord_type', 'order_no', 'price', 'qty']
    trd_cols = ['ReceiveTime', 'data_type', 'exchange', 'channel_no', 'seq', 'ticker', 'int_time',
                'trade_flag', 'price', 'qty', 'bid_no', 'ask_no']
    ent = _tick_frame(a[a['data_type'] == 1], TICK_ENTRUST, ent_cols, exchange, scale, hundredths)
    trd = _tick_frame(a[a['data_type'] == 2], TICK_TRADE, trd_cols, exchange, scale, hundredths)
    for d in (ent, trd):
        for c in ('side', 'ord_type', 'trade_flag'):
            if c in d:
                d[c] = d[c].str.decode('ascii')
    return ent, trd


SSE_GJ_X10_COLS = ['qty'] + ['BVol%d' % i for i in range(1, 11)] + ['SVol%d' % i for i in range(1, 11)]


def read_snapshot(path, exchange=None, source=None, start=0, count=None, fix_sse_gj_x10=True, **kw):
    """
    3s 快照 -> DataFrame. exchange 字段为 0 (DZ/HX) 时用参数/路径补齐 (sh3s 目录按 SSE).
    fix_sse_gj_x10: SSE 的 GJ/GJ2 源对 5/6/9 开头 (基金/股票/B股) 的 qty 与十档量 BVol1..10/SVol1..10 为 ×10
      (与同日 HX 源按 (ticker,data_time) 对齐比值恒 10.0; 0/1/2 开头债券恒 1.0; tot_bid_vol/tot_ask_vol 不 ×10),
      默认对这些行的 SSE_GJ_X10_COLS 整除 10 还原成股. (旧参数名 fix_sse_gj_qty 仍接受.)
    """
    if 'fix_sse_gj_qty' in kw:
        fix_sse_gj_x10 = kw.pop('fix_sse_gj_qty')
    if kw:
        raise TypeError('unexpected kwargs: %s' % list(kw))
    ex_p, src_p, _ = source_from_path(path)
    exchange = exchange if exchange is not None else ex_p
    if isinstance(exchange, str):
        exchange = {'SSE': 1, 'SZSE': 2}.get(exchange.upper(), 0)
    source = (source or src_p or '').upper()
    a = read_dat(path, OB3S, 0, count, start)
    d = pd.DataFrame(a)
    d['ticker'] = decode_ticker(d['ticker'].values)
    if exchange:
        d.loc[d['exchange'] == 0, 'exchange'] = exchange
    d['recv'] = recv_to_datetime(d['ReceiveTime'].values)
    d['ex_time_ms'] = int_time_to_ms(d['data_time'].values)
    if fix_sse_gj_x10 and exchange == 1 and source in ('GJ', 'GJ2'):
        m = ~d['ticker'].str[:1].isin(('0', '1', '2'))          # 债券 (0/1/2 开头) 不缩放
        d.loc[m, SSE_GJ_X10_COLS] = d.loc[m, SSE_GJ_X10_COLS] // 10
    return d


def read_index(path, start=0, count=None):
    """指数文件 -> DataFrame (沪深混合; SSE update_time=HHMMSS, SZSE=YYYYMMDDHHMMSSmmm)."""
    a = read_dat(path, INDEX, 0, count, start)
    d = pd.DataFrame(a[[n for n in INDEX.names if n not in ('len', 'rdtsc2')]])
    d['symbol'] = decode_ticker(d['symbol'].values)
    d['recv'] = recv_to_datetime(d['ReceiveTime'].values)
    d['ex_time_ms'] = int_time_to_ms(d['update_time'].values.astype('int64'))
    return d


# ============================================================== 自动识别 / 检查
_TICKER_RE = re.compile(rb'(?<![0-9])[0-9]{6}\x00')


def _stride_from_bytes(buf):
    """在字节块里找 6 位代码+NUL 的出现位置, 返回 (间距众数, 记录内偏移众数)."""
    pos = np.array([m.start() for m in _TICKER_RE.finditer(buf)])
    if len(pos) < 20:
        return None, None
    gaps = np.diff(pos)
    gaps = gaps[gaps > 0]
    vals, cnts = np.unique(gaps, return_counts=True)
    stride = int(vals[cnts.argmax()])
    offs = np.bincount(pos % stride).argmax()
    return stride, int(offs)


def _plausible_epoch_us(x):
    x = np.asarray(x, dtype='int64')
    return (x > 1_500_000_000_000_000) & (x < 2_000_000_000_000_000)   # 2017-07 .. 2033-05


def _score(a, name):
    """对一小块记录做合理性打分 (0..1)."""
    if len(a) == 0:
        return 0.0
    ok = _plausible_epoch_us(a['ReceiveTime']).mean()
    if name == 'TICK':
        ok *= np.isin(a['data_type'], [1, 2, 3]).mean()
        ok *= np.isin(a['exchange'], [1, 2]).mean()
        keep = a[a['data_type'] != 3]
        if len(keep):
            ok *= np.mean([len(t.split(b'\0', 1)[0]) == 6 and t[:6].isdigit() for t in keep['ticker']])
    elif name == 'OB3S':
        ok *= np.mean([len(t.split(b'\0', 1)[0]) in (6, 8) and t[:6].isdigit() for t in a['ticker']])
        ok *= np.isin(a['exchange'], [0, 1, 2]).mean()
        ok *= np.mean((a['last_price'] >= 0) & (a['last_price'] < 1e6) & (a['BPrice1'] >= 0))
    elif name == 'INDEX':
        ok *= np.mean([len(t.split(b'\0', 1)[0]) == 6 and t[:6].isdigit() for t in a['symbol']])
        ok *= np.isin(a['exchange'], [1, 2]).mean()
        ok *= np.mean((a['last_px'] >= 0) & (a['last_px'] < 1e6))
    return float(ok)


def detect_dtype(path, nbytes=4 << 20):
    """自动识别 dtype 名称: 字节间距探测 + 各候选 dtype 的合理性打分 + 文件大小整除性. 返回 (name, info)."""
    size = os.path.getsize(path)
    with open(path, 'rb') as fp:
        buf = fp.read(min(nbytes, size))
    stride, toff = _stride_from_bytes(buf)
    info = {'size': size, 'stride_detected': stride, 'ticker_offset': toff}
    scores = {}
    for name in ('TICK', 'OB3S', 'INDEX'):
        dt = DTYPES[name]
        if size < dt.itemsize:
            continue
        a = np.frombuffer(buf[: (len(buf) // dt.itemsize) * dt.itemsize], dtype=dt)[:5000]
        s = _score(a, name)
        if stride is not None:
            s *= 1.0 if stride == dt.itemsize else 0.1
            tick_off = {'TICK': 32, 'OB3S': 8, 'INDEX': 16}[name]
            s *= 1.0 if toff == tick_off else 0.3
        s *= 1.0 if size % dt.itemsize == 0 else 0.9   # 3s 文件尾部常有残片, 只轻微扣分
        scores[name] = round(s, 4)
    info['scores'] = scores
    best = max(scores, key=scores.get) if scores else None
    if best is None or scores[best] < 0.5:
        return None, info
    return best, info


def _tail(path, dt, count):
    n = nrec(path, dt)
    return read_dat(path, dt, 0, count, max(0, n - count))


def peek(path, verbose=True):
    """自动识别 dtype 并给出首/尾记录时间、记录数、residual 等. 返回 dict."""
    name, info = detect_dtype(path)
    out = {'path': path, 'dtype': name, **info}
    if name is None:
        if verbose:
            print(f'{path}: 无法识别 (scores={info["scores"]}, stride={info["stride_detected"]})')
        return out
    dt = DTYPES[name]
    n = nrec(path, dt)
    out['nrec'] = n
    out['residual_bytes'] = info['size'] % dt.itemsize
    head = read_dat(path, dt, 0, 2000, 0)
    tail = _tail(path, dt, 2000)
    tail = tail[_plausible_epoch_us(tail['ReceiveTime'])]
    out['recv_first'] = str(recv_to_datetime(head['ReceiveTime'][:1])[0]) if len(head) else None
    out['recv_last'] = str(recv_to_datetime(tail['ReceiveTime'][-1:])[0]) if len(tail) else None
    if name == 'TICK':
        out['exchange'] = sorted(set(int(x) for x in np.unique(head['exchange'])) | set(int(x) for x in np.unique(tail['exchange'])))
        out['channels_head'] = sorted(int(x) for x in np.unique(head['channel_no']))
        out['data_type_head'] = {int(k): int(v) for k, v in zip(*np.unique(head['data_type'], return_counts=True))}
        mid = read_dat(path, dt, 0, 2000, n // 2)
        nz = mid[mid['data_type'] != 3]
        out['int_time_example(mid)'] = int(nz['int_time'][0]) if len(nz) else None
        out['int_time_digits'] = len(str(int(nz['int_time'].max()))) if len(nz) else None
        out['data_type_mid'] = {int(k): int(v) for k, v in zip(*np.unique(mid['data_type'], return_counts=True))}
    elif name == 'OB3S':
        out['exchange'] = sorted(int(x) for x in np.unique(head['exchange']))
        out['data_time_first'] = int(head['data_time'][0]) if len(head) else None
        out['data_time_last'] = int(tail['data_time'][-1]) if len(tail) else None
    elif name == 'INDEX':
        out['exchange'] = sorted(int(x) for x in np.unique(head['exchange']))
        out['update_time_first'] = int(head['update_time'][0]) if len(head) else None
        out['update_time_last'] = int(tail['update_time'][-1]) if len(tail) else None
    ex, src, leaf = source_from_path(path)
    out['path_hint'] = f'{ex}/{src}/{leaf}'
    if verbose:
        for k, v in out.items():
            print(f'  {k:18s}: {v}')
    return out


def head(path, n=5, dtype_name=None, start=0, max_scan=4_000_000):
    """
    读取并解码从第 start 条起的前 n 条, 返回 DataFrame (tick 返回 (entrust, trade) 各 n 条).
    start 可为 'mid' (文件中点). tick 文件开头可能有大段 data_type=3 空记录, 会继续往后扫直到凑够 n 条 (上限 max_scan 条).
    """
    name = dtype_name or detect_dtype(path)[0]
    if name is None:
        raise ValueError('cannot detect dtype for ' + path)
    dt = DTYPES[name]
    if start == 'mid':
        start = nrec(path, dt) // 2
    if name == 'TICK':
        ents, trds, ne, nt, pos = [], [], 0, 0, start
        while (ne < n or nt < n) and pos < nrec(path, dt) and pos - start < max_scan:
            e, t = read_tick(path, start=pos, count=DEFAULT_COUNT)
            if ne < n:                       # 只保留还缺的行, 避免把整块 200k 行都攒在内存里
                ents.append(e.head(n - ne)); ne += min(len(e), n - ne)
            if nt < n:
                trds.append(t.head(n - nt)); nt += min(len(t), n - nt)
            pos += DEFAULT_COUNT
        e0, t0 = read_tick(path, start=0, count=0)          # 空表模板 (start>=nrec 或全为 type=3 时返回空)
        return (pd.concat(ents) if ents else e0), (pd.concat(trds) if trds else t0)
    if name == 'OB3S':
        return read_snapshot(path, start=start, count=n)
    return read_index(path, start=start, count=n)


def summary(path, dtype_name=None, full=False, block=DEFAULT_COUNT):
    """记录数 / 本地时间范围 / 交易所时间范围 / ticker 数 / 类型计数. full=False 时只扫 head+mid+tail 三块."""
    name = dtype_name or detect_dtype(path)[0]
    if name is None:
        raise ValueError('cannot detect dtype for ' + path)
    dt = DTYPES[name]
    n = nrec(path, dt)
    if full:
        blocks = (a for _, a in iter_dat(path, dt, block))
        mode = 'full-scan'
    else:
        blocks = (read_dat(path, dt, 0, block, s) for s in (0, n // 2, max(0, n - block)))
        mode = 'sampled(head/mid/tail x %d)' % block
    tickers = set()
    rt_min = rt_max = None
    ext_min = ext_max = None
    counts = {}
    nrows = 0
    ratio = []
    tcol = 'symbol' if name == 'INDEX' else 'ticker'
    xcol = {'TICK': 'int_time', 'OB3S': 'data_time', 'INDEX': 'update_time'}[name]
    for a in blocks:
        a = a[_plausible_epoch_us(a['ReceiveTime'])]
        if len(a) == 0:
            continue
        nrows += len(a)
        if name == 'TICK':
            for k, v in zip(*np.unique(a['data_type'], return_counts=True)):
                counts[int(k)] = counts.get(int(k), 0) + int(v)
            a = a[a['data_type'] != 3]
            if len(a) == 0:
                continue
        tickers.update(t.split(b'\0', 1)[0] for t in a[tcol])
        lo, hi = int(a['ReceiveTime'].min()), int(a['ReceiveTime'].max())
        rt_min = lo if rt_min is None else min(rt_min, lo)
        rt_max = hi if rt_max is None else max(rt_max, hi)
        x = a[xcol].astype('int64')
        x = x[x > 0]
        if len(x):
            ext_min = int(x.min()) if ext_min is None else min(ext_min, int(x.min()))
            ext_max = int(x.max()) if ext_max is None else max(ext_max, int(x.max()))
        if name == 'OB3S':
            pfx = np.array([t[:1] for t in a['ticker']])
            m = (a['qty'] > 0) & (a['last_price'] > 0) & ~np.isin(pfx, [b'0', b'1', b'2'])   # 只看股票/基金 (债券 SSE-GJ 不 ×10)
            if m.any():
                ratio.append(np.median(a['turnover'][m] / (a['qty'][m] * a['last_price'][m])))
    res = {'path': path, 'dtype': name, 'itemsize': dt.itemsize, 'nrec_total': n, 'scan_mode': mode,
           'rows_scanned': nrows, 'n_tickers_seen': len(tickers),
           'recv_min': str(recv_to_datetime([rt_min])[0]) if rt_min else None,
           'recv_max': str(recv_to_datetime([rt_max])[0]) if rt_max else None,
           f'{xcol}_min': ext_min, f'{xcol}_max': ext_max}
    if name == 'TICK':
        res['data_type_counts'] = counts
    if name == 'OB3S' and ratio:
        res['turnover/(qty*last) median'] = round(float(np.median(ratio)), 4)
        res['qty_unit_hint'] = ('qty & BVol/SVol are x10 shares for stocks/funds/B-shares (SSE-GJ/GJ2 source; bonds x1; '
                                'read_snapshot fix_sse_gj_x10 handles it)') if np.median(ratio) < 0.3 else 'qty in shares'
    return res


# ============================================================== CLI
def _main(argv):
    pd.set_option('display.width', 250)
    pd.set_option('display.max_columns', 60)
    if len(argv) < 3 or argv[1] not in ('peek', 'head', 'summary'):
        print(__doc__)
        return 1
    cmd, path = argv[1], argv[2]
    if cmd == 'peek':
        print(f'== peek {path}')
        peek(path)
    elif cmd == 'head':
        rest = argv[3:]
        if rest and rest[0] == 'mid':          # head <file> mid  (省略 n)
            n, start = 5, 'mid'
        else:
            n = int(rest[0]) if rest else 5
            start = rest[1] if len(rest) > 1 else 0
            start = start if start == 'mid' else int(start)
        name = detect_dtype(path)[0]
        if name is None:
            raise ValueError('cannot detect dtype for ' + path)
        print(f'== head {n} {path}  (dtype={name}, start={start})')
        r = head(path, n, name, start)
        if isinstance(r, tuple):
            print('-- entrust --'); print(r[0].to_string())
            print('-- trade --'); print(r[1].to_string())
        else:
            print(r.to_string())
    elif cmd == 'summary':
        full = '--full' in argv
        print(f'== summary {path}')
        for k, v in summary(path, full=full).items():
            print(f'  {k:28s}: {v}')
    return 0


if __name__ == '__main__':
    sys.exit(_main(sys.argv))
