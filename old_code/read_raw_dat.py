#!/root/anaconda3/bin/python3
# -*- coding: utf-8 -*-
"""
Verified record structs for /mnt/data_center/New_Raw/{SSE,SZSE}/{GJ,GJ2,DZ,HX}/{3s,tick,cbtick,stocktick,index}/tick_quote_YYYYMMDD.dat
(empirically probed 2026-09-03, see struct_probe/probe2_out.txt & probe3_out.txt).

All files: NO file header (offset=0); every record starts with ReceiveTime = local receive time, epoch microseconds (UTC).
  tick/cbtick/stocktick : 128 B  -> TICK (== cbscript utils/dtype.py dz_tick_quote_utc), split by data_type:
                          1=entrust -> view ENTRUST, 2=trade -> view TRADE, 3=empty placeholder (SSE only; ticker='', union all zero)
  3s                    : 424 B  -> OB424  (ReceiveTime + myob_t(408) + rdtsc). dtype.py has no exact match;
                          reading dtype.py dz_myob_quote at offset=8 gives the same fields except that
                          'trades_count' is really a double pre_close and 'reserve2' is the NEXT record's ReceiveTime.
  index                 : 128 B  -> IDX128 (dz_index_t(112) + len(i8, =112) + tsc2(u8)); dtype.py dz_index_t (112 B) mis-strides.
Usage:
  from read_raw_dat import read_tick, read_ob, read_index
  ent, trd = read_tick(path, count=200000, start=0)        # numpy structured arrays; price /1e4, qty as-is (SZSE: /100 = shares? see report)
  ob = read_ob(path, count=200000, start=0)
NEVER np.fromfile a whole file: they are 1-40 GB. Use count / start (record index).
"""
import os
import numpy as np

TICK = np.dtype([
    ('ReceiveTime', 'i8'), ('data_type', 'i4'), ('exchange', 'i4'), ('channel_no', 'i4'), ('seq', 'i8'),
    ('ticker', 'S16'), ('int_time', 'i8'), ('m', (np.void, 40)),
    ('ori_seq', 'i8'), ('biz_idx', 'i8'), ('ori_no_idx', 'i8'), ('rdtsc', 'i8')], align=True)
ENTRUST = np.dtype([
    ('ReceiveTime', 'i8'), ('data_type', 'i4'), ('exchange', 'i4'), ('channel_no', 'i4'), ('seq', 'i8'),
    ('ticker', 'S16'), ('int_time', 'i8'),
    ('side', 'S1'), ('ord_type', 'S1'), ('order_no', 'i8'), ('price', 'i8'), ('qty', 'i8'), ('r1', 'i8'),
    ('ori_seq', 'i8'), ('biz_idx', 'i8'), ('ori_no_idx', 'i8'), ('rdtsc', 'i8')], align=True)
TRADE = np.dtype([
    ('ReceiveTime', 'i8'), ('data_type', 'i4'), ('exchange', 'i4'), ('channel_no', 'i4'), ('seq', 'i8'),
    ('ticker', 'S16'), ('int_time', 'i8'),
    ('trade_flag', 'S1'), ('price', 'i8'), ('qty', 'i8'), ('bid_no', 'i8'), ('ask_no', 'i8'),
    ('ori_seq', 'i8'), ('biz_idx', 'i8'), ('ori_no_idx', 'i8'), ('rdtsc', 'i8')], align=True)
OB424 = np.dtype([
    ('ReceiveTime', 'i8'), ('ticker', 'S16'), ('exchange', 'i4'), ('last_price', 'f8'), ('qty', 'i8'),
    ('turnover', 'f8'), ('pre_close', 'f8'),
    *[('BPrice%d' % i, 'f8') for i in range(1, 11)], *[('SPrice%d' % i, 'f8') for i in range(1, 11)],
    *[('BVol%d' % i, 'i8') for i in range(1, 11)], *[('SVol%d' % i, 'i8') for i in range(1, 11)],
    ('data_time', 'i8'), ('tot_bid_vol', 'i4'), ('tot_ask_vol', 'i4'),
    ('avg_weight_bid_price', 'f8'), ('avg_weight_ask_price', 'f8'), ('rdtsc', 'u8')], align=True)
IDX128 = np.dtype([
    ('ReceiveTime', 'i8'), ('rdtsc', 'i8'), ('symbol', 'S16'), ('exchange', 'i4'), ('update_time', 'u8'),
    ('pre_close_px', 'f8'), ('open_px', 'f8'), ('turnover', 'f8'), ('volume', 'u8'), ('high_px', 'f8'),
    ('low_px', 'f8'), ('last_px', 'f8'), ('close_px', 'f8'), ('len', 'i8'), ('tsc2', 'u8')], align=True)
assert TICK.itemsize == ENTRUST.itemsize == TRADE.itemsize == 128 and OB424.itemsize == 424 and IDX128.itemsize == 128


def nrec(path, dtp):
    return os.path.getsize(path) // dtp.itemsize


def _read(path, dtp, start, count):
    with open(path, 'rb') as fp:
        fp.seek(start * dtp.itemsize)
        return np.fromfile(fp, dtype=dtp, count=count)


def read_tick(path, count=200000, start=0):
    a = _read(path, TICK, start, count)
    return a[a['data_type'] == 1].view(ENTRUST), a[a['data_type'] == 2].view(TRADE)


def read_ob(path, count=200000, start=0):
    return _read(path, OB424, start, count)


def read_index(path, count=200000, start=0):
    return _read(path, IDX128, start, count)


def tail(path, dtp, count=20):
    n = nrec(path, dtp)
    return _read(path, dtp, max(0, n - count), count)


if __name__ == '__main__':
    import sys
    p = sys.argv[1]
    leaf = p.split('/')[-2]
    if leaf == '3s':
        a = read_ob(p, 5); print(a[['ReceiveTime', 'ticker', 'exchange', 'last_price', 'qty', 'BPrice1', 'SPrice1', 'data_time']])
    elif leaf == 'index':
        a = read_index(p, 5); print(a[['ReceiveTime', 'symbol', 'exchange', 'update_time', 'last_px']])
    else:
        e, t = read_tick(p, 5000); print(e[:3][['ReceiveTime', 'channel_no', 'seq', 'ticker', 'int_time', 'side', 'ord_type', 'price', 'qty']]); print(t[:3][['ReceiveTime', 'channel_no', 'seq', 'ticker', 'int_time', 'trade_flag', 'price', 'qty', 'bid_no', 'ask_no']])
