#!/root/anaconda3/bin/python3
# -*- coding: utf-8 -*-
"""Probe v3: decode with the empirically-derived structs, sample from mid-file (trading hours)."""
import os, re, sys, datetime as dt
import numpy as np, pandas as pd
DTYPE_PY = '/home/colin/git/cbscript/utils/dtype.py'
ns = {}
exec(compile(open(DTYPE_PY, encoding='utf-8').read().split("if __name__ == '__main__':")[0], DTYPE_PY, 'exec'), ns)
dz_tick_quote_utc = ns['dz_tick_quote_utc']
ent = ns['dz_tick_quote_entrust_all_utc']
trd = ns['dz_tick_quote_trade_all_ext_utc']
dz_index_t = ns['dz_index_t']

# 424-byte 3s snapshot: ReceiveTime + myob_t(408) + rdtsc  (== dz_myob_quote read at offset 8)
ob424 = np.dtype([
    ('ReceiveTime', 'i8'), ('ticker', 'S16'), ('exchange', 'i4'), ('last_price', 'f8'), ('qty', 'i8'),
    ('turnover', 'f8'), ('f48', 'f8'),
    *[('BPrice%d' % i, 'f8') for i in range(1, 11)], *[('SPrice%d' % i, 'f8') for i in range(1, 11)],
    *[('BVol%d' % i, 'i8') for i in range(1, 11)], *[('SVol%d' % i, 'i8') for i in range(1, 11)],
    ('data_time', 'i8'), ('tot_bid_vol', 'i4'), ('tot_ask_vol', 'i4'),
    ('avg_weight_bid_price', 'f8'), ('avg_weight_ask_price', 'f8'), ('rdtsc', 'u8')], align=True)
assert ob424.itemsize == 424, ob424.itemsize
idx128 = np.dtype(dz_index_t.descr + [('r1', 'i8'), ('tsc2', 'u8')], align=True)
assert idx128.itemsize == 128, idx128.itemsize

SIX = re.compile(r'^\d{6}$')
def cstr(b): return bytes(b).split(b'\x00', 1)[0].decode('latin1')
def fmt_us(v): return dt.datetime.fromtimestamp(int(v) / 1e6, dt.timezone(dt.timedelta(hours=8))).strftime('%H:%M:%S.%f')
def nrec_of(path, itemsize):
    return os.path.getsize(path) // itemsize
def read(path, dtp, start, count):
    with open(path, 'rb') as fp:
        fp.seek(start * dtp.itemsize)
        return np.fromfile(fp, dtype=dtp, count=count)

TICK = ['SSE/GJ2/cbtick/tick_quote_20260902.dat', 'SSE/GJ2/tick/tick_quote_20260902.dat', 'SZSE/GJ2/tick/tick_quote_20260902.dat',
        'SZSE/DZ/tick/tick_quote_20260429.dat', 'SSE/DZ/cbtick/tick_quote_20260429.dat', 'SSE/DZ/stocktick/tick_quote_20251210.dat',
        'SSE/HX/tick/tick_quote_20260114.dat', 'SZSE/GJ/tick/tick_quote_20251013.dat']
OB = ['SSE/GJ2/3s/tick_quote_20260902.dat', 'SZSE/GJ2/3s/tick_quote_20260902.dat', 'SZSE/DZ/3s/tick_quote_20260429.dat',
      'SSE/DZ/3s/tick_quote_20260429.dat', 'SSE/GJ/3s/tick_quote_20251117.dat', 'SSE/HX/3s/tick_quote_20260114.dat', 'SZSE/GJ/3s/tick_quote_20260114.dat']
IDX = ['SZSE/DZ/index/tick_quote_20260429.dat']
R = '/mnt/data_center/New_Raw/'

print('#' * 30, 'TICK files: dz_tick_quote_utc(128) offset=0, mid-file 200k sample')
for rel in TICK:
    p = R + rel; n = nrec_of(p, 128); mid = n // 2
    a = read(p, dz_tick_quote_utc, mid, 200000)
    print('=' * 100); print(rel, f'nrec={n:,} sample@{mid:,}')
    rt = a['ReceiveTime']
    print(f'  ReceiveTime {fmt_us(rt[0])} .. {fmt_us(rt[-1])} monotone={bool(np.all(np.diff(rt) >= 0))}')
    vals, cnt = np.unique(a['data_type'], return_counts=True); print('  data_type:', dict(zip(vals.tolist(), cnt.tolist())))
    it = a['int_time']; print(f'  int_time min={it.min()} max={it.max()} ndigits={len(str(it.max()))}')
    ok = all(np.all(np.diff(a['seq'][a['channel_no'] == c].astype('i8')) == 1) for c in np.unique(a['channel_no']))
    print('  seq consecutive (+1) within channel:', ok, ' channels:', np.unique(a['channel_no']).tolist())
    e = a[a['data_type'] == 1].view(ent); t = a[a['data_type'] == 2].view(trd)
    if len(e):
        pr = e['price'] / 1e4; q = e['qty']
        print(f'  ENTRUST n={len(e)} price/1e4: min={pr.min():.3f} med={np.median(pr[pr>0]):.3f} max={pr.max():.3f} zero={np.mean(pr==0):.3f}; qty min={q.min()} med={np.median(q)} max={q.max()}',
              ' side:', dict(zip(*[x.tolist() for x in np.unique(e['side'], return_counts=True)])), ' ord_type:', dict(zip(*[x.tolist() for x in np.unique(e['ord_type'], return_counts=True)])))
        for r in (e[0], e[len(e)//2], e[-1]):
            print(f'    RT={fmt_us(r["ReceiveTime"])} ch={r["channel_no"]} seq={r["seq"]} ticker={cstr(r["ticker"])} int_time={r["int_time"]} side={r["side"]} ord_type={r["ord_type"]} order_no={r["order_no"]} price={r["price"]} qty={r["qty"]} r1={r["r1"]} ori_seq={r["ori_seq"]} biz_idx={r["biz_idx"]} ori_no_idx={r["ori_no_idx"]} rdtsc={r["rdtsc"]}')
    if len(t):
        pr = t['price'] / 1e4; q = t['qty']
        print(f'  TRADE   n={len(t)} price/1e4: min={pr.min():.3f} med={np.median(pr[pr>0]):.3f} max={pr.max():.3f} zero={np.mean(pr==0):.3f}; qty min={q.min()} med={np.median(q)} max={q.max()}',
              ' trade_flag:', dict(zip(*[x.tolist() for x in np.unique(t['trade_flag'], return_counts=True)])))
        for r in (t[0], t[len(t)//2], t[-1]):
            print(f'    RT={fmt_us(r["ReceiveTime"])} ch={r["channel_no"]} seq={r["seq"]} ticker={cstr(r["ticker"])} int_time={r["int_time"]} flag={r["trade_flag"]} price={r["price"]} qty={r["qty"]} bid_no={r["bid_no"]} ask_no={r["ask_no"]} ori_seq={r["ori_seq"]} biz_idx={r["biz_idx"]} ori_no_idx={r["ori_no_idx"]} rdtsc={r["rdtsc"]}')
    x = a[a['data_type'] == 3]
    if len(x):
        u = x.view(ent)
        print(f'  TYPE3   n={len(x)} tickers={np.unique([cstr(s) for s in x["ticker"]])[:5].tolist()} int_time uniq={np.unique(x["int_time"])[:5].tolist()} union-bytes-allzero={bool(np.all(np.frombuffer(x["m"].tobytes(), "u1") == 0))} ori_seq uniq={np.unique(x["ori_seq"])[:5].tolist()} rdtsc uniq={np.unique(x["rdtsc"])[:3].tolist()}')
        d = np.diff(x['ReceiveTime'][x['channel_no'] == x['channel_no'][0]]) / 1e3
        print(f'    type3 ReceiveTime spacing (ms) within channel {x["channel_no"][0]}: median={np.median(d):.1f} min={d.min():.1f} max={d.max():.1f}; type3 seq of first 5: {x["seq"][:5].tolist()}')
        # neighbours: does seq of type3 interleave with 1/2 in same channel?
        ch = x['channel_no'][0]; sub = a[a['channel_no'] == ch]
        i = np.flatnonzero(sub['data_type'] == 3)[0]
        print('    context around a type3 record (same channel): ' + '; '.join(f'seq={r["seq"]} type={r["data_type"]} ticker={cstr(r["ticker"])} int_time={r["int_time"]}' for r in sub[max(0, i-2): i+3]))
    sys.stdout.flush()

print('#' * 30, '3s snapshot files: ob424 (ReceiveTime+myob_t+rdtsc) offset=0, mid-file 200k sample')
for rel in OB:
    p = R + rel; n = nrec_of(p, 424); mid = n // 2
    a = read(p, ob424, mid, 200000)
    print('=' * 100); print(rel, f'nrec={n:,} sample@{mid:,}')
    rt = a['ReceiveTime']
    print(f'  ReceiveTime {fmt_us(rt[0])} .. {fmt_us(rt[-1])} monotone={bool(np.all(np.diff(rt) >= 0))}')
    print('  exchange:', dict(zip(*[x.tolist() for x in np.unique(a['exchange'], return_counts=True)])))
    tk = np.array([cstr(s) for s in a['ticker']]); u = np.unique(tk)
    nb = sum(not SIX.match(s) for s in u)
    print(f'  distinct ticker={len(u)} non6digit={nb} examples={u[:4].tolist()}..{u[-3:].tolist()}')
    dtm = a['data_time']; print(f'  data_time min={dtm.min()} max={dtm.max()} ndigits={len(str(dtm.max()))} monotone={bool(np.all(np.diff(dtm) >= 0))}')
    lp = a['last_price']; b1 = a['BPrice1']; s1 = a['SPrice1']
    both = (b1 > 0) & (s1 > 0)
    print(f'  last_price>0 frac={np.mean(lp>0):.3f} med={np.median(lp[lp>0]):.3f} max={lp.max():.2f}; BPrice1<=SPrice1 (both>0) frac={np.mean(b1[both] <= s1[both]):.4f}; BVol1 med={np.median(a["BVol1"][a["BVol1"]>0]) if np.any(a["BVol1"]>0) else 0}')
    # turnover/qty vs last price -> qty units
    m = (a['qty'] > 0) & (lp > 0)
    ratio = (a['turnover'][m] / a['qty'][m]) / lp[m]
    print(f'  (turnover/qty)/last_price median={np.median(ratio):.4f}  (1 => qty in shares & turnover in CNY)')
    # f48 hypothesis: per-ticker constant?
    df = pd.DataFrame({'tk': tk, 'f48': a['f48'], 'lp': lp})
    g = df[df.lp > 0].groupby('tk')['f48'].nunique()
    print(f'  f48 (offset 48, double): per-ticker nunique median={g.median()} max={g.max()}; f48/last_price median={np.median((df.f48/df.lp)[df.lp>0]):.4f}  -> likely pre_close if ~1 and const')
    print(f'  tot_bid_vol med={np.median(a["tot_bid_vol"])} tot_ask_vol med={np.median(a["tot_ask_vol"])} avg_weight_bid_price med={np.median(a["avg_weight_bid_price"]):.3f} avg_weight_ask_price med={np.median(a["avg_weight_ask_price"]):.3f} rdtsc monotone={bool(np.all(np.diff(a["rdtsc"].astype("i8")) >= 0))}')
    # per-ticker snapshot spacing
    t0 = tk[0]; sel = a[tk == t0]
    if len(sel) > 2:
        d = np.diff(sel['ReceiveTime']) / 1e6
        print(f'  ticker {t0}: {len(sel)} snapshots in sample, ReceiveTime spacing s: median={np.median(d):.2f} min={d.min():.2f} max={d.max():.2f}; data_time seq={sel["data_time"][:5].tolist()}')
    for r in (a[0], a[len(a)//2], a[-1]):
        print(f'    RT={fmt_us(r["ReceiveTime"])} ticker={cstr(r["ticker"])} ex={r["exchange"]} last={r["last_price"]} qty={r["qty"]} turnover={r["turnover"]:.2f} f48={r["f48"]} B1={r["BPrice1"]}x{r["BVol1"]} S1={r["SPrice1"]}x{r["SVol1"]} B2={r["BPrice2"]} S2={r["SPrice2"]} data_time={r["data_time"]} totB={r["tot_bid_vol"]} totA={r["tot_ask_vol"]} avgB={r["avg_weight_bid_price"]:.3f} avgA={r["avg_weight_ask_price"]:.3f} rdtsc={r["rdtsc"]}')
    sys.stdout.flush()

print('#' * 30, 'INDEX file: idx128 (dz_index_t + r1 i8 + tsc2 u8) offset=0')
for rel in IDX:
    p = R + rel; n = nrec_of(p, 128)
    h = read(p, idx128, 0, 20); t = read(p, idx128, n - 20, 20); a = read(p, idx128, n // 2, 200000)
    print('=' * 100); print(rel, f'nrec={n:,}')
    print(f'  ReceiveTime first={h["ReceiveTime"][0]} ({fmt_us(h["ReceiveTime"][0])}) last={t["ReceiveTime"][-1]} ({fmt_us(t["ReceiveTime"][-1])}) monotone(mid)={bool(np.all(np.diff(a["ReceiveTime"]) >= 0))}')
    print(f'  update_time first={h["update_time"][0]} last={t["update_time"][-1]} mid min={a["update_time"].min()} max={a["update_time"].max()}')
    sy = np.array([cstr(s) for s in a['symbol']]); u = np.unique(sy)
    print(f'  distinct symbol(mid 200k)={len(u)} examples={u[:8].tolist()} .. {u[-4:].tolist()}; exchange={np.unique(a["exchange"]).tolist()}')
    print(f'  last_px>0 frac={np.mean(a["last_px"]>0):.3f} med={np.median(a["last_px"][a["last_px"]>0]):.2f}; r1 uniq={np.unique(a["r1"])[:6].tolist()}; volume med={np.median(a["volume"])}')
    # also try plain dz_index_t (112) to show it fails
    b = read(p, dz_index_t, n // 2, 5)
    print('  [check] reading same region with dz_index_t(112): symbols=', [cstr(s) for s in b['symbol']])
    for r in (h[0], a[len(a)//2], t[-1]):
        print(f'    RT={fmt_us(r["ReceiveTime"])} sym={cstr(r["symbol"])} ex={r["exchange"]} update_time={r["update_time"]} pre_close={r["pre_close_px"]:.2f} open={r["open_px"]:.2f} turnover={r["turnover"]:.0f} vol={r["volume"]} high={r["high_px"]:.2f} low={r["low_px"]:.2f} last={r["last_px"]:.2f} close={r["close_px"]:.2f} r1={r["r1"]} tsc2={r["tsc2"]} rdtsc={r["rdtsc"]}')
