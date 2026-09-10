#!/root/anaconda3/bin/python3
# -*- coding: utf-8 -*-
"""Empirical struct probe v2 for SSE/SZSE raw .dat files (read-only).
- record stride detected from ticker-pattern spacing in the first 4 MB
- true data end detected by backward scan for trailing zero padding
- candidate dtypes = dtype.py np.dtypes (+ synthesized ReceiveTime-prefixed variants)
"""
import os, re, sys, datetime as dt
from collections import Counter
import numpy as np

DTYPE_PY = '/home/colin/git/cbscript/utils/dtype.py'
ns = {}
src = open(DTYPE_PY, encoding='utf-8').read().split("if __name__ == '__main__':")[0]
exec(compile(src, DTYPE_PY, 'exec'), ns)
base = {k: v for k, v in ns.items() if isinstance(v, np.dtype) and v.names}
cands = dict(base)
for k, v in base.items():
    if 'ReceiveTime' not in v.names:
        cands['utc+' + k] = np.dtype([('ReceiveTime', 'i8')] + v.descr)

FILES = [l.strip() for l in open('/tmp/probe_files.txt') if l.strip()]
SIX = re.compile(rb'^\d{6}$')
STR_FIELDS = ('ticker', 'symbol', 'm_symbol', 'Symbol', 'szCode', 'wind_code')
TICK_RE = re.compile(rb'(?<![0-9])[0-9]{6}\x00')

def cstr(b): return b.split(b'\x00', 1)[0]
def date_of(path): return int(re.search(r'(\d{8})', os.path.basename(path)).group(1))
def epoch_us_ok(v, d):
    d0 = dt.datetime(d // 10000, d // 100 % 100, d % 100)
    return (d0 - dt.timedelta(days=1)).timestamp() * 1e6 <= v <= (d0 + dt.timedelta(days=2)).timestamp() * 1e6
def fmt_us(v):
    try: return dt.datetime.fromtimestamp(int(v) / 1e6, dt.timezone(dt.timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S.%f')
    except Exception: return '?'

def sym_field(dtp):
    for f in STR_FIELDS:
        if f in dtp.names: return f
    return None

def score(rec, d):
    s = 0.0; n = 0
    names = rec.dtype.names
    f = sym_field(rec.dtype)
    if f is None: return 0.0
    s += np.mean([bool(SIX.match(cstr(bytes(x)))) for x in rec[f]]); n += 1
    if 'ReceiveTime' in names:
        s += np.mean([epoch_us_ok(x, d) for x in rec['ReceiveTime']]); n += 1
    if 'exchange' in names:
        s += np.mean(np.isin(rec['exchange'], [0, 1, 2])); n += 1
    fl = [f for f in names if rec.dtype[f].kind == 'f']
    if fl:
        vals = np.concatenate([np.asarray(rec[f], dtype='f8').ravel() for f in fl])
        s += np.mean(np.isfinite(vals) & (np.abs(vals) < 1e12)); n += 1
    return s / n

def decode_row(r, fields=None, maxf=16):
    out = []
    for f in (fields or r.dtype.names[:maxf]):
        v = r[f]
        if r.dtype[f].kind == 'S': v = cstr(bytes(v)).decode('latin1')
        elif r.dtype[f].kind == 'V': v = bytes(v)[:8].hex() + '..'
        elif isinstance(v, np.floating): v = round(float(v), 4)
        else: v = int(v)
        out.append(f'{f}={v}')
    return ' '.join(out)

def last_nonzero(path, size, chunk=16 << 20):
    with open(path, 'rb') as fp:
        pos = size
        while pos > 0:
            start = max(0, pos - chunk)
            fp.seek(start)
            buf = fp.read(pos - start)
            b = np.frombuffer(buf, dtype='u1')
            nz = np.flatnonzero(b)
            if len(nz): return start + int(nz[-1])
            pos = start
    return -1

for path in FILES:
    size = os.path.getsize(path)
    d = date_of(path)
    with open(path, 'rb') as fp:
        head_bytes = fp.read(4 << 20)
    print('=' * 110)
    print(f'FILE {path}\n  size={size:,}  date={d}  head16={head_bytes[:16].hex()}')
    pos = [m.start() for m in TICK_RE.finditer(head_bytes)]
    diffs = Counter(np.diff(pos).tolist())
    stride, cnt = diffs.most_common(1)[0]
    first_tick_off = pos[0]
    print(f'  ticker-stride detection: stride={stride} (mode {cnt}/{len(pos)-1} diffs, top5={diffs.most_common(5)}), first ticker byte offset={first_tick_off}')
    lnz = last_nonzero(path, size)
    print(f'  last nonzero byte at {lnz:,}; trailing zero bytes={size - 1 - lnz:,} ({(size-1-lnz)/size*100:.2f}% of file)')
    results = []
    for off in (0, 8):
        for name, dtp in cands.items():
            if dtp.itemsize != stride: continue
            sf = sym_field(dtp)
            if sf is None: continue
            if (off + dtp.fields[sf][1]) % stride != first_tick_off % stride: continue
            data_len = lnz + 1 - off
            nrec = -(-data_len // stride)  # ceil
            with open(path, 'rb') as fp:
                fp.seek(off); head = np.fromfile(fp, dtype=dtp, count=20)
                fp.seek(off + (nrec - 20) * stride); tail = np.fromfile(fp, dtype=dtp, count=20)
            sc = (score(head, d) + score(tail, d)) / 2
            results.append((sc, off, name, dtp, nrec, head, tail))
    results.sort(key=lambda x: -x[0])
    if not results:
        print('  !! NO dtype in dtype.py matches stride/ticker-offset. raw first record hex:')
        print('   ', head_bytes[:stride].hex())
        continue
    print(f'  structural candidates (itemsize==stride & symbol offset match): {len(results)}')
    for sc, off, name, dtp, nrec, head, tail in results:
        print(f'   score={sc:.3f} off={off} dtype={name} itemsize={dtp.itemsize} nrec={nrec:,}  size%itemsize={(size-off)%dtp.itemsize}')
    sc, off, name, dtp, nrec, head, tail = results[0]
    print(f'  --> BEST: dtype={name} offset={off} itemsize={dtp.itemsize} nrec(true)={nrec:,}  nrec(if whole file)={(size-off)//dtp.itemsize:,}')
    names = dtp.names
    tfields = [f for f in names if f in ('ReceiveTime', 'int_time', 'data_time', 'cb_time', 'm_update_time', 'm_quote_update_time', 'update_time', 'nTime', 'local_ts', 'rdtsc', 'te')]
    for f in tfields:
        a, b = int(head[f][0]), int(tail[f][-1])
        extra = f'  ({fmt_us(a)} .. {fmt_us(b)})' if a > 1e15 else ''
        print(f'    {f}: first={a} last={b}{extra}')
    if 'ReceiveTime' in names:
        print('    ReceiveTime monotone(head,tail):', bool(np.all(np.diff(head['ReceiveTime'].astype('i8')) >= 0)), bool(np.all(np.diff(tail['ReceiveTime'].astype('i8')) >= 0)))
    with open(path, 'rb') as fp:
        fp.seek(off); samp = np.fromfile(fp, dtype=dtp, count=min(200000, nrec))
    sf = sym_field(dtp)
    tick = np.array([cstr(bytes(x)) for x in samp[sf]])
    u = np.unique(tick)
    bad = [x for x in u if not SIX.match(x)]
    print(f'    200k-sample: n={len(samp):,} distinct {sf}={len(u)} examples={[x.decode("latin1") for x in u[:6]]} ... {[x.decode("latin1") for x in u[-3:]]} non6digit={len(bad)} {[x.decode("latin1") for x in bad[:5]]}')
    for f in ('data_type', 'exchange', 'type', 'm_msg_type', 'm_data_type', 'm_sec_type', 'h_m_msg_type', 'm_trade_phase_code', 'side', 'ord_type', 'trade_flag'):
        if f in names:
            vals, cnt = np.unique(samp[f], return_counts=True)
            print(f'    {f} distribution: ' + ', '.join(f'{v!r}:{c}' for v, c in zip(vals, cnt)))
    if 'channel_no' in names and 'seq' in names:
        okc = True
        for ch in np.unique(samp['channel_no']):
            sq = samp['seq'][samp['channel_no'] == ch].astype('i8')
            if not np.all(np.diff(sq) > 0):
                okc = False; bad = np.where(np.diff(sq) <= 0)[0]
                print(f'    channel {ch}: seq NOT strictly increasing at {len(bad)} places (e.g. idx {bad[:3]}, gaps>1: {int(np.sum(np.diff(sq) > 1))})')
        print('    seq strictly increasing within channel (200k sample):', okc)
        chs, cc = np.unique(samp['channel_no'], return_counts=True)
        print(f'    channels: {dict(zip(chs.tolist(), cc.tolist()))}')
    for f in [f for f in names if f in ('price', 'last_price', 'last_px', 'm_last_price', 'm_last_px', 'LastPrice', 'BPrice1', 'SPrice1', 'BVol1', 'qty', 'turnover', 'volume')]:
        v = samp[f].astype('f8'); nz = v[v > 0]
        print(f'    {f}: min={v.min():.6g} max={v.max():.6g} median(>0)={np.median(nz) if len(nz) else 0:.6g} zero_frac={np.mean(v == 0):.3f}')
    for f in ('int_time', 'data_time', 'cb_time', 'update_time', 'm_update_time', 'm_quote_update_time'):
        if f in names:
            v = samp[f].astype('i8'); print(f'    {f}: min={v.min()} max={v.max()}  monotone={bool(np.all(np.diff(v) >= 0))}')
    print('    sample rows:')
    for tag, r in (('first', head[0]), ('mid', samp[len(samp) // 2]), ('last', tail[-1])):
        print(f'      [{tag}] ' + decode_row(r, maxf=18))
    sys.stdout.flush()
