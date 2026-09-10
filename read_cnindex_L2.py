#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
read_cnindex_L2.py — SSE/SZSE 指数快照 (INDEX) 解析器

数据布局 (沪深指数同目录, 一个指数一个 parquet, 一天一目录, 2022~2026 共 1127 个交易日):
  INDEX_ROOT\<date>\sh000001.parquet   上证指数 (sh* = SSE, ~200 个/天)
  INDEX_ROOT\<date>\sz399001.parquet   深证成指 (sz* = SZSE, ~354 个/天)

源是老二进制 INDEX 128B (沪深混流) 转出的 parquet (16 列), 时间口径已实测:
  ReceiveTime = 落地时间 epoch 微秒 UTC, 文件内单调不减;
  cb_time = HHMMSSmmm (沪深统一, 老二进制里 SSE=HHMMSS / SZSE=17 位的问题已不存在);
  Time 列 = cb_time + date 的墙钟, 冗余; index 列 = 原混流行号, 无意义。两者均丢弃。

用法:
  全量运行: run_exp/run_read_cnindex.py (逐天解析全部日期, 以 csv 完成标记断点续跑)
  单天冒烟: python read_cnindex_L2.py (内置 _test, 跑 20260428 两种落盘模式)
  或 import (index_root/md_root 必传, 不在库内硬编码路径):
  from read_cnindex_L2 import CNIndexL2
  r = CNIndexL2(index_root=INDEX_ROOT, md_root=MD_ROOT)
  r.parse('20260428')                       # 逐指数落盘 (默认, write_together=False)
  r.parse('20260428', write_together=True)  # 全天一张表落盘
  # 输出 (与股票同目录结构, 指数代码 000xxx/399xxx 不与股票 6/0/3 开头冲突):
  #   MD_ROOT\single\20260428\SH000001.feather            每个指数全天快照 (write_together=False)
  #   MD_ROOT\single\20260428\20260428_INDEX.csv          当日指数清单
  #   MD_ROOT\full\20260428.feather                       全天一张表 (write_together=True)
  #   MD_ROOT\full\20260428_INDEX.csv                     当日指数清单
  # 之后取数直接读 feather, 不再碰 parquet。

字段口径:
  - 指数无十档盘口, 仅 OHLC + last/close + volume/turnover;
  - close_price 盘中 0, SSE 仅尾几条回填, SZSE 全天 0, 取收盘用 last_price 尾行;
  - lowest_price 开盘前 0, 统计需限 send_timestamp >= 开盘;
  - volume 输出统一为"股" (2026-09-10 实测核实, 判据: turnover/volume 中位 = 成份股均价, 应在个位~几十元/股):
      SSE  全时期(V1/V2/V3) 源单位=手  -> 解码时 x100 (600,507,498 手 x100 = 60,050,749,800 股
           = 上证指数 20260428 官方成交量, 完全一致; x100 为整数无损变换, 可 /100 还原);
      SZSE V1/V3 源单位=股 原样; 仅 V2 时代(20220331~20220630) 源单位=手 -> x100
           (399001 额/量中位: V1 16.5~18.5 / V2 1526~2011 / V3 15.5~30.9, 手股切换与 schema 切换日完全重合);
    转换后另有 turnover/volume 中位 0.5~500 元/股 的逐文件兜底校验, 超区间打印告警;
  - turnover = 累计成交额 (元), 各时期两所口径一致 (SSE 20260428 官方 1,113,955,561,936 元与本数据 1.11e12 一致);
  - volume/turnover 均为开盘至今累计值。
"""
import os

import numpy as np
import pandas as pd

from file_io import create_directory, join_path
from config_schema import INDEX_COLUMN


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DICT_EXCHANGE = {'sh': ('SSE', 1), 'sz': ('SZSE', 2)}   # 文件名前两位 -> (exchange, exchange_id)


# ============================================================== 指数源 parquet 列定义
# 相当于股票侧的 OB3S: 定义"源"的结构 (read 时校验), 输出结构见 config_schema.INDEX_COLUMN_DTYPE
# 实测 MD_ROOT\CNIndex 全量 (1127 天) 有三种源 schema (按日期切换):
#   V1 59列: 20220104 ~ 20220330  老版带十档盘口/涨跌停 (实测全 0 无信息量, 丢弃);
#            无 PreClosePrice/ClosePrice/rdtsc/date, ReceiveTime 是 float64, 多 local_ts
#   V2 15列: 20220331 ~ 20220630  16 列标准版减 index 列 (cb_time/date int32, Exchange int64, 值口径相同)
#   V3 16列: 20220701 起          标准版 (2023~2026 隔天抽样全一致)
# 另: 20260423 一天文件名损坏为 sh('000001',).parquet —— 前 2 位仍是 sh/sz, 代码取文件内 Symbol 列
INDEX_SOURCE_V1 = [
    ('index',         'int64'),          # 原二进制混流中的行号, 无意义, 丢弃
    ('cb_time',       'int64'),          # 交易所时间 HHMMSSmmm
    ('Time',          'datetime64[us]'), # cb_time + date 墙钟, 冗余, 丢弃
    ('ReceiveTime',   'float64'),        # 落地时间 epoch 微秒 (float 存储, 解码时转 int64)
    ('Symbol',        'str'),            # 指数代码 6 位
    *[('SPrice%d' % i, 'float64') for i in range(1, 11)],   # 十档卖价 — 实测全 0, 丢弃
    *[('SVol%d' % i,   'int64') for i in range(1, 11)],     # 十档卖量 — 实测全 0, 丢弃
    *[('BPrice%d' % i, 'float64') for i in range(1, 11)],   # 十档买价 — 实测全 0, 丢弃
    *[('BVol%d' % i,   'int64') for i in range(1, 11)],     # 十档买量 — 实测全 0, 丢弃
    ('Volume',        'int64'),          # 成交量; 此时代 SSE=手 / SZSE=股 (判据: 额/量中位=成交均价, 399001 实测 16.5~18.5 元/股), 解码时 SSE x100 -> 股
    ('Turnover',      'float64'),        # 成交额 (元)
    ('TotalBuyOrderSize',  'int64'),     # 委买总量 — 实测全 0, 丢弃
    ('TotalSellOrderSize', 'int64'),     # 委卖总量 — 实测全 0, 丢弃
    ('WeightedBuyPrice',   'float64'),   # 加权买价 — 无对应物, 丢弃
    ('WeightedSellPrice',  'float64'),   # 加权卖价 — 无对应物, 丢弃
    ('OpenPrice',     'float64'),        # 开盘点位; 开盘前为 0
    ('HighestPrice',  'float64'),        # 日内最高点位 (累计)
    ('LowestPrice',   'float64'),        # 日内最低点位 (累计); 开盘前为 0
    ('LastPrice',     'float64'),        # 最新点位
    ('local_ts',      'int64'),          # 本地大整数时间戳 (非 rdtsc 对应物), 丢弃
    ('Exchange',      'int32'),          # 1=SSE, 2=SZSE
    ('UpperLimitPrice', 'float64'),      # 涨停价 — 指数无意义 (SSE 填 9999.9999), 丢弃
    ('LowerLimitPrice', 'float64'),      # 跌停价 — 指数无意义 (恒 0), 丢弃
]
INDEX_SOURCE_V2 = [
    ('ReceiveTime',   'int64'),          # 本地落地时间, epoch 微秒 (UTC), 文件内单调不减
    ('rdtsc',         'int64'),          # 接收机 CPU TSC 周期计数, 非行情数据
    ('Symbol',        'str'),            # 指数代码 6 位; 此时代带 '.SZ'/'.SH' 后缀, 解码时截掉
    ('Exchange',      'int64'),          # 1=SSE, 2=SZSE
    ('cb_time',       'int32'),          # 交易所时间 HHMMSSmmm
    ('PreClosePrice', 'float64'),        # 昨收盘点位 (含修正)
    ('OpenPrice',     'float64'),        # 开盘点位; 开盘前为 0
    ('Turnover',      'float64'),        # 成交额 (元), 日内单调不减
    ('Volume',        'uint64'),         # 成交量; 此时代 SSE=手 / SZSE=手 (判据: 399001 额/量中位 1526~2011, 恰为均价 x100), 解码时一律 x100 -> 股
    ('HighestPrice',  'float64'),        # 日内最高点位 (累计, 单调不减)
    ('LowestPrice',   'float64'),        # 日内最低点位 (累计); 开盘前为 0
    ('LastPrice',     'float64'),        # 最新点位
    ('ClosePrice',    'float64'),        # 收盘点位; 盘中 0, SSE 仅尾几条回填, SZSE 全天 0
    ('date',          'int32'),          # YYYYMMDD, 与目录名一致
    ('Time',          'datetime64[us]'), # cb_time + date 的墙钟, 冗余, 丢弃
]
INDEX_SOURCE_V3 = [
    ('index',         'int64'),          # 原二进制混流中的行号, 无意义, 丢弃
    ('ReceiveTime',   'int64'),          # 本地落地时间, epoch 微秒 (UTC), 文件内单调不减
    ('rdtsc',         'int64'),          # 接收机 CPU TSC 周期计数, 非行情数据
    ('Symbol',        'str'),            # 指数代码 6 位: 000xxx 上证 / 399xxx 深证, 与文件名一致
    ('Exchange',      'int32'),          # 1=SSE, 2=SZSE, 与文件名前缀一致 (实测无一例外)
    ('cb_time',       'uint64'),         # 交易所时间 HHMMSSmmm (沪深统一; 老 128B 二进制里 SSE=HHMMSS / SZSE=17 位的问题已不存在)
    ('PreClosePrice', 'float64'),        # 昨收盘点位 (含修正)
    ('OpenPrice',     'float64'),        # 开盘点位; 开盘前为 0
    ('Turnover',      'float64'),        # 成交额 (元), 日内单调不减
    ('Volume',        'uint64'),         # 成交量; 此时代 SSE=手 / SZSE=股 (判据: 399001 额/量中位 15.5~30.9 元/股), 解码时 SSE x100 -> 股
    ('HighestPrice',  'float64'),        # 日内最高点位 (累计, 单调不减)
    ('LowestPrice',   'float64'),        # 日内最低点位 (累计); 开盘前为 0
    ('LastPrice',     'float64'),        # 最新点位
    ('ClosePrice',    'float64'),        # 收盘点位; 盘中 0, SSE 仅尾几条回填, SZSE 全天 0
    ('date',          'int64'),          # YYYYMMDD, 与目录名一致
    ('Time',          'datetime64[us]'), # cb_time + date 的墙钟, 冗余, 丢弃
]
INDEX_SOURCE_COLUMNS = [[x[0] for x in v] for v in (INDEX_SOURCE_V1, INDEX_SOURCE_V2, INDEX_SOURCE_V3)]   # 三种合法源列集合


# ============================================================== 解析器
class CNIndexL2:
    """SSE/SZSE 指数快照解析器: 一个 parquet 一个指数, 逐文件解码落盘。"""

    def __init__(self, index_root: str, md_root: str):
        self.index_root = index_root       # 源: 日期目录根, 由调用方传入
        self.md_root = md_root             # 输出根: <md_root>\full\ 或 <md_root>\single\<date>\

    # ---------- 解析 ----------
    def parse(self, date: str, write_together: bool = False) -> None:
        """读取 <date>/ 下全部 parquet -> 解码落盘, 无返回值。
        当日符号清单 (symbol + day_records) 落盘为 <out_dir>/<date>_INDEX.csv。"""
        src_dir = join_path(self.index_root, date)
        out_dir = join_path(self.md_root, 'full') if write_together else join_path(self.md_root, 'single', date)
        create_directory(out_dir, last_as_directory=True)
        #
        frames = []                                    # 逐文件解码; 两种落盘模式都先攒全天一张表
        for file_name in sorted(os.listdir(src_dir)):
            if not file_name.endswith('.parquet'):
                continue
            stem = file_name[:-len('.parquet')]           # 'sh000001'
            src = pd.read_parquet(join_path(src_dir, file_name))
            g = self._decode(src, stem, date)
            frames.append(g)
        #
        df = pd.concat(frames, ignore_index=True)
        symbols = df.groupby('symbol', sort=False).size().rename('day_records').reset_index()
        if write_together:                             # 全天一张表
            df.to_feather(join_path(out_dir, f'{date}.feather'), compression='zstd')
        else:
            for symbol, g in df.groupby('symbol', sort=False): # 逐指数落盘
                g.reset_index(drop=True).to_feather(join_path(out_dir, f'{symbol}.feather'), compression='zstd')
        symbols.to_csv(join_path(out_dir, f'{date}_INDEX.csv'), index=False)
        print(f'INDEX {date}: {len(symbols)} symbols, {symbols["day_records"].sum()} records -> {out_dir}', flush=True)

    # ---------- 解码 ----------
    @staticmethod
    def _decode(src: pd.DataFrame, stem: str, date: str) -> pd.DataFrame:
        """parquet 源表 -> 解码 DataFrame: 按 INDEX_COLUMN 声明空表, 逐列填充。
        stem = 文件名去后缀, 如 'sh000001'; 坏名天 "sh('000001',)" 前 2 位仍是 'sh',
        指数代码统一取文件内 Symbol 列 (不依赖文件名)。
        兼容三种源 schema (V1 59列 / V2 15列 / V3 16列), 输出统一为 17 列 INDEX_COLUMN
        (含 source_version = 源 schema 版本标记 'V1'/'V2'/'V3');
        V1 缺 PreClosePrice/ClosePrice/rdtsc -> 填 0, 十档等无信息量列丢弃。"""
        version = CNIndexL2._detect_version(src, stem)     # 'V1' / 'V2' / 'V3'
        prefix = stem[:2]                              # 'sh' / 'sz' (坏名天也成立)
        exchange, exchange_id = DICT_EXCHANGE[prefix]
        d = pd.DataFrame(index=np.arange(len(src)), columns=INDEX_COLUMN)
        #
        d['exchange'] = exchange                       # 交易所 (str)
        d['symbol'] = prefix.upper() + src['Symbol'].astype(str).str.split('.').str[0]   # 'SH000001'; V2 时代 Symbol 带 '.SZ'/'.SH' 后缀, 截掉
        d['exchange_id'] = exchange_id
        #
        # receive: epoch 微秒 (UTC) -> 北京墙钟 -> 字符串。
        # 向量化格式化 (strftime 的底层 _format_native_types 占解码 ~60%, 详见股票侧 _decode_fast 实测):
        # epoch us +8h 直接转 datetime64[us] 输出 'YYYY-MM-DDTHH:MM:SS.ffffff', 再把 'T' 换 ' '。
        BJ_US = 8 * 3600 * 1_000_000                              # 北京 = UTC + 8h (微秒)
        d['receive_timestamp'] = src['ReceiveTime'].values.astype('int64')
        recv_str = np.datetime_as_string((d['receive_timestamp'].values + BJ_US).astype('datetime64[us]'), unit='us')
        d['receive_timestr'] = np.char.replace(recv_str, 'T', ' ')
        #
        # send: HHMMSSmmm -> 补零9位 -> 拼上日期一步解析成 datetime (北京时间墙钟);
        # send_timestr 同样从 epoch us 向量化格式化, 与 receive 完全同构。
        send_dt = pd.to_datetime(date + src['cb_time'].astype(str).str.zfill(9),
                                 format='%Y%m%d%H%M%S%f').astype('datetime64[us]')
        d['send_timestamp'] = send_dt.astype('int64') - BJ_US     # 北京墙钟 -> epoch 微秒 (UTC)
        send_str = np.datetime_as_string((d['send_timestamp'].values + BJ_US).astype('datetime64[us]'), unit='us')
        d['send_timestr'] = np.char.replace(send_str, 'T', ' ')
        #
        d['pre_close_price'] = src['PreClosePrice'].values if 'PreClosePrice' in src.columns else 0   # V1 无 -> 0
        d['open_price'] = src['OpenPrice'].values
        d['highest_price'] = src['HighestPrice'].values
        d['lowest_price'] = src['LowestPrice'].values
        d['last_price'] = src['LastPrice'].values
        d['close_price'] = src['ClosePrice'].values if 'ClosePrice' in src.columns else 0             # V1 无 -> 0
        d['volume'] = CNIndexL2._unify_volume(src['Volume'].values, src['Turnover'].values, exchange, version, date, stem)
        d['turnover'] = src['Turnover'].values
        d['rdtsc'] = src['rdtsc'].values if 'rdtsc' in src.columns else 0                             # V1 无 -> 0
        d['source_version'] = version
        return d

    # ---------- 源 schema 版本识别 ----------
    @staticmethod
    def _detect_version(src: pd.DataFrame, stem: str) -> str:
        """按源列集合识别 schema 版本: V1(59列) / V2(15列) / V3(16列); 未知列集则断言失败。"""
        cols = list(src.columns)
        for v, ref in zip(('V1', 'V2', 'V3'), INDEX_SOURCE_COLUMNS):
            if cols == ref:
                return v
        raise AssertionError(f'源列与三种已知 schema 均不一致: {stem} (列数 {len(cols)})')

    # ---------- volume 单位统一 ----------
    @staticmethod
    def _unify_volume(vol, turnover, exchange: str, version: str, date: str, stem: str):
        """源 volume -> 统一"股"。单位不按交易所固定, 而跟随源 schema 版本
        (2026-09-10 实测, 详见模块 docstring):

        判据: turnover/volume 中位 = 成份股成交均价, 正确口径下应在个位~几十元/股 (指数行情不会上千)。
          V1 (20220104~20220330): SSE=手, SZSE=股   (399001 额/量中位 16.5~18.5 元/股)
          V2 (20220331~20220630): SSE=手, SZSE=手   (399001 额/量中位 1526~2011, 恰为均价 x100)
          V3 (20220701 起):       SSE=手, SZSE=股   (399001 额/量中位 15.5~30.9 元/股)
        即: SSE 恒为手 -> 一律 x100; SZSE 仅 V2 为手 -> x100, 其余原样。
        铁证: 000001 20260428 收盘 600,507,498 手 x100 = 60,050,749,800 股
              = 上证指数官方成交量 (分毫不差)。

        x100 为 int64 无损变换 (最大 715 亿股 << int64 上限), 可随时 /100 还原。
        兜底校验: 换算后额/量中位超出 [0.5, 500] 元/股则打印告警 (防未来口径再变)。"""
        vol = vol.astype('int64')
        if exchange == 'SSE':                        # SSE 恒手
            vol = vol * 100
        if exchange == 'SZSE' and version == 'V2':  # SZSE 仅 V2 为手
            vol = vol * 100
        m = (vol > 0) & (turnover > 0)                                      # 盘前量额全 0, 不参与校验
        if m.any():
            r = float(np.median(turnover[m] / vol[m]))
            if not (0.5 <= r <= 500):
                print(f'!! {date} {stem}: volume 换算后额/量中位 {r:.2f} 元/股 超出 [0.5, 500], 请核查口径', flush=True)
        return vol


# ============================================================== 实测: python read_cnindex_L2.py
def _test() -> None:
    """实测: 跑 20260428 一天, 两种落盘模式各一遍, 仅作冒烟测试。
    全量运行见 run_exp/run_read_cnindex.py。"""
    index_root = r'H:\index\CNIndex'   # 源: 1127 个日期目录 (2022~2026), 每目录 ~550 个 parquet
    md_root = r'H:\index'              # 输出: <md_root>\full\ 或 <md_root>\single\<date>\
    r = CNIndexL2(index_root=index_root, md_root=md_root)
    date = '20260428'
    r.parse(date, write_together=True)
    r.parse(date, write_together=False)
    print(f'--- [_test] {date} OK', flush=True)


if __name__ == '__main__':
    _test()
