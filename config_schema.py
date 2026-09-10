#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
config_schema.py — 输出列约定 (顺序即落盘顺序)
  STOCK_COLUMN_DTYPE — data/md/<date>/<symbol>.feather 股票 3s 十档快照 (56 列, read_cnstock_L2.py)
  INDEX_COLUMN_DTYPE — data/md/<date>/<symbol>.feather 指数快照 (17 列, read_cnindex_L2.py)
当日符号清单另存 data/md/<date>_<SSE|SZSE|INDEX>.csv (列: symbol + day_records), 互不覆盖

时间口径: *_timestamp = epoch 微秒 (int); *_timestr = 北京时间字符串
  receive_* = 采集机落地时钟; send_* = 交易所时钟

%f 精度说明: strftime 的 %f 恒定输出 6 位零填充微秒 (如 .250000), 但实际精度取决于源数据——
  receive_timestr: 真微秒 (ReceiveTime 本身是 epoch µs)
  send_timestr:    源 data_time 只有毫秒 3 位, 末 3 位恒 000, 有效精度 = 毫秒
"""

STOCK_COLUMN_DTYPE = [
    ('exchange',              'str'),      # 交易所名: 'SSE' 上交所 / 'SZSE' 深交所; 由归档所在目录判定 (原始 exchange 字段 DZ 源恒 0 不可靠)
    ('symbol',                'str'),      # 证券代码, 交易所前缀 + 6 位代码: 'SH600519' / 'SZ000001'; 原始 S16 字节串按首个 NUL 截断再加前缀
    ('exchange_id',           'int64'),    # 交易所编号: 1=SSE, 2=SZSE; 解析时按目录补齐 (非原始字段的可靠值)
    ('receive_timestamp',     'int64'),    # 采集机落地时间, epoch 微秒 (UTC); 单调不减, 含交易所→采集机的网络/处理延迟
    ('receive_timestr',       'str'),      # 落地时间北京时间可读串 'YYYY-MM-DD HH:MM:SS.ffffff'; 由 receive_timestamp 换算, 真微秒精度
    ('send_timestamp',        'int64'),    # 交易所行情时间, epoch 微秒; 由原始 data_time (HHMMSSmmm) + 归档日期合成, 有效精度仅毫秒
    ('send_timestr',          'str'),      # 交易所时间北京时间可读串 'YYYY-MM-DD HH:MM:SS.ffffff'; 源只有毫秒, 末 3 位恒 '000'
    ('last_price',            'float64'),  # 最新成交价 (元); 开盘前/尚无成交时为 0
    ('qty',                   'int64'),    # 当日累计成交量 (股, 债券为张, 回购单位为手); 日内单调不减
    ('turnover',              'float64'),  # 当日累计成交额 (元); 日内单调不减
    ('pre_close_price',       'float64'),  # 交易所官方昨收盘价 (含除权除息调整); 算涨跌幅直接用它: last_price/pre_close_price - 1
    ('bid_price1',            'float64'),  # 买一档申报价 (元); 十档买价随档位加深递减, 0 = 该档无委托
    ('bid_price2',            'float64'),  # 买二档申报价 (元); 0 = 该档无委托
    ('bid_price3',            'float64'),  # 买三档申报价 (元); 0 = 该档无委托
    ('bid_price4',            'float64'),  # 买四档申报价 (元); 0 = 该档无委托
    ('bid_price5',            'float64'),  # 买五档申报价 (元); 0 = 该档无委托
    ('bid_price6',            'float64'),  # 买六档申报价 (元); 0 = 该档无委托
    ('bid_price7',            'float64'),  # 买七档申报价 (元); 0 = 该档无委托
    ('bid_price8',            'float64'),  # 买八档申报价 (元); 0 = 该档无委托
    ('bid_price9',            'float64'),  # 买九档申报价 (元); 0 = 该档无委托
    ('bid_price10',           'float64'),  # 买十档申报价 (元); 最深可见买档, 0 = 该档无委托
    ('ask_price1',            'float64'),  # 卖一档申报价 (元); 十档卖价随档位加深递增, 0 = 该档无委托
    ('ask_price2',            'float64'),  # 卖二档申报价 (元); 0 = 该档无委托
    ('ask_price3',            'float64'),  # 卖三档申报价 (元); 0 = 该档无委托
    ('ask_price4',            'float64'),  # 卖四档申报价 (元); 0 = 该档无委托
    ('ask_price5',            'float64'),  # 卖五档申报价 (元); 0 = 该档无委托
    ('ask_price6',            'float64'),  # 卖六档申报价 (元); 0 = 该档无委托
    ('ask_price7',            'float64'),  # 卖七档申报价 (元); 0 = 该档无委托
    ('ask_price8',            'float64'),  # 卖八档申报价 (元); 0 = 该档无委托
    ('ask_price9',            'float64'),  # 卖九档申报价 (元); 0 = 该档无委托
    ('ask_price10',           'float64'),  # 卖十档申报价 (元); 最深可见卖档, 0 = 该档无委托
    ('bid_vol1',              'int64'),    # 买一档申报量 (股/张); 与 bid_price1 同档
    ('bid_vol2',              'int64'),    # 买二档申报量 (股/张); 与 bid_price2 同档
    ('bid_vol3',              'int64'),    # 买三档申报量 (股/张); 与 bid_price3 同档
    ('bid_vol4',              'int64'),    # 买四档申报量 (股/张); 与 bid_price4 同档
    ('bid_vol5',              'int64'),    # 买五档申报量 (股/张); 与 bid_price5 同档
    ('bid_vol6',              'int64'),    # 买六档申报量 (股/张); 与 bid_price6 同档
    ('bid_vol7',              'int64'),    # 买七档申报量 (股/张); 与 bid_price7 同档
    ('bid_vol8',              'int64'),    # 买八档申报量 (股/张); 与 bid_price8 同档
    ('bid_vol9',              'int64'),    # 买九档申报量 (股/张); 与 bid_price9 同档
    ('bid_vol10',             'int64'),    # 买十档申报量 (股/张); 与 bid_price10 同档
    ('ask_vol1',              'int64'),    # 卖一档申报量 (股/张); 与 ask_price1 同档
    ('ask_vol2',              'int64'),    # 卖二档申报量 (股/张); 与 ask_price2 同档
    ('ask_vol3',              'int64'),    # 卖三档申报量 (股/张); 与 ask_price3 同档
    ('ask_vol4',              'int64'),    # 卖四档申报量 (股/张); 与 ask_price4 同档
    ('ask_vol5',              'int64'),    # 卖五档申报量 (股/张); 与 ask_price5 同档
    ('ask_vol6',              'int64'),    # 卖六档申报量 (股/张); 与 ask_price6 同档
    ('ask_vol7',              'int64'),    # 卖七档申报量 (股/张); 与 ask_price7 同档
    ('ask_vol8',              'int64'),    # 卖八档申报量 (股/张); 与 ask_price8 同档
    ('ask_vol9',              'int64'),    # 卖九档申报量 (股/张); 与 ask_price9 同档
    ('ask_vol10',             'int64'),    # 卖十档申报量 (股/张); 与 ask_price10 同档
    ('total_bid_vol',         'int32'),    # 委买总量 (真实股数); 0 = 无委买, 开盘前窗口该值可能先于十档出现
    ('total_ask_vol',         'int32'),    # 委卖总量 (真实股数); 0 = 无委卖
    ('avg_weight_bid_price',  'float64'),  # 全量委买加权均价 (元, 含十档外隐藏深度); 可见十档 VWAP 需自算 sum(P*V)/sum(V)
    ('avg_weight_ask_price',  'float64'),  # 全量委卖加权均价 (元, 含十档外隐藏深度)
    ('rdtsc',                 'uint64'),   # 接收机 CPU TSC 周期计数 (÷3000 ≈ 微秒); 仅作落地时序排查用, 非行情数据
]

STOCK_COLUMN = [x[0] for x in STOCK_COLUMN_DTYPE]


# ---------------------------------------------------------- 指数快照 (read_cnindex_L2.py)
INDEX_COLUMN_DTYPE = [
    ('exchange',              'str'),      # 交易所名: 'SSE' 上交所 / 'SZSE' 深交所; 由文件名前缀 sh/sz 判定
    ('symbol',                'str'),      # 指数代码, 交易所前缀 + 6 位代码: 'SH000001' 上证指数 / 'SZ399001' 深证成指; 取自文件名 (源 Symbol 列本身干净)
    ('exchange_id',           'int64'),    # 交易所编号: 1=SSE, 2=SZSE; 与源 Exchange 列一致 (实测 1662 文件无一例外)
    ('receive_timestamp',     'int64'),    # 采集机落地时间, epoch 微秒 (UTC); 源 parquet 的 ReceiveTime 列, 文件内单调不减
    ('receive_timestr',       'str'),      # 落地时间北京时间可读串 'YYYY-MM-DD HH:MM:SS.ffffff'; 真微秒精度
    ('send_timestamp',        'int64'),    # 交易所时间, epoch 微秒; 由源 cb_time (HHMMSSmmm, 沪深统一) + 日期合成, 有效精度仅毫秒
    ('send_timestr',          'str'),      # 交易所时间北京时间可读串; 源只有毫秒, 末 3 位恒 '000'
    ('pre_close_price',       'float64'),  # 昨收盘点位 (含修正); 指数无除权, 直接用于涨跌幅
    ('open_price',            'float64'),  # 开盘点位; 开盘前 (sh 约 09:25 前, sz 约 09:30 前) 为 0
    ('highest_price',         'float64'),  # 日内最高点位 (累计值, 单调不减)
    ('lowest_price',          'float64'),  # 日内最低点位 (累计值, 单调不减); 开盘前为 0, 统计需限 send_timestamp >= 开盘
    ('last_price',            'float64'),  # 最新点位; 开盘前 = 昨收 (与 pre_close_price 不同, 是前一交易日收盘价)
    ('close_price',           'float64'),  # 收盘点位; 盘中为 0, SSE 仅最后几条回填, SZSE 全天为 0; 下游取收盘用 last_price 尾行
    ('volume',                'int64'),    # 成交量, 统一为"股" (源口径: SSE 恒手 x100, SZSE 仅 V2 时代手 x100, 其余原生股; 详见 read_cnindex_L2.py _unify_volume); 开盘至今累计, 日内单调不减
    ('turnover',              'float64'),  # 成交额 (元); 日内单调不减
    ('rdtsc',                 'uint64'),   # 接收机 CPU TSC 周期计数; 仅作落地时序排查用, 非行情数据
    ('source_version',        'str'),      # 源 parquet 的 schema 版本标记: 'V1'/'V2'/'V3' (也代表推送节奏时代, 2023-05-09 起 3s 降为 12~21s)
]

INDEX_COLUMN = [x[0] for x in INDEX_COLUMN_DTYPE]