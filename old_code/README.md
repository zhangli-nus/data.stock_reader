# SSE / SZSE 原始行情数据说明

> 面向使用者的《上交所 / 深交所原始 Level-2 行情落盘数据》说明。
> 数据根目录 `/mnt/data_center/New_Raw/`（只读；`/home/data/New_Raw` 是同一份的软链）。
> 读取工具：`/home/leo/sse_szse_data/read_sse_szse.py`（自包含，不依赖 `/home/colin`）；示例输出 `demo_output.txt`。
> 结论均以 2026-09-02 / 2026-04-29 / 2025-12-10 等真实文件抽样核对（`struct_probe/probe*_out.txt`）；源码引用格式 `文件:行号`。
> 盘点时间 2026-09-03，最新数据日 20260902。

---

## 1. 数据在哪

### 1.1 目录树

```
/mnt/data_center/New_Raw/
├── SSE/                         上交所
│   ├── DZ/      {3s, cbtick, stocktick, stockorder, stocktrade, dump}
│   ├── GJ/      {3s, cbtick, tick, dump}
│   ├── GJ2/     {3s, cbtick, tick, dump}
│   ├── HX/      {3s, tick, all, ftp}
│   └── CFFEX/   {3s, tick}
└── SZSE/                        深交所
    ├── DZ/      {3s, tick, index, dump}
    ├── DZ2/     {3s, sh3s, tick, index, dump}
    ├── GJ/      {3s, tick}
    ├── GJ2/     {3s, tick}
    ├── HX/      {tick, dump, ftp}
    └── CFFEX/   {tick}
```

合计约 38 TB（SSE ≈ 13.6 T，SZSE ≈ 24.4 T）；最大三项 `SZSE/DZ/tick` 4.4 T、`SZSE/DZ2/tick` 3.4 T、`SZSE/GJ2/tick` 2.8 T。

### 1.2 来源（第二级目录）的含义

| 目录 | 含义 | 采集机/线路 | 证据 |
|---|---|---|---|
| `DZ` | 东证（东方证券）托管机 1 号；深市走盛立 HPF UDP 原始包 | `download/dzsse.py`、`download/dzszse.py`（代码里叫 `NDZ`） | `dz_szse_fill_kernel_all_mkt/main.c:84-128` 把 HPF 包填成 `tick_quote_t` |
| `DZ2` | 东证托管机 2 号（同源冗余，代码里叫 `NDZ1`/`DZ2`）；`sh3s` 是它抓的**上交所** 3s | `download/dz2.py:18-50` | |
| `GJ` | 国君（国泰君安）托管机 1 号 | `download/gjsse.py`、`gjszse.py` | |
| `GJ2` | 国君托管机 2 号（当前 SSE 唯一仍在更新的逐笔来源） | `download/gj2szse.py` | |
| `HX` | 华鑫（TORA/NGTS/XTS 线路）；`ftp` 是华鑫通过 FTP 给的官方 CSV | `download/hxsse.py`、`hxszse.py` | |
| `CFFEX` | 采集机名叫 `cffex4`，采的仍是**沪/深股票** 3s + 逐笔，不是中金所期货 | 实测 `SSE/CFFEX/3s` 内含 588190 等 SSE 代码 | |

### 1.3 叶子目录（LEAF）的含义

| LEAF | 含义 | 记录结构 | 证据 |
|---|---|---|---|
| `3s` | 交易所 L2 3 秒十档快照（自建订单簿快照格式），每个代码约 3 s 一条（债券 30 s） | `OB3S` 424 B | 远端目录 `quote_3s_download/Data`（`download/*.py`）；实测 `data_time` 步长 3 s |
| `sh3s` | `DZ2` 机器抓的**上交所** 3s（`SZSE/DZ2/sh3s` 里是 51xxxx/60xxxx 代码） | `OB3S` | `download/dz2.py:22-29` `quote_sse_3s_download` |
| `tick` | 逐笔委托 + 逐笔成交混合流；SZSE 全市场，SSE 的 GJ2/HX 为全市场（channel 1-6、20，HX 还含 801） | `TICK` 128 B | `obconstructor/tick_quote.h:6-7` `ENTRUST_QUOTE 1 / TRADE_QUOTE 2` |
| `cbtick` | 沪市**债券/可转债频道 channel_no=801** 的逐笔（含 204001 等回购） | `TICK` | `download/dzsse.py:21-27` prefix `cbtick`；实测仅 801 |
| `stocktick` | 沪市股票逐笔委托+成交合流（2024-10-15 起，channel 1-6） | `TICK` | 实测 |
| `stockorder` / `stocktrade` | 沪市股票逐笔委托 / 成交分开落地（2023-04-18 ~ 2024-06-28 有效） | `TICK`（只含委托 / 只含成交） | `download/dzsse.py:28-43`；`dat2parquet/dzsse.py:28-35` |
| `index` | 指数快照，**沪深两所指数在同一文件**（exchange=1 上证 000xxx、2 深证 399xxx） | `INDEX` 128 B | `download/dzszse.py:27-33`；`dtype.py:639` "东证指数行情结构" |
| `dump` / `all` | 交易机 `shm_dump` 整块共享内存转储（老格式，2022–2023），**无本地时间** | `len(u64)+payload` | `SSE/HX/all/README.md`："这是shm_dump数据,不带本地时间" |
| `ftp` | 华鑫官方 CSV（NGTS/XTS 全字段，沪深混在一起） | CSV | 见 §3.6 |

### 1.4 日期范围 / 是否仍在更新（详见 TASK C 盘点）

**仍在采集（最新 20260902）**

| 目录 | 起始 | 备注 |
|---|---|---|
| `SSE/GJ2/tick`, `SSE/GJ2/cbtick`, `SSE/GJ2/3s` | 20231121 / 20231213 / 20231229 | 最近 4 天（20260828–0902）保留已解包 `.dat`，像滚动保留 |
| `SSE/GJ/3s`, `SSE/GJ/cbtick` | 20231228 / 20240102 | `SSE/GJ/cbtick` 归档全部是 xz |
| `SZSE/DZ/{3s,index,tick}` | 20220629 / 20220701 / 20220629 | 20260317–0429 的 `.dat` 仍在盘上 |
| `SZSE/DZ2/{3s,sh3s,index,tick}` | 20220824 | |
| `SZSE/GJ/{3s,tick}` | 20230601 | `SZSE/GJ/tick` 20230718–20231107 连续 75 天为空文件 |
| `SZSE/GJ2/{3s,tick}` | 20230922 / 20231115 | |
| `SZSE/HX/{tick,dump}` | 20231218 / 20220824 | 归档全部 xz |

**已停止**

| 目录 | 最后日期 | 备注 |
|---|---|---|
| `SSE/DZ/3s`, `SSE/DZ/cbtick` | 20260430 | |
| `SSE/DZ/stocktick` | 20260401 | 之后 SSE 无东证 feed |
| `SSE/HX/3s`, `SSE/HX/tick` | 20260408 / 20260407 | |
| `SSE/DZ/stockorder`, `stocktrade` | 名义 20241011，**实际 20240628**（之后 68 个空档） | 20241015 起并入 `stocktick` |
| `SSE/CFFEX/*`, `SZSE/CFFEX/tick` | 20250805 | 2024-10 起大量缺日 |
| `SSE/GJ/tick` | 20240105 | 改存 `SSE/GJ/cbtick` |
| 各 `dump` / `all` | 20231121–20231213 | |
| `SSE/HX/ftp`, `SZSE/HX/ftp` | 零散 7 天（20250512、0815、1010、1013、1014、20260113、0114、20260402） | 非日常采集 |

已知缺失日：`20230120` 在 DZ/DZ2/HX 全系缺失；`20260114` 在 SSE/GJ2 全系缺失；`SSE/GJ/3s` 缺 20240226–0301。

---

## 2. 文件格式

### 2.1 归档 → .dat

- 归档命名 `<server>_<prefix>_<remotefile>_<YYYYMMDD>.tar.bz2`（`download/dzszse.py:42-46` `localname = "%s_%s_%s"`），如 `dzszse_tick_tick_quote_20260902.tar.bz2`、`gj2sse_3s_gj2sse_20260902.tar.bz2`。
- 每个 tar 只有一个成员：`tick_quote_YYYYMMDD.dat`。
- **约 1/3 的 `.tar.bz2` 实际是 XZ 压缩**（magic `FD 37 7A 58 5A 00`）：DZ/DZ2 全系 20231012 及以前、`SSE/GJ/cbtick`、`SSE/GJ/tick`、`SZSE/GJ/tick`、`SZSE/HX/dump`、`SZSE/HX/tick`、`SSE/GJ2/dump`、`SZSE/DZ2/dump` 整段都是 xz。解包用 `tar xf`（自动识别）或先 `file` 看 magic；`tar xjf` 会失败。
- 解包只能解到自己的可写目录（`/mnt/data_center` 只读；`.dat` 单文件 6–45 GB，注意空间）。
- `dump`/`all` 归档成员是 `<broker>_<shmkey>_<YYYYMMDDHH>.dat`（如 `dongzheng_20220101_2023020615.dat`），shm key → 内容：20220101 SZSE 3s、20220103 SZSE 自建盘口、20220106 SZSE tick、20220104 SSE 3s、20220105 SSE 自建盘口、20220107 SSE 可转债 tick、20220111/20220112 SSE 股票逐笔委托/成交、666666 策略状态（`raw_struct.py:649-653, 673-680`；`dump_dz.py:42-57`）。

### 2.2 tick_quote_YYYYMMDD.dat 的物理布局（当前格式，2023-12 之后）

| 项目 | 说明 | 证据 |
|---|---|---|
| 文件头 | **没有**。`offset=0` 直接是第 1 条记录 | `save_tbt_quote/main.c:112-119` `write_quote()`：`g_tick=get_time_of_day(); fwrite(&g_tick,8); fwrite(data,len)` |
| 每条记录前 8 字节 | **本地落地时间 `ReceiveTime`，epoch 微秒（UTC）**。落盘时跳过了 shm 记录首部的 `uint64 len`（`main.c:144,198` `write_quote(shm+pos+8, len-8)`），所以 gdb 里 `struct tick_quote` 的 `len` 槽位（`quote_my_stock.h:81`）在文件里就是 ReceiveTime | 实测每个文件首 8 字节解码为当日 09:05:01 / 09:14:00 / 09:15:00 |
| 记录定长 | 逐笔 128 B；3s 快照 424 B；指数 128 B。小端，C 8 字节对齐（`align=True`） | `np.dtype(...).itemsize` 实算；gdb `sizeof(struct tick_quote)=128` |
| 文件尾 | 文件大小是 4096 倍数（页对齐写），3s 文件末尾常有 8–400 B 残片（`size % 424 != 0`），tick/index 整除。`np.fromfile(count=)` 自动丢弃半条；末尾偶有全零记录，按 `ReceiveTime>0` 过滤 | 实测 |
| 文件名日期 | 用 `gmtime`（`main.c:214-219`）——当日 08:00 前启动会得到前一天的文件名 | |
| 异常头 | 若采集进程被 Ctrl-C，`QuitSignalHandler`（`main.c:289-300`）会把 8 B 全零 `binary_head_t` 覆写到文件开头 → 第 1 条 `ReceiveTime=0`；正常收盘的文件没有这个问题 | |
| shm_dump 老文件 | `dump`/`all` 目录：每条 = `len(u64) + payload`，**没有本地时间**；cbscript `raw_struct.py` 各类用 `fp.read(8)` 跳过第 1 个 len，再用尾部多带 8 B 保留字段的 dtype（`r2`/`ppp2`/`RT`/`reserve2`）吞掉下一条的 len——这就是 "8 字节头" 的来历 | `raw_struct.py:247-254, 413-419` |

### 2.3 老格式（2022-06 ~ 2023-11）

3s 与 index 的二进制格式在 **2023-11-01 ~ 2023-12-01 之间**（DZ）切换：SSE 3s 456 B（`dz_sh_dtype_utc`，`dtype.py:840`）→ 424 B；SZSE 3s 392 B（`dz_sz_dtype_utc`，`dtype.py:715`）→ 424 B；index 112 B（`dz_index_t`，`dtype.py:640`，2023-10-09 已是 128 B）→ 128 B。HX 3s 从 2023-04 起就是 424 B。**逐笔 128 B 自 2022 起未变。** 老格式请直接用 cbscript 的 `DZSSEQuoteReader / DZSZSEQuoteReader / DZIndexReader`（`raw_struct_utc.py:296 / 216 / 405`，offset=0）。

---

## 3. 字段定义

通用约定：小端；`S16` 代码字段以 `'\0'` 结尾，**NUL 之后可能是任意残留字节**（GJ 见 `"\0L2"`/`"\0ZW"`/`"\0ZC"`，DZ 为固定残留如 `b'\x00\x00\x00eP\x1a\xcd\xbf\xc0\xfa'`，HX 为全 NUL 填充），一律按第一个 NUL 截断（`utils/common.py:76 symbol_decode`）。`exchange`：`1=SSE 2=SZSE`（`obconstructor/tick_quote.h:20-21 EXCHG_SH/EXCHG_SZ`）。

### 3.1 逐笔记录 `TICK`（128 B）— tick / cbtick / stocktick / stockorder / stocktrade

C 原型：`stock_quote_distribute_server/quote_my_stock.h:47-108`（`struct tick_quote_entrust` L48-60、`struct tick_quote_trade` L63-76、`struct tick_quote` L79-108）；实际写盘程序 `save_tbt_quote/main.c:55-65`。cbscript：`utils/dtype.py:339 dz_tick_quote_utc`，委托平铺 `dz_tick_quote_entrust_all_utc`（L146），成交平铺 `dz_tick_quote_trade_all_ext_utc`（L283）。

**公共头（56 B）**

| offset | 字段 | numpy | 字节 | 含义 / 编码 | 依据 |
|---|---|---|---|---|---|
| 0 | `ReceiveTime` | i8 | 8 | 本地接收时间，epoch 微秒 UTC；`pd.to_datetime(x, unit='us', utc=True).tz_convert('Asia/Shanghai')` | `main.c:112-119`；实测 |
| 8 | `data_type` | i4 | 4 | `1` 委托（含撤单）`2` 成交 `3` 空/占位记录（仅 SSE；ticker 空、int_time=0、union 全 0，但占 seq） | `quote_my_stock.h:83-84`；`tick_quote.h:6-7`；实测 |
| 12 | `exchange` | i4 | 4 | `1` SSE `2` SZSE | `tick_quote.h:20-21` |
| 16 | `channel_no` | i4 | 4 | SSE：1–6 股票/基金、20、801 债券/可转债；SZSE：2011–2015 股票、2021–2025、2031–2035、2061（131810 等回购）、2071 | 实测 |
| 24 | `seq` | i8 | 8 | 频道内序号，从 1 严格连续（type=3 也占号）；SZSE 新委托 `order_no == seq` | `quote_my_stock.h:89-90`；`fill_kernel main.c:94` |
| 32 | `ticker` | S16 | 16 | 证券代码，不含交易所，NUL 结尾（后有垃圾） | `quote_my_stock.h:91-92` |
| 48 | `int_time` | i8 | 8 | 交易所时间，**格式随线路不同**：GJ/GJ2/HX `HHMMSSmmm`（9 位，9 点前前导零省略为 8 位）；DZ-SZSE `YYYYMMDDHHMMSSmmm`（17 位）；DZ-SSE stocktick `HHMMSScc`（**百分秒**，10:00 前 7 位 `9300570`=09:30:05.70、之后 8 位 `10535678`=10:53:56.78）；DZ-SSE cbtick `HHMMSSmmm` | 实测 §2.5/2.7/2.10/2.11 |

**委托 union（data_type=1，`TICK_ENTRUST`）**

| offset | 字段 | numpy | 字节 | 含义 / 编码 | 依据 |
|---|---|---|---|---|---|
| 56 | `side` | S1 | 1 | `'1'` 买 `'2'` 卖（`'G'` 借入 `'F'` 出借，未见） | `quote_my_stock.h:50` |
| 57 | `ord_type` | S1 | 1 | `'2'` 限价（GJ/GJ2/HX、DZ-SZSE）、**`'0'` 限价（DZ-SSE）**、`'1'` 市价、`'U'` 本方最优、`'C'` 撤单 | `quote_my_stock.h:52`（'0' 限价）；`dtype.py:19`（'2' 限价）；实测 |
| 64 | `order_no` | i8 | 8 | 委托号；撤单时 = 被撤原委托号 | `quote_my_stock.h:54-55` |
| 72 | `price` | i8 | 8 | 委托价 **×10000**（SSE 可转债三位小数 → `1663920`=166.392）；撤单：SZSE / HX-SSE 为 0，GJ2-SSE / DZ-SSE 为原委托价 | 实测 |
| 80 | `qty` | i8 | 8 | 委托量：**SSE ×1000，SZSE ×100**（SSE 股票 `100000`=100 股；SZSE `70000`=700 股；SSE 债券 `/1000` 得"手"，×10 为张） | 实测；`dtype.py:1010 div_value SSE=10` |
| 88 | `r1` | i8 | 8 | 对齐填充，恒 0 | |

**成交 union（data_type=2，`TICK_TRADE`）**

| offset | 字段 | numpy | 字节 | 含义 / 编码 | 依据 |
|---|---|---|---|---|---|
| 56 | `trade_flag` | S1 | 1 | SSE：`'B'` 主动买 `'S'` 主动卖 `'N'` 未知（集合竞价）；SZ：`'4'` 撤 `'F'` 成交（注释"20210526: 此项无意义"）。**实测只有 SSE-GJ2 填 B/S/N，其余线路为空** | `quote_my_stock.h:65-66`；实测 |
| 64 | `price` | i8 | 8 | 成交价 ×10000。**注意 cbscript `dz_tick_quote_trade_all_utc.price` 标成 `f8`（`dtype.py:268`）是错的**，union 子结构 `dz_tick_quote_entrust.price`（L24）/`dz_tick_quote_trade.price`（L34）同样是 `f8`；用 `_ext_utc`（L301 `i8`） | 实测 |
| 72 | `qty` | i8 | 8 | 成交量：SSE ×1000，SZSE ×100 | 实测 |
| 80 | `bid_no` | i8 | 8 | 买方委托号 | `quote_my_stock.h:72-73` |
| 88 | `ask_no` | i8 | 8 | 卖方委托号 | `quote_my_stock.h:74-75` |

**公共尾（32 B）**

| offset | 字段 | numpy | 字节 | 含义 | 依据 |
|---|---|---|---|---|---|
| 96 | `ori_seq` | i8 | 8 | 实际是两个 int：低 32 位 `pkg_loss`、高 32 位 `is_old`；一般 0；`SSE/DZ/cbtick/20260429` 恒 `4294967296` 即 is_old=1。`main.c:184/195`：一旦某轮 `read_tick_quote` 内检测到 seq 断档，其后所有记录 is_old=1（`goto repeat` 不重置）；`main.c:143`：由 query shm 补回的记录也置 1。故 is_old=1 表示“补包 / 断档之后”，不等于整段回放 | `save_tbt_quote/main.c:61-62,143,166,184,195`；`quote_my_stock.h:102` |
| 104 | `biz_idx` | i8 | 8 | 业务序号，一般 0 | `main.c:63` |
| 112 | `ori_no_idx` | i8 | 8 | 实际 `tsc0`，一般 0 | `main.c:64` |
| 120 | `rdtsc` | u8 | 8 | 接收机 CPU TSC 计数（cbscript 按 3 GHz 换算 µs：`dstruct.py:35 /3000`；cbscript `dtype.py:358` 标 i8，当前值 <2^63 等价） | `main.c:65` |

SZSE 撤单在 fill_kernel 里被转成 `data_type=1, ord_type='C'` 的委托，`price=0`、`qty=撤单量`、`order_no=被撤原单号`、`side` 由 bid/ask_app_seq 非零判定（`dz_szse_fill_kernel_all_mkt/main.c:100-115`；`ord_type` 在 L88 写 `LIMIT_ORDER`，实测落盘为 `'2'`）。

### 3.2 3 秒快照 `OB3S`（424 B）— 3s / sh3s

**SSE 与 SZSE、GJ/GJ2/DZ/HX 七个来源的二进制布局完全相同**（stride 424、ticker@8，全部通过盘中校验），只是内容语义有差异（见表后备注）。cbscript `utils/dtype.py` 里**没有**完全对应的定义：最接近 `dz_myob_quote`（L362，424 B 含尾部 `reserve2`）——用 offset=8 读它等价于本布局，但其 `trades_count`（L374）槽位实盘是 double 昨收、`reserve2` 读到的是下一条的 ReceiveTime。C 原型 `obconstructor/tick_quote.h:79-110 myob_t`（`seq` 槽注释为成交笔数，`data_time` L104）。

| offset | 字段 | numpy | 字节 | 含义 / 单位 | 备注 |
|---|---|---|---|---|---|
| 0 | `ReceiveTime` | i8 | 8 | 本地接收时间 epoch 微秒，单调不减 | |
| 8 | `ticker` | S16 | 16 | 代码，NUL 截断 | |
| 24 | `exchange` | i4 | 4 (+4 pad) | GJ/GJ2：1 SSE 2 SZSE；**DZ/HX：恒 0（未填）**，交易所需从目录名取 | 实测 |
| 32 | `last_price` | f8 | 8 | 最新价（元）；债券开盘前为 0 | `dtype.py:368` |
| 40 | `qty` | i8 | 8 | 累计成交量（股/张，未缩放）。**SSE 的 GJ/GJ2 源对 5/6/9 开头（基金/股票/B 股）为 ×10，0/1/2 开头债券为 ×1**（同日 20251117 GJ/HX 按 (ticker,data_time) 对齐 106,816 行：5/6/9 比值恒 10.0，0/1/2 恒 1.0；`turnover/(qty·last)` 中位 0.100 vs 1.00；DZ/HX-SSE 与所有 SZSE 为 1.00） | 实测 §2.1/2.12 |
| 48 | `turnover` | f8 | 8 | 累计成交额（元） | `dtype.py:372` |
| 56 | `pre_close_price` | f8 | 8 | **[推断]** 昨收：开盘前已非零、与 last 中位比 0.99–1.01、债券=100；同一 ticker 全天唯一 | cbscript 此槽叫 `trades_count i8`（L374） |
| 64 | `BPrice1..10` | f8×10 | 80 | 十档买价（元） | |
| 144 | `SPrice1..10` | f8×10 | 80 | 十档卖价 | |
| 224 | `BVol1..10` | i8×10 | 80 | 十档买量（股/张）。**SSE-GJ/GJ2 源对股票/基金/B 股同样 ×10**（GJ/HX 对齐比值恒 10.0；债券 1.0） | 实测 |
| 304 | `SVol1..10` | i8×10 | 80 | 十档卖量（同上，SSE-GJ/GJ2 股票 ×10） | 实测 |
| 384 | `data_time` | i8 | 8 | 交易所时间 `HHMMSSmmm`（SSE 带毫秒如 `121219726`，SZSE 整秒 `112827000`） | `dtype.py:383-384` "HHMMSS3f" |
| 392 | `tot_bid_vol` | i4 | 4 | 委买总量（股，真实股数）。SSE 各源（GJ/GJ2/DZ/HX）对 5xxxxx 基金 / 6xxxxx 股票 / 9xxxxx B 股 非零率 97.7%–100%，0xxxxx 国债约 80%，**1xxxxx/2xxxxx/7xxxxx 债券为 0**（非零率 0–3%；SSE 文件里债券代码占 ~85%，早先“恒 0”的抽样结论是被债券淹没）；SZSE 各源有值。**SSE-GJ/GJ2 源此字段不 ×10**（GJ/HX 比值 1.0） | 实测（4 源各 20 万条） |
| 396 | `tot_ask_vol` | i4 | 4 | 委卖总量（同上） | |
| 400 | `avg_weight_bid_price` | f8 | 8 | 加权平均委买价（元；有值范围同 tot_bid_vol，`avg_weight_bid_price/BPrice1` 中位 0.975–0.98） | |
| 408 | `avg_weight_ask_price` | f8 | 8 | 加权平均委卖价（同上） | |
| 416 | `rdtsc` | u8 | 8 | 接收机 TSC（cbscript `dtype.py:394` 标 i8，等价） | |

SSE 与 SZSE 的差异只在内容：SSE 文件覆盖 24–26k 个代码（大量 1xxxxx/2xxxxx 债券，盘中 `last>0` 仅 13–38%），SZSE GJ 约 22–24k、SZSE DZ 仅 4,068（股票/基金，无债券）。

### 3.3 指数 `INDEX`（128 B）— index

cbscript `dz_index_t`（`dtype.py:640`，112 B，注释"东证指数行情结构"）**再加 16 B**：写入长度多写了下一条 shm 记录的 `len` 与 `rdtsc`。用 112 B 读会从第 2 条起错位（`raw_struct_utc.py:405 DZIndexReader` 有此问题）。

| offset | 字段 | numpy | 字节 | 含义 |
|---|---|---|---|---|
| 0 | `ReceiveTime` | i8 | 8 | 本地接收时间 epoch 微秒 |
| 8 | `rdtsc` | u8 | 8 | TSC（cbscript `dtype.py:642` 标 i8，等价） |
| 16 | `symbol` | S16 | 16 | 指数代码，NUL 截断 |
| 32 | `exchange` | i4 | 4 (+4 pad) | 1 上证（000xxx）2 深证（399xxx），**同一文件混合** |
| 40 | `update_time` | u8 | 8 | **SSE：`HHMMSS`（如 121505）；SZSE：`YYYYMMDDHHMMSSmmm`**（`DZIndexReader` 的 `>1e9 ? %1e9 : ×1000` 逻辑对应此） |
| 48 | `pre_close_px` | f8 | 8 | 昨收（点） |
| 56 | `open_px` | f8 | 8 | 开盘 |
| 64 | `turnover` | f8 | 8 | 成交额（元） |
| 72 | `volume` | u8 | 8 | 成交量（股） |
| 80 | `high_px` / 88 `low_px` / 96 `last_px` / 104 `close_px` | f8×4 | 32 | 高 / 低 / 最新 / 收盘（盘中 0） |
| 112 | `len` | i8 | 8 | 无意义（=112 或 0） |
| 120 | `rdtsc2` | u8 | 8 | 无意义 |

### 3.4 老格式快照（仅 2022-06 ~ 2023-11 与 dump 目录，供对照）

- `dz_sz_dtype` / `dz_sz_dtype_utc`（`dtype.py:687 / 715`，392 B，`pack(1)`）= 盛立 `sze_hpf_lev2_pkt`（`dz_szse_fill_kernel_all_mkt/sze_hpf_define.h:93-117`）：包头 43 B（`m_sequence i4, m_tick1/2 i2, m_msg_type S1(21 replace 22 idx 23 order 24 exec 29 tree), m_security_type, m_sub_security_type, m_symbol S9, m_exchange_id S1(101), m_quote_update_time i8 YYYYMMDDHHMMSSmmm, m_channel_no u2, m_sequence_num i8, m_md_stream_id u4`）+ `m_trade_phase_code S1` + `m_trade_num/m_total_qty/m_total_value i8`（量 ×100，额 ×1e6）+ 6 个价格 `u4 ×10000` + 委买/卖加权价与总量 + `m_lpv m_iopv m_upper_limit_price m_low_limit_price m_open_interest u4` + 十档 `BPrice u4/BVol i8` 交错 + `rdtsc`。读取器 `DZSZSEQuoteReader`（`raw_struct_utc.py:216`，价 /10000、量 /100、额 /1e6：L278-283）。
- `dz_sh_dtype` / `dz_sh_dtype_utc`（`dtype.py:795 / 840`，456 B）= 东证 SSE MDGW 原始快照：26 B 头（`h_m_msg_type, h_m_qt_year/month/day, h_m_send_time, h_m_seq_lost_flag …`）+ `m_update_time u4`（`HHMMSSss`，>180000 视为已带毫秒否则 ×1000，`raw_struct.py:590`）+ `m_symbol S9` + 价格 `u4 ×1000` + `m_total_trade_number u4, m_total_qty i8, m_total_value i8` + 委买/卖总量与加权价 + `m_yield_to_maturity u4` + 十档（每档 `S4 保留 + u4 价 + i8 量`）+ `rdtsc`。读取器 `DZSSEQuoteReader`（`raw_struct_utc.py:296`，价 /1000；11 开头沪可转债量再 /1000、额 /1e5：L394-399）。
- `my_market_ob_t`（`dtype.py:584`，344 B，万得/XTP 整型快照 ×10000）、`numpy_stock_dtype`（L511，360 B）、`xtp_market_data_t`（L550，752 B，XTP `XTPMarketDataStruct`，价格 double，`data_time` YYYYMMDDHHMMSSfff）、`xtp_transaction_t`（L464，128 B，XTP 逐笔成交）——对应更早的 `SSE/XTP`、`SZSE/OB` 目录，磁盘上已不存在。

### 3.5 代码表汇总

| 字段 | 取值 |
|---|---|
| `exchange` | 1 SSE，2 SZSE，0 未填（DZ/HX 3s） |
| `data_type` | 1 委托，2 成交，3 空/占位（仅 SSE） |
| `side` | '1' 买，'2' 卖，'G' 借入，'F' 出借 |
| `ord_type` | '2' 限价（GJ/HX/DZ-SZSE），'0' 限价（DZ-SSE），'1' 市价，'U' 本方最优，'C' 撤单 |
| `trade_flag` | 'B' 主动买，'S' 主动卖，'N' 未知（SSE-GJ2 才填）；'F' 成交 / '4' 撤（SZ，实际为空） |
| 逐笔 `price` | 整数 ÷ 10000 = 元 |
| 逐笔 `qty` | SSE 整数 ÷ 1000，SZSE 整数 ÷ 100 = 股 / 张（SSE 债券为"手"） |
| 快照价格/量 | 已是元 / 股（SSE-GJ/GJ2 快照对股票/基金/B 股的 `qty` 与 `BVol/SVol1..10` 再 ÷10，债券与 `tot_bid/ask_vol` 不缩放） |

### 3.6 HX ftp CSV（华鑫官方导出）

沪深混在同一文件，`ExchangeID` 1 SSE 2 SZSE；时间为 `HHMMSSmmm` 整数（`72042000`=07:20:42.000）；**浮点写成 `十进制@小端 double 十六进制`**（如 `14.8600000000@b81e85eb51b82d40`），读取时 `str.split('@')[0]` 再 `astype(float)`。`_A` 包为全量，`_B` 包的 OrderDetail/Transaction/Bond* 仅表头。

| 文件 | 列 |
|---|---|
| `MarketData.csv`（十档快照） | `SecurityID, ExchangeID, DataTimeStamp, PreClosePrice, OpenPrice, NumTrades, TotalVolumeTrade, TotalValueTrade, TotalBidVolume, AvgBidPrice, TotalAskVolume, AvgAskPrice, HighestPrice, LowestPrice, LastPrice, BidPrice1, BidVolume1, AskPrice1, AskVolume1, … BidPrice10, BidVolume10, AskPrice10, AskVolume10 …`（94 列，含撤单统计） |
| `XTSMarketData.csv` | 同上，每档多 `BidNNumOrders / AskNNumOrders`（SSE 债券 XTS 平台） |
| `Transaction.csv`（SZSE 逐笔成交） | `ExchangeID, SecurityID, TradeTime, TradePrice, TradeVolume, ExecType, MainSeq, SubSeq, BuyNo, SellNo, Info1, Info2, Info3, TradeBSFlag, BizIndex, LocalTimeStamp` |
| `OrderDetail.csv`（SZSE 逐笔委托） | `ExchangeID, SecurityID, OrderTime, Price, Volume, Side, OrderType, MainSeq, SubSeq, Info1, Info2, Info3, OrderNO, OrderStatus, BizIndex, LocalTimeStamp` |
| `NGTSTick.csv`（SSE 逐笔合并） | `ExchangeID, SecurityID, MainSeq, SubSeq, TickTime, TickType, BuyNo, SellNo, Price, Volume, TradeMoney, Side, TradeBSFlag, MDSecurityStat, Info1, Info2, Info3, LocalTimeStamp` |
| `XTSTick.csv`（SSE 债券逐笔） | 同 NGTSTick |
| `FirstLevel.csv` / `XTSFirstLevel.csv` / `BondFirstLevel.csv`（一档 45 笔队列） | `ExchangeID, SecurityID, DataTimeStamp, BuyPrice, SellPrice, BuyNum, SellNum, BuyVolume1..45, …`（尾列实为 LocalTimeStamp） |
| `Index.csv` | `ExchangeID, SecurityID, DataTimeStamp, PreCloseIndex, OpenIndex, HighIndex, LowIndex, LastIndex, Turnover, TotalVolumeTraded, Info1, Info2, Info3, CloseIndex, LocalTimeStamp` |
| `IOPV.csv`（仅 SSE ftp） | `SecurityID, ExchangeID, DataTimeStamp, IOPV, MDSecurityStat, Info1, Info2, Info3, LocalTimeStamp` |
| `BondMarketData.csv` | `SecurityID, ExchangeID, DataTimeStamp, PreClosePrice, OpenPrice, AvgPreClosePrice, NumTrades, TotalVolumeTrade, TotalValueTrade, AuctionVolumeTrade, AuctionValueTrade, TotalBidVolume, AvgBidPrice, TotalAskVolume, AvgAskPrice, HighestPrice, LowestPrice, LastPrice, AuctionLastPrice, AvgPrice, PriceUpDown1, PriceUpDown2, ClosePrice, MDSecurityStat, BidPrice1, BidVolume1, Bid1NumOrders, …` |
| `BondOrderDetail.csv` / `BondTransaction.csv` | 同 OrderDetail / Transaction 去掉 TradeBSFlag/BizIndex |
| `PHMarketData.csv` / `PHTransaction.csv`（科创板盘后固定价格） | `SecurityID, ExchangeID, DataTimeStamp, ClosePrice, MDSecurityStat, NumTrades, TotalVolumeTrade, TotalValueTrade, TotalBidVolume, TotalAskVolume, WithdrawBuyNumber, WithdrawBuyAmount, WithdrawSellNumber, WithdrawSellAmount, BidOrderQty, BidNumOrders, AskOrderQty, AskNumOrders, …` / `…, TradePrice, TradeVolume, TradeMoney, ExecType, …, TradeBSFlag, LocalTimeStamp` |
| `ResendOrderDetail.csv` / `ResendTransaction.csv` | 仅表头 |

---

## 4. 目录 → dtype 对照表（探针实测）

`read_sse_szse.py` 中 `TICK / OB3S / INDEX` 三个 dtype；`verified=yes` 表示已用真实 `.dat` 做 stride 探测 + 首/中/尾三段合理性校验（`struct_probe/probe3_out.txt`）。

| 目录 | 样本文件 | 记录 | dtype | offset | `int_time`/`data_time` 口径 | 特殊点 | verified |
|---|---|---|---|---|---|---|---|
| `SSE/GJ2/tick` | 20260902 (25.8 GB, 201.5M 条) | 128 | `TICK` | 0 | HHMMSSmmm | ch 1-6,20；头尾 type=3；`trade_flag` B/S；`ord_type` '2'/'C' | yes |
| `SSE/GJ2/cbtick` | 20260902 (6.3 GB, 48.9M) | 128 | `TICK` | 0 | HHMMSSmmm | 仅 ch 801；`trade_flag` B/S/N | yes |
| `SSE/GJ2/3s` | 20260902 (15.2 GB, 35.9M) | 424 | `OB3S` | 0 | HHMMSSmmm（带毫秒） | exchange=1；**股票/基金/B股 qty 与十档量 ×10（债券 ×1）**；tot_bid_vol 股票有值、债券 0；26k ticker | yes |
| `SSE/GJ/3s` | 20251117 (19.1 GB) | 424 | `OB3S` | 0 | 同上 | 同上（股票 qty/十档量 ×10） | yes |
| `SSE/GJ/cbtick`, `SSE/GJ/tick` | 无已解包 .dat | 128 | `TICK`（同 GJ2，推断） | 0 | | | no |
| `SSE/DZ/3s` | 20260429 (12.9 GB, 30.3M) | 424 | `OB3S` | 0 | HHMMSSmmm | **exchange=0**；qty=股；该日 14:31 截止 | yes |
| `SSE/DZ/cbtick` | 20260429 (0.77 GB) | 128 | `TICK` | 0 | HHMMSSmmm（无前导零） | `ord_type` **'0'** 限价；trade_flag 空；`ori_seq`=2^32（回放）；该日只到 09:44 | yes |
| `SSE/DZ/stocktick` | 20251210 (20.1 GB, 157M) | 128 | `TICK` | 0 | **HHMMSScc 百分秒（10:00 前 7 位）** | ch 1-6；`ord_type` '0'/'C'；trade_flag 空 | yes |
| `SSE/DZ/stockorder`, `stocktrade` | 仅 tar | 128 | `TICK`（只含委托 / 只含成交） | 0 | | `dat2parquet/dzsse.py:28-35` | no |
| `SSE/HX/3s` | 20260114 (12.9 GB) | 424 | `OB3S` | 0 | HHMMSSmmm | exchange=0；qty=股 | yes |
| `SSE/HX/tick` | 20260114 (39.3 GB, 307M) | 128 | `TICK` | 0 | HHMMSSmmm | **股票 ch1-6,20 与债券 ch801 同一文件**；撤单 price=0；trade_flag 空 | yes |
| `SZSE/GJ2/tick` | 20260902 (35.4 GB, 276M) | 128 | `TICK` | 0 | HHMMSSmmm | ch 2011-2071；`ord_type` 1/2/C/U；trade_flag 空；qty ×100 | yes |
| `SZSE/GJ2/3s` | 20260902 (7.2 GB, 17.0M) | 424 | `OB3S` | 0 | HHMMSS000 | exchange=2；qty=股；tot_bid_vol 有值 | yes |
| `SZSE/GJ/tick` | 20251013 (37.8 GB) | 128 | `TICK` | 0 | HHMMSSmmm | 同 GJ2 | yes |
| `SZSE/GJ/3s` | 20260114 (7.6 GB) | 424 | `OB3S` | 0 | HHMMSS000 | 同 GJ2 | yes |
| `SZSE/DZ/tick` | 20260429 (40.8 GB, 319M) | 128 | `TICK` | 0 | **YYYYMMDDHHMMSSmmm 17 位** | ch 2011-2061 | yes |
| `SZSE/DZ/3s` | 20260429 (6.7 GB, 15.8M) | 424 | `OB3S` | 0 | HHMMSS000 | exchange=0；仅 4,068 ticker（无债券） | yes |
| `SZSE/DZ/index` | 20260429 (0.43 GB, 3.36M) | 128 | `INDEX` | 0 | SSE HHMMSS / SZSE 17 位 | 沪深混合 554 个指数 | yes |
| `SZSE/DZ2/{tick,3s,sh3s,index}` | 仅 tar | 128/424/424/128 | 同 DZ（推断） | 0 | | | no |
| `SZSE/HX/tick` | 仅 tar | 128 | `TICK`（推断） | 0 | | | no |
| `SSE/CFFEX/{3s,tick}`, `SZSE/CFFEX/tick` | 仅 tar | 424/128 | `OB3S`/`TICK`（TASK B 抽样 424/128） | 0 | | | no |
| `*/dump`, `*/all` | shm_dump | len+payload | 老 dtype（`dz_tick_quote` L316 / `dz_myob_quote` L362 / `dz_sz_dtype` L687 / `dz_sh_dtype` L795）+ offset=8 | 8 | | 无本地时间 | no |

---

## 5. 如何读

### 5.1 `read_sse_szse.py`（推荐，自包含）

```bash
P=/root/anaconda3/bin/python3
S=/home/leo/sse_szse_data/read_sse_szse.py
$P $S peek    /mnt/data_center/New_Raw/SZSE/GJ2/tick/tick_quote_20260902.dat   # 自动识别 dtype + 记录数 + 首尾时间
$P $S head    /mnt/data_center/New_Raw/SSE/GJ2/3s/tick_quote_20260902.dat 5     # 开头 5 条(已解码)
$P $S head    /mnt/data_center/New_Raw/SSE/GJ2/3s/tick_quote_20260902.dat 5 mid # 文件中点 5 条
$P $S summary /mnt/data_center/New_Raw/SZSE/DZ/index/tick_quote_20260429.dat --full  # 记录数/时间范围/ticker 数
```

```python
import sys; sys.path.insert(0, '/home/leo/sse_szse_data')
from read_sse_szse import *

f = '/mnt/data_center/New_Raw/SZSE/GJ2/tick/tick_quote_20260902.dat'
n = nrec(f, 'TICK')                                   # 276,375,584 条
a = read_dat(f, 'TICK', offset=0, start=n//2, count=200_000)   # 原始结构化 ndarray (seek + count, 不整读)
ent, trd = read_tick(f, start=n//2, count=200_000)    # 两张 DataFrame: 去掉 type=3、ticker 截断、price/1e4、qty 按交易所缩放、recv 上海时区、ex_time_ms
ob = read_snapshot('/mnt/data_center/New_Raw/SSE/GJ2/3s/tick_quote_20260902.dat', start=0, count=100_000)  # SSE-GJ/GJ2 源股票/基金/B股的 qty+BVol/SVol 自动 //10
ix = read_index('/mnt/data_center/New_Raw/SZSE/DZ/index/tick_quote_20260429.dat')
for start, blk in iter_dat(f, 'TICK', count=200_000): ...       # 全天分块遍历 (25 GB 约几分钟)
mm = read_dat(f, 'TICK', memmap=True)                 # np.memmap 惰性视图, mm[i] 随机访问, 不占内存

# 解码工具
decode_ticker(a['ticker'])                            # bytes -> str, NUL 截断
recv_to_datetime(a['ReceiveTime'])                    # epoch us -> Asia/Shanghai
int_time_to_datetime(ent['int_time'], '20260902')     # HHMMSSmmm / 17 位 / HHMMSS -> datetime;  hundredths=True 处理 DZ-SSE stocktick 7–8 位百分秒
name, info = detect_dtype(f)                          # 'TICK' | 'OB3S' | 'INDEX'
```

要点：`read_dat` 默认 `count=200_000`，`count=None` 也不会整读；`head` 对 tick 文件会自动往后扫过开头的 `data_type=3` 空记录；`summary` 默认抽 head/mid/tail 三块（`--full` 全扫）。

同目录还有两个前期探针阶段写的等价模块（字段名略有不同，可互换）：`sse_szse_dtypes.py`（`TICK128/OB3S/INDEX128`，`read_block/tick_to_df/ob3s_to_df`）与 `read_new_raw.py`（`QUOTE_3S_DT/TICK_RAW_DT/INDEX_DT`，`read_chunk/to_frame`），以及 `read_raw_dat.py`、`struct_probe/`。

### 5.2 沿用 Colin 的 cbscript（`/home/colin/git/cbscript`，2023-11 快照）

```python
import sys; sys.path.insert(0, '/home/colin/git/cbscript')
from utils import dtype                                # 导入无副作用
import numpy as np
with open(f, 'rb') as fp:
    fp.seek(start * 128); a = np.fromfile(fp, dtype=dtype.dz_tick_quote_utc, count=200_000)
ent = a[a['data_type'] == 1].view(dtype.dz_tick_quote_entrust_all_utc)      # dtype.py:146
trd = a[a['data_type'] == 2].view(dtype.dz_tick_quote_trade_all_ext_utc)    # dtype.py:283 (price i8) — 不要用 L250 的 f8 版
```

- 逐笔：`dataprod/raw_struct_utc.py:114 DZTickReader`（offset=0，按 data_type 分流）方向正确，但 L134 用了 `dz_tick_quote_trade_all_utc`（price f8，错），且 L151-152 统一 `qty/100`（对 SSE 应 /1000）。
- 3s / index（2023-12 之后）：`DZSSEQuoteReader / DZSZSEQuoteReader / DZIndexReader`（`raw_struct_utc.py:296 / 216 / 405`）用的是老 456/392/112 B 结构，**对当前文件全部错位，不能直接用**；3s 可用 `dtype.dz_myob_quote` + `offset=8`（即 `raw_struct.py:413 DZSZSEMYOBReader` 的读法，但不要做它的 /10000、/100 缩放）。
- parquet 转换入口：`dat2parquet/{dzsse,dzszse,dz2szse,hxszse}.py`（`dzsse.py:66-71` 调 `DZSSETickReader(tfile, 0, 'entrust'/'trade')`），输出 `/mnt/data_center/StockDaily/{broker}/{Y}/{Y}.{M}.{D}/{sh|sz}{symbol}.parquet`（`cfg.yaml:12-20`，`common.py:118-145`，月日不补零），broker 如 `DZUTC / DZENTRUSTUTC / DZTRADEUTC / DZINDEXUTC / GJ2UTC / HXUTC`；读取 API `dapi/dapi/stock/loader.py:9 load_stock(date=, code=, broker=)`。注意 StockDaily 已更新到 2026.9.2，说明生产用的是另一份更新的代码，本机 cbscript 只是旧拷贝。
- 数据检查：`datacheck/*.py`；tick 目录里的 `tool_check_stock_quote`（ELF）+ `check.sh` 按 channel 输出 last seq / last time 到 `info/out.info`。

---

## 6. 已知坑

1. **成交价 dtype 错误**：`dtype.py:268 dz_tick_quote_trade_all_utc.price` 标 `f8`，实盘是 `i8 ×10000`（按 f8 解出 1e-318 量级垃圾）→ 用 `_ext_utc`（L301）。`dtype.py:24`、L34 的 `dz_tick_quote_entrust` / `dz_tick_quote_trade` 子结构 `price` 亦为 `f8`（配合 `dz_tick_quote['m'].view()` 同样解出垃圾），同样不能用；委托应用 L146，成交用 L283。只有 `*_all / *_all_utc / *_ext_utc` 的 price 是 `i8`。
2. **逐笔数量缩放按交易所不同**：SSE ×1000（100000=100 股），SZSE ×100（70000=700 股）；cbscript 一律 /100（`raw_struct_utc.py:69,152`）对 SSE 多 10 倍。SSE 债券 /1000 得"手"（1 手=10 张，`dtype.py:1010 div_value`）。
3. **3s 快照 SSE-GJ/GJ2 源对股票/基金/B 股（5/6/9 开头）的 `qty` 与十档量 `BVol1..10/SVol1..10` 为 ×10**（`turnover/(qty·last)`≈0.10；600000: qty=427,146,830 vs 实际 42.9M 股；同日 GJ/HX 对齐 106,816 行，qty/BVol/SVol 比值恒 10.0），**0/1/2 开头债券为 ×1，`tot_bid_vol/tot_ask_vol` 不 ×10**（比值恒 1.0，故 GJ 源 `tot_bid_vol/ΣBVol` 中位仅 0.3–0.4，DZ/HX 为 4.7–5.1）；DZ/HX-SSE 与全部 SZSE 为 1.0。`read_snapshot` 默认对 SSE+GJ/GJ2 的非 0/1/2 开头行把 `qty+BVol+SVol` 整除 10（`fix_sse_gj_x10=True`，旧名 `fix_sse_gj_qty` 仍接受）。
4. **3s 记录 offset 56 是 double 昨收（推断）**，不是 cbscript 标的 `trades_count i8`；3s 文件没有成交笔数字段。`tot_bid_vol / tot_ask_vol / avg_weight_*`：SSE 各源对股票/基金/B 股有值（≈100%），对 1xxxxx/2xxxxx/7xxxxx 债券为 0；SZSE 各源有值。
5. **`exchange` 字段在 DZ/HX 的 3s 里恒 0**，交易所要从目录名取（`read_snapshot` 自动从路径补）。index 文件沪深混合。
6. **`int_time` 三种格式**：9 位 `HHMMSSmmm`（GJ/GJ2/HX、DZ-SSE cbtick；9 点前无前导零为 8 位）、17 位 `YYYYMMDDHHMMSSmmm`（DZ-SZSE tick、DZ index 的 SZSE 行）、7–8 位 `HHMMSScc` 百分秒（DZ-SSE stocktick；10:00 前 7 位 `9300570`=09:30:05.70，之后 `10535678`=10:53:56.78）。cbscript 用 `int_time<230000000` 判 8/9 位并按 `%H%M%S%f` 解析（`raw_struct_utc.py:57-61`），对百分秒格式会错 10 倍毫秒；`read_tick` 对 DZ+SSE 自动识别（块内最大值 <1e8 且 HH 位为 9–15，或 leaf 为 stocktick/stockorder/stocktrade）。index 的 SSE 行 `update_time` 只有 `HHMMSS` 秒级。
7. **时间戳口径**：`ReceiveTime` 是本地落地时间（UTC epoch µs，转上海时区 +8h），`int_time/data_time` 是交易所时间（naive 本地）；两者相差数毫秒到数百毫秒，DZ 补包（`main.c:123-156` 从 query shm 补回）的记录落地顺序可能晚于后续序号——按 `(channel_no, seq)` 排序而非落地顺序。`SSE/DZ/cbtick/tick_quote_20260429.dat` 整天在 09:44:02–09:44:04 落地，是一次回放且只到 09:44。
8. **`data_type=3` 占位记录**（仅 SSE）：ticker 空、int_time=0、union 全 0，集中在 09:14–09:25 与 15:00–15:35，成片出现（cbtick 头部 4.4 万条），但占 seq；读时过滤（`raw_struct_utc.py:130-131` 也这么做）。
9. **`ticker` NUL 后有残留字节**（GJ `b'600356\x00L2'`, `b'002269\x00ZW'`, `b'603259\x00ZC'`；DZ 固定残留；HX 全 NUL），必须按第一个 NUL 截断（`utils/common.py:76 symbol_decode`）；不要 `.strip()`。
10. **`ord_type` 限价代码不统一**：DZ-SSE 用 `'0'`（`quote_my_stock.h:52`、`obconstructor/tick_quote.h:10 LIMIT_ORDER '0'`），GJ/GJ2/HX/DZ-SZSE 用 `'2'`（`dtype.py:19`）。注意 `obconstructor_sh/tick_quote.h:10` 与 `obconstructor_only_order/tick_quote.h:10` 定义 `LIMIT_ORDER '2'`；DZ-SZSE 的 `dz_szse_fill_kernel_all_mkt/main.c:88` 写 `LIMIT_ORDER` 但目录内无该宏，实测落盘为 `'2'`（用的是 '2' 版头文件）——以数据为准。撤单 `price`：SZSE、HX-SSE 为 0，GJ2-SSE、DZ-SSE 为原委托价。
11. **`trade_flag` 只有 SSE-GJ2 有值**（B/S/N），SZSE 各源及 SSE-DZ/HX 为空；SZSE 撤单不走 trade_flag='4'，而是 `data_type=1, ord_type='C'`。
12. **index 文件 128 B 而非 `dz_index_t` 112 B**（多 16 B 垃圾）；文件大小恰好同时被 112 与 128 整除，用 112 读第 2 条起错位。
13. **3s/index 老格式切换点 2023-11/12**（§2.3），2023-12 以后的 3s/index 不能用 `raw_struct_utc.py` 的 QuoteReader/IndexReader。
14. **可转债 / 债券代码**：SSE 可转债 11xxxx（110/111/113/118…），债券 1xxxxx/2xxxxx，回购 204001；SZSE 可转债 12xxxx，回购 131810；SSE 债券行情在单独 `cbtick`（GJ/GJ2/DZ）或与股票混在 `tick`（HX）。SSE 可转债价格三位小数（tick_size 0.01 vs SZSE 0.001，`dtype.py:1007`）。3s 里债券开盘前 `last_price=0`、`pre_close=100`。
15. **归档伪装**：约 1/3 `.tar.bz2` 实为 xz（§2.1）；`SZSE/GJ/3s` 与 `SZSE/GJ/tick` 在 20230601–20230921 前缀互换；`SSE/GJ2/tick` 与 `SSE/GJ2/cbtick` 同名归档内容不同；`SSE/DZ/3s` 混入 1 个 `dz2szse_3s_*_20230508`；34 个 rsync 残片 `.<name>.tar.bz2.XXXXXX`。
16. **残缺日**：`SSE/DZ/stocktick/tick_quote_20250916.dat` 0 字节；`SZSE/DZ/tick/tick_quote_20260427.dat` 仅 2.6 G（正常 35–45 G）；`SSE/DZ/3s/20260429` 到 14:31；`SSE/DZ/cbtick/20260429` 到 09:44；`SSE/DZ/stockorder|stocktrade` 20240701–20241011 全空；`SZSE/GJ/tick` 20230718–20231107 全空。使用前先 `peek` 看 `recv_last`。
17. **文件大小与 IO**：单文件 6–45 GB，`np.fromfile` 必须带 `count=`（≤200k）并 `seek`；整文件顺序扫描 25 GB 约数分钟；`np.memmap` 可随机访问但不要对其做整体 fancy-index。
18. **cbscript 的其它注释坑**：`dat2parquet/hxszse.py:62-73` 把 shm key 20220103/20220106 交给了错的读取器（与 `raw_struct.py:649-653` 矛盾）；`dump_dz.py:42-48` 有 SSE/SZSE 互换补丁（早期 dump 目录文件混放）；`DZSSEQuoteReader.fix_time`（`raw_struct_utc.py:375-401`）"sse 20221109 quote time error" 默认关闭；`DZIndexReader` "fix 20220701" 硬编码时间窗（L433-434）。
