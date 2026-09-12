#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
copy_szse_after_done.py — SZSE 流水线完成监视 + 产物拷贝 (D 盘 -> H 盘)

等 SZSE 解析流水线 (run_read_cnstock.py, 4 worker) 跑完后, 把 D:\processed\SZSE 下的
全部 feather/csv 拷贝到 H:\processed\SZSE\。

启动后的行为:
  1. 夜里启动先进入等待心跳: 每 IDLE_POLL_MIN 分钟 (默认 5) 打印一条 waiting (保持进程活跃,
     避免被操作系统挂起/回收), 直到 CHECK_AFTER 点 (默认 05:00); 若启动时已过该点立即开始检查;
  2. 之后每 POLL_MIN 分钟 (默认 30) 检查一次完成口径:
     succsse.txt 账本中 SZSE 条数 == H:\raw\SZSE 归档数 (与流水线的跳过口径同源);
  3. 判定完成 -> 执行拷贝: D:\processed\SZSE\* -> H:\processed\SZSE\
     · 同名且同大小 -> 跳过 (断点续拷, 中断重跑不重复搬);
     · 同名不同大小 -> 覆盖 (H 盘此前同盘试跑留下的 8 个残片 feather 会被完整文件覆盖掉);
  4. 拷完核验: D/H 两侧文件数与总字节数一致才报成功, 否则报缺失清单。

超时保护: 若到 MAX_WAIT_UNTIL 点 (默认当天 17:00) 账本仍未填满 (流水线中断/有天失败),
放弃退出不拷贝, 等人工处置。运行日志追加写在 LOG_PATH。

用量预估: 152 天 feather (~2GB/天) + 152 个 csv, 约 300GB; 瓶颈在 H 盘 HDD 写 (~100-150MB/s),
预计 1 小时上下。用法: 后台运行 python run_exp/copy_szse_after_done.py (无参数, 配置见下方常量)。
"""
import os
import shutil
import time
from datetime import datetime

SUCC_TXT = r'H:\raw\succsse.txt'      # 流水线成功账本 (完成口径)
RAW_DIR = r'H:\raw\SZSE'              # 源归档目录 (数 .tar.bz2 个数 = 应完成天数)
SRC_DIR = r'D:\processed\SZSE'        # 拷贝源 (流水线产物, SSD)
DST_DIR = r'H:\processed\SZSE'        # 拷贝目标 (HDD)
LOG_PATH = r'H:\raw\copy_szse.log'    # 运行日志 (追加, 与账本同目录便于回看)
CHECK_AFTER = 5                       # 从几点起开始检查 (本地时钟, 小时)
IDLE_POLL_MIN = 5                     # 等待期心跳间隔 (分钟, 只打 waiting 不做事)
POLL_MIN = 30                         # 完成检查间隔 (分钟)
MAX_WAIT_UNTIL = 17                   # 最晚等到几点, 超时放弃 (当天本地时钟, 小时)


def log(msg: str) -> None:
    """追加写日志 + 同步到 stdout。"""
    line = f'{time.strftime("%Y-%m-%d %H:%M:%S")}  {msg}'
    print(line, flush=True)
    with open(LOG_PATH, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


def szse_progress() -> tuple:
    r"""完成口径 (与流水线跳过口径同源): 账本 SZSE 条数 vs H:\raw\SZSE 归档数。"""
    total = len([fn for fn in os.listdir(RAW_DIR) if fn.endswith('.tar.bz2')])
    done = 0
    if os.path.isfile(SUCC_TXT):
        with open(SUCC_TXT, encoding='utf-8') as f:
            for line in f:
                parts = line.rstrip('\n').split('\t')
                if len(parts) >= 3 and parts[1] == 'SZSE':
                    done += 1
    return done, total


def wait_until_done() -> bool:
    r"""等待期每 IDLE_POLL_MIN 分钟打一条 waiting 心跳到 CHECK_AFTER 点 (保活),
    然后每 POLL_MIN 分钟查一次账本, 满了返回 True, 超时返回 False。"""
    now = datetime.now()
    start = now.replace(hour=CHECK_AFTER, minute=0, second=0, microsecond=0)
    if now < start:                                   # 夜里启动: 心跳等待到检查时点
        log(f'等待 {CHECK_AFTER}:00 开始检查 (当前 {now:%H:%M}, 心跳每 {IDLE_POLL_MIN} 分钟, '
            f'之后每 {POLL_MIN} 分钟查一次账本)')
        while datetime.now() < start:
            log('waiting ...')
            time.sleep(min(IDLE_POLL_MIN * 60, (start - datetime.now()).total_seconds()))
    deadline = start.replace(hour=MAX_WAIT_UNTIL)     # 当天最晚时点
    while True:
        done, total = szse_progress()
        if done >= total:
            log(f'流水线已完成: 账本 SZSE {done}/{total}')
            return True
        if datetime.now() >= deadline:
            log(f'!! 到 {MAX_WAIT_UNTIL}:00 仍未完成 ({done}/{total}), 放弃退出, 等人工处置')
            return False
        log(f'未完成 {done}/{total}, {POLL_MIN} 分钟后再查')
        time.sleep(POLL_MIN * 60)


def copy_all() -> None:
    r"""D:\processed\SZSE -> H:\processed\SZSE 全量拷贝 (同名同大小跳过 = 断点续拷)。"""
    files = sorted(fn for fn in os.listdir(SRC_DIR) if os.path.isfile(os.path.join(SRC_DIR, fn)))
    os.makedirs(DST_DIR, exist_ok=True)
    n_copy = n_skip = 0
    t0 = time.perf_counter()
    for i, fn in enumerate(files, 1):
        src = os.path.join(SRC_DIR, fn)
        dst = os.path.join(DST_DIR, fn)
        if os.path.isfile(dst) and os.path.getsize(dst) == os.path.getsize(src):
            n_skip += 1                               # 上次拷过的, 不重复搬
            continue
        shutil.copy2(src, dst)                        # 拷贝 + 保留时间戳
        n_copy += 1
        log(f'[{i}/{len(files)}] {fn} ({os.path.getsize(src) / 1e9:.2f} GB)')
    log(f'拷贝结束: {n_copy} 个新拷 / {n_skip} 个跳过, '
        f'耗时 {(time.perf_counter() - t0) / 60:.0f} 分钟')

    # 核验: 两侧文件数与总字节数一致
    def _stat(d):
        fs = [os.path.join(d, fn) for fn in os.listdir(d) if os.path.isfile(os.path.join(d, fn))]
        return len(fs), sum(os.path.getsize(p) for p in fs)
    n_src, b_src = _stat(SRC_DIR)
    n_dst, b_dst = _stat(DST_DIR)
    if (n_src, b_src) == (n_dst, b_dst):
        log(f'核验通过: D/H 两侧均 {n_dst} 个文件, 共 {b_dst / 1e9:.1f} GB')
    else:
        missing = set(os.listdir(SRC_DIR)) - set(os.listdir(DST_DIR))
        log(f'!! 核验不一致: D {n_src} 个/{b_src / 1e9:.1f} GB vs H {n_dst} 个/{b_dst / 1e9:.1f} GB; '
            f'缺失: {sorted(missing) if missing else "无 (可能为大小不匹配)"}')


def main() -> None:
    log(f'=== 启动: 监视 SZSE 流水线完成, 之后拷贝 {SRC_DIR} -> {DST_DIR} ===')
    if not wait_until_done():
        return                                        # 超时/中断: 不拷贝
    if not os.path.isdir(SRC_DIR):
        log(f'!! 源目录不存在: {SRC_DIR}, 退出')
        return
    copy_all()
    log('=== 全部完成, 退出 ===')


if __name__ == '__main__':
    main()
