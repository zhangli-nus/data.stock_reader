#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
raw_data_flatten_dates.py — 把 data/<年份>/<日期> 扁平化为 data/<YYYYMMDD>

目标结构:
    data/2022/2022.1.10/...  ->  data/20220110/...
    data/2022/2022.1.4/...   ->  data/20220104/...
    data/2023/2023.5.1/...   ->  data/20230501/...

规则:
    1) 去掉“年份”这一层目录(扁平化)。
    2) 日期目录从 YYYY.M.D(月/日不补零) 重命名为 YYYYMMDD(8位补零)。

默认只做 dry-run(打印计划, 不改动任何文件)。
加 --apply 才真正执行。

用法:
    python raw_data_flatten_dates.py          # 只预览
    python raw_data_flatten_dates.py --apply  # 真正执行
"""

import os
import sys
import shutil

BASE = os.path.dirname(os.path.abspath(__file__))


def parse_date(name):
    """把 '2022.1.10' 解析为 '20220110'；非法返回 None。"""
    parts = name.split(".")
    if len(parts) != 3:
        return None
    if not all(p.isdigit() for p in parts):
        return None
    y, m, d = parts
    return f"{int(y):04d}{int(m):02d}{int(d):02d}"


def main():
    apply = "--apply" in sys.argv
    mode = "【真正执行】" if apply else "【预览-DRY RUN, 不改动文件】"
    print(f"{mode}")
    print(f"根目录: {BASE}\n")

    if not apply:
        print("（这只是预览。确认无误后加 --apply 再跑一次）\n")

    years = sorted(
        e.name for e in os.scandir(BASE)
        if e.is_dir() and e.name.isdigit()
    )

    moved = 0
    skipped = 0
    errors = []

    for y in years:
        year_path = os.path.join(BASE, y)
        date_dirs = [
            e.name for e in os.scandir(year_path)
            if e.is_dir()
        ]
        for dn in sorted(date_dirs):
            src = os.path.join(year_path, dn)
            new_name = parse_date(dn)
            if new_name is None:
                print(f"  [跳过-非日期目录] {y}/{dn}")
                skipped += 1
                continue
            dst = os.path.join(BASE, new_name)

            if os.path.abspath(src) == os.path.abspath(dst):
                # 已经到位, 无需处理
                continue

            if os.path.exists(dst):
                # 目标已存在: 把源内容合并进目标, 避免覆盖丢失
                print(f"  [合并] {y}/{dn} -> {new_name} (目标已存在, 合并内容)")
                if apply:
                    try:
                        for item in os.scandir(src):
                            shutil.move(item.path, os.path.join(dst, item.name))
                        os.rmdir(src)
                    except Exception as e:
                        errors.append((src, str(e)))
            else:
                print(f"  [移动] {y}/{dn} -> {new_name}")
                if apply:
                    try:
                        shutil.move(src, dst)
                    except Exception as e:
                        errors.append((src, str(e)))
            moved += 1

    # 清理已空的年份目录
    print("\n--- 清理空的年份目录 ---")
    for y in years:
        year_path = os.path.join(BASE, y)
        if not os.path.exists(year_path):
            continue
        try:
            remaining = list(os.scandir(year_path))
        except Exception:
            remaining = []
        if len(remaining) == 0:
            print(f"  [删除空目录] {y}/")
            if apply:
                try:
                    os.rmdir(year_path)
                except Exception as e:
                    errors.append((year_path, str(e)))
        else:
            print(f"  [保留-仍非空] {y}/  (剩余 {len(remaining)} 项, 请人工检查)")

    print("\n========== 汇总 ==========")
    print(f"处理日期目录: {moved}")
    print(f"跳过:         {skipped}")
    print(f"错误:         {len(errors)}")
    for path, err in errors:
        print(f"  ! {path}: {err}")
    if not apply:
        print("\n>>> 这是预览, 未改动任何文件。确认后运行: python raw_data_flatten_dates.py --apply")


if __name__ == "__main__":
    main()
