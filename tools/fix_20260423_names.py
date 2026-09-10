#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
fix_20260423_names.py — 修复 H:\index\CNIndex\20260423 的坏文件名

现状: 文件名形如 sh('000001',).parquet (导出脚本把 Python repr 泄漏进文件名)
目标: 改成 sh000001.parquet / sz399001.parquet (与其它日期目录命名一致)

规则: 前 2 位 sh/sz 保留 + 单引号内的 6 位代码 + .parquet
改名前检查: 目标名若已存在则跳过该文件 (正常不会发生, 全天 554 个全是坏名)
先统计打印, 确认无误后逐个 os.rename, 最后报告结果。
幂等: 无坏名时 0 改动直接退出。

用法:  python fix_20260423_names.py
"""
import os

DAY_DIR = r'H:\index\CNIndex\20260423'


def fix_name(fname: str) -> str:
    """sh('000001',).parquet -> sh000001.parquet"""
    prefix = fname[:2]                                  # 'sh' / 'sz'
    code = fname.split("'")[1]                          # 单引号内的 6 位代码
    return f'{prefix}{code}.parquet'


if __name__ == '__main__':
    files = sorted(f for f in os.listdir(DAY_DIR) if f.endswith('.parquet'))
    bad = [f for f in files if "(" in f]
    good = [f for f in files if "(" not in f]
    print(f'共 {len(files)} 个 parquet: 坏名 {len(bad)}, 正常名 {len(good)}')

    # 无坏名则直接结束 (幂等)
    if not bad:
        print('无坏名, 无需处理')
        raise SystemExit(0)

    # 预览改名映射 (前 5 个)
    pairs = [(f, fix_name(f)) for f in bad]
    for old, new in pairs[:5]:
        print(f'  {old!r} -> {new}')
    if len(pairs) > 5:
        print(f'  ... 其余 {len(pairs) - 5} 个同理')

    # 目标名冲突检查
    conflict = [new for _, new in pairs if os.path.exists(os.path.join(DAY_DIR, new))]
    if conflict:
        print(f'!! 有 {len(conflict)} 个目标名已存在, 中止不改动: {conflict[:5]}')
        raise SystemExit(1)

    # 逐个改名
    n = 0
    for old, new in pairs:
        os.rename(os.path.join(DAY_DIR, old), os.path.join(DAY_DIR, new))
        n += 1
    print(f'完成: {n} 个文件已改名')
    # 复查
    remain = [f for f in os.listdir(DAY_DIR) if "(" in f]
    print(f'复查: 剩余坏名 {len(remain)} 个', '(应为 0)')
