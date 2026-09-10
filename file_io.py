# -*- coding: utf-8 -*-
"""
文件与路径相关的小工具。

这些函数只做轻量封装，方便项目里统一调用。删除类函数仍然会真实删除
文件或目录，调用前需要由上层代码确认目标路径是安全的。
"""
from __future__ import annotations

import os
import shutil


def create_directory(file_dir: str, last_as_directory: bool = False) -> None:
    """
    创建目录。

    file_dir:
    文件路径或目录路径。

    last_as_directory:
    True 时把 file_dir 整体视为目录；
    False 时把最后一级视为文件名，只创建它的父目录。
    """
    if last_as_directory:
        dir_path = file_dir
    else:
        dir_path = os.path.dirname(file_dir)

    # 如果 dir_path 非空且不存在，则创建
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)


def remove_directory(file_dir: str) -> None:
    """
    如果目录存在，则递归删除整个目录树。

    注意：这是实际删除操作，不会移动到回收站。
    """
    if os.path.exists(file_dir):
        shutil.rmtree(file_dir)


def join_path(*path_list: str) -> str:
    """
    安全地拼接多个路径片段，自动处理不同系统的路径分隔符。
    """
    return str(os.path.join(*path_list))


def exist_file(file_path: str) -> bool:
    """
    检查文件或目录是否存在。
    """
    return os.path.exists(file_path)


def remove_file(file_path: str) -> None:
    """
    如果文件存在，则删除文件。

    注意：这是实际删除操作，不会移动到回收站。
    """
    if os.path.exists(file_path):
        os.remove(file_path)

def get_basename(file_path: str) -> str:
    return os.path.basename(file_path)

if __name__ == "__main__":
    # 测试路径拼接
    a = "/home/zhangli"
    b = "1"
    c = "2"
    p = join_path(a, b, c)
    print("拼接路径:", p, flush=True)
    print("追加后缀:", p + "/.", flush=True)

    # 测试创建目录（将 'c' 视为文件名，创建 './a/b/'）
    create_directory("./a/b/c", last_as_directory=False)

    # 测试创建目录（将 './a/b/c' 整体视为目录）
    create_directory("./a/b/c", last_as_directory=True)
    remove_directory("./b")
    remove_directory("./a")
    #
    basename = get_basename(a)
    print(basename)
