from __future__ import annotations

import io
import logging
import os
from typing import List


def tail(file_path: str | bytes, n: int) -> List[str]:
    """Tails the last n lines of a file"""
    with open(file_path, 'rb') as file:
        file.seek(0, os.SEEK_END)
        buffer = io.BytesIO()
        remaining = n + 1
        while remaining > 0 and file.tell() > 0:
            block_size = min(4096, file.tell())
            file.seek(-block_size, os.SEEK_CUR)
            block = file.read(block_size)
            buffer.write(block)
            file.seek(-block_size, os.SEEK_CUR)
            remaining -= block.count(b'\n')
        buffer.seek(0, os.SEEK_SET)
        return buffer.read().decode(errors='replace').splitlines()


def validate_pattern(path: str | bytes, pattern: str, logger: logging.Logger, num_of_lines: int = 100) -> bool:
    """Tails the last n lines of a file and checks if pattern is in any of them
    :param path: path to file
    :param pattern: pattern to look for
    :param logger: logger
    :param num_of_lines: number of lines to tail
    :return: True if pattern is found, raise AssertionError otherwise"""
    logger.info(f'Tailing last {num_of_lines} lines of {path}')
    lines = tail(path, num_of_lines)
    if not any(pattern in line for line in lines):
        for line in lines:
            logger.warning(line)
        raise StopIteration(f'pattern "{pattern}" not found')
    logger.info(f'pattern "{pattern}" found')
    return True
