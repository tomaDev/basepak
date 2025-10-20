from __future__ import annotations

import logging
import os
from typing import AnyStr, List


def tail(file_path: AnyStr, n: int = 50, block_size: int = 4096, encoding: str = "utf-8") -> List[str]:
    """Return the last n lines of a file efficiently."""
    if n <= 0:
        return []

    with open(file_path, "rb") as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell()
        blocks: list[bytes] = []
        need = n + 1

        while pos > 0 and need > 0:
            read_size = min(block_size, pos)
            pos -= read_size
            f.seek(pos, os.SEEK_SET)
            data = f.read(read_size)
            blocks.append(data)
            need -= data.count(b"\n")

        buf = b"".join(reversed(blocks))
        lines = buf.splitlines()  # handles \n, \r\n, final line w/o newline
        tail_bytes = lines[-n:]
        return [b.decode(encoding, errors="replace") for b in tail_bytes]


def validate_pattern(path: AnyStr, pattern: str, logger: logging.Logger, num_of_lines: int = 100) -> bool:
    """Tails the last n lines of a file and checks if pattern is in any of them
    :param path: path to file
    :param pattern: pattern to look for
    :param logger: logger
    :param num_of_lines: number of lines to tail
    :return: True if pattern is found
    :raises AssertionError: if pattern is not found
    """
    logger.info(f'Tailing last {num_of_lines} lines of {path}')
    lines = tail(path, num_of_lines)
    if not any(pattern in line for line in lines):
        for line in lines:
            logger.warning(line)
        raise StopIteration(f'pattern "{pattern}" not found')
    logger.info(f'pattern "{pattern}" found')
    return True
