from __future__ import annotations

import logging
from typing import Dict, Optional, List, Callable

import psutil

from . import log, time


class Tracker:
    _instance = None
    _tasks: Dict[str, Dict[str, Dict[str, str]]] = dict()
    FAILURE_STATUSES = ['failed', 'timeout', 'unknown', 'aborted']
    SUCCESS_STATUSES = ['succeeded', 'completed', 'skipped']

    def __new__(cls):  # Singleton
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def upsert(cls, task: str, phase: str, status: str, description: Optional[str] = ''):
        if not cls._tasks.get(task):
            cls._tasks[task] = dict()
        cls._tasks[task][phase] = {
            'status': status,
            'description': description,
        }

    @classmethod
    def get_task_last_failed_phase(cls, task: str) -> str:
        if not cls._tasks.get(task):
            return ''
        return next((x for x in cls._tasks[task].keys() if cls._tasks[task][x]['status'] in cls.FAILURE_STATUSES), '')

    @classmethod
    def get(cls, task: Optional[str] = None, phase: Optional[str] = None) -> dict:
        if not task and not phase:
            return cls._tasks
        if task and not phase:
            return cls._tasks.get(task, {})
        if not task and phase:
            return {k: v.get(phase, {}) for k, v in cls._tasks.items()}
        if task and phase:
            return cls._tasks.get(task, {}).get(phase, {})

    @classmethod
    def status_summary(cls) -> dict:
        all_statuses = cls.get()
        return {
            'failed': [{k: v} for k, v in all_statuses.items() if Tracker.is_failed(k)],
            'succeeded': [{k: v} for k, v in all_statuses.items() if Tracker.is_succeeded(k)],
        }

    @classmethod
    def task_summary(cls, task: str) -> Dict[str, str]:
        if not cls._tasks.get(task):
            return dict()
        return {
            'status': next((x['status'] for x in cls._tasks[task].values()
                            if x.get('status') not in cls.SUCCESS_STATUSES), 'succeeded'),
            'notes': ', '.join([x['description'] for x in cls._tasks[task].values() if x.get('description')]),
        }

    @classmethod
    def is_task_failed(cls, task: str) -> bool:
        return cls.task_summary(task).get('status', 'unknown') in cls.FAILURE_STATUSES

    @classmethod
    def is_failed(cls, task: Optional[str] = None) -> bool:
        """
        @param task: task name to check, if None - check all tasks
        @return: True if task failed, False otherwise
        """
        if not task:
            return any([cls.is_task_failed(x) for x in cls._tasks.keys()])
        return cls.is_task_failed(task)

    @classmethod
    def failed_tasks(cls, *tasks: str) -> List[str]:
        """
        @param tasks: list of tasks to check for summary status, if None - check all tasks
        @return: List of tasks that failed
        """
        if not tasks:
            tasks = cls._tasks.keys()
        return [x for x in tasks if cls.is_task_failed(x)]

    @classmethod
    def is_succeeded(cls, task: Optional[str] = None) -> bool:
        return not cls.is_failed(task)


def validate_os_thresholds(thresholds: dict[str, Optional[float]], logger: logging.Logger, mode: str) -> None:
    if not thresholds:
        logger.warning('No thresholds provided - skipping')
        return
    _await_stat(thresholds.get('MEMORY_PERCENT'), stat=_get_virtual_memory, name='memory', logger=logger, mode=mode)
    _await_stat(thresholds.get('CPU_PERCENT'), stat=_get_load_avg, name='load avg', logger=logger, mode=mode)


def _await_stat(threshold: Optional[float] = None, iterations: Optional[int] = 60, stat: Callable = None,
                name: str = None, logger: logging.Logger = None, mode='dry-run') -> None:
    logger = logger or log.get_logger(name='plain')
    if threshold is None:
        logger.warning(f'No {name} threshold provided - skipping')
        return
    if mode != 'normal':
        return
    running_stat = stat()
    logger.debug(f'Initial {name} usage: {running_stat: .2f}%')
    if running_stat < threshold:
        return
    ratio = 3  # aging ratio, to give more weight to the more recent values
    logger.warning(f'Awaiting {name} average usage to undershoot {threshold}%...')
    for i in range(iterations):
        running_stat = (running_stat * (ratio - 1) + stat()) / ratio
        logger.info(f'{i: >2} of {iterations}: {running_stat: .2f}%')
        if running_stat < threshold:
            return
        time.sleep(1)
    raise AssertionError(f'{name} usage threshold: {threshold}%. Current usage: {running_stat: .2f}%')  # noqa w0202


def _get_load_avg() -> float:
    return psutil.getloadavg()[0] / psutil.cpu_count()


def _get_virtual_memory() -> float:
    return psutil.virtual_memory()._asdict()['percent']  # noqa w0212
