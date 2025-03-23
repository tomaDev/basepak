from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional

import psutil

from . import log, time


class Tracker:
    """Singleton class to track Task status and notes"""
    _instance = None
    _tasks: Dict[str, Dict[str, Dict[str, str]]] = dict()
    FAILURE_STATUSES = ['failed', 'timeout', 'unknown', 'aborted']
    SUCCESS_STATUSES = ['succeeded', 'completed', 'skipped']

    def __new__(cls):  # Singleton
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def upsert(cls, task: str, phase: str, status: str, description: Optional[str] = '') -> None:
        if not cls._tasks.get(task):
            cls._tasks[task] = dict()
        cls._tasks[task][phase] = {
            'status': status,
            'description': description,
        }

    @classmethod
    def get_task_last_failed_phase(cls, task: str) -> str:
        """Get the last failed phase of a task
        :param task: the task name
        :return: phase name if found, else empty string
        """
        if not cls._tasks.get(task):
            return ''
        return next((x for x in cls._tasks[task].keys() if cls._tasks[task][x]['status'] in cls.FAILURE_STATUSES), '')

    @classmethod
    def get(cls, task: Optional[str] = None, phase: Optional[str] = None) -> dict:
        """Get all tasks, a specific task, all phases of a task, or a specific phase of a task
        :param task: the task name
        :param phase: phase name
        :return: dict of tasks, a specific task, all phases of a task, or a specific phase of a task
        """
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
        """Get a summary of all tasks statuses
        :return: {'failed': [{task: status}, ...], 'succeeded': [{task: status}, ...]}
        """
        all_statuses = cls.get()
        return {
            'failed': [{k: v} for k, v in all_statuses.items() if Tracker.is_failed(k)],
            'succeeded': [{k: v} for k, v in all_statuses.items() if Tracker.is_succeeded(k)],
        }

    @classmethod
    def task_summary(cls, task: str) -> Dict[str, str]:
        """Get the statuses and notes of a task
        :param task: the task name
        :return: {'status': status, 'notes': notes}
        """
        if not cls._tasks.get(task):
            return dict()
        return {
            'status': next((x['status'] for x in cls._tasks[task].values()
                            if x.get('status') not in cls.SUCCESS_STATUSES), 'succeeded'),
            'notes': ', '.join([x['description'] for x in cls._tasks[task].values() if x.get('description')]),
        }

    @classmethod
    def is_task_failed(cls, task: str) -> bool:
        """Check if a task has failed
        :param task: task name
        :return: True if task failed, False otherwise
        """
        return cls.task_summary(task).get('status', 'unknown') in cls.FAILURE_STATUSES

    @classmethod
    def is_failed(cls, *tasks: Optional[str]) -> bool:
        """Check if a task or all tasks have failed
        :param tasks: task names to check for summary status, if None - check all tasks
        :return: True if task failed, False otherwise
        """
        if not tasks:
            tasks = cls._tasks.keys()
        return any([cls.is_task_failed(x) for x in tasks])

    @classmethod
    def failed_tasks(cls, *tasks: Optional[str]) -> List[str]:
        """Get a list of tasks that failed
        :param tasks: task names to check for summary status. Defaults to all tasks
        :return: List of tasks that failed
        """
        if not tasks:
            tasks = cls._tasks.keys()
        return [x for x in tasks if cls.is_task_failed(x)]

    @classmethod
    def is_succeeded(cls, *tasks: Optional[str]) -> bool:
        """Check if a task or all tasks have succeeded
        :param tasks: task names to check for summary status, if None - check all tasks
        :return: True if task succeeded, False otherwise
        """
        return not cls.is_failed(*tasks)


def validate_os_thresholds(thresholds: dict[str, Optional[float]], logger: logging.Logger, mode: str, iterations: Optional[int] = 60) -> None:
    """Validate OS thresholds
    :param thresholds: the thresholds to validate
    :param logger: logger instance
    :param mode: execution mode
    :param iterations: number of 1s interval iterations to recheck if threshold is exceeded
    :raises AssertionError: if threshold is exceeded
    """
    if not thresholds:
        logger.warning('No thresholds provided - skipping')
        return
    _await_stat(thresholds.get('MEMORY_PERCENT'), iterations=iterations, stat=_get_virtual_memory, name='memory', logger=logger, mode=mode)
    _await_stat(thresholds.get('CPU_PERCENT'), iterations=iterations, stat=_get_load_avg, name='load avg', logger=logger, mode=mode)


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
    return psutil.virtual_memory()._asdict()['percent']  # w0212
