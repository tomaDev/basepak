from __future__ import annotations

import datetime
import logging
import subprocess
import sys
from abc import ABC, abstractmethod
from functools import partial
from typing import Callable, Optional


class Eventer(ABC):
    """Generic events class"""
    def __init__(self, url: str):
        self.url = url

        self.send_started = partial(self.send_event, status='started', severity='info')
        self.send_succeeded = partial(self.send_event, status='succeeded', severity='info')
        self.send_failed = partial(self.send_event, status='failed', severity='major')
        self.send_timeout = partial(self.send_event, status='timeout', severity='major')

    @abstractmethod
    def send_event(self, task: str, phase: str, status: str, description: str, severity: str):
        ...


class Task(ABC):
    """Generic task class"""
    import requests

    def __init__(
            self,
            name: str,
            session: requests.Session,
            eventer: Eventer,
            logger: logging.getLogger = None,
            spec: dict = None,
            exec_mode: str = None,
    ):
        self.name = name
        self.session = session
        self.eventer = eventer
        self.logger = logger or logging.getLogger()
        self.spec = (spec or dict()).copy()  # shallow copy for performance. Careful with mutable values!
        self.phases = tuple(self.spec.get('PHASES') or ('require', 'setup', 'execute', 'validate'))
        self.exec_mode = exec_mode or self.spec.get('MODE') or 'dry-run'
        self._phase = 'created'
        self.status = 'succeeded'

    def __call__(self, *args, **kwargs) -> any:
        return self.__init__(*args, **kwargs)

    def post_status(self, status: Optional[str] = None, *args, **kwargs) -> None:
        """Save task status to tracker and post to eventer
        :param status: task status
        """
        self.status = status or self.status
        try:
            event = getattr(self.eventer, f'send_{self.status}')
        except AttributeError:
            raise AttributeError(f'Eventer has no method send_{self.status}')
        from .stats import Tracker
        tracker = Tracker()
        tracker.upsert(task=self.name, phase=self._phase, status=self.status, **kwargs)
        event(self.name, self.get_phase, *args, **kwargs)

    @classmethod
    def monitor_raise_on_fail(cls, print_prefix: Optional[str] = '') -> Callable:
        """Convenience decorator to manage task phasing and status reporting, and raise on failure
        :param print_prefix: prefix for print messages
        :return: decorator
        """
        return cls.monitor(print_prefix=print_prefix, raise_on_fail=True)

    @classmethod
    def monitor(cls, print_prefix: Optional[str] = '', raise_on_fail: Optional[bool] = False) -> Callable:
        """Decorator to manage task phasing and status reporting

        Caller must include these attributes in the decorated class:
        - set_phase
        - post_status
        - status
        - logger

        :param print_prefix: prefix for print messages
        :param raise_on_fail: toggle whether raise on failure
        :return: decorator
        """
        def decorator(func):
            def wrapper(self, *args, **kwargs):
                import click
                import requests

                from .stats import Tracker
                self.set_phase(func.__name__)
                self.post_status('started')
                phase = self.get_phase
                logger = self.logger
                banner_pattern = f'### {print_prefix} {self.name} {phase}' + ' {} ###'
                status = 'failed'
                notes = ''
                logger.info(banner_pattern.format(''))
                result = None

                if Tracker.is_task_failed(self.name):
                    status = 'aborted'
                    last_failed_phase = Tracker.get_task_last_failed_phase(self.name)
                    logger.warning(f'{self.name} {status=} {last_failed_phase=}')
                else:
                    start_time = datetime.datetime.now()
                    try:
                        result = func(self, *args, **kwargs)
                        notes = str(datetime.datetime.now() - start_time)
                        status = 'succeeded'
                    except KeyboardInterrupt:
                        logger.warning('KeyboardInterrupt')
                        sys.exit(1)
                    except click.exceptions.Abort:
                        raise click.Abort()
                    except (TimeoutError, requests.exceptions.Timeout) as e:
                        status = 'timeout'
                        notes = f'{phase} Timeout: {e}'
                        logger.error(notes)
                    except subprocess.CalledProcessError as e:
                        notes = f'{phase} {e.stderr}'
                    except (FileNotFoundError, AssertionError, StopIteration, NameError, RuntimeError, ValueError) as e:
                        notes = f'{phase} {type(e).__name__}: {e}'
                        logger.error(notes)
                    except (IndexError, KeyError) as e:
                        notes = f'{phase} {type(e).__name__}: {e}'
                        logger.exception(e)
                    except Exception as e:
                        if not notes:
                            notes = f'{phase} Exception: {e}'
                            logger.error(notes)
                self.post_status(status, description=str(notes))
                logger.info(banner_pattern.format(self.status))
                if raise_on_fail and status not in Tracker.SUCCESS_STATUSES:
                    logger.error(notes)
                    raise click.Abort(notes)
                return result
            return wrapper
        return decorator

    def require(self, *args, **kwargs) -> any:
        return

    def setup(self,  *args, **kwargs) -> any:
        return

    @abstractmethod
    def execute(self,  *args, **kwargs) -> any:
        ...

    def validate(self, *args, **kwargs) -> any:
        return

    @property
    def get_phase(self) -> str:
        """Get the task phase"""
        return self._phase

    def set_phase(self, phase: str) -> None:
        """Set the task phase
        :param phase: the phase name
        """
        self._phase = phase
        self.status = 'pending'

    def run(self, *args, **kwargs):
        """Run the task phases"""
        self.logger.debug(f'Plan {self.name} started run')
        for phase in self.phases:
            try:
                runnable = getattr(self, phase)
            except AttributeError:
                self.logger.error(f'{self.name} has no phase {phase}\nSkipping...')
                continue
            runnable(*args, **kwargs)
        self.logger.debug(f'Plan {self.name} completed with status {self.status}')


class Plan(Task):
    """Generic plan class for managing tasks"""
    from collections.abc import Sequence
    from enum import Enum
    from typing import Dict, List, Optional

    def __init__(self, name: str, session, eventer: Eventer, logger: logging.Logger, spec: dict,
                 tasks: Optional[List[str | Enum]] = None, task_map: Optional[Dict[str, Task]] = None):
        super().__init__('plan_' + name, session, eventer, logger, spec, exec_mode='normal')
        self.task_map = task_map or dict()
        self.tasks = list()
        self.add_tasks(tasks, session, eventer, logger, spec)
        self.post_status()

    def add_tasks(self, plan: Sequence[str], *args, **kwargs) -> None:
        """Add tasks to the plan
        :param plan: sequence of task names
        """
        if not plan:
            self.logger.warning('No tasks to add')
            return
        for task in plan:
            if task not in self.task_map:
                self.logger.warning(f'Task {task} not supported - skipping')
                continue
            self.tasks.append(self.task_map[task](task, *args, **kwargs))

    def require(self, *args, **kwargs) -> Dict[str, List[str]]:
        """Run the Require phase for all tasks
        :return: results
        """
        return self._iter_tasks('require', *args, **kwargs)

    def setup(self, *args, **kwargs) -> Dict[str, List[str]]:
        """Run the setup phase for all tasks
        :return: results
        """
        return self._iter_tasks('setup', *args, **kwargs)

    def execute(self, *args, **kwargs) -> Dict[str, List[str]]:
        """Run the execute phase for all tasks
        :return: results
        """
        return self._iter_tasks('execute', *args, **kwargs)

    def validate(self, *args, **kwargs) -> Dict[str, List[str]]:
        """Run the validate phase for all tasks
        :return: results
        """
        return self._iter_tasks('validate', *args, **kwargs)

    def _iter_tasks(self, phase_name, *args, **kwargs) -> Dict[str, List[str]]:
        """Run the phase for each task
        :param phase_name: phase name
        :return: results"""
        def phase_func(self, *args, **kwargs):
            self.logger.debug(f'{phase_name=}\ntasks={[task.name for task in self.tasks]}')
            if self.exec_mode == 'dry-run':
                return
            results = dict()
            for task in self.tasks:
                task_func = getattr(task, phase_name)
                task_func(*args, **kwargs)
                if not results.get(task.status):
                    results[task.status] = list()
                results[task.status].append(task.name)
            return results

        return phase_func(self, *args, **kwargs)
