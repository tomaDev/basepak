from __future__ import annotations

from .abstract_classes import Task, Eventer


class Plan(Task):
    import logging

    from enum import Enum
    from typing import List, Dict, Sequence, Optional

    def __init__(self, name: str, session, eventer: Eventer, logger: logging.Logger, spec: dict,
                 tasks: Optional[List[str | Enum]] = None, task_map: Optional[Dict[str, Task]] = None):
        super().__init__('plan_' + name, session, eventer, logger, spec, exec_mode='normal')
        self.task_map = task_map or dict()
        self.tasks = list()
        self.add_tasks(tasks, session, eventer, logger, spec)
        self.post_status()

    def add_tasks(self, plan: Sequence[str], *args, **kwargs):
        if not plan:
            self.logger.warning('No tasks to add')
            return
        for task in plan:
            if task not in self.task_map:
                self.logger.warning(f'Task {task} not supported - skipping')
                continue
            self.tasks.append(self.task_map[task](task, *args, **kwargs))

    def require(self, *args, **kwargs):
        return self._iter_tasks('require', *args, **kwargs)

    def setup(self, *args, **kwargs):
        return self._iter_tasks('setup', *args, **kwargs)

    def execute(self, *args, **kwargs):
        return self._iter_tasks('execute', *args, **kwargs)

    def validate(self, *args, **kwargs) -> dict:
        return self._iter_tasks('validate', *args, **kwargs)

    def _iter_tasks(self, phase_name, *args, **kwargs):
        def phase_func(self, *args, **kwargs):  # noqa
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
