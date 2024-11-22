# test_tasks.py

import logging
from unittest.mock import MagicMock

import pytest

from basepak import platform_api
from basepak.tasks import Plan, Task


@pytest.fixture
def mock_session():
    # Mock requests.Session
    return MagicMock(name='session')

@pytest.fixture
def mock_eventer():
    return platform_api.DummyPlatformEvents('dummy', 'dummy', 'dummy', 'ua', 'TestEventer')

@pytest.fixture
def mock_logger():
    logger = logging.getLogger('test_logger')
    logger.setLevel(logging.DEBUG)
    return logger

@pytest.fixture
def dummy_task_class():
    class DummyTask(Task):
        def execute(self, *args, **kwargs):
            self.logger.info(f'Executing {self.name}')

        def require(self, *args, **kwargs):
            self.logger.info(f'Requiring {self.name}')

        def setup(self, *args, **kwargs):
            self.logger.info(f'Setting up {self.name}')

        def validate(self, *args, **kwargs):
            self.logger.info(f'Validating {self.name}')
    return DummyTask

@pytest.fixture
def task_map(dummy_task_class, mock_session, mock_eventer, mock_logger):
    return {
        'task1': dummy_task_class,
        'task2': dummy_task_class,
        'task3': dummy_task_class,
    }


def test_plan_initialization(mock_session, mock_eventer, mock_logger, task_map):
    # Initialize a Plan with a list of tasks
    spec = {'PHASES': ['require', 'setup', 'execute', 'validate']}
    tasks = ['task1', 'task2']
    plan = Plan(name='test_plan', session=mock_session, eventer=mock_eventer,
                logger=mock_logger, spec=spec, tasks=tasks, task_map=task_map)

    assert plan.name == 'plan_test_plan'
    assert len(plan.tasks) == 2
    assert plan.tasks[0].name == 'task1'
    assert plan.tasks[1].name == 'task2'

def test_plan_add_tasks(mock_session, mock_eventer, mock_logger, task_map):
    spec = {}
    plan = Plan(name='test_plan', session=mock_session, eventer=mock_eventer,
                logger=mock_logger, spec=spec, tasks=[], task_map=task_map)

    plan.add_tasks(['task1', 'task3'], session=mock_session, eventer=mock_eventer)
    assert len(plan.tasks) == 2
    assert plan.tasks[0].name == 'task1'
    assert plan.tasks[1].name == 'task3'

def test_plan_add_unsupported_task(caplog, mock_session, mock_eventer, mock_logger, task_map):
    spec = {}
    plan = Plan(name='test_plan', session=mock_session, eventer=mock_eventer,
                logger=mock_logger, spec=spec, tasks=[], task_map=task_map)

    with caplog.at_level(logging.WARNING):
        plan.add_tasks(['task1', 'unsupported_task'], session=mock_session, eventer=mock_eventer)
        assert 'Task unsupported_task not supported - skipping' in caplog.text
    assert len(plan.tasks) == 1
    assert plan.tasks[0].name == 'task1'

def test_plan_run_phases(mock_session, mock_eventer, mock_logger, task_map, caplog):
    spec = {'PHASES': ['require', 'setup', 'execute', 'validate']}
    tasks = ['task1', 'task2']
    plan = Plan(name='test_plan', session=mock_session, eventer=mock_eventer,
                logger=mock_logger, spec=spec, tasks=tasks, task_map=task_map)

    with caplog.at_level(logging.INFO):
        plan.run()
        # Check that each phase was called for each task
        expected_messages = [
            'Requiring task1',
            'Requiring task2',
            'Setting up task1',
            'Setting up task2',
            'Executing task1',
            'Executing task2',
            'Validating task1',
            'Validating task2',
        ]
        for message in expected_messages:
            assert message in caplog.text

def test_plan_execute_phase(mock_session, mock_eventer, mock_logger, task_map, caplog):
    # Test executing the 'execute' phase
    spec = {'PHASES': ['execute']}
    tasks = ['task1']
    plan = Plan(name='test_plan', session=mock_session, eventer=mock_eventer,
                logger=mock_logger, spec=spec, tasks=tasks, task_map=task_map)

    with caplog.at_level(logging.INFO):
        plan.execute()
        assert 'Executing task1' in caplog.text

def test_plan_missing_phase(mock_session, mock_eventer, mock_logger, task_map, caplog):
    # Test handling a missing phase in a task
    spec = {'PHASES': ['nonexistent_phase']}
    tasks = ['task1']
    plan = Plan(name='test_plan', session=mock_session, eventer=mock_eventer,
                logger=mock_logger, spec=spec, tasks=tasks, task_map=task_map)

    with caplog.at_level(logging.ERROR):
        plan.run()
        assert 'plan_test_plan has no phase nonexistent_phase\nSkipping...' in caplog.text

def test_plan_empty_tasks(mock_session, mock_eventer, mock_logger):
    # Test initializing a Plan with no tasks
    spec = {}
    plan = Plan(name='test_plan', session=mock_session, eventer=mock_eventer,
                logger=mock_logger, spec=spec, tasks=[], task_map={})

    assert plan.tasks == []
    plan.run()  # Should run without errors even if there are no tasks

def test_plan_status_tracking(mock_session, mock_eventer, mock_logger, task_map):
    # Test that the Plan updates status correctly
    spec = {}
    tasks = ['task1', 'task2']
    plan = Plan(name='test_plan', session=mock_session, eventer=mock_eventer,
                logger=mock_logger, spec=spec, tasks=tasks, task_map=task_map)

    # Mock the tasks to have different statuses
    plan.tasks[0].status = 'succeeded'
    plan.tasks[1].status = 'failed'

    results = plan.execute()
    assert results == {'succeeded': ['task1'], 'failed': ['task2']}
