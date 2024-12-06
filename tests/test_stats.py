import logging
from unittest.mock import patch

import pytest

from basepak import stats


@pytest.fixture
def tracker_instance():  # Ensure a fresh instance for each test
    stats.Tracker._instance = None
    stats.Tracker._tasks = dict()
    return stats.Tracker()

@pytest.fixture
def mock_logger():
    logger = logging.getLogger('test_logger')
    logger.setLevel(logging.DEBUG)
    return logger


def test_tracker_singleton():
    tracker1 = stats.Tracker()
    tracker2 = stats.Tracker()
    assert tracker1 is tracker2

def test_tracker_upsert_and_get(tracker_instance):
    tracker_instance.upsert('task1', 'phase1', 'succeeded', 'description1')
    tracker_instance.upsert('task1', 'phase2', 'failed', 'description2')

    assert tracker_instance.get('task1') == {
        'phase1': {'status': 'succeeded', 'description': 'description1'},
        'phase2': {'status': 'failed', 'description': 'description2'},
    }

def test_tracker_get_task_last_failed_phase(tracker_instance):
    tracker_instance.upsert('task1', 'phase1', 'succeeded')
    tracker_instance.upsert('task1', 'phase2', 'failed')
    tracker_instance.upsert('task1', 'phase3', 'succeeded')

    failed_phase = tracker_instance.get_task_last_failed_phase('task1')
    assert failed_phase == 'phase2'

def test_tracker_status_summary(tracker_instance):
    tracker_instance.upsert('task1', 'phase1', 'succeeded')
    tracker_instance.upsert('task2', 'phase1', 'failed')
    tracker_instance.upsert('task3', 'phase1', 'aborted')

    summary = tracker_instance.status_summary()
    assert summary == {
        'failed': [{'task2': tracker_instance.get('task2')}, {'task3': tracker_instance.get('task3')}],
        'succeeded': [{'task1': tracker_instance.get('task1')}],
    }

def test_tracker_task_summary(tracker_instance):
    tracker_instance.upsert('task1', 'phase1', 'failed', 'error occurred')
    tracker_instance.upsert('task1', 'phase2', 'succeeded', 'all good')

    summary = tracker_instance.task_summary('task1')
    assert summary == {
        'status': 'failed',
        'notes': 'error occurred, all good',
    }

def test_tracker_is_task_failed(tracker_instance):
    tracker_instance.upsert('task1', 'phase1', 'failed')
    tracker_instance.upsert('task2', 'phase1', 'succeeded')

    assert tracker_instance.is_task_failed('task1') is True
    assert tracker_instance.is_task_failed('task2') is False

def test_tracker_is_failed(tracker_instance):
    tracker_instance.upsert('task1', 'phase1', 'failed')
    tracker_instance.upsert('task2', 'phase1', 'succeeded')

    assert tracker_instance.is_failed('task1') is True
    assert tracker_instance.is_failed('task2') is False
    assert tracker_instance.is_failed() is True  # At least one task failed

def test_tracker_failed_tasks(tracker_instance):
    tracker_instance.upsert('task1', 'phase1', 'failed')
    tracker_instance.upsert('task2', 'phase1', 'succeeded')
    tracker_instance.upsert('task3', 'phase1', 'timeout')

    failed = tracker_instance.failed_tasks()
    assert set(failed) == {'task1', 'task3'}

def test_tracker_is_succeeded(tracker_instance):
    tracker_instance.upsert('task1', 'phase1', 'succeeded')
    tracker_instance.upsert('task2', 'phase1', 'completed')

    assert tracker_instance.is_succeeded('task1') is True
    assert tracker_instance.is_succeeded('task2') is True
    assert tracker_instance.is_succeeded() is True  # All tasks succeeded

    tracker_instance.upsert('task3', 'phase1', 'failed')
    assert tracker_instance.is_succeeded() is False  # Not all tasks succeeded
    assert tracker_instance.is_succeeded('task1', 'task3') is False
    assert tracker_instance.is_succeeded('task1', 'task2') is True


@pytest.mark.parametrize('thresholds', [
    {},
    {'MEMORY_PERCENT': 99.0, 'CPU_PERCENT': 99.5},
    {'MEMORY_PERCENT': None},
    {'CPU_PERCENT': None},
])
def test_validate_os_thresholds_passing(thresholds):
    logger = logging.getLogger('test_logger')
    stats.validate_os_thresholds(thresholds, logger, mode='normal')

def test_validate_os_thresholds_exceeded(mock_logger):
    thresholds = {'MEMORY_PERCENT': 0.1, 'CPU_PERCENT': 0.5}
    with patch('time.sleep') as mock_sleep:
        with pytest.raises(AssertionError):
            stats.validate_os_thresholds(thresholds, mock_logger, mode='normal', iterations=3)
        mock_sleep.assert_called()
