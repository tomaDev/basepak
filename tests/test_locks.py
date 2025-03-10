import os
import shutil
import threading
import time

import click
import pytest

from basepak.locks import clean_locks, group_lock


@pytest.fixture
def ctx():
    """Provides a click.Context object with specific 'cli_name' and 'click_group_name' to avoid conflicts.
    Cleans up the lock file directory after each test to ensure isolation.
    """
    ctx = click.Context(click.Command('test'))
    ctx.obj = {'cli_name': 'testcli', 'click_group_name': 'testgroup'}
    yield ctx
    lock_file_dir = os.path.join('/tmp', ctx.obj.get('cli_name'))
    if os.path.exists(lock_file_dir):
        shutil.rmtree(lock_file_dir)


def test_lock_and_release(monkeypatch, ctx):
    monkeypatch.setattr(click, 'get_current_context', lambda: ctx)

    @group_lock
    def test_func(ctx):
        time.sleep(0.5)  # Simulate some work
        return 'success'

    def run_func():
        test_func(ctx)

    thread = threading.Thread(target=run_func)
    thread.start()
    time.sleep(0.1)  # Give the thread time to acquire the lock

    # Now, try to run test_func again; it should raise click.Abort
    with pytest.raises(click.Abort):
        test_func(ctx)
    clean_locks(ctx)  # Clean up the lock file directory
    test_func(ctx)  # Now, test_func should run successfully
    thread.join()
