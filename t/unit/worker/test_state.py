import os
import pickle
import sys
from importlib import import_module
from time import time
from unittest.mock import Mock, patch

import pytest

from celery import uuid
from celery.exceptions import WorkerShutdown, WorkerTerminate
from celery.platforms import EX_OK
from celery.utils.collections import LimitedSet
from celery.worker import state


@pytest.fixture
def reset_state():
    yield
    state.active_requests.clear()
    state.revoked.clear()
    state.revoked_stamps.clear()
    state.total_count.clear()


class MockShelve(dict):
    filename = None
    in_sync = False
    closed = False

    def open(self, filename, **kwargs):
        self.filename = filename
        return self

    def sync(self):
        self.in_sync = True

    def close(self):
        self.closed = True


class MyPersistent(state.Persistent):
    storage = MockShelve()


class test_maybe_shutdown:

    def teardown_method(self):
        state.should_stop = None
        state.should_terminate = None

    def test_should_stop(self):
        state.should_stop = True
        with pytest.raises(WorkerShutdown):
            state.maybe_shutdown()
        state.should_stop = 0
        with pytest.raises(WorkerShutdown):
            state.maybe_shutdown()
        state.should_stop = False
        try:
            state.maybe_shutdown()
        except SystemExit:
            raise RuntimeError('should not have exited')
        state.should_stop = None
        try:
            state.maybe_shutdown()
        except SystemExit:
            raise RuntimeError('should not have exited')

        state.should_stop = 0
        try:
            state.maybe_shutdown()
        except SystemExit as exc:
            assert exc.code == 0
        else:
            raise RuntimeError('should have exited')

        state.should_stop = 303
        try:
            state.maybe_shutdown()
        except SystemExit as exc:
            assert exc.code == 303
        else:
            raise RuntimeError('should have exited')

    @pytest.mark.parametrize('should_stop', (None, False, True, EX_OK))
    def test_should_terminate(self, should_stop):
        state.should_stop = should_stop
        state.should_terminate = True
        with pytest.raises(WorkerTerminate):
            state.maybe_shutdown()


@pytest.mark.usefixtures('reset_state')
class test_Persistent:

    @pytest.fixture
    def p(self):
        return MyPersistent(state, filename='celery-state')

    def test_close_twice(self, p):
        p._is_open = False
        p.close()

    def test_constructor(self, p):
        assert p.db == {}
        assert p.db.filename == p.filename

    def test_save(self, p):
        p.db['foo'] = 'bar'
        p.save()
        assert p.db.in_sync
        assert p.db.closed

    def add_revoked(self, p, *ids):
        for id in ids:
            p.db.setdefault('revoked', LimitedSet()).add(id)

    def test_merge(self, p, data=['foo', 'bar', 'baz']):
        state.revoked.update(data)
        p.merge()
        for item in data:
            assert item in state.revoked

    def test_merge_dict(self, p):
        p.clock = Mock()
        p.clock.adjust.return_value = 626
        d = {'revoked': {'abc': time()}, 'clock': 313}
        p._merge_with(d)
        p.clock.adjust.assert_called_with(313)
        assert d['clock'] == 626
        assert 'abc' in state.revoked

    def test_sync_clock_and_purge(self, p):
        passthrough = Mock()
        passthrough.side_effect = lambda x: x
        with patch('celery.worker.state.revoked') as revoked:
            d = {'clock': 0}
            p.clock = Mock()
            p.clock.forward.return_value = 627
            p._dumps = passthrough
            p.compress = passthrough
            p._sync_with(d)
            revoked.purge.assert_called_with()
            assert d['clock'] == 627
            assert 'revoked' not in d
            assert d['zrevoked'] is revoked

    def test_sync(self, p,
                  data1=['foo', 'bar', 'baz'], data2=['baz', 'ini', 'koz']):
        self.add_revoked(p, *data1)
        for item in data2:
            state.revoked.add(item)
        p.sync()

        assert p.db['zrevoked']
        pickled = p.decompress(p.db['zrevoked'])
        assert pickled
        saved = pickle.loads(pickled)
        for item in data2:
            assert item in saved


class SimpleReq:

    def __init__(self, name):
        self.id = uuid()
        self.name = name


@pytest.mark.usefixtures('reset_state')
class test_state:

    def test_accepted(self, requests=[SimpleReq('foo'),
                                      SimpleReq('bar'),
                                      SimpleReq('baz'),
                                      SimpleReq('baz')]):
        for request in requests:
            state.task_accepted(request)
        for req in requests:
            assert req in state.active_requests
        assert state.total_count['foo'] == 1
        assert state.total_count['bar'] == 1
        assert state.total_count['baz'] == 2

    def test_ready(self, requests=[SimpleReq('foo'),
                                   SimpleReq('bar')]):
        for request in requests:
            state.task_accepted(request)
        assert len(state.active_requests) == 2
        for request in requests:
            state.task_ready(request)
        assert len(state.active_requests) == 0


class test_state_configuration():

    @staticmethod
    def import_state():
        with patch.dict(sys.modules):
            del sys.modules['celery.worker.state']
            return import_module('celery.worker.state')

    @patch.dict(os.environ, {
        'CELERY_WORKER_REVOKES_MAX': '50001',
        'CELERY_WORKER_SUCCESSFUL_MAX': '1001',
        'CELERY_WORKER_REVOKE_EXPIRES': '10801',
        'CELERY_WORKER_SUCCESSFUL_EXPIRES': '10801',
    })
    def test_custom_configuration(self):
        state = self.import_state()
        assert state.REVOKES_MAX == 50001
        assert state.SUCCESSFUL_MAX == 1001
        assert state.REVOKE_EXPIRES == 10801
        assert state.SUCCESSFUL_EXPIRES == 10801

    def test_default_configuration(self):
        state = self.import_state()
        assert state.REVOKES_MAX == 50000
        assert state.SUCCESSFUL_MAX == 1000
        assert state.REVOKE_EXPIRES == 10800
        assert state.SUCCESSFUL_EXPIRES == 10800


@pytest.mark.usefixtures('reset_state')
class test_revoked:
    def test_revoked_maxlen(self):
        """Test that revoked set respects REVOKES_MAX limit"""
        # Fill beyond REVOKES_MAX
        for i in range(state.REVOKES_MAX + 10):
            state.revoked.add(f'task_{i}')
        assert len(state.revoked) <= state.REVOKES_MAX

    def test_revoked_expires(self):
        """Test that revoked tasks expire after REVOKE_EXPIRES seconds"""
        with patch('time.monotonic') as mock_time:
            mock_time.return_value = 100.0
            state.revoked.add('task_1')

            # Check task exists
            assert 'task_1' in state.revoked

            # Move time forward just before expiration
            mock_time.return_value = 100.0 + state.REVOKE_EXPIRES - 1
            state.revoked.purge()  # Force purge
            assert 'task_1' in state.revoked

            # Move time forward past expiration
            mock_time.return_value = 100.0 + state.REVOKE_EXPIRES + 1
            state.revoked.purge()  # Force purge
            assert 'task_1' not in state.revoked

    def test_revoked_contains(self):
        """Test membership testing of revoked tasks"""
        task_id = 'task_1'
        assert task_id not in state.revoked
        state.revoked.add(task_id)
        assert task_id in state.revoked

    def test_revoked_stamps_update(self):
        """Test revoked_stamps gets updated when revoking by headers"""
        header = 'test_stamp'
        stamp = 'stamp_1'

        assert header not in state.revoked_stamps
        state.revoked_stamps[header] = [stamp]  # This is how control.revoke_by_stamped_headers does it
        assert header in state.revoked_stamps
        assert stamp in state.revoked_stamps[header]

    def test_revoked_persistence(self):
        """Test that revoked set can be serialized/deserialized"""
        original_tasks = {'task_1', 'task_2', 'task_3'}
        for task_id in original_tasks:
            state.revoked.add(task_id)

        # Test pickling/unpickling
        pickled = pickle.dumps(state.revoked)
        unpickled = pickle.loads(pickled)

        assert isinstance(unpickled, type(state.revoked))
        assert all(task in unpickled for task in original_tasks)

    def test_revoked_stamps_list_values(self):
        """Test revoked_stamps handles list of stamps"""
        header = 'test_stamp'
        stamps = ['stamp_1', 'stamp_2', 'stamp_3']

        state.revoked_stamps[header] = stamps
        assert all(stamp in state.revoked_stamps[header] for stamp in stamps)

    def test_revoked_stamps_update_existing(self):
        """Test updating existing stamps in revoked_stamps"""
        header = 'test_stamp'
        initial_stamps = ['stamp_1', 'stamp_2']
        new_stamps = ['stamp_3', 'stamp_4']

        state.revoked_stamps[header] = initial_stamps
        # Simulate how control.revoke_by_stamped_headers updates stamps
        state.revoked_stamps[header] = initial_stamps + new_stamps

        assert all(
            stamp in state.revoked_stamps[header]
            for stamp in initial_stamps + new_stamps)

    def test_revoked_clear(self):
        """Test clearing revoked set and stamps"""
        # Add some revoked tasks
        state.revoked.add('task_1')
        state.revoked_stamps['header'] = ['stamp_1']

        # Clear state
        state.revoked.clear()
        state.revoked_stamps.clear()

        assert len(state.revoked) == 0
        assert len(state.revoked_stamps) == 0

    def test_revoked_purge_expired(self):
        """Test purging expired tasks doesn't affect stamps"""
        with patch('time.monotonic') as mock_time:
            mock_time.return_value = 100.0
            state.revoked.add('task_1')
            state.revoked_stamps['header'] = ['stamp_1']

            # Move time past expiration and purge
            mock_time.return_value = 100.0 + state.REVOKE_EXPIRES + 1
            state.revoked.purge()

            # Revoked task should be gone but stamps remain
            assert 'task_1' not in state.revoked
            assert state.revoked_stamps['header'] == ['stamp_1']

    def test_revoked_stamps_multiple_headers(self):
        """Test handling multiple headers in revoked_stamps"""
        headers = {
            'header1': ['stamp1_1', 'stamp1_2'],
            'header2': ['stamp2_1', 'stamp2_2']
        }

        for header, stamps in headers.items():
            state.revoked_stamps[header] = stamps

        for header, stamps in headers.items():
            assert all(stamp in state.revoked_stamps[header] for stamp in stamps)
