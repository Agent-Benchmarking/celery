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
class test_request_sets:
    """Tests for request tracking collections in worker state."""

    def setup_method(self):
        state.reset_state()

    @pytest.fixture
    def make_requests(self):
        """Create request objects with given names."""
        def _make(*names):
            requests = [SimpleReq(name) for name in names]
            # Return a single item for single requests, list otherwise
            return requests[0] if len(requests) == 1 else requests
        return _make

    def test_reserved_requests(self, make_requests):
        """Tasks are properly tracked in reserved_requests set."""
        req1, req2 = make_requests('task1', 'task2')

        assert len(state.reserved_requests) == 0

        state.task_reserved(req1)
        state.task_reserved(req2)

        assert len(state.reserved_requests) == 2
        assert req1 in state.reserved_requests
        assert req2 in state.reserved_requests

        state.task_ready(req1)
        assert len(state.reserved_requests) == 1
        assert req1 not in state.reserved_requests
        assert req2 in state.reserved_requests

    def test_active_requests(self, make_requests):
        """Tasks are properly tracked in active_requests set."""
        req1, req2 = make_requests('task1', 'task2')

        assert len(state.active_requests) == 0

        state.task_accepted(req1)
        state.task_accepted(req2)

        assert len(state.active_requests) == 2
        assert req1 in state.active_requests
        assert req2 in state.active_requests

        state.task_ready(req1)
        assert len(state.active_requests) == 1
        assert req1 not in state.active_requests
        assert req2 in state.active_requests

    def test_successful_requests(self, make_requests):
        """Only successfully completed tasks are added to successful_requests."""
        req1, req2 = make_requests('task1', 'task2')

        assert len(state.successful_requests) == 0

        state.task_accepted(req1)
        state.task_accepted(req2)

        state.task_ready(req1, successful=False)
        assert len(state.successful_requests) == 0

        state.task_ready(req2, successful=True)
        assert len(state.successful_requests) == 1
        assert req2.id in state.successful_requests
        assert req1.id not in state.successful_requests

    def test_request_lifecycle(self, make_requests):
        """Task properly transitions through reserved, active, and successful states."""
        req = make_requests('task_lifecycle')

        assert len(state.reserved_requests) == 0
        assert len(state.active_requests) == 0
        assert len(state.successful_requests) == 0

        state.task_reserved(req)
        assert req in state.reserved_requests
        assert req not in state.active_requests

        state.task_accepted(req)
        assert req in state.reserved_requests  # Still in reserved
        assert req in state.active_requests

        state.task_ready(req, successful=True)
        assert req not in state.reserved_requests
        assert req not in state.active_requests
        assert req.id in state.successful_requests

    @pytest.mark.parametrize('test_limit,num_tasks,scenario', [
        (5, 5, "Exact limit"),
        (5, 10, "Exceeding limit"),
        (1, 3, "Minimum limit")
    ])
    def test_successful_requests_limit(self, test_limit, num_tasks, scenario):
        """successful_requests respects its configured size limit."""
        original_limit = state.successful_requests.maxlen
        try:
            state.successful_requests.maxlen = test_limit

            # Create and complete tasks
            for i in range(num_tasks):
                req = SimpleReq(f'task{i}')
                state.task_accepted(req)
                state.task_ready(req, successful=True)

            # For exact limit, expect all tasks to be kept
            # For other cases, expect only up to the limit
            expected = min(test_limit, num_tasks)
            assert len(state.successful_requests) == expected, \
                f"{scenario}: Expected {expected} tasks in successful_requests"

        finally:
            state.successful_requests.maxlen = original_limit

    def test_reset_state_clears_request_sets(self, make_requests):
        """reset_state properly clears all request tracking collections."""
        req1, req2 = make_requests('task1', 'task2')

        state.task_reserved(req1)
        state.task_accepted(req2)
        state.task_ready(req1, successful=True)

        assert state.reserved_requests or state.active_requests or state.successful_requests

        state.reset_state()

        assert len(state.reserved_requests) == 0
        assert len(state.active_requests) == 0
        assert len(state.successful_requests) == 0
