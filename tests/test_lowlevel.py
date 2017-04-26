import datetime
import unittest
import threading
import time

from elasticsearch import Elasticsearch
from elasticsearch_opentracing import TracingTransport, init_tracing, \
        enable_tracing, disable_tracing, set_active_span, clear_active_span, \
        get_active_span, _clear_tracing_state
from mock import patch
from .dummies import *

@patch('elasticsearch.Transport.perform_request')
class TestTracing(unittest.TestCase):
    def setUp(self):
        self.tracer = DummyTracer()
        self.es = Elasticsearch(transport_class=TracingTransport)

    def tearDown(self):
        _clear_tracing_state()

    def test_tracing(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        main_span = DummySpan()
        set_active_span(main_span)
        enable_tracing()

        body = {"any": "data", "timestamp": datetime.datetime.now()}
        self.es.index(index='test-index', doc_type='tweet', id=1, body=body)
        self.assertEqual(1, len(self.tracer.spans))
        self.assertEqual(self.tracer.spans[0].operation_name, '/test-index/tweet/1')
        self.assertEqual(self.tracer.spans[0].is_finished, True)
        self.assertEqual(self.tracer.spans[0].child_of, main_span)
        self.assertEqual(self.tracer.spans[0].tags, {
            'component': 'elasticsearch-py',
            'db.type': 'elasticsearch',
            'db.statement': body,
            'span.kind': 'client',
            'elasticsearch.url': '/test-index/tweet/1',
            'elasticsearch.method': 'PUT',
        })

    def test_trace_none(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        set_active_span(DummySpan())

        self.es.get(index='test-index', doc_type='tweet', id=3)
        self.assertEqual(0, len(self.tracer.spans))

    def test_trace_all_requests(self, mock_perform_req):
        init_tracing(self.tracer)

        for i in range(3):
            self.es.get(index='test-index', doc_type='tweet', id=i)

        self.assertEqual(3, len(self.tracer.spans))
        self.assertTrue(all(map(lambda x: x.is_finished, self.tracer.spans)))

        enable_tracing()
        disable_tracing() # Shouldnt prevent further tracing
        self.es.get(index='test-index', doc_type='tweet', id=4)

        self.assertEqual(4, len(self.tracer.spans))
        self.assertTrue(all(map(lambda x: x.is_finished, self.tracer.spans)))
        self.assertTrue(all(map(lambda x: x.child_of is None, self.tracer.spans)))

    def test_trace_all_requests_span(self, mock_perform_req):
        init_tracing(self.tracer)

        main_span = DummySpan()
        set_active_span(main_span)

        for i in range(3):
            self.es.get(index='test-index', doc_type='tweet', id=i)

        self.assertEqual(3, len(self.tracer.spans))
        self.assertTrue(all(map(lambda x: x.is_finished, self.tracer.spans)))
        self.assertTrue(all(map(lambda x: x.child_of == main_span, self.tracer.spans)))

    def test_disable_tracing(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        enable_tracing()
        disable_tracing()
        self.assertEqual(0, len(self.tracer.spans))

        self.es.get(index='test-index', doc_type='tweet', id=1)
        self.assertEqual(0, len(self.tracer.spans))

        disable_tracing() # shouldn't cause a problem

    def test_disable_tracing_span(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        main_span = DummySpan()
        set_active_span(main_span)

        # Make sure the active span was preserved
        enable_tracing()
        disable_tracing()
        self.assertEqual(main_span, get_active_span())

        # Make sure it was preserved, by tracing.
        enable_tracing()

        self.es.get(index='test-index', doc_type='tweet', id=1)
        self.assertEqual(1, len(self.tracer.spans))
        self.assertEqual(main_span, self.tracer.spans[0].child_of)

    def test_clear_span(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        enable_tracing()

        set_active_span(DummySpan())
        clear_active_span()

        self.es.get(index='test-index', doc_type='tweet', id=1)
        self.assertEqual(1, len(self.tracer.spans))
        self.assertEqual(None, self.tracer.spans[0].child_of)

    def test_trace_error(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        main_span = DummySpan()
        set_active_span(main_span)
        enable_tracing()
        mock_perform_req.side_effect = RuntimeError()

        try:
            self.es.get(index='test-index', doc_type='tweet', id=1)
        except RuntimeError as exc:
            catched_exc = exc

        self.assertEqual(1, len(self.tracer.spans))
        self.assertEqual(True, self.tracer.spans[0].is_finished)
        self.assertEqual(main_span, self.tracer.spans[0].child_of)
        self.assertEqual('true', self.tracer.spans[0].tags['error'])
        self.assertEqual(catched_exc, self.tracer.spans[0].tags['error.object'])

    def test_trace_after_error(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        enable_tracing()
        mock_perform_req.side_effect = RuntimeError()

        try:
            self.es.get(index='test-index', doc_type='tweet', id=1)
        except RuntimeError as exc:
            pass

        self.tracer.clear()

        # Should not cause any further tracing
        mock_perform_req.side_effect = None
        self.es.get(index='test-index', doc_type='tweet', id=1)
        self.assertEqual(0, len(self.tracer.spans))

    def test_multithreading(self, mock_perform_req):
        init_tracing(self.tracer)
        ev = threading.Event()

        # 1. Start tracing from thread-1; make thread-2 wait
        # 2. Trace something from thread-2, make thread-1 before finishing.
        # 3. Check the spans got different parents, and are in the expected order.
        def target1():
            set_active_span(DummySpan())
            enable_tracing()
            self.es.get(index='test-index', doc_type='tweet', id=1)

            ev.set()
            ev.wait()

            disable_tracing()

        def target2():
            ev.wait()

            enable_tracing()
            self.es.get(index='test-index', doc_type='tweet', id=2)

            ev.set()
            disable_tracing()

        t1 = threading.Thread(target=target1)
        t2 = threading.Thread(target=target2)
        
        t1.start()
        t2.start()

        t1.join()
        t2.join()

        self.assertEqual(2, len(self.tracer.spans))
        self.assertTrue(all(map(lambda x: x.is_finished, self.tracer.spans)))
        self.assertEqual([False, True], map(lambda x: x.child_of is None, self.tracer.spans))

