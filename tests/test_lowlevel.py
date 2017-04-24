import unittest
import threading
import time

from elasticsearch import Elasticsearch
from elasticsearch_opentracing import TracingTransport, init_tracing, \
        trace_one, start_tracing, finish_tracing
from mock import patch
from .dummies import *

@patch('elasticsearch.Transport.perform_request')
class TestTracing(unittest.TestCase):
    def setUp(self):
        self.tracer = DummyTracer()
        self.es = Elasticsearch(transport_class=TracingTransport)

    def test_trace_one(self, mock_perform_req):
        init_tracing(self.tracer)

        main_span = DummySpan()
        trace_one(parent_span=main_span)

        self.es.get(index='test-index', doc_type='tweet', id=1)
        self.assertEqual(1, len(self.tracer.spans))
        self.assertEqual(self.tracer.spans[0].operation_name, '/test-index/tweet/1')
        self.assertEqual(self.tracer.spans[0].is_finished, True)
        self.assertEqual(self.tracer.spans[0].child_of, main_span)
        self.assertEqual(self.tracer.spans[0].tags, {
            'component': 'elasticsearch-py',
            'db.type': 'elasticsearch',
            'span.kind': 'client',
            'elasticsearch.url': '/test-index/tweet/1',
            'elasticsearch.method': 'GET',
        })

    def test_trace_none(self, mock_perform_req):
        init_tracing(self.tracer)

        self.es.get(index='test-index', doc_type='tweet', id=3)
        self.assertEqual(0, len(self.tracer.spans))

    def test_trace_all_requests(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=True)

        for i in range(3):
            self.es.get(index='test-index', doc_type='tweet', id=i)

        self.assertEqual(3, len(self.tracer.spans))
        self.assertTrue(all(map(lambda x: x.is_finished, self.tracer.spans)))

        start_tracing(DummySpan())
        finish_tracing() # Shouldnt prevent further tracing
        self.es.get(index='test-index', doc_type='tweet', id=4)

        self.assertEqual(4, len(self.tracer.spans))
        self.assertTrue(all(map(lambda x: x.is_finished, self.tracer.spans)))
        self.assertTrue(all(map(lambda x: x.child_of is None, self.tracer.spans)))

    def test_trace_stop(self, mock_perform_req):
        init_tracing(self.tracer)

        start_tracing()
        finish_tracing()
        self.assertEqual(0, len(self.tracer.spans))

        self.es.get(index='test-index', doc_type='tweet', id=1)
        self.assertEqual(0, len(self.tracer.spans))

        finish_tracing() # shouldn't cause a problem

    def test_trace_error(self, mock_perform_req):
        init_tracing(self.tracer)

        main_span = DummySpan()
        start_tracing(main_span)
        mock_perform_req.side_effect = RuntimeError()

        try:
            self.es.get(index='test-index', doc_type='tweet', id=1)
        except RuntimeError as exc:
            pass

        self.assertEqual(1, len(self.tracer.spans))
        self.assertEqual(True, self.tracer.spans[0].is_finished)
        self.assertEqual(main_span, self.tracer.spans[0].child_of)
        self.assertEqual('true', self.tracer.spans[0].tags['error'])
        self.assertEqual(True, self.tracer.spans[0].tags['error.object'] != None)

    def test_trace_after_error(self, mock_perform_req):
        init_tracing(self.tracer)

        start_tracing()
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
            start_tracing(DummySpan())
            self.es.get(index='test-index', doc_type='tweet', id=1)

            ev.set()
            ev.wait()

            finish_tracing()

        def target2():
            ev.wait()

            start_tracing()
            self.es.get(index='test-index', doc_type='tweet', id=2)

            ev.set()
            finish_tracing()

        t1 = threading.Thread(target=target1)
        t2 = threading.Thread(target=target2)
        
        t1.start()
        t2.start()

        t1.join()
        t2.join()

        self.assertEqual(2, len(self.tracer.spans))
        self.assertTrue(all(map(lambda x: x.is_finished, self.tracer.spans)))
        self.assertEqual([False, True], map(lambda x: x.child_of is None, self.tracer.spans))

