import datetime
import unittest
import threading

from opentracing.mocktracer import MockTracer
from mock import patch

from elasticsearch import Elasticsearch
from elasticsearch_opentracing import TracingTransport, init_tracing, \
    enable_tracing, disable_tracing, set_active_span, get_active_span, _clear_tracing_state
from .dummies import *

@patch('elasticsearch.Transport.perform_request')
class TestTracing(unittest.TestCase):
    def setUp(self):
        self.tracer = MockTracer()
        self.es = Elasticsearch(transport_class=TracingTransport)

    def tearDown(self):
        _clear_tracing_state()
        self.tracer.reset()

    def test_tracing(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False, prefix='Prod007')

        mock_perform_req.return_value = {'hits': []}

        enable_tracing()

        with self.tracer.start_active_span('parentSpan') as scope:
            main_span = scope.span
            body = {"any": "data", "timestamp": datetime.datetime.now()}
            res = self.es.index(index='test-index', doc_type='tweet', id=1,
                                body=body, params={'refresh': True})

        self.assertEqual(mock_perform_req.return_value, res)
        spans = self.tracer.finished_spans()
        self.assertEqual(2, len(spans))

        es_span = spans[0]
        self.assertEqual(es_span.operation_name, 'Prod007/test-index/tweet/1')
        self.assertEqual(es_span.parent_id, main_span.context.span_id)
        self.assertEqual(es_span.tags, {
            'component': 'elasticsearch-py',
            'db.type': 'elasticsearch',
            'db.statement': body,
            'span.kind': 'client',
            'elasticsearch.url': '/test-index/tweet/1',
            'elasticsearch.method': 'PUT',
            'elasticsearch.params': {'refresh': True},
        })

    def test_trace_none(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        self.es.get(index='test-index', doc_type='tweet', id=3)
        self.assertEqual(0, len(self.tracer.finished_spans()))

    def test_trace_all_requests(self, mock_perform_req):
        init_tracing(self.tracer)

        for i in range(3):
            self.es.get(index='test-index', doc_type='tweet', id=i)

        spans = self.tracer.finished_spans()
        self.assertEqual(3, len(spans))

        enable_tracing()
        disable_tracing()  # Shouldnt prevent further tracing
        self.es.get(index='test-index', doc_type='tweet', id=4)

        spans = self.tracer.finished_spans()
        self.assertEqual(4, len(spans))
        self.assertTrue(all((lambda x: x.parent_id is None, spans)))

    def test_trace_all_requests_span(self, mock_perform_req):
        init_tracing(self.tracer)

        main_span = self.tracer.start_span()
        set_active_span(main_span)

        for i in range(3):
            self.es.get(index='test-index', doc_type='tweet', id=i)

        spans = self.tracer.finished_spans()
        self.assertEqual(3, len(spans))
        self.assertTrue(all(map(lambda x: x.parent_id == main_span.context.span_id, spans)))

    def test_trace_bool_payload(self, mock_perform_req):
        init_tracing(self.tracer)

        # Some operations, as creating an index, return a bool value.
        mock_perform_req.return_value = False

        mapping = "{'properties': {'body': {}}}"
        res = self.es.indices.create('test-index', body=mapping)
        self.assertFalse(res)

        spans = self.tracer.finished_spans()
        self.assertEqual(1, len(spans))

    def test_trace_result_tags(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        mock_perform_req.return_value = {
            'found': False,
            'timed_out': True,
            'took': 7
        }
        enable_tracing()
        self.es.get(index='test-index', doc_type='tweet', id=1)

        spans = self.tracer.finished_spans()
        self.assertEqual(1, len(spans))
        self.assertEqual('False', spans[0].tags['elasticsearch.found'])
        self.assertEqual('True', spans[0].tags['elasticsearch.timed_out'])
        self.assertEqual('7', spans[0].tags['elasticsearch.took'])

    def test_disable_tracing(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        enable_tracing()
        disable_tracing()
        self.assertEqual(0, len(self.tracer.finished_spans()))

        self.es.get(index='test-index', doc_type='tweet', id=1)
        self.assertEqual(0, len(self.tracer.finished_spans()))

        disable_tracing()  # shouldn't cause a problem

    def test_disable_tracing_span_legacy(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        main_span = self.tracer.start_span()
        set_active_span(main_span)

        # Make sure the active span was preserved
        enable_tracing()
        disable_tracing()
        self.assertEqual(main_span, get_active_span())

        # Make sure it was preserved, by tracing.
        enable_tracing()

        self.es.get(index='test-index', doc_type='tweet', id=1)
        self.assertEqual(1, len(self.tracer.finished_spans()))
        self.assertEqual(main_span.context.span_id, self.tracer.finished_spans()[0].parent_id)


    def test_disable_tracing_span(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        with self.tracer.start_active_span('parentSpan') as scope:
            main_span = scope.span
            enable_tracing()
            disable_tracing()
            enable_tracing()
            self.es.get(index='test-index', doc_type='tweet', id=1)

        self.assertEqual(2, len(self.tracer.finished_spans()))
        self.assertEqual(main_span.context.span_id, self.tracer.finished_spans()[0].parent_id)

    def test_trace_error(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        with self.tracer.start_active_span('parentSpan') as scope:
            main_span = scope.span
            enable_tracing()
            mock_perform_req.side_effect = RuntimeError()

            caught_exc = None
            try:
                self.es.get(index='test-index', doc_type='tweet', id=1)
            except RuntimeError as exc:
                caught_exc = exc

        spans = self.tracer.finished_spans()
        self.assertEqual(2, len(spans))

        span = spans[0]
        self.assertEqual(main_span.context.span_id, span.parent_id)
        self.assertEqual(True, span.tags['error'])
        self.assertEqual(caught_exc, span.tags['error.object'])

    def test_trace_after_error(self, mock_perform_req):
        init_tracing(self.tracer, trace_all_requests=False)

        enable_tracing()
        mock_perform_req.side_effect = RuntimeError()

        caught_exc = None
        try:
            self.es.get(index='test-index', doc_type='tweet', id=1)
        except RuntimeError as exc:
            caught_exc = exc

        mock_perform_req.side_effect = None
        self.es.get(index='test-index', doc_type='tweet', id=1)

        spans = self.tracer.finished_spans()
        self.assertEqual(2, len(spans))

        error_span, span = spans
        self.assertEqual(True, error_span.tags['error'])
        self.assertEqual(caught_exc, error_span.tags['error.object'])
        self.assertNotIn('error', span.tags)

    def test_multithreading(self, mock_perform_req):
        init_tracing(self.tracer)
        ev = threading.Event()

        # 1. Start tracing from thread-1; make thread-2 wait
        # 2. Trace something from thread-2, make thread-1 before finishing.
        # 3. Check the spans got different parents, and are in the expected order.
        def target1():
            with self.tracer.start_active_span('parentSpan'):
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

        spans = self.tracer.finished_spans()
        self.assertEqual(3, len(spans))
        self.assertEqual([False, True, True], [s.parent_id is None for s in spans])
