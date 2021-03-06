import unittest

import elasticsearch_opentracing
from elasticsearch_opentracing import init_tracing

from .dummies import *

class TestGlobalCalls(unittest.TestCase):
    def test_init(self):
        tracer = DummyTracer()
        init_tracing(tracer)
        self.assertEqual(tracer, elasticsearch_opentracing.g_tracer)
        self.assertEqual(True, elasticsearch_opentracing.g_trace_all_requests)
        self.assertEqual('Elasticsearch', elasticsearch_opentracing.g_trace_prefix)

    def test_init_subtracer(self):
        tracer = DummyTracer(with_subtracer=True)
        init_tracing(tracer)
        self.assertEqual(tracer._tracer, elasticsearch_opentracing.g_tracer)
        self.assertEqual(True, elasticsearch_opentracing.g_trace_all_requests)
        self.assertEqual('Elasticsearch', elasticsearch_opentracing.g_trace_prefix)

    def test_init_trace_all_requests(self):
        init_tracing(DummyTracer(), trace_all_requests=False)
        self.assertEqual(False, elasticsearch_opentracing.g_trace_all_requests)

        init_tracing(DummyTracer(), trace_all_requests=True)
        self.assertEqual(True, elasticsearch_opentracing.g_trace_all_requests)

    def test_init_trace_prefix(self):
        init_tracing(DummyTracer(), prefix='Prod007')
        self.assertEqual('Prod007', elasticsearch_opentracing.g_trace_prefix)

        init_tracing(DummyTracer(), prefix='')
        self.assertEqual('', elasticsearch_opentracing.g_trace_prefix)
