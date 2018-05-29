import datetime
import unittest
import threading
import time

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q, DocType, Integer, Keyword, Text, Mapping
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.response import Response
from elasticsearch_opentracing import TracingTransport, init_tracing, \
        enable_tracing, disable_tracing, set_active_span, clear_active_span, \
        get_active_span, _clear_tracing_state
from mock import patch
from .dummies import *


class Article(DocType):
    title = Text(analyzer='snowball', fields={'raw': Keyword()})
    body = Text(analyzer='snowball')

    class Meta:
        index = 'test-index'
        mapping = Mapping('article')

@patch('elasticsearch.Transport.perform_request')
class TestTracing(unittest.TestCase):
    def setUp(self):
        self.tracer = DummyTracer()
        connections.create_connection(hosts=['127.0.0.1'],
                                      transport_class=TracingTransport)

    def tearDown(self):
        _clear_tracing_state()

    def test_search(self, mock_perform_req):
        init_tracing(self.tracer)

        mock_perform_req.return_value = {'hits': {'hits': []}}

        s = Search(index='test-index') \
            .filter('term', author='testing')
        res = s.execute()

        self.assertTrue(isinstance(res, Response))
        self.assertEqual(1, len(self.tracer.spans))
        self.assertEqual(self.tracer.spans[0].operation_name, 'Elasticsearch/test-index/_search')
        self.assertEqual(self.tracer.spans[0].is_finished, True)
        self.assertEqual(self.tracer.spans[0].tags, {
            'component': 'elasticsearch-py',
            'db.type': 'elasticsearch',
            'db.statement': {
                'query': {'bool': {'filter': [{'term': {'author': 'testing'}}]}}
            },
            'span.kind': 'client',
            'elasticsearch.url': '/test-index/_search',
            'elasticsearch.method': 'GET',
        })

    def test_create(self, mock_perform_req):
        init_tracing(self.tracer)

        Article.init()

        self.assertEqual(2, len(self.tracer.spans))
        self.assertEqual(map(lambda x: x.operation_name, self.tracer.spans), [
            u'Elasticsearch/test-index',
            u'Elasticsearch/test-index/_mapping/article'
        ])
        self.assertTrue(all(map(lambda x: x.is_finished, self.tracer.spans)))

    def test_index(self, mock_perform_req):
        init_tracing(self.tracer)

        mock_perform_req.return_value = {'result': 'created'}

        article = Article(
            meta={'id': 2},
            title='About searching',
            body='A few words here, a few words there',
        )
        res = article.save()

        self.assertTrue(res)
        self.assertEqual(1, len(self.tracer.spans))
        self.assertEqual(self.tracer.spans[0].operation_name, u'Elasticsearch/test-index/article/2')
        self.assertEqual(self.tracer.spans[0].is_finished, True)
        self.assertEqual(self.tracer.spans[0].tags, {
            'component': 'elasticsearch-py',
            'db.type': 'elasticsearch',
            'db.statement': {
                'body': 'A few words here, a few words there',
                'title': 'About searching'
            },
            'span.kind': 'client',
            'elasticsearch.url': '/test-index/article/2',
            'elasticsearch.method': 'PUT',
        })
