from distutils.version import LooseVersion
import unittest

from opentracing.mocktracer import MockTracer

from elasticsearch_dsl import Search, DocType, Keyword, Text, VERSION
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.response import Response
from elasticsearch_opentracing import TracingTransport, init_tracing, _clear_tracing_state
from mock import patch

elasticsearch_dsl_version = LooseVersion('.'.join(str(i) for i in VERSION))


class Article(DocType):
    title = Text(analyzer='snowball', fields={'raw': Keyword()})
    body = Text(analyzer='snowball')

    class Meta:
        index = 'test-index'
        if elasticsearch_dsl_version >= LooseVersion('6.0.0'):
            doc_type = 'article'

    if elasticsearch_dsl_version >= LooseVersion('6.2.0'):
        class Index:
            name = 'test-index'


@patch('elasticsearch.Transport.perform_request')
class TestTracing(unittest.TestCase):
    def setUp(self):
        self.tracer = MockTracer()
        connections.create_connection(hosts=['127.0.0.1'],
                                      transport_class=TracingTransport)

    def tearDown(self):
        _clear_tracing_state()
        self.tracer.reset()

    def test_search(self, mock_perform_req):
        init_tracing(self.tracer)

        mock_perform_req.return_value = {'hits': {'hits': []}}

        s = Search(index='test-index') \
            .filter('term', author='testing')
        res = s.execute()

        spans = self.tracer.finished_spans()
        self.assertEqual(1, len(spans))
        span = spans[0]
        self.assertTrue(isinstance(res, Response))
        self.assertEqual(span.operation_name, 'Elasticsearch/test-index/_search')
        self.assertEqual(span.tags, {
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

        spans = self.tracer.finished_spans()

        expected_operations = ['Elasticsearch/test-index', 'Elasticsearch/test-index/_mapping/article']
        if elasticsearch_dsl_version >= LooseVersion('6.2.0'):
            expected_operations.insert(1, 'Elasticsearch/test-index/_settings')

        self.assertEqual([s.operation_name for s in spans], expected_operations)

    def test_index(self, mock_perform_req):
        init_tracing(self.tracer)

        mock_perform_req.return_value = {'created': True, 'result': 'created'}

        article = Article(
            meta={'id': 2},
            title='About searching',
            body='A few words here, a few words there',
        )
        res = article.save()

        spans = self.tracer.finished_spans()
        self.assertTrue(res)
        self.assertEqual(1, len(spans))
        span = spans[0]
        self.assertEqual(span.operation_name, 'Elasticsearch/test-index/article/2')
        self.assertEqual(span.tags, {
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

