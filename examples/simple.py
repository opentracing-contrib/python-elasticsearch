from datetime import datetime
from elasticsearch import Elasticsearch

import opentracing
import elasticsearch_opentracing

# Your OpenTracing-compatible tracer here.
tracer = opentracing.Tracer()

if __name__ == '__main__':
    elasticsearch_opentracing.init_tracing(tracer)

    es = Elasticsearch('127.0.0.1', transport_class=elasticsearch_opentracing.TracingTransport)

    with tracer.start_span('main span') as main_span:
        elasticsearch_opentracing.set_active_span(main_span)

        doc = {
            'author': 'john',
            'text': 'Find me if you can',
            'timestamp': datetime.now(),
        }

        es.index(index='test-index', doc_type='tweet', id=1, body=doc) # Traced
        res = es.get(index='test-index', doc_type='tweet', id=1) # Traced too
        print(res['_source'])

        elasticsearch_opentracing.clear_active_span()

