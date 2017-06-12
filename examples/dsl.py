from elasticsearch import Elasticsearch, Transport
from elasticsearch_dsl import Search, Q

import opentracing
import elasticsearch_opentracing

# Your OpenTracing-compatible tracer here.
tracer = opentracing.Tracer()

if __name__ == '__main__':
    elasticsearch_opentracing.init_tracing(tracer)

    client = Elasticsearch('127.0.0.1',
                           transport_class=elasticsearch_opentracing.TracingTransport)

    with tracer.start_span('main span') as main_span:
        elasticsearch_opentracing.set_active_span(main_span)

        s = Search(using=client, index='test-index') \
            .filter('term', author='linus') \
            .query('match', text='git')

        res = s.execute()
        for item in res:
            print(item)
