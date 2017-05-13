from elasticsearch import Elasticsearch, Transport
from elasticsearch_dsl import Search, Q

import lightstep
import elasticsearch_opentracing

tracer = lightstep.Tracer(
    component_name='elasticsearch-dsl',
    access_token='{your_lightstep_token}'
)

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
