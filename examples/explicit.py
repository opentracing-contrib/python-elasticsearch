from datetime import datetime
from elasticsearch import Elasticsearch

import lightstep
import elasticsearch_opentracing

tracer = lightstep.Tracer(
    component_name='elasticsearch-explicit',
    access_token='{your_lightstep_token}'
)

if __name__ == '__main__':
    elasticsearch_opentracing.init_tracing(tracer, trace_all_requests=False)

    es = Elasticsearch('127.0.0.1', transport_class=elasticsearch_opentracing.TracingTransport)

    # We want to trace only the creation of the document, not the
    # index creation not checking the document was actually created.

    with tracer.start_span('main span') as main_span:
        elasticsearch_opentracing.set_active_span(main_span)

        es.indices.create('test-index', ignore=400)

        doc = {
            'author': 'john',
            'text': 'Find me if you can',
            'timestamp': datetime.now(),
        }

        elasticsearch_opentracing.enable_tracing()
        es.index(index='test-index', doc_type='tweet', id=1, body=doc) # Traced
        elasticsearch_opentracing.disable_tracing()

        res = es.get(index='test-index', doc_type='tweet', id=1) # Not traced
        print(res['_source'])

        elasticsearch_opentracing.clear_active_span()

