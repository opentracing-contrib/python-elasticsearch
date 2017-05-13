from datetime import datetime
from elasticsearch import Transport
from elasticsearch_dsl import DocType, Date, Integer, Keyword, Text
from elasticsearch_dsl.connections import connections

import lightstep
import elasticsearch_opentracing

tracer = lightstep.Tracer(
    component_name='elasticsearch-map',
    access_token='{your_lightstep_token}'
)

class Article(DocType):
    title = Text(analyzer='snowball', fields={'raw': Keyword()})
    body = Text(analyzer='snowball')
    tags = Keyword()
    published_from = Date()

    class Meta:
        index = 'test-index'

if __name__ == '__main__':
    elasticsearch_opentracing.init_tracing(tracer)

    connections.create_connection(hosts=['127.0.0.1'],
                                  transport_class=elasticsearch_opentracing.TracingTransport)

    Article.init()

    # Have a master span only for the ingestion/get.
    with tracer.start_span('main span') as span:
        elasticsearch_opentracing.set_active_span(span)

        article = Article(
            meta={'id': 7},
            title='About searching',
            body='A few words here, a few words there',
            tags = ['elastic', 'search'],
            published_from=datetime.now()
        )
        article.save()

        article = Article.get(id=7)
        print(article)

        elasticsearch_opentracing.clear_active_span()

    # Print the cluster's health before exiting.
    print(connections.get_connection().cluster.health()['status'])

