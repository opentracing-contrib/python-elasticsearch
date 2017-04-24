#########################
ElasticSearch Opentracing
#########################

This package enables distributed tracing for the Python elasticsearch-py and elasticsearch-dsl libraries.

Instalation
===========

Run the following command:

    $ pip install elasticsearch_opentracing

Getting started
===============

Please see the examples directory. Overall, usage requires that a tracer gets set, and initialize the Elasticsearch client specifying `TracingTransport` as tracing class, and mark the next single request to be traced (with a parent span, if any):

.. code-block:: python

    import elasticsearch_opentracing

    elasticsearch_opentracing.init_tracing(tracer) # An OpenTracing compatible tracer.
    es = Elasticsearch(transport_class=elasticsearch_opentracing.TracingTransport)

    elasticsearch_opentracing.trace_one(parent_span=main_span) # parent_span is optional
    res = es.get(index='test-index', doc_type='tweet', id=1)

It's also possible to call `start_tracing` and `finish_tracing` to trace any requests under this block:

.. code-block:: python

    elasticsearch_opentracing.start_tracing(parent_span=main_span) # parent_span is optional

    # Both the index and the query requests will be traced as children of main_span.
    es.index(index='test-index', doc_type='tweet', id=99, body={
        'author': 'linus',
        'text': 'Hello there',
        'timestamp': datetime.now(),
    })
    res = es.get(index='test-index', doc_type='tweet', id=99)

    elasticsearch_opentracing.finish_tracing()

In case of an exception happening under this block, an implicit call to `finish_tracing` will take place, with the request causing the error including error information with it.

Alternatively, you can enable tracing of all requests:

.. code-block:: python

    elasticsearch_opentracing.init_tracing(tracer, trace_all_requests=True)
    es = Elasticsearch(transport_class=elasticsearch_opentracing.TracingTransport)

    # this request will be traced (without a parent span, though)
    res = es.get(index='test-index', doc_type='tweet', id=1)

Multithreading
==============

Tracing and parent span data is kept as thread local data, which means that applications using many threads (Django, Flask, Pyramid, etc) will work just fine.

Further information
===================

If you’re interested in learning more about the OpenTracing standard, please visit `opentracing.io`_ or `join the mailing list`_. If you would like to implement OpenTracing in your project and need help, feel free to send us a note at `community@opentracing.io`_.

.. _opentracing.io: http://opentracing.io/
.. _join the mailing list: http://opentracing.us13.list-manage.com/subscribe?u=180afe03860541dae59e84153&id=19117aa6cd
.. _community@opentracing.io: community@opentracing.io

