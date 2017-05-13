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

Please see the examples directory. Overall, usage requires that a tracer gets set, and initialize the Elasticsearch client specifying `TracingTransport` as tracing class, and optionally set an active span (to be used as parent span when tracing the actual Elasticsearch statements):

.. code-block:: python

    import elasticsearch_opentracing

    elasticsearch_opentracing.init_tracing(tracer) # An OpenTracing compatible tracer.
    es = Elasticsearch(transport_class=elasticsearch_opentracing.TracingTransport)

    elasticsearch_opentracing.set_active_span(main_span) # Optional.

    es.index(index='test-index', doc_type='tweet', id=99, body={
        'author': 'linus',
        'text': 'Hello there',
        'timestamp': datetime.now(),
    })
    res = es.get(index='test-index', doc_type='tweet', id=99)

    elasticsearch_opentracing.clear_active_span()

By default, all Elasticsearch requests are traced. It's possible to have it set to false when initializing the library, and call `enable_tracing` and `disable_tracing` to explicitly trace statements happening within that section:

.. code-block:: python

    elasticsearch_opentracing.init_tracing(tracer, trace_all_requests=False)

    elasticsearch_opentracing.enable_tracing()

    res1 = es.get(index='test-index', doc_type='tweet', id=99)
    res2 = es.get(index='test-index', doc_type='user', id=666)

    elasticsearch_opentracing.disable_tracing()

When using `trace_all_requests`, any calls made to `enable_tracing` and `disable_tracing` are ignored.

In case of an exception happening under this block, an implicit call to `disable_tracing` will take place, with the request causing the error including error information with it.

DSL
===

When using the `elasticsearch-dsl` library (which runs on top of `elasticsearch-py`), the same semantics and calls are used. When creating a default connection, the transport can be specified as well:

.. code-block:: python

    # elasticsearch_dsl.connections.connections
    connections.create_connection(hosts=['127.0.0.1'],
                                  transport_class=elasticsearch_opentracing.TracingTransport)


Multithreading
==============

Tracing and parent span data is kept as thread local data, which means that applications using many threads (Django, Flask, Pyramid, etc) will work just fine.

Further information
===================

If you’re interested in learning more about the OpenTracing standard, please visit `opentracing.io`_ or `join the mailing list`_. If you would like to implement OpenTracing in your project and need help, feel free to send us a note at `community@opentracing.io`_.

.. _opentracing.io: http://opentracing.io/
.. _join the mailing list: http://opentracing.us13.list-manage.com/subscribe?u=180afe03860541dae59e84153&id=19117aa6cd
.. _community@opentracing.io: community@opentracing.io

