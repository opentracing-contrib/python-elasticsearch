import threading
from elasticsearch import Transport

g_tracer = None
g_trace_all_requests = False

tls = threading.local()

def init_tracing(tracer, trace_all_requests=False):
    global g_tracer, g_trace_all_requests
    if hasattr(tracer, '_tracer'):
        tracer = tracer._tracer

    g_tracer = tracer
    g_trace_all_requests = trace_all_requests

def _get_traced_info():
    if not hasattr(tls, 'traced'):
        tls.traced = False
        tls.parent_span = None
        tls.once = False

    return (tls.traced, tls.parent_span, tls.once)

def _set_traced_info(traced, parent_span, once):
    tls.traced = traced
    tls.parent_span = parent_span
    tls.once = once

def _clear_traced_info():
    tls.traced = False
    tls.parent_span = None
    tls.once = False

def trace_one(parent_span=None):
    _set_traced_info(True, parent_span, once=True)

def start_tracing(parent_span=None):
    _set_traced_info(True, parent_span, once=False)

def finish_tracing():
    _clear_traced_info()

class TracingTransport(Transport):
    def __init__(self, *args, **kwargs):
        super(TracingTransport, self).__init__(*args, **kwargs)

    def perform_request(self, method, url, params=None, body=None):
        traced, parent_span, once = _get_traced_info()
        if not (g_trace_all_requests or traced):
            return super(TracingTransport, self).perform_request(method, url, params, body)

        if once:
            _clear_traced_info()

        if g_tracer is None:
            raise RuntimeError('No tracer has been set')

        span = g_tracer.start_span(url, child_of=parent_span)
        span.set_tag('component', 'elasticsearch-py')
        span.set_tag('db.type', 'elasticsearch')
        span.set_tag('span.kind', 'client')
        span.set_tag('elasticsearch.url', url)
        span.set_tag('elasticsearch.method', method)

        if body:
            span.set_tag('db.statement', body)

        try:
            rv = super(TracingTransport, self).perform_request(method, url, params, body)
        except Exception as exc:
            _clear_traced_info() # Discard any tracing info.
            span.set_tag('error', 'true')
            span.set_tag('error.object', exc)
            span.finish()
            raise

        span.finish()

