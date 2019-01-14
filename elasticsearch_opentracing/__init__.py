import threading
import warnings

from elasticsearch import Transport
from opentracing.ext import tags

g_tracer = None
g_trace_all_requests = False
g_trace_prefix = None

tls = threading.local()

def init_tracing(tracer, trace_all_requests=True, prefix='Elasticsearch'):
    global g_tracer, g_trace_all_requests, g_trace_prefix
    if hasattr(tracer, '_tracer'):
        tracer = tracer._tracer

    g_tracer = tracer
    g_trace_all_requests = trace_all_requests
    g_trace_prefix = prefix

def enable_tracing():
    tls.tracing_enabled = True

def disable_tracing():
    tls.tracing_enabled = False

def get_active_span():
    warnings.warn('get_active_span() is deprecated.  Please use tracer.active_span or the ScopeManager directly.',
                  DeprecationWarning)
    return g_tracer.active_span

def set_active_span(span):
    warnings.warn('set_active_span() is deprecated.  Please use tracer.scope_manager.activate() directly.',
                  DeprecationWarning)
    g_tracer.scope_manager.activate(span, False)

def clear_active_span():
    warnings.warn('clear_active_span() is deprecated.', DeprecationWarning)
    set_active_span(None)

def _get_tracing_enabled():
    if g_trace_all_requests:
        return True

    return getattr(tls, 'tracing_enabled', False)

def _clear_tracing_state():
    tls.tracing_enabled = False
    clear_active_span()

# Values to add as tags from the actual
# payload returned by Elasticsearch, if any.
ResultMembersToAdd = [
    'found',
    'timed_out',
    'took',
]

class TracingTransport(Transport):
    def __init__(self, *args, **kwargs):
        super(TracingTransport, self).__init__(*args, **kwargs)

    def perform_request(self, method, url, params=None, body=None):
        if not _get_tracing_enabled():
            return super(TracingTransport, self).perform_request(method, url, params, body)

        if g_tracer is None:
            raise RuntimeError('No tracer has been set')

        op_name = url
        if g_trace_prefix is not None:
            op_name = str(g_trace_prefix) + url

        with g_tracer.start_active_span(op_name) as scope:
            span = scope.span
            span.set_tag(tags.COMPONENT, 'elasticsearch-py')
            span.set_tag(tags.DATABASE_TYPE, 'elasticsearch')
            span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
            span.set_tag('elasticsearch.url', url)
            span.set_tag('elasticsearch.method', method)

            if body:
                span.set_tag(tags.DATABASE_STATEMENT, body)
            if params:
                span.set_tag('elasticsearch.params', params)

            try:
                rv = super(TracingTransport, self).perform_request(method, url, params, body)
            except Exception as exc:
                span.set_tag('error', True)
                span.set_tag('error.object', exc)
                raise

            if isinstance(rv, dict):
                for member in ResultMembersToAdd:
                    if member in rv:
                        span.set_tag('elasticsearch.{0}'.format(member), str(rv[member]))
            return rv
