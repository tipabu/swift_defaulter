"""
Middleware to set default headers for PUT requests.

Users can POST X-Default-Object-* headers to accounts and containers to set
default headers for object PUTs, or X-Default-Container-* headers to accounts
to set defaults for container PUTs. For example, after the sequence

    POST /v1/acct
    X-Default-Object-X-Delete-After: 2592000

    POST /v1/acct/foo
    X-Default-Object-X-Delete-After: 86400

    PUT /v1/acct/foo/o1

    PUT /v1/acct/foo/o2
    X-Default-Object-X-Delete-After: 3600

    PUT /v1/acct/bar/o

    PUT /v1/acct/baz/o

    POST /v1/acct/baz/o
    X-Remove-Delete-At: 1

    PUT /v1/other_acct/quux/o

... acct/foo/o1 will automatically be deleted after 24 hours, acct/foo/o2 will
automatically be deleted after one hour, and acct/bar/o will be deleted after
30 days. Of course, other_acct/quux/o will not be automatically deleted, and
none of this prevents you from later changing or (as with acct/baz/o) removing
the default header.

Configuration options:
    ``use_formatting``
        If true, expose {account}, {container}, and {object} formatting
        variables. This can be useful for example, for setting
        X-Default-Container-X-Versions-Location: .{container}_versions
        Default: False
    ``default-account-*``
    ``default-container-*``
    ``default-object-*``
        Set defaults across the entire cluster.

Requires Swift >= 1.12.0
"""
from swift.common.request_helpers import get_sys_meta_prefix
from swift.common.swob import wsgify
from swift.common.utils import config_true_value
from swift.common.utils import register_swift_info
from swift.proxy.controllers.base import get_account_info
from swift.proxy.controllers.base import get_container_info


class DefaulterMiddleware(object):
    def __init__(self, app, config):
        self.app = app
        self.conf = config

    @wsgify
    def __call__(self, req):
        try:
            vers, acct, cont, obj = req.split_path(2, 4, True)
        except ValueError:
            # /info request, or something similar
            return self.app

        handler = getattr(self, 'do_%s' % req.method.lower(), None)
        if not callable(handler):
            handler = self.get_response_and_translate

        if obj is not None:
            req_type = 'object'
        elif cont is not None:
            req_type = 'container'
        elif acct is not None:
            req_type = 'account'

        return handler(req, req_type)

    def get_response_and_translate(self, req, req_type):
        resp = req.get_response(self.app)
        prefix = get_sys_meta_prefix(req_type) + 'default-'
        for header, value in resp.headers.items():
            if header.lower().startswith(prefix):
                client_header = 'x-default-%s' % header[len(prefix):]
                resp.headers[client_header] = value
        return resp

    def do_post(self, req, req_type):
        if req_type == 'object':
            return self.get_response_and_translate(req, req_type)

        subresources = {
            'account': ('container', 'object'),
            'container': ('object', ),
        }.get(req_type, ())

        header_formats = (
            ('x-default-%s-', False),
            ('x-remove-default-%s-', True),
        )
        for header_format, clear in header_formats:
            for header, value in req.headers.items():
                for subresource in subresources:
                    prefix = header_format % subresource
                    if header.lower().startswith(prefix):
                        sysmeta_header = '%sdefault-%s-%s' % (
                            get_sys_meta_prefix(req_type),
                            subresource,
                            header[len(prefix):])
                        req.headers[sysmeta_header] = '' if clear else value

        return self.get_response_and_translate(req, req_type)

    # TODO: consider adding a copy hook for COPY and versioning
    def do_put(self, req, req_type):
        # We've already done this once, so we know we'll succeed
        vers, acct, cont, obj = req.split_path(2, 4, True)
        format_args = {}
        if acct is not None:
            format_args['account'] = acct

        if cont is not None:
            format_args['container'] = cont

        if obj is not None:
            format_args['object'] = obj

        for key, val in self.get_defaults(req, req_type, format_args).items():
            req.headers.setdefault(key, val)

        # Once we've set the defaults, we just follow the POST flow
        return self.do_post(req, req_type)

    def get_defaults(self, req, req_type, format_args):
        acct_sysmeta = get_account_info(req.environ, self.app)['sysmeta']
        if req_type == 'object':
            cont_sysmeta = get_container_info(req.environ, self.app)['sysmeta']
        else:
            cont_sysmeta = {}

        defaults = {}
        prefix = 'default-%s-' % req_type
        for src in (self.conf, acct_sysmeta, cont_sysmeta):
            for key, value in src.items():
                if key.lower().startswith(prefix):
                    if self.conf['use_formatting']:
                        try:
                            value = value.format(**format_args)
                        except KeyError:
                            # This user may not have specified the default;
                            # don't fail because of someone else
                            pass
                    defaults[key[len(prefix):]] = value
        return defaults


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    conf['use_formatting'] = config_true_value(conf.get(
        'use_formatting', False))

    register_swift_info('defaulter', **conf)

    def filt(app):
        return DefaulterMiddleware(app, conf)
    return filt
