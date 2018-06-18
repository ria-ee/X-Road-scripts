#!/usr/bin/python

import argparse
import xrdinfo
import six
import sys

# By default return listMethods
METHOD_TYPE = 'listMethods'

# Default timeout for HTTP requests
DEFAULT_TIMEOUT = 5.0


def print_error(content):
    """Thread safe and unicode safe error printer."""
    content = u"ERROR: {}\n".format(content)
    if six.PY2:
        sys.stderr.write(content.encode('utf-8'))
    else:
        sys.stderr.write(content)


def main():
    parser = argparse.ArgumentParser(
        description='X-Road listMethods request.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='By default peer TLS certificate is not validated.'
    )
    parser.add_argument(
        'url', metavar='SERVER_URL',
        help='URL of local Security Server accepting X-Road requests.')
    parser.add_argument(
        'client', metavar='CLIENT',
        help='slash separated Client identifier (e.g. '
             '"INSTANCE/MEMBER_CLASS/MEMBER_CODE/SUBSYSTEM_CODE" '
             'or "INSTANCE/MEMBER_CLASS/MEMBER_CODE").')
    parser.add_argument(
        'service', metavar='SERVICE',
        help='slash separated Service identifier (e.g. '
             '"INSTANCE/MEMBER_CLASS/MEMBER_CODE/SUBSYSTEM_CODE").')
    parser.add_argument('-t', metavar='TIMEOUT', help='timeout for HTTP query', type=float)
    parser.add_argument('--allowed', help='return only allowed methods', action='store_true')
    parser.add_argument(
        '--verify', metavar='CERT_PATH',
        help='validate peer TLS certificate using CA certificate file.')
    parser.add_argument(
        '--cert', metavar='CERT_PATH', help='use TLS certificate for HTTPS requests.')
    parser.add_argument('--key', metavar='KEY_PATH', help='private key for TLS certificate.')
    args = parser.parse_args()

    method_type = METHOD_TYPE
    if args.allowed:
        method_type = 'allowedMethods'

    timeout = DEFAULT_TIMEOUT
    if args.t:
        timeout = args.t

    verify = False
    if args.verify:
        verify = args.verify

    cert = None
    if args.cert and args.key:
        cert = (args.cert, args.key)

    if six.PY2:
        # Convert to unicode
        args.client = args.client.decode('utf-8')
        args.service = args.service.decode('utf-8')

    client = args.client.split('/')
    if not (len(client) in (3, 4)):
        print_error(u'Client name is incorrect: "{}"'.format(args.client))
        exit(1)

    service = args.service.split('/')
    if not (len(service) == 4):
        print_error(u'Service name is incorrect: "{}"'.format(args.service))
        exit(1)

    try:
        for method in xrdinfo.methods(
                addr=args.url, client=client, producer=service, method=method_type,
                timeout=timeout, verify=verify, cert=cert):
            line = xrdinfo.stringify(method)
            if six.PY2:
                print(line.encode('utf-8'))
            else:
                print(line)
    except Exception as e:
        print_error(u'{}: {}'.format(type(e).__name__, e))
        exit(1)


if __name__ == '__main__':
    main()
