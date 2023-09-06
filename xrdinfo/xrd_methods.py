#!/usr/bin/python3

"""X-Road listMethods and allowedMethods requests."""

import argparse
import sys
import xrdinfo

# By default return listMethods
DEFAULT_METHOD_TYPE = 'listMethods'

# Default timeout for HTTP requests
DEFAULT_TIMEOUT = 5.0


def print_error(content):
    """Error printer."""
    sys.stderr.write(f'ERROR: {content}\n')


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='X-Road listMethods and allowedMethods requests.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='By default peer TLS certificate is not validated.'
    )
    parser.add_argument(
        'url', metavar='SERVER_URL',
        help='URL of local Security Server accepting X-Road requests.')
    parser.add_argument(
        'client', metavar='CLIENT',
        help='Client identifier consisting of slash separated Percent-Encoded parts (e.g. '
             '"INSTANCE/MEMBER_CLASS/MEMBER_CODE/SUBSYSTEM_CODE" '
             'or "INSTANCE/MEMBER_CLASS/MEMBER_CODE").')
    parser.add_argument(
        'service_subsystem', metavar='SERVICE_SUBSYSTEM',
        help='Service subsystem identifier consisting of slash separated Percent-Encoded parts '
             '(e.g. "INSTANCE/MEMBER_CLASS/MEMBER_CODE/SUBSYSTEM_CODE").')
    parser.add_argument('-t', metavar='TIMEOUT', help='timeout for HTTP query', type=float)
    parser.add_argument('--allowed', help='return only allowed methods', action='store_true')
    parser.add_argument('--rest', help='return REST methods instead of SOAP', action='store_true')
    parser.add_argument(
        '--verify', metavar='CERT_PATH',
        help='validate peer TLS certificate using CA certificate file.')
    parser.add_argument(
        '--cert', metavar='CERT_PATH', help='use TLS certificate for HTTPS requests.')
    parser.add_argument('--key', metavar='KEY_PATH', help='private key for TLS certificate.')
    args = parser.parse_args()

    method_type = DEFAULT_METHOD_TYPE
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

    client = xrdinfo.identifier_parts(args.client)
    if not len(client) in (3, 4):
        print_error(f'Client name is incorrect: "{args.client}"')
        sys.exit(1)

    service_subsystem = xrdinfo.identifier_parts(args.service_subsystem)
    if not len(service_subsystem) == 4:
        print_error(f'Service name is incorrect: "{args.service_subsystem}"')
        sys.exit(1)

    try:
        if args.rest:
            for method in xrdinfo.methods_rest(
                    addr=args.url, client=client, producer=service_subsystem, method=method_type,
                    timeout=timeout, verify=verify, cert=cert):
                print(xrdinfo.identifier(method))
        else:
            for method in xrdinfo.methods(
                    addr=args.url, client=client, producer=service_subsystem, method=method_type,
                    timeout=timeout, verify=verify, cert=cert):
                print(xrdinfo.identifier(method))
    except Exception as err:
        print_error(f'{type(err).__name__}: {err}')
        sys.exit(1)


if __name__ == '__main__':
    main()
