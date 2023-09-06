#!/usr/bin/python3

"""X-Road getWsdl request."""

import argparse
import sys
import xrdinfo

# Default timeout for HTTP requests
DEFAULT_TIMEOUT = 5.0


def print_error(content):
    """Error printer."""
    sys.stderr.write(f'ERROR: {content}\n')


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='X-Road getWsdl request.',
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
        'service', metavar='SERVICE',
        help='Service identifier consisting of slash separated Percent-Encoded parts (e.g. '
             '"INSTANCE/MEMBER_CLASS/MEMBER_CODE/SUBSYSTEM_CODE/SERVICE_CODE/SERVICE_VERSION").')
    parser.add_argument('-t', metavar='TIMEOUT', help='timeout for HTTP query', type=float)
    parser.add_argument(
        '--verify', metavar='CERT_PATH',
        help='validate peer TLS certificate using CA certificate file.')
    parser.add_argument(
        '--cert', metavar='CERT_PATH', help='use TLS certificate for HTTPS requests.')
    parser.add_argument('--key', metavar='KEY_PATH', help='private key for TLS certificate.')
    parser.add_argument(
        '--methods', help='return only method names instead of WSDL', action='store_true')
    args = parser.parse_args()

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

    service = xrdinfo.identifier_parts(args.service)
    if len(service) == 5:
        service.append('')
    if not len(service) == 6:
        print_error(f'Service name is incorrect: "{args.service}"')
        sys.exit(1)

    try:
        wsdl = xrdinfo.wsdl(
            addr=args.url, client=client, service=service, timeout=timeout, verify=verify,
            cert=cert)
        if args.methods:
            for method in xrdinfo.wsdl_methods(wsdl):
                print(xrdinfo.identifier(method))
        else:
            print(wsdl)
    except xrdinfo.XrdInfoError as err:
        print_error(err)
        sys.exit(1)


if __name__ == '__main__':
    main()
