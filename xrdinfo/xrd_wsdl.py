#!/usr/bin/python3

import argparse
import xrdinfo
import sys

# Default timeout for HTTP requests
DEFAULT_TIMEOUT = 5.0


def print_error(content):
    """Error printer."""
    content = "ERROR: {}\n".format(content)
    sys.stderr.write(content)


def main():
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
        help='slash separated Client identifier (e.g. '
             '"INSTANCE/MEMBER_CLASS/MEMBER_CODE/SUBSYSTEM_CODE" '
             'or "INSTANCE/MEMBER_CLASS/MEMBER_CODE").')
    parser.add_argument(
        'service', metavar='SERVICE',
        help='slash separated Service identifier (e.g. '
             '"INSTANCE/MEMBER_CLASS/MEMBER_CODE/SUBSYSTEM_CODE/SERVICE_CODE/SERVICE_VERSION").')
    parser.add_argument('-t', metavar='TIMEOUT', help='timeout for HTTP query', type=float)
    parser.add_argument(
        '--verify', metavar='CERT_PATH',
        help='validate peer TLS certificate using CA certificate file.')
    parser.add_argument(
        '--cert', metavar='CERT_PATH', help='use TLS certificate for HTTPS requests.')
    parser.add_argument('--key', metavar='KEY_PATH', help='private key for TLS certificate.')
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

    client = args.client.split('/')
    if not (len(client) in (3, 4)):
        print_error('Client name is incorrect: "{}"'.format(args.client))
        exit(1)

    service = args.service.split('/')
    if len(service) == 5:
        service.append('')
    if not (len(service) == 6):
        print_error('Service name is incorrect: "{}"'.format(args.service))
        exit(1)

    wsdl = None
    try:
        wsdl = xrdinfo.wsdl(
            addr=args.url, client=client, service=service, timeout=timeout, verify=verify,
            cert=cert)
    except xrdinfo.XrdInfoError as e:
        print_error(e)
        exit(1)

    print(wsdl)


if __name__ == '__main__':
    main()
