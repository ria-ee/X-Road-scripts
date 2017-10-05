#!/usr/bin/python

import argparse
import xrdinfo
import six

# Default timeout for HTTP requests
DEFAULT_TIMEOUT=5.0

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='X-Road allowedMethods request to all members.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='By default peer TLS sertificate is not validated.'
    )
    parser.add_argument('url', metavar='SERVER_URL', help='URL of local Security Server accepting X-Road requests.')
    parser.add_argument('client', metavar='CLIENT', help='slash separated Client identifier (e.g. "INSTANCE/MEMBER_CLASS/MEMBER_CODE/SUBSYSTEM_CODE" or "INSTANCE/MEMBER_CLASS/MEMBER_CODE").')
    parser.add_argument('-t', metavar='TIMEOUT', help='timeout for HTTP query', type=float)
    parser.add_argument('--verify', metavar='CERT_PATH', help='validate peer TLS certificate using CA certificate file.')
    parser.add_argument('--cert', metavar='CERT_PATH', help='use TLS certificate for HTTPS requests.')
    parser.add_argument('--key', metavar='KEY_PATH', help='private key for TLS certificate.')
    parser.add_argument('--instance', metavar='INSTANCE', help='use this instance instead of local X-Road instance.')
    args = parser.parse_args()

    instance = None
    if args.instance:
        instance = args.instance

    timeout = DEFAULT_TIMEOUT
    if args.t:
        timeout = args.t

    verify = False
    if args.verify:
        verify = args.verify

    cert = None
    if args.cert and args.key:
        cert = (args.cert, args.key)

    sharedParams = xrdinfo.sharedParamsSS(addr=args.url, instance=instance, timeout=timeout, verify=verify, cert=cert)

    if six.PY2:
        # Convert to unicode
        args.client = args.client.decode('utf-8')

    client = args.client.split('/')
    if not(len(client) in (3,4)):
        parser.print_help()
        exit(0)

    for subsystem in xrdinfo.registeredSubsystems(sharedParams):
        for method in xrdinfo.methods(addr=args.url, client=client, service=subsystem, method='allowedMethods', timeout=timeout, verify=verify, cert=cert):
            line = u"{}/{}/{}/{}/{}/{}".format(method['xRoadInstance'], method['memberClass'], method['memberCode'], method['subsystemCode'], method['serviceCode'], method['serviceVersion'])
            if six.PY2:
                print(line.encode('utf-8'))
            else:
                print(line)
