#!/usr/bin/python

import argparse
import xrdinfo
import six

# Default timeout for HTTP requests
DEFAULT_TIMEOUT=5.0

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='List IP addresses of X-Road Security Servers.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='You need to provide either Security Server or Central Server address.\n\n'
            'NB! Global configuration signature is not validated when using Central Server.\n'
            'Use local Security Server whenever possible.'
    )
    parser.add_argument('-s', metavar='SECURITY_SERVER', help='DNS name/IP/URL of local Security Server')
    parser.add_argument('-c', metavar='CENTRAL_SERVER', help='DNS name/IP/URL of Central Server/Configuration Proxy')
    parser.add_argument('-t', metavar='TIMEOUT', help='timeout for HTTP query', type=float)
    parser.add_argument('--verify', metavar='CERT_PATH', help='validate peer TLS certificate using CA certificate file.')
    parser.add_argument('--cert', metavar='CERT_PATH', help='use TLS certificate for HTTPS requests.')
    parser.add_argument('--key', metavar='KEY_PATH', help='private key for TLS certificate.')
    parser.add_argument('--instance', metavar='INSTANCE', help='use this instance instead of local X-Road instance (works only with "-s" argument)')
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

    sharedParams = b''
    if args.s:
        sharedParams = xrdinfo.sharedParamsSS(addr=args.s, instance=instance, timeout=timeout, verify=verify, cert=cert)
    elif args.c:
        sharedParams = xrdinfo.sharedParamsCS(addr=args.c, timeout=timeout, verify=verify, cert=cert)
    else:
        parser.print_help()
        exit(0)

    for ip in xrdinfo.serversIPs(sharedParams):
        print(ip)