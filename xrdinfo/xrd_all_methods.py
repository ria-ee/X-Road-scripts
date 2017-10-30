#!/usr/bin/python

import argparse
import xrdinfo
import six
import sys
from six.moves.queue import Queue
from threading import Thread


# By default return listMethods
METHOD = 'listMethods'

# Default timeout for HTTP requests
DEFAULT_TIMEOUT = 5.0

# Do not use threading by default
DEFAULT_THREAD_COUNT = 1


def print_error(content):
    """Thread safe and unicode safe error printer."""
    content = u"ERROR: {}\n".format(content)
    if six.PY2:
        sys.stderr.write(content.encode('utf-8'))
    else:
        sys.stderr.write(content)


def worker():
    while True:
        subsystem = workQueue.get()
        try:
            for method in xrdinfo.methods(addr=args.url, client=client, service=subsystem, method=METHOD, timeout=timeout, verify=verify, cert=cert):
                line = xrdinfo.stringify(method) + '\n'
                if six.PY2:
                    line = line.encode('utf-8')
                # Using thread safe "write" instead of "print"
                sys.stdout.write(line)
        except Exception as e:
            print_error(u'{}: {}'.format(type(e).__name__, e))
        finally:
            workQueue.task_done()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='X-Road listMethods request to all members.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='By default peer TLS sertificate is not validated.'
    )
    parser.add_argument('url', metavar='SERVER_URL', help='URL of local Security Server accepting X-Road requests.')
    parser.add_argument('client', metavar='CLIENT', help='slash separated Client identifier (e.g. "INSTANCE/MEMBER_CLASS/MEMBER_CODE/SUBSYSTEM_CODE" or "INSTANCE/MEMBER_CLASS/MEMBER_CODE").')
    parser.add_argument('-t', metavar='TIMEOUT', help='timeout for HTTP query', type=float)
    parser.add_argument('--allowed', help='return only allowed methods', action='store_true')
    parser.add_argument('--threads', metavar='THREADS', help='amount of threads to use', type=int)
    parser.add_argument('--verify', metavar='CERT_PATH', help='validate peer TLS certificate using CA certificate file.')
    parser.add_argument('--cert', metavar='CERT_PATH', help='use TLS certificate for HTTPS requests.')
    parser.add_argument('--key', metavar='KEY_PATH', help='private key for TLS certificate.')
    parser.add_argument('--instance', metavar='INSTANCE', help='use this instance instead of local X-Road instance.')
    args = parser.parse_args()

    if args.allowed:
        METHOD = 'allowedMethods'

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

    try:
        sharedParams = xrdinfo.sharedParamsSS(addr=args.url, instance=instance, timeout=timeout, verify=verify, cert=cert)
    except Exception as e:
        print_error(u'{}: {}'.format(type(e).__name__, e))
        exit(1)

    if six.PY2:
        # Convert to unicode
        args.client = args.client.decode('utf-8')

    client = args.client.split('/')
    if not(len(client) in (3,4)):
        print_error(u'Client name is incorrect: "{}"'.format(args.client))
        exit(1)

    threadCnt = DEFAULT_THREAD_COUNT
    if args.threads > 0:
        threadCnt = args.threads

    # Create and start new threads
    workQueue = Queue()
    for i in range(threadCnt):
        t = Thread(target=worker)
        t.daemon = True
        t.start()

    # Populate the queue
    for subsystem in xrdinfo.registeredSubsystems(sharedParams):
        workQueue.put(subsystem)

    # block until all tasks are done
    workQueue.join()
