#!/usr/bin/python

from six.moves.queue import Queue
from threading import Thread, Lock, currentThread
import argparse
import hashlib
import itertools
import json
import os
import re
import shutil
import six
import sys
import time
import xrdinfo


# Verbosity of output
VERBOSE = False

# Default timeout for HTTP requests
DEFAULT_TIMEOUT = 5.0

# Do not use threading by default
DEFAULT_THREAD_COUNT = 1

METHODS_HTML_TEMPL = u"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>All methods with WSDL descriptions</title>
</head>
<body>
<h1>All methods with WSDL descriptions</h1>
<p>Report time: {repTime}</p>
<p><a href="history.html">History</a></p>
<p>Latest data in <a href="index.json">JSON</a> form.</p>
<p>This report in <a href="index_{suffix}.json">JSON</a> form.</p>
{body}</body>
</html>
"""

HISTORY_HTML_TEMPL = u"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>History</title>
</head>
<body>
<h1>History</h1>
{body}</body>
</html>
"""

HISTORY_HEADER = u'<h1>History</h1>\n'


def safe_print(content):
    """Thread safe and unicode safe debug printer."""
    content = u'{}\n'.format(content)
    if six.PY2:
        # Using thread safe "write" instead of "print"
        sys.stdout.write(content.encode('utf-8'))
    else:
        sys.stdout.write(content)


def makedirs(path):
    try:
        os.makedirs(path)
    except OSError as e:
        pass
    if not os.path.exists(path):
        safe_print(u'Cannot create directory "{}"'.format(path))
        exit(0)


def hashWsdls(path):
    hashes = {}
    for fileName in os.listdir(path):
        s = re.search('^(\d+)\.wsdl$', fileName)
        if s:
            # Reading as bytes to avoid line ending conversion
            with open(u'{}/{}'.format(path, fileName), 'rb') as f:
                wsdl = f.read()
            hashes[fileName] = hashlib.md5(wsdl).hexdigest()
    return hashes


def saveWsdl(path, hashes, wsdl):
    wsdlHash = hashlib.md5(wsdl.encode('utf-8')).hexdigest()
    maxWsdl = -1
    for fileName in hashes.keys():
        if wsdlHash == hashes[fileName]:
            # Matching WSDL found
            return fileName, hashes
        s = re.search('^(\d+)\.wsdl$', fileName)
        if s:
            if int(s.group(1)) > maxWsdl:
                maxWsdl = int(s.group(1))
    # Creating new file
    newFile = u'{}.wsdl'.format(int(maxWsdl) + 1)
    # Writing as bytes to avoid line ending conversion
    with open(u'{}/{}'.format(path, newFile), 'wb') as f:
        f.write(wsdl.encode('utf-8'))
    hashes[newFile] = wsdlHash
    return newFile, hashes


def worker():
    while True:
        subsystem = workQueue.get()
        try:
            wsdlRelPath = xrdinfo.stringify(subsystem)
            wsdlPath = u'{}/{}'.format(args.path, wsdlRelPath)
            makedirs(wsdlPath)
            hashes = hashWsdls(wsdlPath)

            methodIndex = {}
            skipMethods = False
            for method in xrdinfo.methods(addr=args.url, client=client, service=subsystem, method='listMethods', timeout=timeout, verify=verify, cert=cert):
                if xrdinfo.stringify(method) in methodIndex:
                    # Method already found in previous WSDL's
                    continue

                if skipMethods:
                    # Skipping, because previous getWsdl request timed out
                    if VERBOSE:
                        safe_print(u'{}: {} - SKIPPING\n'.format(currentThread().getName(), xrdinfo.stringify(method)))
                    methodIndex[xrdinfo.stringify(method)] = 'SKIPPED'
                    continue

                try:
                    wsdl = xrdinfo.wsdl(addr=args.url, client=client, service=method, timeout=timeout, verify=verify, cert=cert)
                except xrdinfo.TimeoutError as e:
                    # Skipping all following requests to that subsystem
                    skipMethods = True
                    if VERBOSE:
                        safe_print(u'{}: {} - TIMEOUT\n'.format(currentThread().getName(), xrdinfo.stringify(method)))
                    methodIndex[xrdinfo.stringify(method)] = 'TIMEOUT'
                    continue
                except xrdinfo.XrdInfoError as e:
                    if VERBOSE:
                        safe_print(u'{}: {} - ERROR:\n{}\n'.format(currentThread().getName(), xrdinfo.stringify(method), e))
                    methodIndex[xrdinfo.stringify(method)] = ''
                    continue

                # TODO: update hashes!!!
                wsdlName, hashes = saveWsdl(wsdlPath, hashes, wsdl)
                txt = u'{}: {}\n'.format(currentThread().getName(), wsdlName)
                try:
                    for wsdlMethod in xrdinfo.wsdlMethods(wsdl):
                        methodFullName = xrdinfo.stringify(subsystem + wsdlMethod)
                        methodIndex[methodFullName] = u'{}/{}'.format(wsdlRelPath, wsdlName)
                        txt = txt + u'    {}\n'.format(methodFullName)
                except xrdinfo.XrdInfoError as e:
                    txt = txt + u'WSDL parsing failed: {}\n'.format(e)
                    methodIndex[xrdinfo.stringify(method)] = ''
                if VERBOSE:
                    safe_print(txt)
                    
            with resultsLock:
                results.update(methodIndex)
        except xrdinfo.XrdInfoError as e:
            if VERBOSE:
                safe_print(u'{}: {} - ERROR:\n{}\n'.format(currentThread().getName(), xrdinfo.stringify(subsystem), e))
        except Exception as e:
            safe_print(u'{}: {}: {}\n'.format(currentThread().getName(), type(e).__name__, e))
        finally:
            workQueue.task_done()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='X-Road getWsdl request.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='By default peer TLS sertificate is not validated.'
    )
    parser.add_argument('url', metavar='SERVER_URL', help='URL of local Security Server accepting X-Road requests.')
    parser.add_argument('client', metavar='CLIENT', help='slash separated Client identifier (e.g. "INSTANCE/MEMBER_CLASS/MEMBER_CODE/SUBSYSTEM_CODE" or "INSTANCE/MEMBER_CLASS/MEMBER_CODE").')
    parser.add_argument('path', metavar='PATH', help='path for storing results.')
    parser.add_argument('-v', help='verbose output', action='store_true')
    parser.add_argument('-t', metavar='TIMEOUT', help='timeout for HTTP query', type=float)
    parser.add_argument('--threads', metavar='THREADS', help='amount of threads to use', type=int)
    parser.add_argument('--verify', metavar='CERT_PATH', help='validate peer TLS certificate using CA certificate file.')
    parser.add_argument('--cert', metavar='CERT_PATH', help='use TLS certificate for HTTPS requests.')
    parser.add_argument('--key', metavar='KEY_PATH', help='private key for TLS certificate.')
    parser.add_argument('--instance', metavar='INSTANCE', help='use this instance instead of local X-Road instance.')
    args = parser.parse_args()

    if args.v:
        VERBOSE = True

    if six.PY2:
        # Convert to unicode
        args.path = args.path.decode('utf-8')

    makedirs(args.path)

    instance = None
    if args.instance and six.PY2:
        # Convert to unicode
        instance = args.instance.decode('utf-8')
    elif args.instance:
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

    if six.PY2:
        # Convert to unicode
        args.client = args.client.decode('utf-8')

    client = args.client.split('/')
    if not(len(client) in (3,4)):
        safe_print(u'Client name is incorrect: "{}"'.format(args.client))
        exit(1)

    try:
        sharedParams = xrdinfo.sharedParamsSS(addr=args.url, instance=instance, timeout=timeout, verify=verify, cert=cert)
    except xrdinfo.XrdInfoError as e:
        safe_print(u'Cannot download Global Configuration: {}'.format(e))
        exit(1)

    threadCnt = DEFAULT_THREAD_COUNT
    if args.threads > 0:
        threadCnt = args.threads

    results = {}
    resultsLock = Lock()

    # Create and start new threads
    workQueue = Queue()
    for i in range(threadCnt):
        t = Thread(target=worker)
        t.daemon = True
        t.start()

    # Populate the queue
    try:
        for subsystem in xrdinfo.registeredSubsystems(sharedParams):
            workQueue.put(subsystem)
    except xrdinfo.XrdInfoError as e:
        safe_print(u'Cannot process Global Configuration: {}'.format(e))
        exit(1)

    # block until all tasks are done
    workQueue.join()

    body = ''
    jsonData = []
    for key in sorted(results.keys()):
        method = key.split('/')
        jsonItem = {
            'xRoadInstance': method[0],
            'memberClass': method[1],
            'memberCode': method[2],
            'subsystemCode': method[3],
            'serviceCode': method[4],
            'serviceVersion': method[5],
        }
        if results[key] == 'SKIPPED':
            body = body + u'<p>{} (WSDL skipped due to previous Timeout)</p>\n'.format(key, results[key])
            jsonItem['status'] = 'SKIPPED'
            jsonItem['wsdl'] = ''
        elif results[key] == 'TIMEOUT':
            body = body + u'<p>{} (WSDL query timed out)</p>\n'.format(key, results[key])
            jsonItem['status'] = 'TIMEOUT'
            jsonItem['wsdl'] = ''
        elif results[key]:
            body = body + u'<p>{} (<a href="{}">WSDL</a>)</p>\n'.format(key, results[key])
            jsonItem['status'] = 'OK'
            jsonItem['wsdl'] = results[key]
        else:
            body = body + u'<p>{} (Error while downloading or parsing of WSDL)</p>\n'.format(key, results[key])
            jsonItem['status'] = 'ERROR'
            jsonItem['wsdl'] = ''
        jsonData.append(jsonItem)

    repTime = time.localtime(time.time())
    formatedTime = time.strftime('%Y-%m-%d %H:%M:%S', repTime)
    suffix = time.strftime('%Y%m%d%H%M%S', repTime)
    html = METHODS_HTML_TEMPL.format(repTime=formatedTime, suffix=suffix, body=body)
    with open(u'{}/index_{}.html'.format(args.path, suffix), 'w') as f:
        if six.PY2:
            f.write(html.encode('utf-8'))
        else:
            f.write(html)
    with open(u'{}/index_{}.json'.format(args.path, suffix), 'w') as f:
        if six.PY2:
            f.write(json.dumps(jsonData, indent=2, ensure_ascii=False).encode('utf-8'))
        else:
            json.dump(jsonData, f, indent=2, ensure_ascii=False)

    historyItem = u'<p><a href="{}">{}</a></p>\n'.format(u'index_{}.html'.format(suffix), formatedTime)
    try:
        html = u''
        with open(u'{}/history.html'.format(args.path), 'r') as f:
            for line in f:
                if six.PY2:
                    line = line.decode('utf-8')
                if line == HISTORY_HEADER:
                    line = line + historyItem
                html = html + line
    except Exception as e:
        # Cannot open or parse history.html
        html = HISTORY_HTML_TEMPL.format(repTime=formatedTime, body=historyItem)

    with open(u'{}/history.html'.format(args.path), 'w') as f:
        if six.PY2:
            f.write(html.encode('utf-8'))
        else:
            f.write(html)

    # Replace index with latest report
    shutil.copy(u'{}/index_{}.html'.format(args.path, suffix), u'{}/index.html'.format(args.path))
    shutil.copy(u'{}/index_{}.json'.format(args.path, suffix), u'{}/index.json'.format(args.path))
