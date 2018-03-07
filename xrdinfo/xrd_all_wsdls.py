#!/usr/bin/python

from six.moves.queue import Queue, Empty
from threading import Thread, Event, Lock, current_thread
import argparse
import hashlib
import json
import os
import re
import shutil
import six
import sys
import time
import xrdinfo

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
<p>Report time: {report_time}</p>
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
    except OSError:
        pass
    if not os.path.exists(path):
        safe_print(u'Cannot create directory "{}"'.format(path))
        exit(0)


def hash_wsdls(path):
    hashes = {}
    for file_name in os.listdir(path):
        s = re.search('^(\d+)\.wsdl$', file_name)
        if s:
            # Reading as bytes to avoid line ending conversion
            with open(u'{}/{}'.format(path, file_name), 'rb') as fh:
                wsdl = fh.read()
            hashes[file_name] = hashlib.md5(wsdl).hexdigest()
    return hashes


def save_wsdl(path, hashes, wsdl):
    wsdl_hash = hashlib.md5(wsdl.encode('utf-8')).hexdigest()
    max_wsdl = -1
    for file_name in hashes.keys():
        if wsdl_hash == hashes[file_name]:
            # Matching WSDL found
            return file_name, hashes
        s = re.search('^(\d+)\.wsdl$', file_name)
        if s:
            if int(s.group(1)) > max_wsdl:
                max_wsdl = int(s.group(1))
    # Creating new file
    new_file = u'{}.wsdl'.format(int(max_wsdl) + 1)
    # Writing as bytes to avoid line ending conversion
    with open(u'{}/{}'.format(path, new_file), 'wb') as f:
        f.write(wsdl.encode('utf-8'))
    hashes[new_file] = wsdl_hash
    return new_file, hashes


def worker(params):
    while True:
        # Checking periodically if it is the time to gracefully shutdown
        # the worker.
        try:
            subsystem = params['work_queue'].get(True, 0.1)
        except Empty:
            if params['shutdown'].is_set():
                return
            else:
                continue
        try:
            wsdl_rel_path = xrdinfo.stringify(subsystem)
            wsdl_path = u'{}/{}'.format(params['path'], wsdl_rel_path)
            makedirs(wsdl_path)
            hashes = hash_wsdls(wsdl_path)

            method_index = {}
            skip_methods = False
            for method in xrdinfo.methods(
                    addr=params['url'], client=params['client'], producer=subsystem,
                    method='listMethods', timeout=params['timeout'], verify=params['verify'],
                    cert=params['cert']):
                if xrdinfo.stringify(method) in method_index:
                    # Method already found in previous WSDL's
                    continue

                if skip_methods:
                    # Skipping, because previous getWsdl request timed
                    # out
                    if params['verbose']:
                        safe_print(u'{}: {} - SKIPPING\n'.format(
                            current_thread().getName(), xrdinfo.stringify(method)))
                    method_index[xrdinfo.stringify(method)] = 'SKIPPED'
                    continue

                try:
                    wsdl = xrdinfo.wsdl(
                        addr=params['url'], client=params['client'], service=method,
                        timeout=params['timeout'], verify=params['verify'], cert=params['cert'])
                except xrdinfo.RequestTimeoutError:
                    # Skipping all following requests to that subsystem
                    skip_methods = True
                    if params['verbose']:
                        safe_print(u'{}: {} - TIMEOUT\n'.format(
                            current_thread().getName(), xrdinfo.stringify(method)))
                    method_index[xrdinfo.stringify(method)] = 'TIMEOUT'
                    continue
                except xrdinfo.XrdInfoError as e:
                    if params['verbose']:
                        safe_print(u'{}: {} - ERROR:\n{}\n'.format(
                            current_thread().getName(), xrdinfo.stringify(method), e))
                    method_index[xrdinfo.stringify(method)] = ''
                    continue

                wsdl_name, hashes = save_wsdl(wsdl_path, hashes, wsdl)
                txt = u'{}: {}\n'.format(current_thread().getName(), wsdl_name)
                try:
                    for wsdl_method in xrdinfo.wsdl_methods(wsdl):
                        method_full_name = xrdinfo.stringify(subsystem + wsdl_method)
                        method_index[method_full_name] = u'{}/{}'.format(wsdl_rel_path, wsdl_name)
                        txt = txt + u'    {}\n'.format(method_full_name)
                except xrdinfo.XrdInfoError as e:
                    txt = txt + u'WSDL parsing failed: {}\n'.format(e)
                    method_index[xrdinfo.stringify(method)] = ''
                if params['verbose']:
                    safe_print(txt)

            with params['results_lock']:
                params['results'].update(method_index)
        except xrdinfo.XrdInfoError as e:
            if params['verbose']:
                safe_print(u'{}: {} - ERROR:\n{}\n'.format(
                    current_thread().getName(), xrdinfo.stringify(subsystem), e))
        except Exception as e:
            safe_print(u'{}: {}: {}\n'.format(current_thread().getName(), type(e).__name__, e))
        finally:
            params['work_queue'].task_done()


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
    parser.add_argument('path', metavar='PATH', help='path for storing results.')
    parser.add_argument('-v', help='verbose output', action='store_true')
    parser.add_argument('-t', metavar='TIMEOUT', help='timeout for HTTP query', type=float)
    parser.add_argument(
        '--threads', metavar='THREADS', help='amount of threads to use', type=int, default=0)
    parser.add_argument(
        '--verify', metavar='CERT_PATH',
        help='validate peer TLS certificate using CA certificate file.')
    parser.add_argument(
        '--cert', metavar='CERT_PATH', help='use TLS certificate for HTTPS requests.')
    parser.add_argument('--key', metavar='KEY_PATH', help='private key for TLS certificate.')
    parser.add_argument(
        '--instance', metavar='INSTANCE',
        help='use this instance instead of local X-Road instance.')
    args = parser.parse_args()

    params = {
        'verbose': False,
        'path': args.path,
        'url': args.url,
        'client': args.client,
        'instance': None,
        'timeout': DEFAULT_TIMEOUT,
        'verify': False,
        'cert': None,
        'thread_cnt': DEFAULT_THREAD_COUNT,
        'work_queue': Queue(),
        'results': {},
        'results_lock': Lock(),
        'shutdown': Event()
    }

    if args.v:
        params['verbose'] = True

    if six.PY2:
        # Convert to unicode
        params['path'] = params['path'].decode('utf-8')

    makedirs(params['path'])

    if six.PY2:
        # Convert to unicode
        params['client'] = params['client'].decode('utf-8')

    params['client'] = params['client'].split('/')
    if not (len(params['client']) in (3, 4)):
        safe_print(u'Client name is incorrect: "{}"'.format(args.client))
        exit(1)

    if args.instance and six.PY2:
        # Convert to unicode
        params['instance'] = args.instance.decode('utf-8')
    elif args.instance:
        params['instance'] = args.instance

    if args.t:
        params['timeout'] = args.t

    if args.verify:
        params['verify'] = args.verify

    if args.cert and args.key:
        params['cert'] = (args.cert, args.key)

    if args.threads and args.threads > 0:
        params['thread_cnt'] = args.threads

    shared_params = None
    try:
        shared_params = xrdinfo.shared_params_ss(
            addr=args.url, instance=params['instance'], timeout=params['timeout'],
            verify=params['verify'], cert=params['cert'])
    except xrdinfo.XrdInfoError as e:
        safe_print(u'Cannot download Global Configuration: {}'.format(e))
        exit(1)

    # Create and start new threads
    threads = []
    for _ in range(params['thread_cnt']):
        t = Thread(target=worker, args=(params,))
        t.daemon = True
        t.start()
        threads.append(t)

    # Populate the queue
    try:
        for subsystem in xrdinfo.registered_subsystems(shared_params):
            params['work_queue'].put(subsystem)
    except xrdinfo.XrdInfoError as e:
        safe_print(u'Cannot process Global Configuration: {}'.format(e))
        exit(1)

    # Block until all tasks in queue are done
    params['work_queue'].join()

    # Set shutdown event and wait until all daemon processes finish
    params['shutdown'].set()
    for t in threads:
        t.join()

    results = params['results']

    body = ''
    json_data = []
    for key in sorted(results.keys()):
        method = key.split('/')
        json_item = {
            'xRoadInstance': method[0],
            'memberClass': method[1],
            'memberCode': method[2],
            'subsystemCode': method[3],
            'serviceCode': method[4],
            'serviceVersion': method[5],
        }
        if results[key] == 'SKIPPED':
            body = body + u'<p>{} (WSDL skipped due to previous Timeout)</p>\n'.format(
                key, results[key])
            json_item['status'] = 'SKIPPED'
            json_item['wsdl'] = ''
        elif results[key] == 'TIMEOUT':
            body = body + u'<p>{} (WSDL query timed out)</p>\n'.format(key, results[key])
            json_item['status'] = 'TIMEOUT'
            json_item['wsdl'] = ''
        elif results[key]:
            body = body + u'<p>{} (<a href="{}">WSDL</a>)</p>\n'.format(key, results[key])
            json_item['status'] = 'OK'
            json_item['wsdl'] = results[key]
        else:
            body = body + u'<p>{} (Error while downloading or parsing of WSDL)</p>\n'.format(
                key, results[key])
            json_item['status'] = 'ERROR'
            json_item['wsdl'] = ''
        json_data.append(json_item)

    report_time = time.localtime(time.time())
    formatted_time = time.strftime('%Y-%m-%d %H:%M:%S', report_time)
    suffix = time.strftime('%Y%m%d%H%M%S', report_time)
    html = METHODS_HTML_TEMPL.format(report_time=formatted_time, suffix=suffix, body=body)
    with open(u'{}/index_{}.html'.format(args.path, suffix), 'w') as f:
        if six.PY2:
            f.write(html.encode('utf-8'))
        else:
            f.write(html)
    with open(u'{}/index_{}.json'.format(args.path, suffix), 'w') as f:
        if six.PY2:
            f.write(json.dumps(json_data, indent=2, ensure_ascii=False).encode('utf-8'))
        else:
            json.dump(json_data, f, indent=2, ensure_ascii=False)

    history_item = u'<p><a href="{}">{}</a></p>\n'.format(
        u'index_{}.html'.format(suffix), formatted_time)
    try:
        html = u''
        with open(u'{}/history.html'.format(args.path), 'r') as f:
            for line in f:
                if six.PY2:
                    line = line.decode('utf-8')
                if line == HISTORY_HEADER:
                    line = line + history_item
                html = html + line
    except IOError:
        # Cannot open history.html
        html = HISTORY_HTML_TEMPL.format(body=history_item)

    with open(u'{}/history.html'.format(args.path), 'w') as f:
        if six.PY2:
            f.write(html.encode('utf-8'))
        else:
            f.write(html)

    # Replace index with latest report
    shutil.copy(u'{}/index_{}.html'.format(args.path, suffix), u'{}/index.html'.format(args.path))
    shutil.copy(u'{}/index_{}.json'.format(args.path, suffix), u'{}/index.json'.format(args.path))


if __name__ == '__main__':
    main()
