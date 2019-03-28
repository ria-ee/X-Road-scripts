#!/usr/bin/python

import argparse
import calendar
import json
import os
import re
import six
import time

DEFAULT_GLOBALCONF_PATH = '/etc/xroad/globalconf'

parser = argparse.ArgumentParser(
    description='Get time of X-Road global configuration parts expiration.',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog='Status returns number of seconds until expiration of global configuration part closest'
           ' to expiry.'
)
parser.add_argument('-s', help='Output status only', action='store_true')
parser.add_argument('--inst', metavar='INSTANCES',
                    help='filter output with comma separated list of X-Road instances.')
parser.add_argument('--path', metavar='PATH',
                    help='override the default path to global configuration.')
args = parser.parse_args()

instances = ()
if args.inst and six.PY2:
    # Convert to unicode
    instances = args.inst.decode('utf-8').split(',')
elif args.inst:
    instances = args.inst.split(',')

globalconf_path = DEFAULT_GLOBALCONF_PATH
if args.path and six.PY2:
    # Convert to unicode
    globalconf_path = args.path.decode('utf-8')
elif args.path:
    globalconf_path = args.path

conf_time = 0
for item in os.walk(globalconf_path):
    path = item[0]
    s = re.search('^{}/(.+)$'.format(globalconf_path), path)
    if s and s.group(1) in instances:
        inst = s.group(1)
    else:
        continue
    for fileName in item[2]:
        if fileName.endswith('.metadata'):
            with open('{}/{}'.format(path, fileName), 'r') as f:
                data = json.load(f)
                if data and 'expirationDate' in data:
                    # Example: expirationDate = 2017-12-12T10:02:02.000+02:00
                    expiration = data['expirationDate']
                    t = time.strptime(expiration[:-10], '%Y-%m-%dT%H:%M:%S')

                    # convert local time to UTC
                    epoch = calendar.timegm(t) + time.timezone
                    t = time.gmtime(epoch)
                    expiration = time.strftime('%Y-%m-%d %H:%M:%S', t)

                    s = re.search('^(.+).metadata$', fileName)
                    if not args.s:
                        print('{}\t{}\t{}'.format(expiration, inst, s.group(1)))
                    elif not conf_time or calendar.timegm(t) < conf_time:
                        conf_time = calendar.timegm(t)

if args.s:
    if int(time.time()) > conf_time:
        print(0)
    else:
        print(conf_time - int(time.time()))
