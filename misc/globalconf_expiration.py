#!/usr/bin/python3

"""Get time of X-Road global configuration parts expiration."""

import argparse
import calendar
import json
import os
import re
import time

DEFAULT_GLOBALCONF_PATH = '/etc/xroad/globalconf'


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Get time of X-Road global configuration parts expiration.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Status returns number of seconds until expiration of global configuration part '
               'closest to expiry.'
    )
    parser.add_argument('-s', help='Output status only', action='store_true')
    parser.add_argument('--inst', metavar='INSTANCES',
                        help='filter output with comma separated list of X-Road instances.')
    parser.add_argument('--path', metavar='PATH',
                        help='override the default path to global configuration.')
    args = parser.parse_args()

    instances = ()
    if args.inst:
        instances = args.inst.split(',')

    globalconf_path = DEFAULT_GLOBALCONF_PATH
    if args.path:
        globalconf_path = args.path

    conf_time = 0
    for root, _, files in os.walk(globalconf_path):
        search_inst = re.search(f'^{globalconf_path}/(.+)$', root)
        if search_inst and (len(instances) == 0 or search_inst.group(1) in instances):
            inst = search_inst.group(1)
        else:
            continue
        for file_name in files:
            if file_name.endswith('.metadata'):
                with open(f'{root}/{file_name}', 'r', encoding='utf-8') as metadata_file:
                    data = json.load(metadata_file)
                if data and 'expirationDate' in data:
                    expiration = data['expirationDate']
                    try:
                        # Example: expirationDate = 2017-12-12T10:02:02.000+02:00
                        exp_time = time.strptime(expiration[:-10], '%Y-%m-%dT%H:%M:%S')

                        # convert local time to UTC
                        epoch = calendar.timegm(exp_time) + time.timezone
                        exp_time = time.gmtime(epoch)
                    except ValueError:
                        # Example (6.25+): expirationDate = 2017-12-12T10:02:02Z
                        exp_time = time.strptime(expiration, '%Y-%m-%dT%H:%M:%SZ')

                    expiration = time.strftime('%Y-%m-%d %H:%M:%S', exp_time)

                    if not args.s:
                        # Removing '.metadata' from end of file_name
                        print(f"{expiration}\t{inst}\t{file_name[0:-len('.metadata')]}")
                    elif not conf_time or calendar.timegm(exp_time) < conf_time:
                        conf_time = calendar.timegm(exp_time)

    if args.s:
        if int(time.time()) > conf_time:
            print(0)
        else:
            print(conf_time - int(time.time()))


if __name__ == '__main__':
    main()
