#!/usr/bin/python3

"""Get OCSP production time for X-Road certificates."""

import argparse
import calendar
import os
import re
import sys
import time
from subprocess import Popen, PIPE, check_output
from xml.etree import ElementTree


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Get OCSP production time for X-Road certificates.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Status returns number of seconds since production of oldest OCSP response.'
    )
    parser.add_argument('-s', help='Output status only', action="store_true")
    args = parser.parse_args()

    cache = {}
    for file_name in os.listdir('/var/cache/xroad'):
        if re.match(r'^.*\.ocsp$', file_name):
            out = check_output(
                ['openssl', 'ocsp', '-noverify', '-text', '-respin',
                 f'/var/cache/xroad/{file_name}']).decode('utf-8')
            search = re.search('^ {6}Serial Number: (.+)$', out, re.MULTILINE)
            if search and search.group(1):
                cache[search.group(1)] = out

    ocsp_time = 0
    with open('/etc/xroad/signer/keyconf.xml', 'r', encoding='utf-8') as keyconf:
        root = ElementTree.fromstring(keyconf.read())
        for key in root.findall('./device/key'):
            key_type = 'SIGN' if key.attrib['usage'] == 'SIGNING' else 'AUTH'
            key_id = key.find('./keyId').text
            friendly_name = key.find('./friendlyName').text if \
                key.find('./friendlyName') is not None \
                and key.find('./friendlyName').text is not None else ''
            for cert in key.findall('./cert'):
                if not (cert.attrib['active'] == 'true' and cert.find(
                        './status').text == 'registered'):
                    continue
                contents = cert.find('./contents').text
                # Adding newlines to base64
                contents = '\n'.join([contents[i:i + 76] for i in range(0, len(contents), 76)])
                pem = f'-----BEGIN CERTIFICATE-----\n{contents}\n-----END CERTIFICATE-----\n'
                with Popen(
                        ['openssl', 'x509', '-noout', '-serial'],
                        stdin=PIPE, stdout=PIPE, stderr=PIPE) as proc:
                    stdout, _ = proc.communicate(pem.encode('utf-8'))
                search = re.match('^serial=(.+)$', stdout.decode('utf-8'))
                if search and search.group(1):
                    serial = search.group(1)
                    search = re.search(
                        '^ {4}Produced At: (.+)$', cache.get(serial, ''), re.MULTILINE)
                    if serial in cache and search and re.search(
                            '^ {4}Cert Status: good$', cache.get(serial, ''), re.MULTILINE):
                        produced_time = time.strptime(search.group(1), '%b %d %H:%M:%S %Y %Z')
                        produced = time.strftime('%Y-%m-%d %H:%M:%S', produced_time)
                        if not args.s:
                            print(f'{produced}\t{key_type}\t{key_id}\t{friendly_name}')
                        elif not ocsp_time or calendar.timegm(produced_time) > ocsp_time:
                            ocsp_time = calendar.timegm(produced_time)
                    elif not args.s:
                        print(f'ERROR\t{key_type}\t{key_id}\t{friendly_name}')
                    else:
                        # One of certificates does not have OCSP response
                        print(1000000000)
                        sys.exit(0)

    if args.s and ocsp_time:
        print(int(time.time()) - ocsp_time)


if __name__ == '__main__':
    main()
