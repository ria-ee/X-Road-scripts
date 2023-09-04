#!/usr/bin/python3

"""Get time of X-Road certificates expiration."""

import argparse
import calendar
import re
import time
from subprocess import Popen, PIPE
from xml.etree import ElementTree


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Get time of X-Road certificates expiration.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Status returns number of seconds until expiration of certificate closest to expiry.'
    )
    parser.add_argument('-s', help='Output status only', action="store_true")
    args = parser.parse_args()

    cert_time = 0
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
                    ['openssl', 'x509', '-noout', '-enddate'],
                    stdin=PIPE, stdout=PIPE, stderr=PIPE) as proc:
                stdout, _ = proc.communicate(pem.encode('utf-8'))
            match = re.match('^notAfter=(.+)$', stdout.decode('utf-8'))
            expiration = match.group(1)
            # Convert time format
            exp_time = time.strptime(expiration, '%b %d %H:%M:%S %Y %Z')
            expiration = time.strftime('%Y-%m-%d %H:%M:%S', exp_time)
            if not args.s:
                print(f'{expiration}\t{key_type}\t{key_id}\t{friendly_name}')
            elif not cert_time or calendar.timegm(exp_time) < cert_time:
                cert_time = calendar.timegm(exp_time)

    if args.s and cert_time:
        if int(time.time()) > cert_time:
            print(0)
        else:
            print(cert_time - int(time.time()))


if __name__ == '__main__':
    main()
