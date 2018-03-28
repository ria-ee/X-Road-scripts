#!/usr/bin/python

from subprocess import Popen, PIPE
import argparse
import calendar
import re
import time
import xml.etree.ElementTree as ElementTree

parser = argparse.ArgumentParser(
    description='Get time of X-Road certificates expiration.',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog='Status returns number of seconds until expiration of certificate closest to expiry.'
)
parser.add_argument('-s', help='Output status only', action="store_true")
args = parser.parse_args()

cert_time = 0
with open('/etc/xroad/signer/keyconf.xml', 'r') as keyconf:
    root = ElementTree.fromstring(keyconf.read())
    for key in root.findall('./device/key'):
        key_type = 'SIGN' if key.attrib['usage'] == 'SIGNING' else 'AUTH'
        key_id = key.find('./keyId').text
        friendly_name = key.find('./friendlyName').text if key.find(
            './friendlyName') is not None and key.find('./friendlyName').text is not None else ''
        for cert in key.findall('./cert'):
            if not (cert.attrib['active'] == 'true' and cert.find(
                    './status').text == 'registered'):
                continue
            contents = cert.find('./contents').text
            # Adding newlines to base64
            contents = '\n'.join([contents[i:i + 76] for i in range(0, len(contents), 76)])
            pem = '-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n'.format(contents)
            p = Popen(
                ['openssl', 'x509', '-noout', '-enddate'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
            stdout, stderr = p.communicate(pem.encode('utf-8'))
            r = re.match('^notAfter=(.+)$', stdout.decode('utf-8'))
            expiration = r.group(1)
            # Convert time format
            t = time.strptime(expiration, '%b %d %H:%M:%S %Y %Z')
            expiration = time.strftime('%Y-%m-%d %H:%M:%S', t)
            if not args.s:
                print('{}\t{}\t{}\t{}'.format(expiration, key_type, key_id, friendly_name))
            elif not cert_time or calendar.timegm(t) < cert_time:
                cert_time = calendar.timegm(t)

if args.s and cert_time:
    if int(time.time()) > cert_time:
        print(0)
    else:
        print(cert_time - int(time.time()))
