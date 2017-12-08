#!/usr/bin/python

from subprocess import Popen, PIPE, check_output
import argparse
import base64
import calendar
import os
import re
import time
import xml.etree.ElementTree as ET


parser = argparse.ArgumentParser(
    description='Get OCSP production time for X-Road certificates.',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog='Status returns number of seconds since production of oldest OCSP responce.'
)
parser.add_argument('-s', help='Output status only', action="store_true")
args = parser.parse_args()

cache = {}
for fileName in os.listdir('/var/cache/xroad'):
    if re.match('^.*\.ocsp$', fileName):
        out = check_output(['openssl', 'ocsp', '-noverify', '-text',
            '-respin', '/var/cache/xroad/{}'.format(fileName)]).decode('utf-8')
        r = re.search('^      Serial Number: (.+)$', out, re.MULTILINE)
        if r and r.group(1):
            cache[r.group(1)] = out

ocsp_time = 0
with open('/etc/xroad/signer/keyconf.xml', 'r') as keyconf:
    root = ET.fromstring(keyconf.read())
    for key in root.findall('./device/key'):
        type = 'SIGN' if key.attrib['usage'] == 'SIGNING' else 'AUTH'
        keyId = key.find('./keyId').text
        friendlyName = key.find('./friendlyName').text if key.find('./friendlyName') is not None and key.find('./friendlyName').text is not None else ''
        for cert in key.findall('./cert'):
            if not (cert.attrib['active'] == 'true' and cert.find('./status').text == 'registered'):
                continue
            contents = cert.find('./contents').text
            # Adding newlines to base64
            contents = base64.encodestring(base64.decodestring(contents.encode('utf-8'))).decode('utf-8')
            pem = '-----BEGIN CERTIFICATE-----\n{}-----END CERTIFICATE-----\n'.format(contents)
            p = Popen(['openssl', 'x509', '-noout', '-serial'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
            stdout, stderr = p.communicate(pem.encode('utf-8'))
            r = re.match('^serial=(.+)$', stdout.decode('utf-8'))
            if r and r.group(1):
                serial = r.group(1)
                if serial in cache:
                    r = re.search('^    Produced At: (.+)$', cache[serial] , re.MULTILINE)
                    if r and r.group(1):
                        t = time.strptime(r.group(1), '%b %d %H:%M:%S %Y %Z')
                        produced = time.strftime('%Y-%m-%d %H:%M:%S', t)
                        if not args.s:
                            print('{}\t{}\t{}\t{}'.format(produced, type, keyId, friendlyName))
                        elif not ocsp_time or calendar.timegm(t) > ocsp_time:
                            ocsp_time = calendar.timegm(t)
                elif not args.s:
                    print('ERROR\t{}\t{}\t{}'.format(type, keyId, friendlyName))
                else:
                    # One of certificats does not have OCSP responce
                    print(1000000000)
                    exit(0)

if args.s and ocsp_time:
    print(int(time.time()) - ocsp_time)
