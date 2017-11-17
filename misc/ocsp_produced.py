#!/usr/bin/python

from subprocess import Popen, PIPE, check_output
import base64
import os
import re
import time
import xml.etree.ElementTree as ET


cache = {}
for fileName in os.listdir('/var/cache/xroad'):
    if re.match('^.*\.ocsp$', fileName):
        out = check_output(['openssl', 'ocsp', '-noverify', '-text',
            '-respin', '/var/cache/xroad/{}'.format(fileName)]).decode('utf-8')
        r = re.search('^      Serial Number: (.+)$', out, re.MULTILINE)
        if r and r.group(1):
            cache[r.group(1)] = out

with open('/etc/xroad/signer/keyconf.xml', 'r') as keyconf:
    root = ET.fromstring(keyconf.read())
    for key in root.findall('./device/key'):
        type = 'SIGN' if key.attrib['usage'] == 'SIGNING' else 'AUTH'
        keyId = key.find('./keyId').text
        friendlyName = key.find('./friendlyName').text if key.find('./friendlyName').text is not None else ''
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
                        print('{}\t{}\t{}\t{}'.format(produced, type, keyId, friendlyName))
                else:
                    print('ERROR\t{}\t{}\t{}'.format(type, keyId, friendlyName))
