#!/usr/bin/python

from subprocess import Popen, PIPE
import base64
import re
import time
import xml.etree.ElementTree as ET


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
            p = Popen(['openssl', 'x509', '-noout', '-enddate'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
            stdout, stderr = p.communicate(pem.encode('utf-8'))
            r = re.match('^notAfter=(.+)$', stdout.decode('utf-8'))
            expiration = r.group(1)
            # Convert time format
            t = time.strptime(expiration, '%b %d %H:%M:%S %Y %Z')
            expiration = time.strftime('%Y-%m-%d %H:%M:%S', t)
            print('{}\t{}\t{}\t{}'.format(expiration, type, keyId, friendlyName))
