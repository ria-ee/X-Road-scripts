#!/usr/bin/python

"""X-Road informational module."""

import re
import requests
import socket
import six.moves.urllib.parse as urlparse
import sys
import uuid
import xml.etree.ElementTree as ET
import zipfile
from six import BytesIO

# For module developement
DEBUG = False

# Timeout for requests
DEFAULT_TIMEOUT = 5.0

METHODS_MEMBER_TEMPL = u"""<?xml version="1.0" encoding="utf-8"?>
<SOAP-ENV:Envelope
        xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:xroad="http://x-road.eu/xsd/xroad.xsd"
        xmlns:id="http://x-road.eu/xsd/identifiers">
    <SOAP-ENV:Header>
        <xroad:client id:objectType="MEMBER">
            <id:xRoadInstance>{client[0]}</id:xRoadInstance>
            <id:memberClass>{client[1]}</id:memberClass>
            <id:memberCode>{client[2]}</id:memberCode>
        </xroad:client>
        <xroad:service id:objectType="SERVICE">
            <id:xRoadInstance>{service[0]}</id:xRoadInstance>
            <id:memberClass>{service[1]}</id:memberClass>
            <id:memberCode>{service[2]}</id:memberCode>
            <id:subsystemCode>{service[3]}</id:subsystemCode>
            <id:serviceCode>{method}</id:serviceCode>
        </xroad:service>
        <xroad:id>{uuid}</xroad:id>
        <xroad:protocolVersion>4.0</xroad:protocolVersion>
    </SOAP-ENV:Header>
    <SOAP-ENV:Body>
        <xroad:{method}/>
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
"""

METHODS_SUBSYSTEM_TEMPL = u"""<?xml version="1.0" encoding="utf-8"?>
<SOAP-ENV:Envelope
        xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:xroad="http://x-road.eu/xsd/xroad.xsd"
        xmlns:id="http://x-road.eu/xsd/identifiers">
    <SOAP-ENV:Header>
        <xroad:client id:objectType="SUBSYSTEM">
            <id:xRoadInstance>{client[0]}</id:xRoadInstance>
            <id:memberClass>{client[1]}</id:memberClass>
            <id:memberCode>{client[2]}</id:memberCode>
            <id:subsystemCode>{client[3]}</id:subsystemCode>
        </xroad:client>
        <xroad:service id:objectType="SERVICE">
            <id:xRoadInstance>{service[0]}</id:xRoadInstance>
            <id:memberClass>{service[1]}</id:memberClass>
            <id:memberCode>{service[2]}</id:memberCode>
            <id:subsystemCode>{service[3]}</id:subsystemCode>
            <id:serviceCode>{method}</id:serviceCode>
        </xroad:service>
        <xroad:id>{uuid}</xroad:id>
        <xroad:protocolVersion>4.0</xroad:protocolVersion>
    </SOAP-ENV:Header>
    <SOAP-ENV:Body>
        <xroad:{method}/>
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
"""

# Namespace of X-Road schema
NS = {'xrd': 'http://x-road.eu/xsd/xroad.xsd', 'id': 'http://x-road.eu/xsd/identifiers'}


def sharedParamsSS(addr, instance=None, timeout=DEFAULT_TIMEOUT, verify=False, cert=None):
    """Get shared-params.xml content (encoded bytes) from local Security Server and return encoded bytes.
       By default return info about local X-Road instance.
    """
    try:
        url = addr
        # Add HTTP/HTTPS scheme if missing
        if not urlparse.urlsplit(url).scheme and (verify or cert):
            url = "https://"+url
        elif not urlparse.urlsplit(url).scheme:
            url = "http://"+url
        # Add "/verificationconf" if path is missing
        if urlparse.urlsplit(url).path == '':
            url = url + "/verificationconf"
        elif urlparse.urlsplit(url).path == '/':
            url = url + "verificationconf"
        verConfResponse = requests.get(url, timeout=timeout, verify=verify, cert=cert)
        verConfResponse.raise_for_status()
        zipData = BytesIO()
        zipData.write(verConfResponse.content)
        verConfZip = zipfile.ZipFile(zipData)
        ident = instance
        if ident is None:
            # Use local instance configuration
            identFile = verConfZip.open('verificationconf/instance-identifier')
            ident = identFile.read()
            ident = ident.decode('utf-8')
        sharedParamsFile = verConfZip.open('verificationconf/{}/shared-params.xml'.format(ident))
        sharedParams = sharedParamsFile.read()
        return sharedParams
    except (requests.exceptions.RequestException, zipfile.BadZipfile, KeyError) as e:
        if DEBUG:
            sys.stderr.write("WARNING: {}: {}\n".format(type(e).__name__, e))
        return None

def sharedParamsCS(addr, timeout=DEFAULT_TIMEOUT, verify=False, cert=None):
    """Get shared-params.xml content (encoded bytes) from Central Server/Configuration Proxy and return encoded bytes."""
    try:
        url = addr
        # Add HTTP/HTTPS scheme if missing
        if not urlparse.urlsplit(url).scheme and (verify or cert):
            url = "https://"+url
        elif not urlparse.urlsplit(url).scheme:
            url = "http://"+url
        # Add "/internalconf" if path is missing
        if urlparse.urlsplit(url).path == '':
            url = url + "/internalconf"
        elif urlparse.urlsplit(url).path == '/':
            url = url + "internalconf"
        globalConf = requests.get(url, timeout=timeout, verify=verify, cert=cert)
        globalConf.raise_for_status()
        # Configuration Proxy uses lowercase for "Content-location"
        s = re.search("Content-location: (/.+/shared-params.xml)", globalConf.text, re.IGNORECASE)
        url2 = urlparse.urljoin(url, s.group(1))
        sharedParamsResponse = requests.get(url2, timeout=timeout, verify=verify, cert=cert)
        sharedParamsResponse.raise_for_status()
        sharedParams = sharedParamsResponse.content
        return sharedParams
    except (requests.exceptions.RequestException, AttributeError) as e:
        if DEBUG:
            sys.stderr.write("WARNING: {}: {}\n".format(type(e).__name__, e))
        return

def subsystems(sharedParams):
    """List Subsystems in sharedParams."""
    try:
        root = ET.fromstring(sharedParams)    # ET.fromstring wants encoded bytes as input (PY2)
        instance = root.find("./instanceIdentifier").text
        for member in root.findall("./member"):
            memberClass = member.find("./memberClass/code").text
            memberCode = member.find("./memberCode").text
            for subsystem in member.findall("./subsystem"):
                subsystemCode = subsystem.find("./subsystemCode").text
                yield (instance, memberClass, memberCode, subsystemCode)
    except (TypeError, AttributeError) as e:
        if DEBUG:
            sys.stderr.write("WARNING: {}: {}\n".format(type(e).__name__, e))
        return

def subsystemsWithMembername(sharedParams):
    """List Subsystems in sharedParams with Member name."""
    try:
        root = ET.fromstring(sharedParams)    # ET.fromstring wants encoded bytes as input (PY2)
        instance = root.find("./instanceIdentifier").text
        for member in root.findall("./member"):
            memberId = member.attrib["id"]
            memberClass = member.find("./memberClass/code").text
            memberCode = member.find("./memberCode").text
            memberName = member.find("./name").text
            for subsystem in member.findall("./subsystem"):
                subsystemCode = subsystem.find("./subsystemCode").text
                yield (instance, memberClass, memberCode, subsystemCode, memberName)
    except (TypeError, AttributeError) as e:
        if DEBUG:
            sys.stderr.write("WARNING: {}: {}\n".format(type(e).__name__, e))
        return

def registeredSubsystems(sharedParams):
    """List Subsystems in sharedParams that are attached to Security Server (registered)."""
    try:
        root = ET.fromstring(sharedParams)    # ET.fromstring wants encoded bytes as input (PY2)
        instance = root.find("./instanceIdentifier").text
        for member in root.findall("./member"):
            memberId = member.attrib["id"]
            memberClass = member.find("./memberClass/code").text
            memberCode = member.find("./memberCode").text
            for subsystem in member.findall("./subsystem"):
                subsystemId = subsystem.attrib["id"]
                subsystemCode = subsystem.find("./subsystemCode").text
                if root.findall("./securityServer[client='{}']".format(subsystemId)):
                    yield (instance, memberClass, memberCode, subsystemCode)
    except (TypeError, AttributeError) as e:
        if DEBUG:
            sys.stderr.write("WARNING: {}: {}\n".format(type(e).__name__, e))
        return

def subsystemsWithServer(sharedParams):
    """List Subsystems in sharedParams with Security Server identifiers.
       Return tuple of 9 identifiers for each Security Server that has Subsystem
       Return tuple of 4 identifiers if Subsystem is not registered in any of Security Servers
    """
    try:
        root = ET.fromstring(sharedParams)    # ET.fromstring wants encoded bytes as input (PY2)
        instance = root.find("./instanceIdentifier").text
        for member in root.findall("./member"):
            memberId = member.attrib["id"]
            memberClass = member.find("./memberClass/code").text
            memberCode = member.find("./memberCode").text
            for subsystem in member.findall("./subsystem"):
                subsystemId = subsystem.attrib["id"]
                subsystemCode = subsystem.find("./subsystemCode").text
                for server in root.findall("./securityServer[client='{}']".format(subsystemId)):
                    ownerId = server.find("./owner").text
                    owner = root.find("./member[@id='"+ownerId+"']")
                    ownerClass = owner.find("./memberClass/code").text
                    ownerCode = owner.find("./memberCode").text
                    serverCode = server.find("./serverCode").text
                    address = server.find("./address").text
                    yield (instance, memberClass, memberCode, subsystemCode, instance, ownerClass, ownerCode, serverCode, address)
                else:
                    yield (instance, memberClass, memberCode, subsystemCode)
    except (TypeError, AttributeError) as e:
        if DEBUG:
            sys.stderr.write("WARNING: {}: {}\n".format(type(e).__name__, e))
        return

def servers(sharedParams):
    """List Security Servers in sharedParams."""
    try:
        root = ET.fromstring(sharedParams)    # ET.fromstring wants encoded bytes as input (PY2)
        instance = root.find("./instanceIdentifier").text
        for server in root.findall("./securityServer"):
            ownerId = server.find("./owner").text
            owner = root.find("./member[@id='{}']".format(ownerId))
            memberClass = owner.find("./memberClass/code").text
            memberCode = owner.find("./memberCode").text
            serverCode = server.find("./serverCode").text
            address = server.find("./address").text
            yield (instance, memberClass, memberCode, serverCode, address)
    except (TypeError, AttributeError) as e:
        if DEBUG:
            sys.stderr.write("WARNING: {}: {}\n".format(type(e).__name__, e))
        return

def serversIPs(sharedParams):
    """List IP adresses of Security Servers in sharedParams."""
    try:
        root = ET.fromstring(sharedParams)    # ET.fromstring wants encoded bytes as input (PY2)
        for server in root.findall("./securityServer"):
            address = server.find('address').text
            try:
                for ip in socket.gethostbyname_ex(address)[2]:
                    yield (ip)
            except (socket.gaierror):
                pass
    except (TypeError, AttributeError) as e:
        if DEBUG:
            sys.stderr.write("WARNING: {}: {}\n".format(type(e).__name__, e))
        return

def methods(addr, client, service, method='listMethods', timeout=DEFAULT_TIMEOUT, verify=False, cert=None):
    """Get X-Road listMethods response."""
    url = addr
    # Add HTTP/HTTPS scheme if missing
    if not urlparse.urlsplit(url).scheme and (verify or cert):
        url = "https://"+url
    elif not urlparse.urlsplit(url).scheme:
        url = "http://"+url

    if len(client) == 3 and len(service) == 4:
        data = METHODS_MEMBER_TEMPL.format(client=client, service=service, uuid=uuid.uuid4(), method=method)
    elif len(client) == 4 and len(service) == 4:
        data = METHODS_SUBSYSTEM_TEMPL.format(client=client, service=service, uuid=uuid.uuid4(), method=method)
    else:
        return

    headers = {'content-type': 'text/xml'}
    try:
        listMethodsResponse = requests.post(url, data=data.encode('utf-8'), headers=headers, timeout=timeout, verify=verify, cert=cert)
        listMethodsResponse.raise_for_status()
        # Some servers might return multipart message.
        envel = re.search("<SOAP-ENV:Envelope.+<\/SOAP-ENV:Envelope>", listMethodsResponse.text, re.DOTALL) 
        root = ET.fromstring(envel.group(0).encode('utf-8'))    # ET.fromstring wants encoded bytes as input (PY2)
        if DEBUG and root.find(".//faultstring") is not None:
            sys.stderr.write("WARNING: SOAP FAULT: {}\n".format(root.find(".//faultstring").text))
        for service in root.findall(".//xrd:{}Response/xrd:service".format(method), NS):
            method = {}
            # Elements subsystemCode and serviceVersion may be missing.
            method['xRoadInstance'] = service.find("./id:xRoadInstance", NS).text
            method['memberClass'] = service.find("./id:memberClass", NS).text
            method['memberCode'] = service.find("./id:memberCode", NS).text
            method['subsystemCode'] = service.find("./id:subsystemCode", NS).text if service.find("./id:subsystemCode", NS) is not None else ""
            method['serviceCode'] = service.find("./id:serviceCode", NS).text
            method['serviceVersion'] = service.find("./id:serviceVersion", NS).text if service.find("./id:serviceVersion", NS) is not None else ""
            yield method
    except (requests.exceptions.RequestException, TypeError, AttributeError) as e:
        if DEBUG:
            sys.stderr.write("WARNING: {}: {}\n".format(type(e).__name__, e))
        return

def stringify(list):
    """Convert list/tuple to slash separated string."""
    return u"/".join(list)
