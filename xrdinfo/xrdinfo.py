#!/usr/bin/python

"""X-Road informational module."""

from six import BytesIO
import re
import requests
import socket
import six.moves.urllib.parse as urlparse
import sys
import uuid
import xml.etree.ElementTree as ET
import zipfile


# Timeout for requests
DEFAULT_TIMEOUT = 5.0

REQUEST_MEMBER_TEMPL = u"""<?xml version="1.0" encoding="utf-8"?>
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
{body}
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
"""

REQUEST_SUBSYSTEM_TEMPL = u"""<?xml version="1.0" encoding="utf-8"?>
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
{body}
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
"""

METHODS_BODY_TEMPL = u'        <xroad:{method}/>'

GETWSDL_SERVICE = u'getWsdl'

GETWSDL_BODY_TEMPL = u"""        <xroad:getWsdl>
            <xroad:serviceCode>{serviceCode}</xroad:serviceCode>
            <xroad:serviceVersion>{serviceVersion}</xroad:serviceVersion>
        </xroad:getWsdl>"""

GETWSDL_BODY_TEMPL_NOVERSION = u"""        <xroad:getWsdl>
            <xroad:serviceCode>{serviceCode}</xroad:serviceCode>
        </xroad:getWsdl>"""

# Namespaces of X-Road schemas
NS = {'xrd': 'http://x-road.eu/xsd/xroad.xsd', 'id': 'http://x-road.eu/xsd/identifiers', 'wsdl': 'http://schemas.xmlsoap.org/wsdl/'}


class XrdInfoError(Exception):
    """ XrdInfo generic Exception """
    def __init__(self, exc):
        if isinstance(exc, XrdInfoError):
            # No need to double wrap the exception
            super(XrdInfoError, self).__init__(exc)
        elif isinstance(exc, Exception):
            # Wrapped exception
            super(XrdInfoError, self).__init__(u'{}: {}'.format(type(exc).__name__, exc))
        else:
            # Error message
            super(XrdInfoError, self).__init__(exc)


class SoapFaultError(XrdInfoError):
    """SOAP Fault received."""
    def __init__(self, msg):
        super(SoapFaultError, self).__init__(u'SoapFault: {}'.format(msg))


class TimeoutError(XrdInfoError):
    """Request failed due to timeout."""
    pass


def sharedParamsSS(addr, instance=None, timeout=DEFAULT_TIMEOUT, verify=False, cert=None):
    """Get shared-params.xml content from local Security Server.
       By default return info about local X-Road instance.
    """
    try:
        url = addr
        # Add HTTP/HTTPS scheme if missing
        if not urlparse.urlsplit(url).scheme and (verify or cert):
            url = 'https://'+url
        elif not urlparse.urlsplit(url).scheme:
            url = 'http://'+url
        # Add '/verificationconf' if path is missing
        if urlparse.urlsplit(url).path == '':
            url = url + '/verificationconf'
        elif urlparse.urlsplit(url).path == '/':
            url = url + 'verificationconf'
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
        sharedParamsFile = verConfZip.open(u'verificationconf/{}/shared-params.xml'.format(ident))
        sharedParams = sharedParamsFile.read()
        return sharedParams.decode('utf-8')
    except requests.exceptions.Timeout as e:
        raise TimeoutError(e)
    except Exception as e:
        raise XrdInfoError(e)


def sharedParamsCS(addr, timeout=DEFAULT_TIMEOUT, verify=False, cert=None):
    """Get shared-params.xml content from Central Server/Configuration Proxy.
       Global configuration is not validated, use sharedParamsSS whenever possible.
    """
    try:
        url = addr
        # Add HTTP/HTTPS scheme if missing
        if not urlparse.urlsplit(url).scheme and (verify or cert):
            url = 'https://'+url
        elif not urlparse.urlsplit(url).scheme:
            url = 'http://'+url
        # Add '/internalconf' if path is missing
        if urlparse.urlsplit(url).path == '':
            url = url + '/internalconf'
        elif urlparse.urlsplit(url).path == '/':
            url = url + 'internalconf'
        globalConf = requests.get(url, timeout=timeout, verify=verify, cert=cert)
        globalConf.raise_for_status()
        # Configuration Proxy uses lowercase for 'Content-location'
        s = re.search('Content-location: (/.+/shared-params.xml)', globalConf.text, re.IGNORECASE)
        url2 = urlparse.urljoin(url, s.group(1))
        sharedParamsResponse = requests.get(url2, timeout=timeout, verify=verify, cert=cert)
        sharedParamsResponse.raise_for_status()
        sharedParamsResponse.encoding = 'utf-8'
        return sharedParamsResponse.text
    except requests.exceptions.Timeout as e:
        raise TimeoutError(e)
    except Exception as e:
        raise XrdInfoError(e)


def subsystems(sharedParams):
    """List Subsystems in sharedParams.
       Return tuple: (xRoadInstance, memberClass, memberCode, subsystemCode).
    """
    try:
        root = ET.fromstring(sharedParams.encode('utf-8'))    # ET.fromstring wants encoded bytes as input (PY2)
        instance = u'' + root.find('./instanceIdentifier').text
        for member in root.findall('./member'):
            memberClass = u'' + member.find('./memberClass/code').text
            memberCode = u'' + member.find('./memberCode').text
            for subsystem in member.findall('./subsystem'):
                subsystemCode = u'' + subsystem.find('./subsystemCode').text
                yield (instance, memberClass, memberCode, subsystemCode)
    except Exception as e:
        raise XrdInfoError(e)


def subsystemsWithMembername(sharedParams):
    """List Subsystems in sharedParams with Member name.
       Return tuple: (xRoadInstance, memberClass, memberCode, subsystemCode, Member Name).
    """
    try:
        root = ET.fromstring(sharedParams.encode('utf-8'))    # ET.fromstring wants encoded bytes as input (PY2)
        instance = u'' + root.find('./instanceIdentifier').text
        for member in root.findall('./member'):
            memberClass = u'' + member.find('./memberClass/code').text
            memberCode = u'' + member.find('./memberCode').text
            memberName = u'' + member.find('./name').text
            for subsystem in member.findall('./subsystem'):
                subsystemCode = u'' + subsystem.find('./subsystemCode').text
                yield (instance, memberClass, memberCode, subsystemCode, memberName)
    except Exception as e:
        raise XrdInfoError(e)


def registeredSubsystems(sharedParams):
    """List Subsystems in sharedParams that are attached to Security Server (registered).
       Return tuple: (xRoadInstance, memberClass, memberCode, subsystemCode).
    """
    try:
        root = ET.fromstring(sharedParams.encode('utf-8'))    # ET.fromstring wants encoded bytes as input (PY2)
        instance = u'' + root.find('./instanceIdentifier').text
        for member in root.findall('./member'):
            memberClass = u'' + member.find('./memberClass/code').text
            memberCode = u'' + member.find('./memberCode').text
            for subsystem in member.findall('./subsystem'):
                subsystemId = subsystem.attrib['id']
                subsystemCode = u'' + subsystem.find('./subsystemCode').text
                if root.findall('./securityServer[client="{}"]'.format(subsystemId)):
                    yield (instance, memberClass, memberCode, subsystemCode)
    except Exception as e:
        raise XrdInfoError(e)


def subsystemsWithServer(sharedParams):
    """List Subsystems in sharedParams with Security Server identifiers.
       Return tuple of 9 identifiers for each Security Server that has Subsystem:
       (xRoadInstance, memberClass, memberCode, subsystemCode, Server Owners xRoadInstance,
           Server Owners memberClass, Server Owners memberCode, serverCode, Server Address).
       Return tuple of 4 identifiers if Subsystem is not registered in any of Security Servers:
       (xRoadInstance, memberClass, memberCode, subsystemCode).
    """
    try:
        root = ET.fromstring(sharedParams.encode('utf-8'))    # ET.fromstring wants encoded bytes as input (PY2)
        instance = u'' + root.find('./instanceIdentifier').text
        for member in root.findall('./member'):
            memberClass = u'' + member.find('./memberClass/code').text
            memberCode = u'' + member.find('./memberCode').text
            for subsystem in member.findall('./subsystem'):
                subsystemId = subsystem.attrib['id']
                subsystemCode = u'' + subsystem.find('./subsystemCode').text
                serverFound = False
                for server in root.findall('./securityServer[client="{}"]'.format(subsystemId)):
                    ownerId = server.find('./owner').text
                    owner = root.find('./member[@id="{}"]'.format(ownerId))
                    ownerClass = u'' + owner.find('./memberClass/code').text
                    ownerCode = u'' + owner.find('./memberCode').text
                    serverCode = u'' + server.find('./serverCode').text
                    address = u'' + server.find('./address').text
                    yield (instance, memberClass, memberCode, subsystemCode, instance, ownerClass, ownerCode, serverCode, address)
                    serverFound = True
                if not serverFound:
                    yield (instance, memberClass, memberCode, subsystemCode)
    except Exception as e:
        raise XrdInfoError(e)


def servers(sharedParams):
    """List Security Servers in sharedParams.
       Return tuple: (Server Owners xRoadInstance, Server Owners memberClass, Server Owners memberCode, serverCode, Server Address).
    """
    try:
        root = ET.fromstring(sharedParams.encode('utf-8'))    # ET.fromstring wants encoded bytes as input (PY2)
        instance = u'' + root.find('./instanceIdentifier').text
        for server in root.findall('./securityServer'):
            ownerId = server.find('./owner').text
            owner = root.find('./member[@id="{}"]'.format(ownerId))
            memberClass = u'' + owner.find('./memberClass/code').text
            memberCode = u'' + owner.find('./memberCode').text
            serverCode = u'' + server.find('./serverCode').text
            address = u'' + server.find('./address').text
            yield (instance, memberClass, memberCode, serverCode, address)
    except Exception as e:
        raise XrdInfoError(e)


def addrIPs(address):
    """Resolve DNS name to IP addresses.
       Unresolved DNS names are silently ignored.
    """
    try:
        for ip in socket.gethostbyname_ex(address)[2]:
            yield (u'' + ip)
    except (socket.gaierror):
        # Ignoring DNS name not found error
        pass
    except Exception as e:
        raise XrdInfoError(e)


def serversIPs(sharedParams):
    """List IP adresses of Security Servers in sharedParams.
       Unresolved DNS names are silently ignored.
    """
    try:
        root = ET.fromstring(sharedParams.encode('utf-8'))    # ET.fromstring wants encoded bytes as input (PY2)
        for server in root.findall('./securityServer'):
            address = server.find('address').text
            for ip in addrIPs(address):
                yield (u'' + ip)
    except Exception as e:
        raise XrdInfoError(e)


def methods(addr, client, service, method='listMethods', timeout=DEFAULT_TIMEOUT, verify=False, cert=None):
    """Get X-Road listMethods or allowedMethods response.
       Return tuple: (xRoadInstance, memberClass, memberCode, subsystemCode, serviceCode, serviceVersion).
    """
    url = addr
    # Add HTTP/HTTPS scheme if missing
    if not urlparse.urlsplit(url).scheme and (verify or cert):
        url = 'https://'+url
    elif not urlparse.urlsplit(url).scheme:
        url = 'http://'+url

    body = METHODS_BODY_TEMPL.format(method=method)
    if (len(client) == 3 or client[3] == '') and len(service) == 4:
        data = REQUEST_MEMBER_TEMPL.format(client=client, service=service, uuid=uuid.uuid4(), method=method, body=body)
    elif len(client) == 4 and len(service) == 4:
        data = REQUEST_SUBSYSTEM_TEMPL.format(client=client, service=service, uuid=uuid.uuid4(), method=method, body=body)
    else:
        return

    headers = {'content-type': 'text/xml'}
    try:
        methodsResponse = requests.post(url, data=data.encode('utf-8'), headers=headers, timeout=timeout, verify=verify, cert=cert)
        methodsResponse.raise_for_status()
        methodsResponse.encoding = 'utf-8'

        # Some servers might return multipart message.
        envel = re.search('<SOAP-ENV:Envelope.+<\/SOAP-ENV:Envelope>', methodsResponse.text, re.DOTALL) 
        root = ET.fromstring(envel.group(0).encode('utf-8'))    # ET.fromstring wants encoded bytes as input (PY2)
        if root.find('.//faultstring') is not None:
            raise SoapFaultError(root.find('.//faultstring').text)

        for service in root.findall('.//xrd:{}Response/xrd:service'.format(method), NS):
            method = {}
            # Elements subsystemCode and serviceVersion may be missing.
            method['xRoadInstance'] = service.find('./id:xRoadInstance', NS).text
            method['memberClass'] = service.find('./id:memberClass', NS).text
            method['memberCode'] = service.find('./id:memberCode', NS).text
            method['subsystemCode'] = service.find('./id:subsystemCode', NS).text if service.find('./id:subsystemCode', NS) is not None else ''
            method['serviceCode'] = service.find('./id:serviceCode', NS).text
            method['serviceVersion'] = service.find('./id:serviceVersion', NS).text if service.find('./id:serviceVersion', NS) is not None else ''
            yield (method['xRoadInstance'], method['memberClass'], method['memberCode'], method['subsystemCode'], method['serviceCode'], method['serviceVersion'])
    except requests.exceptions.Timeout as e:
        raise TimeoutError(e)
    except Exception as e:
        raise XrdInfoError(e)


def wsdl(addr, client, service, timeout=DEFAULT_TIMEOUT, verify=False, cert=None):
    """Get X-Road getWsdl response."""
    url = addr
    # Add HTTP/HTTPS scheme if missing
    if not urlparse.urlsplit(url).scheme and (verify or cert):
        url = 'https://'+url
    elif not urlparse.urlsplit(url).scheme:
        url = 'http://'+url

    if service[5]:
        # Service with version
        body = GETWSDL_BODY_TEMPL.format(serviceCode=service[4], serviceVersion=service[5])
    else:
        body = GETWSDL_BODY_TEMPL_NOVERSION.format(serviceCode=service[4])

    if (len(client) == 3 or client[3] == '') and len(service) == 6:
        # Request as member
        data = REQUEST_MEMBER_TEMPL.format(client=client, service=service, uuid=uuid.uuid4(), method=GETWSDL_SERVICE, body=body)
    elif len(client) == 4 and len(service) == 6:
        # Request as subsystem
        data = REQUEST_SUBSYSTEM_TEMPL.format(client=client, service=service, uuid=uuid.uuid4(), method=GETWSDL_SERVICE, body=body)
    else:
        return

    headers = {'content-type': 'text/xml'}
    try:
        wsdlResponse = requests.post(url, data=data.encode('utf-8'), headers=headers, timeout=timeout, verify=verify, cert=cert)
        wsdlResponse.raise_for_status()
        wsdlResponse.encoding = 'utf-8'

        resp = re.search('--xroad.+content-type:text/xml.+<SOAP-ENV:Envelope.+<\/SOAP-ENV:Envelope>.+--xroad.+content-type:text/xml\r\n\r\n(.+)\r\n--xroad.+', wsdlResponse.text, re.DOTALL)
        if resp:
            envel = re.search('<SOAP-ENV:Envelope.+<\/SOAP-ENV:Envelope>', resp.group(1), re.DOTALL)
            if envel:
                # SOAP Fault found instead of WSDL
                root = ET.fromstring(envel.group(0).encode('utf-8'))    # ET.fromstring wants encoded bytes as input (PY2)
                if root.find('.//faultstring') is not None:
                    raise SoapFaultError(root.find('.//faultstring').text)
            else:
                return resp.group(1)
        else:
            envel = re.search('<SOAP-ENV:Envelope.+<\/SOAP-ENV:Envelope>', wsdlResponse.text, re.DOTALL) 
            root = ET.fromstring(envel.group(0).encode('utf-8'))    # ET.fromstring wants encoded bytes as input (PY2)
            if root.find('.//faultstring') is not None:
                raise SoapFaultError(root.find('.//faultstring').text)
            else:
                raise XrdInfoError(u'WSDL not found')
    except requests.exceptions.Timeout as e:
        raise TimeoutError(e)
    except Exception as e:
        raise XrdInfoError(e)


def wsdlMethods(wsdl):
    """Return list of methods in WSDL."""
    try:
        root = ET.fromstring(wsdl.encode('utf-8'))    # ET.fromstring wants encoded bytes as input (PY2)
        for operation in root.findall('.//wsdl:binding/wsdl:operation', NS):
            version = operation.find('./xrd:version', NS).text if operation.find('./xrd:version', NS) is not None else ''
            if 'name' in operation.attrib:
                yield (u'' + operation.attrib['name'], u'' + version)
    except Exception as e:
        raise XrdInfoError(e)


def stringify(items):
    """Convert list/tuple to slash separated string representation of identifier."""
    return u'/'.join(items)
