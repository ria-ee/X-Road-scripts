#!/usr/bin/python

"""X-Road informational module."""

__all__ = [
    'XrdInfoError', 'RequestTimeoutError', 'SoapFaultError', 'shared_params_ss',
    'shared_params_cs', 'subsystems', 'subsystems_with_membername', 'registered_subsystems',
    'subsystems_with_server', 'servers', 'addr_ips', 'servers_ips', 'methods', 'wsdl',
    'wsdl_methods', 'stringify']

from six import BytesIO
import re
import requests
import socket
import six.moves.urllib.parse as urlparse
import uuid
import xml.etree.ElementTree as ElementTree
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
            <id:serviceCode>{service_code}</id:serviceCode>
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
            <id:serviceCode>{service_code}</id:serviceCode>
        </xroad:service>
        <xroad:id>{uuid}</xroad:id>
        <xroad:protocolVersion>4.0</xroad:protocolVersion>
    </SOAP-ENV:Header>
    <SOAP-ENV:Body>
{body}
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
"""

METHODS_BODY_TEMPL = u'        <xroad:{service_code}/>'

GETWSDL_SERVICE_CODE = u'getWsdl'

GETWSDL_BODY_TEMPL = u"""        <xroad:getWsdl>
            <xroad:serviceCode>{service_code}</xroad:serviceCode>
            <xroad:serviceVersion>{service_version}</xroad:serviceVersion>
        </xroad:getWsdl>"""

GETWSDL_BODY_TEMPL_NOVERSION = u"""        <xroad:getWsdl>
            <xroad:serviceCode>{service_code}</xroad:serviceCode>
        </xroad:getWsdl>"""

# Namespaces of X-Road schemas
NS = {'xrd': 'http://x-road.eu/xsd/xroad.xsd',
      'id': 'http://x-road.eu/xsd/identifiers',
      'wsdl': 'http://schemas.xmlsoap.org/wsdl/'}


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


class RequestTimeoutError(XrdInfoError):
    """Request failed due to timeout."""
    pass


class SoapFaultError(XrdInfoError):
    """SOAP Fault received."""

    def __init__(self, msg):
        super(SoapFaultError, self).__init__(u'SoapFault: {}'.format(msg))


def shared_params_ss(addr, instance=None, timeout=DEFAULT_TIMEOUT, verify=False, cert=None):
    """Get shared-params.xml content from local Security Server.
    By default return info about local X-Road instance.
    """
    try:
        url = addr
        # Add HTTP/HTTPS scheme if missing
        if not urlparse.urlsplit(url).scheme and (verify or cert):
            url = 'https://' + url
        elif not urlparse.urlsplit(url).scheme:
            url = 'http://' + url
        # Add '/verificationconf' if path is missing
        if urlparse.urlsplit(url).path == '':
            url = url + '/verificationconf'
        elif urlparse.urlsplit(url).path == '/':
            url = url + 'verificationconf'
        ver_conf_response = requests.get(url, timeout=timeout, verify=verify, cert=cert)
        ver_conf_response.raise_for_status()
        zip_data = BytesIO()
        zip_data.write(ver_conf_response.content)
        ver_conf_zip = zipfile.ZipFile(zip_data)
        ident = instance
        if ident is None:
            # Use local instance configuration
            ident_file = ver_conf_zip.open('verificationconf/instance-identifier')
            ident = ident_file.read()
            ident = ident.decode('utf-8')
        shared_params_file = ver_conf_zip.open(
            u'verificationconf/{}/shared-params.xml'.format(ident))
        shared_params = shared_params_file.read()
        return shared_params.decode('utf-8')
    except requests.exceptions.Timeout as e:
        raise RequestTimeoutError(e)
    except Exception as e:
        raise XrdInfoError(e)


def shared_params_cs(addr, timeout=DEFAULT_TIMEOUT, verify=False, cert=None):
    """Get shared-params.xml content from Central Server/Configuration
    Proxy.
    Global configuration is not validated, use shared_params_ss whenever
    possible.
    """
    try:
        url = addr
        # Add HTTP/HTTPS scheme if missing
        if not urlparse.urlsplit(url).scheme and (verify or cert):
            url = 'https://' + url
        elif not urlparse.urlsplit(url).scheme:
            url = 'http://' + url
        # Add '/internalconf' if path is missing
        if urlparse.urlsplit(url).path == '':
            url = url + '/internalconf'
        elif urlparse.urlsplit(url).path == '/':
            url = url + 'internalconf'
        global_conf = requests.get(url, timeout=timeout, verify=verify, cert=cert)
        global_conf.raise_for_status()
        # Configuration Proxy uses lowercase for 'Content-location'
        s = re.search('Content-location: (/.+/shared-params.xml)', global_conf.text, re.IGNORECASE)
        url2 = urlparse.urljoin(url, s.group(1))
        shared_params_response = requests.get(url2, timeout=timeout, verify=verify, cert=cert)
        shared_params_response.raise_for_status()
        shared_params_response.encoding = 'utf-8'
        return shared_params_response.text
    except requests.exceptions.Timeout as e:
        raise RequestTimeoutError(e)
    except Exception as e:
        raise XrdInfoError(e)


def subsystems(shared_params):
    """List Subsystems in shared_params.
    Return tuple: (xRoadInstance, memberClass, memberCode,
    subsystemCode).
    """
    try:
        # ElementTree.fromstring wants encoded bytes as input (PY2)
        root = ElementTree.fromstring(shared_params.encode('utf-8'))
        instance = u'' + root.find('./instanceIdentifier').text
        for member in root.findall('./member'):
            member_class = u'' + member.find('./memberClass/code').text
            member_code = u'' + member.find('./memberCode').text
            for subsystem in member.findall('./subsystem'):
                subsystem_code = u'' + subsystem.find('./subsystemCode').text
                yield (instance, member_class, member_code, subsystem_code)
    except Exception as e:
        raise XrdInfoError(e)


def subsystems_with_membername(shared_params):
    """List Subsystems in shared_params with Member name.
    Return tuple: (xRoadInstance, memberClass, memberCode,
    subsystemCode, Member Name).
    """
    try:
        # ElementTree.fromstring wants encoded bytes as input (PY2)
        root = ElementTree.fromstring(shared_params.encode('utf-8'))
        instance = u'' + root.find('./instanceIdentifier').text
        for member in root.findall('./member'):
            member_class = u'' + member.find('./memberClass/code').text
            member_code = u'' + member.find('./memberCode').text
            member_name = u'' + member.find('./name').text
            for subsystem in member.findall('./subsystem'):
                subsystem_code = u'' + subsystem.find('./subsystemCode').text
                yield (instance, member_class, member_code, subsystem_code, member_name)
    except Exception as e:
        raise XrdInfoError(e)


def registered_subsystems(shared_params):
    """List Subsystems in shared_params that are attached to Security
    Server (registered).
    Return tuple: (xRoadInstance, memberClass, memberCode,
    subsystemCode).
    """
    try:
        # ElementTree.fromstring wants encoded bytes as input (PY2)
        root = ElementTree.fromstring(shared_params.encode('utf-8'))
        instance = u'' + root.find('./instanceIdentifier').text
        for member in root.findall('./member'):
            member_class = u'' + member.find('./memberClass/code').text
            member_code = u'' + member.find('./memberCode').text
            for subsystem in member.findall('./subsystem'):
                subsystem_id = subsystem.attrib['id']
                subsystem_code = u'' + subsystem.find('./subsystemCode').text
                if root.findall('./securityServer[client="{}"]'.format(subsystem_id)):
                    yield (instance, member_class, member_code, subsystem_code)
    except Exception as e:
        raise XrdInfoError(e)


def subsystems_with_server(shared_params):
    """List Subsystems in shared_params with Security Server
    identifiers.
    Return tuple of 9 identifiers for each Security Server that has
    Subsystem: (xRoadInstance, memberClass, memberCode, subsystemCode,
    Server Owners xRoadInstance, Server Owners memberClass,
    Server Owners memberCode, serverCode, Server Address).
    Return tuple of 4 identifiers if Subsystem is not registered in any
    of Security Servers: (xRoadInstance, memberClass, memberCode,
    subsystemCode).
    """
    try:
        # ElementTree.fromstring wants encoded bytes as input (PY2)
        root = ElementTree.fromstring(shared_params.encode('utf-8'))
        instance = u'' + root.find('./instanceIdentifier').text
        for member in root.findall('./member'):
            member_class = u'' + member.find('./memberClass/code').text
            member_code = u'' + member.find('./memberCode').text
            for subsystem in member.findall('./subsystem'):
                subsystem_id = subsystem.attrib['id']
                subsystem_code = u'' + subsystem.find('./subsystemCode').text
                server_found = False
                for server in root.findall('./securityServer[client="{}"]'.format(subsystem_id)):
                    owner_id = server.find('./owner').text
                    owner = root.find('./member[@id="{}"]'.format(owner_id))
                    owner_class = u'' + owner.find('./memberClass/code').text
                    owner_code = u'' + owner.find('./memberCode').text
                    server_code = u'' + server.find('./serverCode').text
                    address = u'' + server.find('./address').text
                    yield (
                        instance, member_class, member_code, subsystem_code, instance, owner_class,
                        owner_code, server_code, address)
                    server_found = True
                if not server_found:
                    yield (instance, member_class, member_code, subsystem_code)
    except Exception as e:
        raise XrdInfoError(e)


def servers(shared_params):
    """List Security Servers in shared_params.
    Return tuple: (Server Owners xRoadInstance,
    Server Owners memberClass, Server Owners memberCode, serverCode,
    Server Address).
    """
    try:
        # ElementTree.fromstring wants encoded bytes as input (PY2)
        root = ElementTree.fromstring(shared_params.encode('utf-8'))
        instance = u'' + root.find('./instanceIdentifier').text
        for server in root.findall('./securityServer'):
            owner_id = server.find('./owner').text
            owner = root.find('./member[@id="{}"]'.format(owner_id))
            member_class = u'' + owner.find('./memberClass/code').text
            member_code = u'' + owner.find('./memberCode').text
            server_code = u'' + server.find('./serverCode').text
            address = u'' + server.find('./address').text
            yield (instance, member_class, member_code, server_code, address)
    except Exception as e:
        raise XrdInfoError(e)


def addr_ips(address):
    """Resolve DNS name to IP addresses.
    Unresolved DNS names are silently ignored.
    """
    try:
        for ip in socket.gethostbyname_ex(address)[2]:
            yield (u'' + ip)
    except socket.gaierror:
        # Ignoring DNS name not found error
        pass
    except Exception as e:
        raise XrdInfoError(e)


def servers_ips(shared_params):
    """List IP adresses of Security Servers in shared_params.
    Unresolved DNS names are silently ignored.
    """
    try:
        # ElementTree.fromstring wants encoded bytes as input (PY2)
        root = ElementTree.fromstring(shared_params.encode('utf-8'))
        for server in root.findall('./securityServer'):
            address = server.find('address').text
            for ip in addr_ips(address):
                yield (u'' + ip)
    except Exception as e:
        raise XrdInfoError(e)


def methods(
        addr, client, producer, method='listMethods', timeout=DEFAULT_TIMEOUT, verify=False,
        cert=None):
    """Get X-Road listMethods or allowedMethods response.
    Return tuple: (xRoadInstance, memberClass, memberCode,
    subsystemCode, serviceCode, serviceVersion).
    """
    url = addr
    # Add HTTP/HTTPS scheme if missing
    if not urlparse.urlsplit(url).scheme and (verify or cert):
        url = 'https://' + url
    elif not urlparse.urlsplit(url).scheme:
        url = 'http://' + url

    body = METHODS_BODY_TEMPL.format(service_code=method)
    if (len(client) == 3 or client[3] == '') and len(producer) == 4:
        data = REQUEST_MEMBER_TEMPL.format(
            client=client, service=producer, uuid=uuid.uuid4(), service_code=method, body=body)
    elif len(client) == 4 and len(producer) == 4:
        data = REQUEST_SUBSYSTEM_TEMPL.format(
            client=client, service=producer, uuid=uuid.uuid4(), service_code=method, body=body)
    else:
        return

    headers = {'content-type': 'text/xml'}
    try:
        methods_response = requests.post(
            url, data=data.encode('utf-8'), headers=headers, timeout=timeout, verify=verify,
            cert=cert)
        methods_response.raise_for_status()
        methods_response.encoding = 'utf-8'

        # Some servers might return multipart message.
        envel = re.search('<SOAP-ENV:Envelope.+</SOAP-ENV:Envelope>', methods_response.text,
                          re.DOTALL)
        # ElementTree.fromstring wants encoded bytes as input (PY2)
        root = ElementTree.fromstring(envel.group(0).encode('utf-8'))
        if root.find('.//faultstring') is not None:
            raise SoapFaultError(root.find('.//faultstring').text)

        for service in root.findall('.//xrd:{}Response/xrd:service'.format(method), NS):
            result = {
                'xRoadInstance': service.find('./id:xRoadInstance', NS).text,
                'memberClass': service.find('./id:memberClass', NS).text,
                'memberCode': service.find('./id:memberCode', NS).text,
                # Elements subsystemCode may be missing.
                'subsystemCode':
                    service.find('./id:subsystemCode', NS).text
                    if service.find('./id:subsystemCode', NS) is not None
                    else '',
                'serviceCode': service.find('./id:serviceCode', NS).text,
                # Element serviceVersion may be missing.
                'serviceVersion':
                    service.find('./id:serviceVersion', NS).text
                    if service.find('./id:serviceVersion', NS) is not None
                    else ''}
            yield (result['xRoadInstance'], result['memberClass'], result['memberCode'],
                   result['subsystemCode'], result['serviceCode'], result['serviceVersion'])
    except requests.exceptions.Timeout as e:
        raise RequestTimeoutError(e)
    except Exception as e:
        raise XrdInfoError(e)


def wsdl(addr, client, service, timeout=DEFAULT_TIMEOUT, verify=False, cert=None):
    """Get X-Road getWsdl response."""
    url = addr
    # Add HTTP/HTTPS scheme if missing
    if not urlparse.urlsplit(url).scheme and (verify or cert):
        url = 'https://' + url
    elif not urlparse.urlsplit(url).scheme:
        url = 'http://' + url

    if service[5]:
        # Service with version
        body = GETWSDL_BODY_TEMPL.format(service_code=service[4], service_version=service[5])
    else:
        body = GETWSDL_BODY_TEMPL_NOVERSION.format(service_code=service[4])

    if (len(client) == 3 or client[3] == '') and len(service) == 6:
        # Request as member
        data = REQUEST_MEMBER_TEMPL.format(
            client=client, service=service, uuid=uuid.uuid4(), service_code=GETWSDL_SERVICE_CODE,
            body=body)
    elif len(client) == 4 and len(service) == 6:
        # Request as subsystem
        data = REQUEST_SUBSYSTEM_TEMPL.format(
            client=client, service=service, uuid=uuid.uuid4(), service_code=GETWSDL_SERVICE_CODE,
            body=body)
    else:
        return

    headers = {'content-type': 'text/xml'}
    try:
        wsdl_response = requests.post(
            url, data=data.encode('utf-8'), headers=headers, timeout=timeout, verify=verify,
            cert=cert)
        wsdl_response.raise_for_status()
        wsdl_response.encoding = 'utf-8'

        resp = re.search(
            '--xroad.+content-type:text/xml.+<SOAP-ENV:Envelope.+</SOAP-ENV:Envelope>'
            '.+--xroad.+content-type:text/xml.*?\r\n\r\n(.+)\r\n--xroad.+',
            wsdl_response.text, re.DOTALL)
        if resp:
            envel = re.search(
                '<SOAP-ENV:Envelope.+</SOAP-ENV:Envelope>', resp.group(1), re.DOTALL)
            if envel:
                # SOAP Fault found instead of WSDL
                # ElementTree.fromstring wants encoded bytes as input
                # (PY2)
                root = ElementTree.fromstring(envel.group(0).encode('utf-8'))
                if root.find('.//faultstring') is not None:
                    raise SoapFaultError(root.find('.//faultstring').text)
            else:
                return resp.group(1)
        else:
            envel = re.search(
                '<SOAP-ENV:Envelope.+</SOAP-ENV:Envelope>', wsdl_response.text, re.DOTALL)
            # ElementTree.fromstring wants encoded bytes as input (PY2)
            root = ElementTree.fromstring(envel.group(0).encode('utf-8'))
            if root.find('.//faultstring') is not None:
                raise SoapFaultError(root.find('.//faultstring').text)
            else:
                raise XrdInfoError(u'WSDL not found')
    except requests.exceptions.Timeout as e:
        raise RequestTimeoutError(e)
    except Exception as e:
        raise XrdInfoError(e)


def wsdl_methods(wsdl_doc):
    """Return list of methods in WSDL."""
    try:
        # ElementTree.fromstring wants encoded bytes as input (PY2)
        root = ElementTree.fromstring(wsdl_doc.encode('utf-8'))
        for operation in root.findall('.//wsdl:binding/wsdl:operation', NS):
            version = operation.find('./xrd:version', NS).text \
                if operation.find('./xrd:version', NS) is not None else ''
            if 'name' in operation.attrib:
                yield (u'' + operation.attrib['name'], u'' + version)
    except Exception as e:
        raise XrdInfoError(e)


def stringify(items):
    """Convert list/tuple to slash separated string representation of
    identifier.
    """
    return u'/'.join(items)
