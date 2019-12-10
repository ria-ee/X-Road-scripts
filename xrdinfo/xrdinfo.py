#!/usr/bin/python3

"""X-Road informational module."""

__all__ = [
    'XrdInfoError', 'RequestTimeoutError', 'SoapFaultError', 'NotOpenapiServiceError',
    'OpenapiReadError', 'shared_params_ss', 'shared_params_cs', 'subsystems',
    'subsystems_with_membername', 'registered_subsystems', 'subsystems_with_server', 'servers',
    'addr_ips', 'servers_ips', 'methods', 'methods_rest', 'wsdl', 'wsdl_methods', 'openapi',
    'openapi_endpoints', 'identifier', 'identifier_parts']
__version__ = '1.0'
__author__ = 'Vitali Stupin'

import json
import re
import socket
import urllib.parse as urlparse
import uuid
import zipfile
import xml.etree.ElementTree as ElementTree
from io import BytesIO
import requests
import yaml

XRD_REST_VERSION = 'r1'

# Timeout for requests
DEFAULT_TIMEOUT = 5.0

REQUEST_MEMBER_TEMPL = """<?xml version="1.0" encoding="utf-8"?>
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

REQUEST_SUBSYSTEM_TEMPL = """<?xml version="1.0" encoding="utf-8"?>
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

METHODS_BODY_TEMPL = '        <xroad:{service_code}/>'

GETWSDL_SERVICE_CODE = 'getWsdl'

GETWSDL_BODY_TEMPL = """        <xroad:getWsdl>
            <xroad:serviceCode>{service_code}</xroad:serviceCode>
            <xroad:serviceVersion>{service_version}</xroad:serviceVersion>
        </xroad:getWsdl>"""

GETWSDL_BODY_TEMPL_NOVERSION = """        <xroad:getWsdl>
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
            super(XrdInfoError, self).__init__('{}: {}'.format(type(exc).__name__, exc))
        else:
            # Error message
            super(XrdInfoError, self).__init__(exc)


class RequestTimeoutError(XrdInfoError):
    """Request failed due to timeout."""
    pass


class SoapFaultError(XrdInfoError):
    """SOAP Fault received."""

    def __init__(self, msg):
        super(SoapFaultError, self).__init__('SoapFault: {}'.format(msg))


class NotOpenapiServiceError(XrdInfoError):
    """Requested service does not have OpenAPI description."""
    pass


class OpenapiReadError(XrdInfoError):
    """Producer Security Server failed to read OpenAPI description."""
    pass


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
            'verificationconf/{}/shared-params.xml'.format(ident))
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
        root = ElementTree.fromstring(shared_params)
        instance = '' + root.find('./instanceIdentifier').text
        for member in root.findall('./member'):
            member_class = '' + member.find('./memberClass/code').text
            member_code = '' + member.find('./memberCode').text
            for subsystem in member.findall('./subsystem'):
                subsystem_code = '' + subsystem.find('./subsystemCode').text
                yield instance, member_class, member_code, subsystem_code
    except Exception as e:
        raise XrdInfoError(e)


def subsystems_with_membername(shared_params):
    """List Subsystems in shared_params with Member name.
    Return tuple: (xRoadInstance, memberClass, memberCode,
    subsystemCode, Member Name).
    """
    try:
        root = ElementTree.fromstring(shared_params)
        instance = '' + root.find('./instanceIdentifier').text
        for member in root.findall('./member'):
            member_class = '' + member.find('./memberClass/code').text
            member_code = '' + member.find('./memberCode').text
            member_name = '' + member.find('./name').text
            for subsystem in member.findall('./subsystem'):
                subsystem_code = '' + subsystem.find('./subsystemCode').text
                yield instance, member_class, member_code, subsystem_code, member_name
    except Exception as e:
        raise XrdInfoError(e)


def registered_subsystems(shared_params):
    """List Subsystems in shared_params that are attached to Security
    Server (registered).
    Return tuple: (xRoadInstance, memberClass, memberCode,
    subsystemCode).
    """
    try:
        root = ElementTree.fromstring(shared_params)
        instance = '' + root.find('./instanceIdentifier').text
        for member in root.findall('./member'):
            member_class = '' + member.find('./memberClass/code').text
            member_code = '' + member.find('./memberCode').text
            for subsystem in member.findall('./subsystem'):
                subsystem_id = subsystem.attrib['id']
                subsystem_code = '' + subsystem.find('./subsystemCode').text
                if root.findall('./securityServer[client="{}"]'.format(subsystem_id)):
                    yield instance, member_class, member_code, subsystem_code
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
        root = ElementTree.fromstring(shared_params)
        instance = '' + root.find('./instanceIdentifier').text
        for member in root.findall('./member'):
            member_class = '' + member.find('./memberClass/code').text
            member_code = '' + member.find('./memberCode').text
            for subsystem in member.findall('./subsystem'):
                subsystem_id = subsystem.attrib['id']
                subsystem_code = '' + subsystem.find('./subsystemCode').text
                server_found = False
                for server in root.findall('./securityServer[client="{}"]'.format(subsystem_id)):
                    owner_id = server.find('./owner').text
                    owner = root.find('./member[@id="{}"]'.format(owner_id))
                    owner_class = '' + owner.find('./memberClass/code').text
                    owner_code = '' + owner.find('./memberCode').text
                    server_code = '' + server.find('./serverCode').text
                    address = '' + server.find('./address').text
                    yield (
                        instance, member_class, member_code, subsystem_code, instance, owner_class,
                        owner_code, server_code, address)
                    server_found = True
                if not server_found:
                    yield instance, member_class, member_code, subsystem_code
    except Exception as e:
        raise XrdInfoError(e)


def servers(shared_params):
    """List Security Servers in shared_params.
    Return tuple: (Server Owners xRoadInstance,
    Server Owners memberClass, Server Owners memberCode, serverCode,
    Server Address).
    """
    try:
        root = ElementTree.fromstring(shared_params)
        instance = '' + root.find('./instanceIdentifier').text
        for server in root.findall('./securityServer'):
            owner_id = server.find('./owner').text
            owner = root.find('./member[@id="{}"]'.format(owner_id))
            member_class = '' + owner.find('./memberClass/code').text
            member_code = '' + owner.find('./memberCode').text
            server_code = '' + server.find('./serverCode').text
            address = '' + server.find('./address').text
            yield instance, member_class, member_code, server_code, address
    except Exception as e:
        raise XrdInfoError(e)


def addr_ips(address):
    """Resolve DNS name to IP addresses.
    Unresolved DNS names are silently ignored.
    """
    try:
        for ip in socket.gethostbyname_ex(address)[2]:
            yield '' + ip
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
        root = ElementTree.fromstring(shared_params)
        for server in root.findall('./securityServer'):
            address = server.find('address').text
            for ip in addr_ips(address):
                yield '' + ip
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
        envel = re.search(
            '<SOAP-ENV:Envelope.+</SOAP-ENV:Envelope>', methods_response.text, re.DOTALL)
        try:
            root = ElementTree.fromstring(envel.group(0))
        except AttributeError:
            raise XrdInfoError('Received incorrect response')
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


def methods_rest(
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

    if (len(client) == 3 or client[3] == '') and len(producer) == 4:
        client_header = identifier(client[:3])
    elif len(client) == 4 and len(producer) == 4:
        client_header = identifier(client[:4])
    else:
        return
    # "producer" length already checked
    url = urlparse.urljoin(url, '/{}/{}/{}'.format(XRD_REST_VERSION, identifier(producer), method))

    headers = {'X-Road-Client': client_header, 'accept': 'application/json'}
    try:
        methods_response = requests.get(
            url, headers=headers, timeout=timeout, verify=verify, cert=cert)
        methods_response.raise_for_status()
        methods_response.encoding = 'utf-8'

        services = json.loads(methods_response.text)

        for service in services['service']:
            yield (service['xroad_instance'], service['member_class'], service['member_code'],
                   service['subsystem_code'], service['service_code'])

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
                root = ElementTree.fromstring(envel.group(0))
                if root.find('.//faultstring') is not None:
                    raise SoapFaultError(root.find('.//faultstring').text)
            else:
                return resp.group(1)
        else:
            envel = re.search(
                '<SOAP-ENV:Envelope.+</SOAP-ENV:Envelope>', wsdl_response.text, re.DOTALL)
            root = ElementTree.fromstring(envel.group(0))
            if root.find('.//faultstring') is not None:
                raise SoapFaultError(root.find('.//faultstring').text)
            else:
                raise XrdInfoError('WSDL not found')
    except requests.exceptions.Timeout as e:
        raise RequestTimeoutError(e)
    except Exception as e:
        raise XrdInfoError(e)


def wsdl_methods(wsdl_doc):
    """Return list of methods in WSDL."""
    try:
        root = ElementTree.fromstring(wsdl_doc)
        for operation in root.findall('.//wsdl:binding/wsdl:operation', NS):
            version = operation.find('./xrd:version', NS).text \
                if operation.find('./xrd:version', NS) is not None else ''
            if 'name' in operation.attrib:
                yield '' + operation.attrib['name'], '' + version
    except Exception as e:
        raise XrdInfoError(e)


def openapi(addr, client, service, timeout=DEFAULT_TIMEOUT, verify=False, cert=None):
    """Get X-Road getOpenAPI response."""
    url = addr
    # Add HTTP/HTTPS scheme if missing
    if not urlparse.urlsplit(url).scheme and (verify or cert):
        url = 'https://' + url
    elif not urlparse.urlsplit(url).scheme:
        url = 'http://' + url

    if (len(client) == 3 or client[3] == '') and len(service) == 5:
        client_header = identifier(client[:3])
    elif len(client) == 4 and len(service) == 5:
        client_header = identifier(client[:4])
    else:
        return
    # "producer" length already checked
    url = urlparse.urljoin(url, '/{}/{}/getOpenAPI?serviceCode={}'.format(
        XRD_REST_VERSION, identifier(service[:4]), encode_part(service[4])))

    headers = {'X-Road-Client': client_header, 'accept': 'application/json'}
    openapi_response = None
    try:
        openapi_response = requests.get(
            url, headers=headers, timeout=timeout, verify=verify, cert=cert)
        openapi_response.raise_for_status()
        openapi_response.encoding = 'utf-8'
        return openapi_response.text
    except requests.exceptions.Timeout as e:
        raise RequestTimeoutError(e)
    except Exception as e:
        error_type = ''
        try:
            resp = json.loads(openapi_response.text)
            if resp['message'] == 'Invalid service type: REST':
                error_type = 'not_openapi'
            elif re.search('^Failed reading service description from', resp['message']):
                error_type = 'openapi_failed'
        except (AttributeError, ValueError, KeyError):
            # Failed to find precise error.
            pass
        if error_type == 'not_openapi':
            raise NotOpenapiServiceError('Service does not have OpenAPI description')
        if error_type == 'openapi_failed':
            raise OpenapiReadError('Failed reading service OpenAPI description')
        else:
            raise XrdInfoError(e)


def openapi_endpoints(openapi_doc):
    """Return list of endpoints in OpenAPI."""
    data = {}
    try:
        data = yaml.load(openapi_doc, Loader=yaml.FullLoader)
    except yaml.YAMLError:
        try:
            data = json.loads(openapi_doc)
        except json.JSONDecodeError:
            raise XrdInfoError('Can not parse OpenAPI description')

    results = []
    try:
        for path, operations in data['paths'].items():
            for verb, operation in operations.items():
                results.append({
                    'verb': verb, 'path': path, 'summary': operation.get('summary', ''),
                    'description': operation.get('description', '')})
    except Exception:
        raise XrdInfoError('Endpoints not found')

    return results


def encode_part(part):
    """Percent-Encode identifier part."""
    return urlparse.quote(part, safe='')


def identifier(items):
    """Convert list/tuple to slash separated string representation of
    identifier. Each identifier part is Percent-Encoded.
    """
    return '/'.join(map(encode_part, items))


def identifier_parts(ident_str):
    """Convert identifier to list of parts."""
    return list(map(urlparse.unquote, ident_str.split('/')))
