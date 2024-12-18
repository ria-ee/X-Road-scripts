"""X-Road informational module.
This module can be used to query various types of information about
X-Road Members, Subsystems, Servers, Services and Service descriptions.
"""

__all__ = [
    'XrdInfoError', 'RequestTimeoutError', 'SoapFaultError', 'NotOpenapiServiceError',
    'OpenapiReadError', 'soap_request', 'rest_get_request', 'shared_params_ss', 'shared_params_cs',
    'members', 'subsystems', 'subsystems_with_membername', 'registered_subsystems',
    'subsystems_with_server', 'servers', 'addr_ips', 'servers_ips', 'methods', 'methods_rest',
    'wsdl', 'wsdl_methods', 'openapi', 'load_openapi', 'openapi_endpoints', 'identifier',
    'identifier_parts']

import json
from collections.abc import Iterator
from io import BytesIO
import re
import socket
from typing import Any, Sequence, TypeVar
from urllib import parse
import uuid
from xml.etree import ElementTree
import zipfile
import requests
import yaml

XRD_REST_VERSION = 'r1'

# Timeout for requests
DEFAULT_TIMEOUT: float = 5.0

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
NS: dict[str, str] = {
    'xrd': 'http://x-road.eu/xsd/xroad.xsd',
    'id': 'http://x-road.eu/xsd/identifiers',
    'wsdl': 'http://schemas.xmlsoap.org/wsdl/'}

T = TypeVar('T')


class XrdInfoError(Exception):
    """XrdInfo generic Exception."""

    def __init__(self, exc: Exception | str):
        if isinstance(exc, XrdInfoError):
            # No need to double wrap the exception
            super().__init__(exc)
        elif isinstance(exc, Exception):
            # Wrapped exception
            super().__init__(f'{type(exc).__name__}: {exc}')
        else:
            # Error message
            super().__init__(exc)


class RequestTimeoutError(XrdInfoError):
    """Request failed due to timeout."""


class SoapFaultError(XrdInfoError):
    """SOAP Fault received."""

    def __init__(self, msg: str | None):
        if str is not None:
            super().__init__(f'SoapFault: {msg}')
        else:
            super().__init__('SoapFault: <None>')


class NotOpenapiServiceError(XrdInfoError):
    """Requested service does not have OpenAPI description."""


class OpenapiReadError(XrdInfoError):
    """Producer Security Server failed to read OpenAPI description."""


def _fail_none(data: T | None) -> T:
    """Return input data if it is not None or fail with exception"""
    if data is not None:
        return data
    raise XrdInfoError('Unexpected None value')


def _instance_identifier(root: ElementTree.Element) -> str:
    """
    Return instanceIdentifier value from X-Road global configuration.
    Raises exception if expected value does not exist.
    """
    return _fail_none(_fail_none(root.find('./instanceIdentifier')).text)


def _members(root: ElementTree.Element) -> list[ElementTree.Element]:
    """Return member nodes from X-Road global configuration"""
    return root.findall('./member')


def _member(root: ElementTree.Element, member_id: str) -> ElementTree.Element:
    """
    Return member node with specified id
    from X-Road global configuration.
    Raises exception if specified member does not exist.
    """
    return _fail_none(root.find(f'./member[@id="{member_id}"]'))


def _member_class(member: ElementTree.Element) -> str:
    """
    Return memberClass value from member Element.
    Raises exception if expected value does not exist.
    """
    return _fail_none(_fail_none(member.find('./memberClass/code')).text)


def _member_code(member: ElementTree.Element) -> str:
    """
    Return memberClass value from member Element.
    Raises exception if expected value does not exist.
    """
    return _fail_none(_fail_none(member.find('./memberCode')).text)


def _member_name(member: ElementTree.Element) -> str:
    """
    Return member name value from member Element.
    Raises exception if expected value does not exist.
    """
    return _fail_none(_fail_none(member.find('./name')).text)


def _subsystems(member: ElementTree.Element) -> list[ElementTree.Element]:
    """Return subsystem nodes from member Element"""
    return member.findall('./subsystem')


def _subsystem_id(subsystem: ElementTree.Element) -> str:
    """
    Return subsystem id value from subsystem Element.
    Raises exception if expected value does not exist.
    """
    return _fail_none(subsystem.attrib['id'])


def _subsystem_code(subsystem: ElementTree.Element) -> str:
    """
    Return subsystemCode value from subsystem Element.
    Raises exception if expected value does not exist.
    """
    return _fail_none(_fail_none(subsystem.find('./subsystemCode')).text)


def _security_servers(
        root: ElementTree.Element, client_id: str | None = None) -> list[ElementTree.Element]:
    """
    Return securityServer nodes from X-Road global configuration.
    Results are optionally limited by server's client id.
    """
    if client_id:
        return root.findall(f'./securityServer[client="{client_id}"]')
    return root.findall('./securityServer')


def _server_code(server: ElementTree.Element) -> str:
    """
    Return serverCode value from securityServer Element.
    Raises exception if expected value does not exist.
    """
    return _fail_none(_fail_none(server.find('./serverCode')).text)


def _server_address(server: ElementTree.Element) -> str:
    """
    Return address value from securityServer Element.
    Raises exception if expected value does not exist.
    """
    return _fail_none(_fail_none(server.find('./address')).text)


def _server_owner_id(server: ElementTree.Element) -> str:
    """
    Return owner id value from securityServer Element.
    Raises exception if expected value does not exist.
    """
    return _fail_none(_fail_none(server.find('./owner')).text)


def _add_url_scheme(
        addr: str, verify: bool | str = False, cert: str | tuple[str, str] | None = None) -> str:
    """Add HTTP/HTTPS scheme to address if scheme is missing."""
    url = addr
    if not parse.urlsplit(url).scheme and (verify or cert):
        url = 'https://' + url
    elif not parse.urlsplit(url).scheme:
        url = 'http://' + url
    return url


def _encode_part(part: str) -> str:
    """Percent-Encode identifier part."""
    return parse.quote(part, safe='')


def soap_request(
        addr: str, data: str, timeout: float = DEFAULT_TIMEOUT, verify: bool | str = False,
        cert: str | tuple[str, str] | None = None) -> tuple[requests.Response, ElementTree.Element]:
    """X-Road SOAP request.
    Return tuple: (response, xml_root).
    """
    url = _add_url_scheme(addr, verify=verify, cert=cert)
    headers = {'content-type': 'text/xml'}
    try:
        response = requests.post(
            url, data=data.encode('utf-8'), headers=headers, timeout=timeout, verify=verify,
            cert=cert)
        response.raise_for_status()
        response.encoding = 'utf-8'
    except requests.exceptions.Timeout as err:
        raise RequestTimeoutError(err) from err
    except requests.exceptions.RequestException as err:
        raise XrdInfoError(err) from err

    # Searching for SOAP envelope and ignoring MIME parts in response
    envelope = re.search(
        '<SOAP-ENV:Envelope.+</SOAP-ENV:Envelope>', response.text, re.DOTALL)
    if envelope is None:
        raise XrdInfoError('SOAP envelope was not found in response')

    try:
        root = ElementTree.fromstring(envelope.group(0))
    except (AttributeError, ElementTree.ParseError) as err:
        raise XrdInfoError('Received incorrect SOAP response') from err
    fault_string = root.find('.//faultstring')
    if fault_string is not None:
        raise SoapFaultError(fault_string.text)

    return response, root


def rest_get_request(
        url: str, client_header: str, timeout: float = DEFAULT_TIMEOUT, verify: bool | str = False,
        cert: str | tuple[str, str] | None = None) -> requests.Response:
    """X-Road REST GET request."""
    headers = {'X-Road-Client': client_header, 'accept': 'application/json'}
    try:
        response = requests.get(
            url, headers=headers, timeout=timeout, verify=verify, cert=cert)
        response.encoding = 'utf-8'
        if 400 <= response.status_code < 600:
            # Trying to raise more precise exception
            resp = json.loads(response.text)
            if resp['message'] == 'Invalid service type: REST':
                raise NotOpenapiServiceError(
                    'Service does not have OpenAPI description')
            if re.search('^Failed reading service description from', resp['message']):
                raise OpenapiReadError('Failed reading service OpenAPI description')
            raise XrdInfoError(f"RestError: {resp['type']}: {resp['message']}")
        return response
    except requests.exceptions.Timeout as err:
        raise RequestTimeoutError(err) from err
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def shared_params_ss(
        addr: str, instance: str | None = None, timeout: float = DEFAULT_TIMEOUT,
        verify: bool | str = False, cert: str | tuple[str, str] | None = None) -> str:
    """Get shared-params.xml content from local Security Server.
    By default, return info about local X-Road instance.
    """
    try:
        url = _add_url_scheme(addr, verify=verify, cert=cert)
        # Add '/verificationconf' if path is missing
        if parse.urlsplit(url).path == '':
            url = url + '/verificationconf'
        elif parse.urlsplit(url).path == '/':
            url = url + 'verificationconf'
        ver_conf_response = requests.get(url, timeout=timeout, verify=verify, cert=cert)
        ver_conf_response.raise_for_status()
        zip_data = BytesIO()
        zip_data.write(ver_conf_response.content)
        with zipfile.ZipFile(zip_data) as ver_conf_zip:
            ident = instance
            if ident is None:
                # Use local instance configuration
                with ver_conf_zip.open('verificationconf/instance-identifier') as ident_file:
                    ident = ident_file.read().decode('utf-8')
            with ver_conf_zip.open(
                    f'verificationconf/{ident}/shared-params.xml') as shared_params_file:
                return shared_params_file.read().decode('utf-8')
    except requests.exceptions.Timeout as err:
        raise RequestTimeoutError(err) from err
    except Exception as err:
        raise XrdInfoError(err) from err


def shared_params_cs(
        addr: str, timeout: float = DEFAULT_TIMEOUT, verify: bool | str = False,
        cert: str | tuple[str, str] | None = None) -> str:
    """Get shared-params.xml content from Central Server/Configuration
    Proxy.
    Global configuration is not validated, use shared_params_ss whenever
    possible.
    """
    try:
        url = _add_url_scheme(addr, verify=verify, cert=cert)
        # Add '/internalconf' if path is missing
        if parse.urlsplit(url).path == '':
            url = url + '/internalconf'
        elif parse.urlsplit(url).path == '/':
            url = url + 'internalconf'
        global_conf = requests.get(url, timeout=timeout, verify=verify, cert=cert)
        global_conf.raise_for_status()
        # Configuration Proxy uses lowercase for 'Content-location'
        search_res = re.search(
            'Content-location: (/.+/shared-params.xml)', global_conf.text, re.IGNORECASE)
        if search_res is None:
            raise XrdInfoError('Shared parameters URI was not found')
        url2 = parse.urljoin(url, search_res.group(1))
        shared_params_response = requests.get(url2, timeout=timeout, verify=verify, cert=cert)
        shared_params_response.raise_for_status()
        shared_params_response.encoding = 'utf-8'
        return shared_params_response.text
    except requests.exceptions.Timeout as err:
        raise RequestTimeoutError(err) from err
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def members(shared_params: str) -> Iterator[tuple[str, str, str]]:
    """List Members in shared_params.
    Return tuple: (xRoadInstance, memberClass, memberCode).
    """
    try:
        root = ElementTree.fromstring(shared_params)
        instance = _instance_identifier(root)
        for member in _members(root):
            member_class = _member_class(member)
            member_code = _member_code(member)
            yield instance, member_class, member_code
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def subsystems(shared_params: str) -> Iterator[tuple[str, str, str, str]]:
    """List Subsystems in shared_params.
    Return tuple: (xRoadInstance, memberClass, memberCode,
    subsystemCode).
    """
    try:
        root = ElementTree.fromstring(shared_params)
        instance = _instance_identifier(root)
        for member in _members(root):
            member_class = _member_class(member)
            member_code = _member_code(member)
            for subsystem in _subsystems(member):
                subsystem_code = _subsystem_code(subsystem)
                yield instance, member_class, member_code, subsystem_code
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def subsystems_with_membername(shared_params: str) -> Iterator[tuple[str, str, str, str, str]]:
    """List Subsystems in shared_params with Member name.
    Return tuple: (xRoadInstance, memberClass, memberCode,
    subsystemCode, MemberName).
    """
    try:
        root = ElementTree.fromstring(shared_params)
        instance = _instance_identifier(root)
        for member in _members(root):
            member_class = _member_class(member)
            member_code = _member_code(member)
            member_name = _member_name(member)
            for subsystem in _subsystems(member):
                subsystem_code = _subsystem_code(subsystem)
                yield instance, member_class, member_code, subsystem_code, member_name
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def registered_subsystems(shared_params: str) -> Iterator[tuple[str, str, str, str]]:
    """List Subsystems in shared_params that are attached to Security
    Server (registered).
    Return tuple: (xRoadInstance, memberClass, memberCode,
    subsystemCode).
    """
    try:
        root = ElementTree.fromstring(shared_params)
        instance = _instance_identifier(root)
        for member in _members(root):
            member_class = _member_class(member)
            member_code = _member_code(member)
            for subsystem in _subsystems(member):
                subsystem_id = _subsystem_id(subsystem)
                subsystem_code = _subsystem_code(subsystem)
                if _security_servers(root, subsystem_id):
                    yield instance, member_class, member_code, subsystem_code
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def subsystems_with_server(shared_params: str) -> Iterator[
       tuple[str, str, str, str, str, str, str, str, str] | tuple[str, str, str, str]]:
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
        instance = _instance_identifier(root)
        for member in _members(root):
            member_class = _member_class(member)
            member_code = _member_code(member)
            for subsystem in _subsystems(member):
                subsystem_id = _subsystem_id(subsystem)
                subsystem_code = _subsystem_code(subsystem)
                server_found = False
                for server in _security_servers(root, subsystem_id):
                    owner_id = _server_owner_id(server)
                    owner = _member(root, owner_id)
                    owner_class = _member_class(owner)
                    owner_code = _member_code(owner)
                    server_code = _server_code(server)
                    address = _server_address(server)
                    yield (
                        instance, member_class, member_code, subsystem_code, instance, owner_class,
                        owner_code, server_code, address)
                    server_found = True
                if not server_found:
                    yield instance, member_class, member_code, subsystem_code
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def servers(shared_params: str) -> Iterator[tuple[str, str, str, str, str]]:
    """List Security Servers in shared_params.
    Return tuple: (Server Owners xRoadInstance,
    Server Owners memberClass, Server Owners memberCode, serverCode,
    Server Address).
    """
    try:
        root = ElementTree.fromstring(shared_params)
        instance = _instance_identifier(root)
        for server in _security_servers(root):
            owner_id = _server_owner_id(server)
            owner = _member(root, owner_id)
            member_class = _member_class(owner)
            member_code = _member_code(owner)
            server_code = _server_code(server)
            address = _server_address(server)
            yield instance, member_class, member_code, server_code, address
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def addr_ips(address: str) -> Iterator[str]:
    """Resolve DNS name to IP addresses.
    Unresolved DNS names are silently ignored.
    """
    try:
        yield from socket.gethostbyname_ex(address)[2]
    except socket.gaierror:
        # Ignoring DNS name not found error
        pass
    except Exception as err:
        raise XrdInfoError(err) from err


def servers_ips(shared_params: str) -> Iterator[str]:
    """List IP addresses of Security Servers in shared_params.
    Unresolved DNS names are silently ignored.
    """
    try:
        root = ElementTree.fromstring(shared_params)
        for server in _security_servers(root):
            address = _server_address(server)
            yield from addr_ips(address)
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def methods(
        addr: str, client: Sequence[str], producer: Sequence[str], method: str = 'listMethods',
        timeout: float = DEFAULT_TIMEOUT, verify: bool | str = False,
        cert: str | tuple[str, str] | None = None) -> Iterator[tuple[str, str, str, str, str, str]]:
    """Get X-Road listMethods or allowedMethods response.
    Return tuple: (xRoadInstance, memberClass, memberCode,
    subsystemCode, serviceCode, serviceVersion).
    """
    body = METHODS_BODY_TEMPL.format(service_code=method)
    if (len(client) == 3 or client[3] == '') and len(producer) == 4:
        data = REQUEST_MEMBER_TEMPL.format(
            client=client, service=producer, uuid=uuid.uuid4(), service_code=method, body=body)
    elif len(client) == 4 and len(producer) == 4:
        data = REQUEST_SUBSYSTEM_TEMPL.format(
            client=client, service=producer, uuid=uuid.uuid4(), service_code=method, body=body)
    else:
        raise XrdInfoError('Incorrect client or producer identifier length')

    _, root = soap_request(addr, data, timeout=timeout, verify=verify, cert=cert)
    try:
        for service in root.findall(f'.//xrd:{method}Response/xrd:service', NS):
            subsystem_code_el = service.find('./id:subsystemCode', NS)
            service_version_el = service.find('./id:serviceVersion', NS)
            result = {
                'xRoadInstance': _fail_none(_fail_none(
                    service.find('./id:xRoadInstance', NS)).text),
                'memberClass': _fail_none(_fail_none(service.find('./id:memberClass', NS)).text),
                'memberCode': _fail_none(_fail_none(service.find('./id:memberCode', NS)).text),
                # Element subsystemCode may be missing
                'subsystemCode':
                    _fail_none(subsystem_code_el.text)
                    if subsystem_code_el is not None
                    else '',
                'serviceCode': _fail_none(_fail_none(service.find('./id:serviceCode', NS)).text),
                # Element serviceVersion may be missing
                'serviceVersion':
                    _fail_none(service_version_el.text)
                    if service_version_el is not None
                    else ''}
            yield (result['xRoadInstance'], result['memberClass'], result['memberCode'],
                   result['subsystemCode'], result['serviceCode'], result['serviceVersion'])
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def methods_rest(
        addr: str, client: Sequence[str], producer: Sequence[str], method: str = 'listMethods',
        timeout: float = DEFAULT_TIMEOUT, verify: bool | str = False,
        cert: str | tuple[str, str] | None = None) -> Iterator[tuple[str, str, str, str, str]]:
    """Get X-Road listMethods or allowedMethods response.
    Return tuple: (xRoadInstance, memberClass, memberCode,
    subsystemCode, serviceCode).
    """
    if (len(client) == 3 or client[3] == '') and len(producer) == 4:
        client_header = identifier(client[:3])
    elif len(client) == 4 and len(producer) == 4:
        client_header = identifier(client[:4])
    else:
        raise XrdInfoError('Incorrect client or producer identifier length')

    url = _add_url_scheme(addr, verify=verify, cert=cert)
    url = parse.urljoin(url, f'/{XRD_REST_VERSION}/{identifier(producer)}/{method}')
    response = rest_get_request(url, client_header, timeout=timeout, verify=verify, cert=cert)

    try:
        services = response.json()
        for service in services['service']:
            yield (service['xroad_instance'], service['member_class'], service['member_code'],
                   service['subsystem_code'], service['service_code'])
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def wsdl(
        addr: str, client: Sequence[str], service: Sequence[str],
        timeout: float = DEFAULT_TIMEOUT, verify: bool | str = False,
        cert: str | tuple[str, str] | None = None) -> str:
    """Get X-Road getWsdl response."""
    if len(service) != 6:
        raise XrdInfoError('Incorrect service identifier length')

    if service[5]:
        # Service with version
        body = GETWSDL_BODY_TEMPL.format(service_code=service[4], service_version=service[5])
    else:
        body = GETWSDL_BODY_TEMPL_NOVERSION.format(service_code=service[4])

    if len(client) == 3 or client[3] == '':
        # Request as member
        data = REQUEST_MEMBER_TEMPL.format(
            client=client, service=service, uuid=uuid.uuid4(), service_code=GETWSDL_SERVICE_CODE,
            body=body)
    elif len(client) == 4:
        # Request as subsystem
        data = REQUEST_SUBSYSTEM_TEMPL.format(
            client=client, service=service, uuid=uuid.uuid4(), service_code=GETWSDL_SERVICE_CODE,
            body=body)
    else:
        raise XrdInfoError('Incorrect client identifier length')

    wsdl_response, _ = soap_request(addr, data, timeout=timeout, verify=verify, cert=cert)

    resp = re.search(
        '--xroad.+content-type:text/xml.+<SOAP-ENV:Envelope.+</SOAP-ENV:Envelope>'
        '.+--xroad.+content-type:text/xml.*?\r\n\r\n(.+)\r\n--xroad.+',
        wsdl_response.text, re.DOTALL)
    if resp:
        return resp.group(1)
    raise XrdInfoError('WSDL not found')


def wsdl_methods(wsdl_doc: str) -> Iterator[tuple[str, str]]:
    """Return list of methods in WSDL."""
    try:
        root = ElementTree.fromstring(wsdl_doc)
        for operation in root.findall('.//wsdl:binding/wsdl:operation', NS):
            version_el = operation.find('./xrd:version', NS)
            version = _fail_none(version_el.text) if version_el is not None else ''
            if 'name' in operation.attrib:
                yield operation.attrib['name'], version
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def openapi(
        addr: str, client: Sequence[str], service: Sequence[str],
        timeout: float = DEFAULT_TIMEOUT, verify: bool | str = False,
        cert: str | tuple[str, str] | None = None) -> str:
    """Get X-Road getOpenAPI response."""
    if (len(client) == 3 or client[3] == '') and len(service) == 5:
        client_header = identifier(client[:3])
    elif len(client) == 4 and len(service) == 5:
        client_header = identifier(client[:4])
    else:
        raise XrdInfoError('Incorrect client or service identifier length')

    url = _add_url_scheme(addr, verify=verify, cert=cert)
    url = parse.urljoin(
        url, f'/{XRD_REST_VERSION}/{identifier(service[:4])}'
             f'/getOpenAPI?serviceCode={_encode_part(service[4])}')
    try:
        return rest_get_request(
            url, client_header, timeout=timeout, verify=verify, cert=cert).text
    except XrdInfoError:
        # Re-raising XrdInfo sub-exception before it gets
        # overwritten by generic XrdInfoError
        raise
    except Exception as err:
        raise XrdInfoError(err) from err


def load_openapi(openapi_doc: str) -> tuple[Any, str]:
    """Load OpenAPI description into Python object.
    Return tuple: (data, document_type).
    """
    try:
        # Checking JSON first, because YAML is a superset of JSON
        data = json.loads(openapi_doc)
        return data, 'json'
    except json.JSONDecodeError:
        try:
            data = yaml.safe_load(openapi_doc)
            return data, 'yaml'
        except yaml.YAMLError as err:
            raise XrdInfoError('Can not parse OpenAPI description') from err


def openapi_endpoints(openapi_doc: str) -> list[dict[str, Any]]:
    """Return list of endpoints in OpenAPI."""
    data, _ = load_openapi(openapi_doc)

    results = []
    try:
        for path, operations in data['paths'].items():
            for verb, operation in operations.items():
                if verb in ['get', 'put', 'post', 'delete', 'options', 'head', 'patch', 'trace']:
                    results.append({
                        'verb': verb, 'path': path, 'summary': operation.get('summary', ''),
                        'description': operation.get('description', '')})
    except Exception as err:
        raise XrdInfoError('Endpoints not found') from err

    if not results:
        # OpenAPI without endpoints is not considered valid
        raise XrdInfoError('Endpoints not found')

    return results


def identifier(items: Sequence[str]) -> str:
    """Convert list/tuple to slash separated string representation of
    identifier. Each identifier part is Percent-Encoded.
    """
    return '/'.join(map(_encode_part, items))


def identifier_parts(ident_str: str) -> list[str]:
    """Convert identifier to list of parts."""
    return list(map(parse.unquote, ident_str.split('/')))
