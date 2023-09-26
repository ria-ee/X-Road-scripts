#!/usr/bin/python3

"""X-Road Health and Environment monitoring collector for Zabbix."""

# pip install py-zabbix requests
import argparse
import calendar
import configparser
import logging
import queue
import re
import sys
import threading
import time
import uuid
from urllib.parse import urlsplit
from xml.etree import ElementTree
import requests
from pyzabbix import ZabbixMetric, ZabbixSender, ZabbixAPI

# Dict containing default configuration
DEFAULT_PARAMS = {
    # Collect Environmental Monitoring data instead of Health data
    'envmon': False,
    # Debug levels:
    # 0 = Errors only; 1 = Simple debug; 2 = Detailed debug;
    # 3 = Debug py-zabbix (use with thread_count=1).
    'debug': 2,
    # Zabbix configuration
    'zabbix_url': 'http://localhost/',
    'zabbix_sender_port': 10051,
    'zabbix_user': 'zabbix_user',
    'zabbix_pass': 'zabbix_pass',
    'zabbix_group_id': '2',  # "Linux servers"
    'zabbix_trapper_type': '2',  # "Zabbix trapper"
    # EnvMon template configuration
    'envmon_template_id': '10242',
    'envmon_template_name': 'X-Road Environmental monitoring',
    # Zabbix may reject metrics if they are sent to quickly after host creation or items addition
    # Sleep after host changes helps to avoid this problem
    # Sleep should be longer than Zabbix Server CacheUpdateFrequency value (default 60s)
    'sleep_after_host_change': 0,
    # Security server URL used by Central monitoring:
    'server_url': 'http://xrd0.ss.dns',
    # Path to TLS certificate and key in case when Security Server
    # requires TLS authentication
    'tls_cert': '',
    'tls_key': '',
    # Path to Security Server certificate CA for server certificate validation
    'tls_ca': '',
    # Client subsystem that performs health data collection:
    'monitoring_client_inst': 'INST',
    'monitoring_client_class': 'GOV',
    'monitoring_client_member': '00000000',
    'monitoring_client_subsystem': 'Central monitoring',
    # How many threads to use for data querying.
    'thread_count': 2,
    # Timeout for http requests
    'timeout': 15.0,
    # List of servers to collect data from
    'servers': ''}

# Default configuration file
CONF_FILE = 'metrics.cfg'

# Configuration section used
CONF_SECTION = 'metrics'

# Namespace of monitoring service
NS = {'m': 'http://x-road.eu/xsd/monitoring',
      'om': 'http://x-road.eu/xsd/op-monitoring.xsd',
      'id': 'http://x-road.eu/xsd/identifiers'}

# Zabbix value types:
#     "0": Numeric (float)
#     "1": Character
#     "2": Log
#     "3": Numeric (unsigned)
#     "4": Text
# Definitions of Server Items
SERVER_HEALTH_ITEMS = [
    {'key': 'monitoringStartupTimestamp', 'type': '3', 'units': 'ms', 'history': '7',
     'description': 'The Unix timestamp in milliseconds when the monitoring system was started.'},
    {'key': 'statisticsPeriodSeconds', 'type': '3', 'units': 's', 'history': '7',
     'description': 'Duration of the statistics period in seconds.'}]

# Definitions of Service Items
SERVICE_HEALTH_ITEMS = [
    {'key': 'successfulRequestCount', 'type': '3', 'units': None, 'history': '7',
     'description': 'The number of successful requests occurred during the last period.'},
    {'key': 'unsuccessfulRequestCount', 'type': '3', 'units': None, 'history': '7',
     'description': 'The number of unsuccessful requests occurred during the last period.'},
    {'key': 'requestMinDuration', 'type': '3', 'units': 'ms', 'history': '7',
     'description': 'The minimum duration of the request in milliseconds.'},
    {'key': 'requestAverageDuration', 'type': '0', 'units': 'ms', 'history': '7',
     'description': 'The average duration of the request in milliseconds.'},
    {'key': 'requestMaxDuration', 'type': '3', 'units': 'ms', 'history': '7',
     'description': 'The maximum duration of the request in milliseconds.'},
    {'key': 'requestDurationStdDev', 'type': '0', 'units': 'ms', 'history': '7',
     'description': 'The standard deviation of the duration of the requests.'},
    {'key': 'requestMinSoapSize', 'type': '3', 'units': 'B', 'history': '7',
     'description': 'The minimum SOAP message size of the request in bytes.'},
    {'key': 'requestAverageSoapSize', 'type': '0', 'units': 'B', 'history': '7',
     'description': 'The average SOAP message size of the request in bytes.'},
    {'key': 'requestMaxSoapSize', 'type': '3', 'units': 'B', 'history': '7',
     'description': 'The maximum SOAP message size of the request in bytes.'},
    {'key': 'requestSoapSizeStdDev', 'type': '0', 'units': 'B', 'history': '7',
     'description': 'The standard deviation of the SOAP message size of the request.'},
    {'key': 'responseMinSoapSize', 'type': '3', 'units': 'B', 'history': '7',
     'description': 'The minimum SOAP message size of the response in bytes.'},
    {'key': 'responseAverageSoapSize', 'type': '0', 'units': 'B', 'history': '7',
     'description': 'The average SOAP message size of the response in bytes.'},
    {'key': 'responseMaxSoapSize', 'type': '3', 'units': 'B', 'history': '7',
     'description': 'The maximum SOAP message size of the response in bytes.'},
    {'key': 'responseSoapSizeStdDev', 'type': '0', 'units': 'B', 'history': '7',
     'description': 'The standard deviation of the SOAP message size of the response.'}]

ENVMON_METRICS = [
    {'name': 'proxyVersion', 'type': 'stringMetric'},
    {'name': 'CommittedVirtualMemory', 'type': 'histogramMetric'},
    {'name': 'FreePhysicalMemory', 'type': 'histogramMetric'},
    {'name': 'FreeSwapSpace', 'type': 'histogramMetric'},
    {'name': 'OpenFileDescriptorCount', 'type': 'histogramMetric'},
    {'name': 'SystemCpuLoad', 'type': 'histogramMetric'},
    {'name': 'DiskSpaceFree_/', 'type': 'numericMetric'},
    {'name': 'DiskSpaceTotal_/', 'type': 'numericMetric'},
    {'name': 'MaxFileDescriptorCount', 'type': 'numericMetric'},
    {'name': 'OperatingSystem', 'type': 'stringMetric'},
    {'name': 'TotalPhysicalMemory', 'type': 'numericMetric'},
    {'name': 'TotalSwapSpace', 'type': 'numericMetric'}]

HEALTH_REQUEST_TEMPLATE = """<SOAP-ENV:Envelope
       xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
       xmlns:id="http://x-road.eu/xsd/identifiers"
       xmlns:xrd="http://x-road.eu/xsd/xroad.xsd"
       xmlns:om="http://x-road.eu/xsd/op-monitoring.xsd">
    <SOAP-ENV:Header>
        <xrd:client id:objectType="SUBSYSTEM">
            <id:xRoadInstance>{monitor_instance}</id:xRoadInstance>
            <id:memberClass>{monitor_class}</id:memberClass>
            <id:memberCode>{monitor_member}</id:memberCode>
            <id:subsystemCode>{monitor_subsystem}</id:subsystemCode>
        </xrd:client>
        <xrd:service id:objectType="SERVICE">
            <id:xRoadInstance>{instance}</id:xRoadInstance>
            <id:memberClass>{member_class}</id:memberClass>
            <id:memberCode>{member_code}</id:memberCode>
            <id:serviceCode>getSecurityServerHealthData</id:serviceCode>
        </xrd:service>
        <xrd:securityServer id:objectType="SERVER">
            <id:xRoadInstance>{instance}</id:xRoadInstance>
            <id:memberClass>{member_class}</id:memberClass>
            <id:memberCode>{member_code}</id:memberCode>
            <id:serverCode>{server_code}</id:serverCode>
        </xrd:securityServer>
        <xrd:id>{uuid}</xrd:id>
        <xrd:protocolVersion>4.0</xrd:protocolVersion>
    </SOAP-ENV:Header>
    <SOAP-ENV:Body>
        <om:getSecurityServerHealthData/>
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
"""

ENVMON_REQUEST_TEMPLATE = """<SOAP-ENV:Envelope
       xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
       xmlns:id="http://x-road.eu/xsd/identifiers"
       xmlns:xrd="http://x-road.eu/xsd/xroad.xsd"
       xmlns:m="http://x-road.eu/xsd/monitoring">
    <SOAP-ENV:Header>
        <xrd:client id:objectType="SUBSYSTEM">
            <id:xRoadInstance>{monitor_instance}</id:xRoadInstance>
            <id:memberClass>{monitor_class}</id:memberClass>
            <id:memberCode>{monitor_member}</id:memberCode>
            <id:subsystemCode>{monitor_subsystem}</id:subsystemCode>
        </xrd:client>
        <xrd:service id:objectType="SERVICE">
            <id:xRoadInstance>{instance}</id:xRoadInstance>
            <id:memberClass>{member_class}</id:memberClass>
            <id:memberCode>{member_code}</id:memberCode>
            <id:serviceCode>getSecurityServerMetrics</id:serviceCode>
        </xrd:service>
        <xrd:securityServer id:objectType="SERVER">
            <id:xRoadInstance>{instance}</id:xRoadInstance>
            <id:memberClass>{member_class}</id:memberClass>
            <id:memberCode>{member_code}</id:memberCode>
            <id:serverCode>{server_code}</id:serverCode>
        </xrd:securityServer>
        <xrd:id>{uuid}</xrd:id>
        <xrd:protocolVersion>4.0</xrd:protocolVersion>
    </SOAP-ENV:Header>
    <SOAP-ENV:Body>
        <m:getSecurityServerMetrics>
            <m:outputSpec>
                <m:outputField>proxyVersion</m:outputField>
                <m:outputField>CommittedVirtualMemory</m:outputField>
                <m:outputField>FreePhysicalMemory</m:outputField>
                <m:outputField>FreeSwapSpace</m:outputField>
                <m:outputField>OpenFileDescriptorCount</m:outputField>
                <m:outputField>SystemCpuLoad</m:outputField>
                <m:outputField>DiskSpaceFree_/</m:outputField>
                <m:outputField>DiskSpaceTotal_/</m:outputField>
                <m:outputField>MaxFileDescriptorCount</m:outputField>
                <m:outputField>OperatingSystem</m:outputField>
                <m:outputField>TotalPhysicalMemory</m:outputField>
                <m:outputField>TotalSwapSpace</m:outputField>
                <m:outputField>Packages</m:outputField>
                <m:outputField>Certificates</m:outputField>
            </m:outputSpec>
        </m:getSecurityServerMetrics>
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
"""

HEALTH_REQUEST_MEMBER_TEMPLATE = """<SOAP-ENV:Envelope
       xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
       xmlns:id="http://x-road.eu/xsd/identifiers"
       xmlns:xrd="http://x-road.eu/xsd/xroad.xsd"
       xmlns:om="http://x-road.eu/xsd/op-monitoring.xsd">
    <SOAP-ENV:Header>
        <xrd:client id:objectType="MEMBER">
            <id:xRoadInstance>{monitor_instance}</id:xRoadInstance>
            <id:memberClass>{monitor_class}</id:memberClass>
            <id:memberCode>{monitor_member}</id:memberCode>
        </xrd:client>
        <xrd:service id:objectType="SERVICE">
            <id:xRoadInstance>{instance}</id:xRoadInstance>
            <id:memberClass>{member_class}</id:memberClass>
            <id:memberCode>{member_code}</id:memberCode>
            <id:serviceCode>getSecurityServerHealthData</id:serviceCode>
        </xrd:service>
        <xrd:securityServer id:objectType="SERVER">
            <id:xRoadInstance>{instance}</id:xRoadInstance>
            <id:memberClass>{member_class}</id:memberClass>
            <id:memberCode>{member_code}</id:memberCode>
            <id:serverCode>{server_code}</id:serverCode>
        </xrd:securityServer>
        <xrd:id>{uuid}</xrd:id>
        <xrd:protocolVersion>4.0</xrd:protocolVersion>
    </SOAP-ENV:Header>
    <SOAP-ENV:Body>
        <om:getSecurityServerHealthData/>
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
"""

ENVMON_REQUEST_MEMBER_TEMPLATE = """<SOAP-ENV:Envelope
       xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
       xmlns:id="http://x-road.eu/xsd/identifiers"
       xmlns:xrd="http://x-road.eu/xsd/xroad.xsd"
       xmlns:m="http://x-road.eu/xsd/monitoring">
    <SOAP-ENV:Header>
        <xrd:client id:objectType="MEMBER">
            <id:xRoadInstance>{monitor_instance}</id:xRoadInstance>
            <id:memberClass>{monitor_class}</id:memberClass>
            <id:memberCode>{monitor_member}</id:memberCode>
        </xrd:client>
        <xrd:service id:objectType="SERVICE">
            <id:xRoadInstance>{instance}</id:xRoadInstance>
            <id:memberClass>{member_class}</id:memberClass>
            <id:memberCode>{member_code}</id:memberCode>
            <id:serviceCode>getSecurityServerMetrics</id:serviceCode>
        </xrd:service>
        <xrd:securityServer id:objectType="SERVER">
            <id:xRoadInstance>{instance}</id:xRoadInstance>
            <id:memberClass>{member_class}</id:memberClass>
            <id:memberCode>{member_code}</id:memberCode>
            <id:serverCode>{server_code}</id:serverCode>
        </xrd:securityServer>
        <xrd:id>{uuid}</xrd:id>
        <xrd:protocolVersion>4.0</xrd:protocolVersion>
    </SOAP-ENV:Header>
    <SOAP-ENV:Body>
        <m:getSecurityServerMetrics>
            <m:outputSpec>
                <m:outputField>proxyVersion</m:outputField>
                <m:outputField>CommittedVirtualMemory</m:outputField>
                <m:outputField>FreePhysicalMemory</m:outputField>
                <m:outputField>FreeSwapSpace</m:outputField>
                <m:outputField>OpenFileDescriptorCount</m:outputField>
                <m:outputField>SystemCpuLoad</m:outputField>
                <m:outputField>DiskSpaceFree_/</m:outputField>
                <m:outputField>DiskSpaceTotal_/</m:outputField>
                <m:outputField>MaxFileDescriptorCount</m:outputField>
                <m:outputField>OperatingSystem</m:outputField>
                <m:outputField>TotalPhysicalMemory</m:outputField>
                <m:outputField>TotalSwapSpace</m:outputField>
                <m:outputField>Packages</m:outputField>
                <m:outputField>Certificates</m:outputField>
            </m:outputSpec>
        </m:getSecurityServerMetrics>
    </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
"""


def print_debug(content):
    """Thread safe and unicode safe debug printer."""
    content = f'{threading.current_thread().name}: {content}\n'
    sys.stdout.write(content)


def print_error(content):
    """Thread safe and unicode safe error printer."""
    content = f"{threading.current_thread().name}: ERROR: '{content}'\n"
    sys.stderr.write(content)


def load_conf(conf_arg):
    """ Load configuration from file."""
    params = DEFAULT_PARAMS
    config = configparser.RawConfigParser()
    conf_name = CONF_FILE
    if conf_arg:
        # Using configuration file provided by user
        conf_name = conf_arg

    try:
        config.read(conf_name)
    except configparser.Error as err:
        print_error(f"Cannot load configuration '{conf_name}'\nDetail: {err}")
        sys.exit(1)

    if CONF_SECTION not in config.sections():
        if conf_arg is None:
            # Configuration file may be missing if default
            # configuration is up-to-date
            if params['debug']:
                print_debug('Configuration not found, using default values.')
            return params
        # User provided configuration is incorrect
        print_error(f"No [{CONF_SECTION}] section found in configuration file '{conf_name}'.")
        sys.exit(1)

    # All items found in configuration file
    conf_items = dict(config.items(CONF_SECTION)).keys()

    try:
        if 'debug' in conf_items:
            params['debug'] = config.getint(CONF_SECTION, 'debug')
        if 'zabbix_url' in conf_items:
            params['zabbix_url'] = config.get(CONF_SECTION, 'zabbix_url')
        if 'zabbix_sender_port' in conf_items:
            params['zabbix_sender_port'] = config.getint(CONF_SECTION, 'zabbix_sender_port')
        if 'zabbix_user' in conf_items:
            params['zabbix_user'] = config.get(CONF_SECTION, 'zabbix_user')
        if 'zabbix_pass' in conf_items:
            params['zabbix_pass'] = config.get(CONF_SECTION, 'zabbix_pass')
        if 'zabbix_group_id' in conf_items:
            params['zabbix_group_id'] = config.get(CONF_SECTION, 'zabbix_group_id')
        if 'zabbix_trapper_type' in conf_items:
            params['zabbix_trapper_type'] = config.get(CONF_SECTION, 'zabbix_trapper_type')
        if 'envmon_template_id' in conf_items:
            params['envmon_template_id'] = config.get(CONF_SECTION, 'envmon_template_id')
        if 'envmon_template_name' in conf_items:
            params['envmon_template_name'] = config.get(CONF_SECTION, 'envmon_template_name')
        if 'sleep_after_host_change' in conf_items:
            params['sleep_after_host_change'] = int(config.get(CONF_SECTION, 'sleep_after_host_change'))
        if 'server_url' in conf_items:
            params['server_url'] = config.get(CONF_SECTION, 'server_url')
        if 'tls_cert' in conf_items:
            params['tls_cert'] = config.get(CONF_SECTION, 'tls_cert')
        if 'tls_key' in conf_items:
            params['tls_key'] = config.get(CONF_SECTION, 'tls_key')
        if 'tls_ca' in conf_items:
            params['tls_ca'] = config.get(CONF_SECTION, 'tls_ca')
        if 'monitoring_client_inst' in conf_items:
            params['monitoring_client_inst'] = config.get(
                CONF_SECTION, 'monitoring_client_inst')
        if 'monitoring_client_class' in conf_items:
            params['monitoring_client_class'] = config.get(
                CONF_SECTION, 'monitoring_client_class')
        if 'monitoring_client_member' in conf_items:
            params['monitoring_client_member'] = config.get(
                CONF_SECTION, 'monitoring_client_member')
        if 'monitoring_client_subsystem' in conf_items:
            params['monitoring_client_subsystem'] = config.get(
                CONF_SECTION, 'monitoring_client_subsystem')
        if 'thread_count' in conf_items:
            params['thread_count'] = config.getint(CONF_SECTION, 'thread_count')
        if 'timeout' in conf_items:
            params['timeout'] = config.getfloat(CONF_SECTION, 'timeout')
        if 'servers' in conf_items:
            params['servers'] = config.get(CONF_SECTION, 'servers')
    except ValueError as err:
        print_error(f"Incorrect value found in configuration file '{conf_name}'.\nDetail: {err}")
        sys.exit(1)

    if params['debug']:
        print_debug(f"Configuration loaded from '{conf_name}'.")

    return params


def get_template_name(params, template_id):
    """Query Template name from Zabbix."""
    try:
        template = params['zapi'].template.get(
            templateids=template_id,
            output=['templateid', 'host'],
        )
        if not template:
            return None
        return template[0]['host']
    except Exception as err:
        if params['debug'] > 1:
            print_debug(f'get_template_name: {err}')
        return None


def check_template(params, host_id, parent_templates):
    """Check if EnvMon Template is added to Host and add the Host if
    necessary.
    """
    parent_template_ids = [item['templateid'] for item in parent_templates]
    if params['envmon_template_id'] not in parent_template_ids:
        # Add template to host
        if params['debug']:
            print_debug(f"Adding EnvMon Template to HostId '{host_id}' to Zabbix.")
        try:
            result = params['zapi'].host.update(
                hostid=host_id,
                templates=[
                    {
                        'templateid': params['envmon_template_id']
                    }
                ],
            )
            params['host_changed'] = True
            if not result['hostids'][0] == host_id:
                return None
        except Exception as err:
            if params['debug'] > 1:
                print_debug(f"check_template: {err}")
            return None
    return True


def get_host(params, host_name):
    """Query Host data from Zabbix."""
    try:
        host = params['zapi'].host.get(
            filter={'host': [host_name]},
            output=['hostid', 'host', 'name', 'status'],
            selectItems=['key_'],
            selectParentTemplates=['templateid'],
        )
        if not host:
            return None
        return host[0]
    except Exception as err:
        if params['debug'] > 1:
            print_debug(f"get_host: {err}")
        return None


def add_host(params, host_name, host_visible_name):
    """Add Host to Zabbix."""
    try:
        result = params['zapi'].host.create(
            host=host_name,
            name=host_visible_name,
            description=host_visible_name,
            interfaces=[
                {
                    'type': 1,
                    'main': 1,
                    'useip': 1,
                    'ip': '127.0.0.1',
                    'dns': '',
                    'port': '10050'
                }
            ],
            groups=[
                {
                    'groupid': params['zabbix_group_id']
                }
            ],
            templates=[
                {
                    'templateid': params['envmon_template_id']
                }
            ] if params['envmon'] else []
        )
        params['host_changed'] = True
        return result['hostids'][0]
    except Exception as err:
        if params['debug'] > 1:
            print_debug(f'add_host: {err}')
        return None


def check_host(params, host_name, host_visible_name):
    """Check if Host is added to Zabbix and add the Host if
    necessary.
    """
    host_data = get_host(params, host_name)
    if host_data is None:
        if params['debug']:
            print_debug(f"Adding Host '{host_name}' to Zabbix.")
        if add_host(params, host_name, host_visible_name):
            host_data = get_host(params, host_name)
    return host_data


def add_item(params, host_id, item, tag):
    """Add Item to Zabbix."""
    try:
        result = params['zapi'].item.create(
            hostid=host_id,
            name=item['name'] if 'name' in item else item['key'],
            key_=item['key'],
            type=params['zabbix_trapper_type'],
            trapper_hosts='0.0.0.0/0',
            value_type=item['type'],
            units=item['units'],
            history=item['history']+'d',
            description=item['description'],
            tags=[{'tag': 'Service', 'value': tag}] if tag else [],
        )
        params['host_changed'] = True
        return result['itemids']
    except Exception as err:
        if params['debug'] > 1:
            print_debug(f'add_item: {err}')
        return None


def check_server_items(params, host_id, host_items):
    """Check if Server Items are already added to the Host, and adds
    missing Items.
    """
    for item in SERVER_HEALTH_ITEMS:
        if item['key'] not in host_items:
            if params['debug']:
                print_debug(f"Adding item: '{item['key']}' for host_id '{host_id}'.")
            if add_item(params, host_id, item, None) is None:
                return None
    return True


def check_service_items(params, host_id, host_items, service_name, service_key):
    """Check if Service Items are already added to the Host, and adds
    missing Items.
    """
    for const_item in SERVICE_HEALTH_ITEMS:
        item = const_item.copy()
        item['name'] = f"{service_name}[{item['key']}]"
        item['key'] = f"{service_key}[{item['key']}]"
        if item['key'] not in host_items:
            if params['debug']:
                print_debug(f"Adding item: '{item['key']}' for host_id '{host_id}'.")
            if add_item(params, host_id, item, service_name) is None:
                return None
    return True


def get_service_name(service):
    """Get service name from XML element."""
    elem = service.find('./id:xRoadInstance', NS)
    x_road_instance = elem.text if elem is not None else ''
    elem = service.find('./id:memberClass', NS)
    member_class = elem.text if elem is not None else ''
    elem = service.find('./id:memberCode', NS)
    member_code = elem.text if elem is not None else ''
    elem = service.find('./id:subsystemCode', NS)
    subsystem_code = elem.text if elem is not None else ''
    elem = service.find('./id:serviceCode', NS)
    service_code = elem.text if elem is not None else ''
    elem = service.find('./id:serviceVersion', NS)
    service_version = elem.text if elem is not None else ''
    return (
        f'{x_road_instance}/{member_class}/{member_code}/'
        f'{subsystem_code}/{service_code}/{service_version}')


def get_metric(params, node, server):
    """Convert XML metric to ZabbixMetric.
       Return Zabbix packet elements.
    """
    if params is None or node is None or server is None:
        return None

    res = []
    nsp = '{' + NS['m'] + '}'

    if node.tag in (nsp + 'stringMetric', nsp + 'numericMetric'):
        try:
            name = node.find('./m:name', NS).text
            # Some names may have '/' character which is forbidden by
            # Zabbix
            name = name.replace('/', '')
            res.append(ZabbixMetric(server, name, node.find('./m:value', NS).text))
            return res
        except AttributeError:
            if params['debug'] > 1:
                print_debug(f'get_metric: Incorrect node: {ElementTree.tostring(node)}')
            return None
    elif node.tag == nsp + 'histogramMetric':
        try:
            name = node.find('./m:name', NS).text
            res.append(ZabbixMetric(server, name + '_updated', node.find('./m:updated', NS).text))
            res.append(ZabbixMetric(server, name + '_min', node.find('./m:min', NS).text))
            res.append(ZabbixMetric(server, name + '_max', node.find('./m:max', NS).text))
            res.append(ZabbixMetric(server, name + '_mean', node.find('./m:mean', NS).text))
            res.append(ZabbixMetric(server, name + '_median', node.find('./m:median', NS).text))
            res.append(ZabbixMetric(server, name + '_stddev', node.find('./m:stddev', NS).text))
            return res
        except AttributeError:
            if params['debug'] > 1:
                print_debug(f'get_metric: Incorrect node: {ElementTree.tostring(node)}')
            return None
    else:
        return None


def get_x_road_packages(params, node, server):
    """Convert XML Packages metric to ZabbixMetric (includes only X-Road
    packages)
    Return Zabbix packet elements.
    """
    if params is None or node is None or server is None:
        return None

    res = []

    try:
        name = node.find('./m:name', NS).text
        data = ''
        for pack in node.findall('./m:stringMetric', NS):
            package_name = pack.find('./m:name', NS).text
            if 'xroad' in package_name or 'xtee' in package_name:
                data += f"{package_name}: {pack.find('./m:value', NS).text}\n"
        res.append(ZabbixMetric(server, name, data))
        return res
    except AttributeError:
        if params['debug'] > 1:
            print_debug(f'get_x_road_packages: Incorrect node: {ElementTree.tostring(node)}')
        return None


def get_certificates(params, node, server):
    """Convert XML Certificates metric to ZabbixMetric
    Return Zabbix packet elements.
    """
    if params is None or node is None or server is None:
        return None

    res = []

    try:
        name = node.find('./m:name', NS).text
        data = ''
        max_not_before = None
        min_not_after = None
        for certificate in node.findall('./m:metricSet', NS):
            sha1_hash = certificate.find(".//m:stringMetric[m:name='sha1Hash']", NS)
            not_before = certificate.find(".//m:stringMetric[m:name='notBefore']", NS)
            not_before_value = not_before.find('./m:value', NS).text
            not_after = certificate.find(".//m:stringMetric[m:name='notAfter']", NS)
            not_after_value = not_after.find('./m:value', NS).text
            certificate_type = certificate.find(".//m:stringMetric[m:name='certificateType']", NS)
            certificate_type_value = certificate_type.find('./m:value', NS).text
            active = certificate.find(".//m:stringMetric[m:name='active']", NS)
            active_value = active.find('./m:value', NS).text
            data += (f"sha1Hash: {sha1_hash.find('./m:value', NS).text}\n"
                     f"notBefore: {not_before_value}\nnotAfter: {not_after_value}\n"
                     f"certificateType: {certificate_type_value}\nactive: {active_value}\n\n")
            # Not checking validity of disabled or client certificates
            if active_value == 'false' or certificate_type_value == 'INTERNAL_IS_CLIENT_TLS':
                continue
            not_before_time = time.strptime(not_before_value, '%Y-%m-%dT%H:%M:%SZ')
            not_after_time = time.strptime(not_after_value, '%Y-%m-%dT%H:%M:%SZ')
            if max_not_before is None or max_not_before < not_before_time:
                max_not_before = not_before_time
            if min_not_after is None or min_not_after > not_after_time:
                min_not_after = not_after_time

        # Adding Certificates metric
        res.append(ZabbixMetric(server, name, data))

        # Adding Certificates_validity metric
        current_time = time.gmtime()
        if current_time < max_not_before or current_time > min_not_after:
            # Some certificate is not yet valid or already expired
            res.append(ZabbixMetric(server, name + '_validity', '0'))
        else:
            res.append(ZabbixMetric(server, name + '_validity', str(
                calendar.timegm(min_not_after) - calendar.timegm(current_time))))
        return res
    except AttributeError:
        if params['debug'] > 1:
            print_debug(f'get_certificates: Incorrect node: {ElementTree.tostring(node)}')
        return None


def host_mon(shared_params, server_data):
    """Query Host monitoring data (Health or EnvMon) and save to Zabbix.
    """
    # Examples of server_data:
    # INST/GOV/00000000/00000000_1/xrd0.ss.dns
    # INST/GOV/00000001/00000001_1/xrd1.ss.dns
    # INST/COM/00000002/00000002_1/xrd2.ss.dns
    # Server name part is "greedy" match to allow server names to have
    # "/" character
    match = re.match('^(.+?)/(.+?)/(.+?)/(.+)/(.+?)$', server_data)

    # Creating copy of params to be able to modify that without affecting other threads.
    params = shared_params.copy()
    params['host_changed'] = False

    if match is None or match.lastindex != 5:
        print_error(f"Incorrect server string '{server_data}'!")
        return

    host_visible_name = match.group(0)
    host_name = re.sub('[^0-9a-zA-Z-]+', '.', host_visible_name)

    if params['debug']:
        print_debug(f"Processing Host '{host_name}'.")

    # Check if Host is added to Zabbix and adds the Host if necessary
    host_data = check_host(params, host_name, host_visible_name)
    if host_data is None:
        print_error(f"Cannot add Host '{host_name}' to Zabbix!")
        return

    # Check if Host is disabled (status == 1)
    if host_data['status'] == 1:
        print_error(f"Host '{host_name}' is disabled.")
        return

    # Getting list of Items already added to Host
    host_items = [item['key_'] for item in host_data['items']]

    if params['envmon']:
        # Check if Host has envmon template in "parentTemplates"
        if check_template(params, host_data['hostid'], host_data['parentTemplates']) is None:
            print_error(f"Cannot add EnvMon Template to Host '{host_name}'!")
            return
    else:
        # Adding missing Server Items
        if check_server_items(params, host_data['hostid'], host_items) is None:
            print_error(f"Cannot add some of the Items for Host '{host_name}'!")
            return

    # Request body
    if params['monitoring_client_subsystem']:
        body_template = ENVMON_REQUEST_TEMPLATE if params['envmon'] else HEALTH_REQUEST_TEMPLATE
    else:
        body_template = ENVMON_REQUEST_MEMBER_TEMPLATE if params['envmon'] else \
            HEALTH_REQUEST_MEMBER_TEMPLATE
    body = body_template.format(
        monitor_instance=params['monitoring_client_inst'],
        monitor_class=params['monitoring_client_class'],
        monitor_member=params['monitoring_client_member'],
        monitor_subsystem=params['monitoring_client_subsystem'], instance=match.group(1),
        member_class=match.group(2), member_code=match.group(3), server_code=match.group(4),
        uuid=uuid.uuid4()
    )

    cert = None
    if params['tls_cert'] and params['tls_key']:
        cert = (params['tls_cert'], params['tls_key'])

    verify = False
    if params['tls_ca']:
        verify = params['tls_ca']

    headers = {'Content-type': 'text/xml;charset=UTF-8'}

    try:
        response = requests.post(
            params['server_url'], data=body, headers=headers, timeout=params['timeout'],
            verify=verify, cert=cert)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print_error(f"Cannot get response for '{host_visible_name}' ({type(err).__name__}: {err})!")
        return

    try:
        # Skipping multipart headers
        envel = re.search('<SOAP-ENV:Envelope.+</SOAP-ENV:Envelope>', response.text, re.DOTALL)
        root = ElementTree.fromstring(
            # ElementTree.fromstring wants encoded bytes as input (PY2)
            envel.group(0).encode('utf-8'))
        metrics = root.find(
            './/m:getSecurityServerMetricsResponse/m:metricSet' if params['envmon']
            else './/om:getSecurityServerHealthDataResponse', NS)
        if metrics is None:
            raise Exception('No data')
    except Exception as err:
        print_error(
            f"Cannot parse response of '{host_visible_name}' ({type(err).__name__}: {err})!")
        if params['debug'] > 1:
            print_debug(f'host_mon -> Response: {response.content}')
        return

    # Packet of Zabbix metrics
    packet = []

    if params['envmon']:
        # Host metrics
        for item in ENVMON_METRICS:
            metric = None
            metric_element = metrics.find(
                f".//m:{item['type']}[m:name='{item['name']}']", NS)
            if metric_element is not None:
                metric = get_metric(params, metric_element, host_name)
            if metric is not None:
                packet += metric
            else:
                print_error(f"Metric '{item['name']}' for Host '{host_name}' is not available!")

        # It might not be a good idea to store full Package list in
        # Zabbix.
        # As a compromise we filter only X-Road packages.
        metric = None
        metric_element = metrics.find(".//m:metricSet[m:name='Packages']", NS)
        if metric_element is not None:
            metric = get_x_road_packages(params, metric_element, host_name)
        if metric is not None:
            packet += metric
        else:
            print_error(f"MetricSet 'Packages' for Host '{host_name}' is not available!")

        # Certificates metrics.
        # Checking validity information of SIGN, AUTH and Security
        # Server's TLS certificates that are enabled.
        # INTERNAL_IS_CLIENT_TLS will not influence X-Road
        # because Security Server does not check for validity of
        # client TLS certificates.
        metric = None
        metric_element = metrics.find(".//m:metricSet[m:name='Certificates']", NS)
        if metric_element is not None:
            metric = get_certificates(params, metric_element, host_name)
        if metric is not None:
            packet += metric
        else:
            print_error(f"MetricSet 'Certificates' for Host '{host_name}' is not available!")
    else:
        # Host metrics
        for item in SERVER_HEALTH_ITEMS:
            metric_path = f"./om:{item['key']}"
            metric_key = item['key']
            try:
                packet.append(ZabbixMetric(host_name, metric_key, metrics.find(
                    metric_path, NS).text))
            except AttributeError:
                print_error(f"Metric '{metric_key}' for Host '{host_name}' is not available!")

        for service_events in metrics.findall('om:servicesEvents/om:serviceEvents', NS):
            service_name = get_service_name(service_events.find('./om:service', NS))
            service_key = re.sub('[^0-9a-zA-Z-]+', '.', service_name)

            # Check if Service Items are added
            if check_service_items(
                    params, host_data['hostid'], host_items, service_name, service_key) is None:
                print_error(
                    f"Cannot add some of the service '{service_name}' Items to Host '{host_name}'!")
                return

            # Service metrics
            for item in SERVICE_HEALTH_ITEMS:
                metric_path = f".//om:{item['key']}"
                metric_key = f"{service_key}[{item['key']}]"
                try:
                    packet.append(ZabbixMetric(host_name, metric_key, service_events.find(
                        metric_path, NS).text))
                except AttributeError:
                    pass

    # Zabbix may reject metrics if they are sent to quickly after host creation or items addition
    # Sleep after host changes helps to avoid this problem
    # Sleep should be longer than Zabbix Server CacheUpdateFrequency value (default 60s)
    if params['host_changed'] and params['sleep_after_host_change']:
        if params['debug']:
            print_debug(
                f"Waiting {params['sleep_after_host_change']} seconds for Zabbix cache to be updated "
                f"after changes to host '{host_name}'.")
        time.sleep(params['sleep_after_host_change'])

    # Pushing metrics to Zabbix
    sender = ZabbixSender(zabbix_server=urlsplit(params['zabbix_url']).hostname,
                          zabbix_port=params['zabbix_sender_port'])
    try:
        if params['debug']:
            if params['envmon']:
                print_debug(f"Saving Environment metrics for Host '{host_name}'.")
            else:
                print_debug(f"Saving Health metrics for Host '{host_name}'.")

        send_result = sender.send(packet)
        if params['debug']:
            print_debug(send_result)
    except Exception as err:
        print_error(f"Cannot save metrics for Host '{host_name}'!\n{err}")


def worker(params):
    """Main function for worker threads"""
    while True:
        # Checking periodically if it is the time to gracefully shut down
        # the worker.
        try:
            item = params['work_queue'].get(True, 0.1)
        except queue.Empty:
            if params['shutdown'].is_set():
                return
            continue
        try:
            # Calling main processing function
            host_mon(params, item)
        except Exception as err:
            print_error(f"Unexpected error: {type(err).__name__}: {err}")
        finally:
            params['work_queue'].task_done()


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='X-Road Health and Environment monitoring collector for Zabbix.')
    parser.add_argument('-c', '--config', help='Configuration file location')
    parser.add_argument(
        '--env', action='store_true', help='Collect Environment data instead of Health data')
    args = parser.parse_args()

    params = load_conf(args.config)

    if args.env:
        # Starting EnvMon instead on Health data collection
        params['envmon'] = True

    if params['envmon']:
        if params['debug']:
            print_debug('Collecting Environmental Monitoring.')
    else:
        if params['debug']:
            print_debug('Collecting Health Monitoring.')

    # Create ZabbixAPI class instance
    try:
        params['zapi'] = ZabbixAPI(
            url=params['zabbix_url'], user=params['zabbix_user'], password=params['zabbix_pass'])
    except Exception as err:
        print_error(f"Cannot connect to Zabbix.\nURL: {params['zabbix_url']}\nDetail: {err}")
        sys.exit(1)

    # Debug py-zabbix.
    # NB! Not debuging initial connection to avoid passwords being
    # logged to stdout.
    if params['debug'] > 2:
        stream = logging.StreamHandler(sys.stdout)
        stream.setLevel(logging.DEBUG)
        log = logging.getLogger('pyzabbix')
        log.addHandler(stream)
        log.setLevel(logging.DEBUG)

    params['api_version'] = params['zapi'].api_version()

    if params['debug']:
        print(f"Connected to Zabbix API version {params['api_version']}")

    # Check if EnvMon Template exists
    if params['envmon'] and not get_template_name(
            params, params['envmon_template_id']) == params['envmon_template_name']:
        print_error(
            f"EnvMon Template (id='{params['envmon_template_id']}', "
            f"name='{params['envmon_template_name']}') not found in Zabbix!")
        sys.exit(1)

    # Working queue (list of servers to load the data from)
    params['work_queue'] = queue.Queue()

    # Event used to signal threads to shut down.
    params['shutdown'] = threading.Event()

    # Create and start new threads
    threads = []
    for _ in range(params['thread_count']):
        thread = threading.Thread(target=worker, args=(params,))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    # Populate the queue
    if params['servers']:
        # Using list of servers from configuration file
        for line in params['servers'].splitlines():
            params['work_queue'].put(line)
    else:
        # Fill work_queue with stdin values
        for line in sys.stdin:
            params['work_queue'].put(line)

    # Block until all tasks are done
    params['work_queue'].join()  # type: ignore

    # Set shutdown event and wait until all daemon processes finish
    params['shutdown'].set()
    for thread in threads:
        thread.join()

    if params['debug']:
        print_debug('Main program: Exiting.')


if __name__ == '__main__':
    main()
