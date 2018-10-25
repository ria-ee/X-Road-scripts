#!/usr/bin/python

"""X-Road Health and Environment monitoring collector for Zabbix."""

# pip install py-zabbix
from pyzabbix import ZabbixMetric, ZabbixSender, ZabbixAPI
import argparse
import re
import requests
import sys
import threading
import uuid
import six
import time
import calendar
import logging
import xml.etree.ElementTree as ElementTree
import six.moves.urllib.parse as urlparse
from distutils.version import LooseVersion
from six.moves import queue
from six.moves import configparser

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
    # Security server URL used by Central monitoring:
    'server_url': 'http://xrd0.ss.dns',
    # Path to TLS certificate and key in case when Security Server
    # requires TLS authentication
    'tls_cert': '',
    'tls_key': '',
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
    'servers': u''}

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

HEALTH_REQUEST_TEMPLATE = u"""<SOAP-ENV:Envelope
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

ENVMON_REQUEST_TEMPLATE = u"""<SOAP-ENV:Envelope
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

HEALTH_REQUEST_MEMBER_TEMPLATE = u"""<SOAP-ENV:Envelope
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

ENVMON_REQUEST_MEMBER_TEMPLATE = u"""<SOAP-ENV:Envelope
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
    content = u"{}: {}\n".format(threading.currentThread().getName(), content)
    if six.PY2:
        # Using thread safe "write" instead of "print"
        sys.stdout.write(content.encode('utf-8'))
    else:
        sys.stdout.write(content)


def print_error(content):
    """Thread safe and unicode safe error printer."""
    content = u"{}: ERROR: '{}'\n".format(threading.currentThread().getName(), content)
    if six.PY2:
        sys.stderr.write(content.encode('utf-8'))
    else:
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
    except configparser.Error as e:
        print_error(u"Cannot load configuration '{}'\nDetail: {}".format(conf_name, e))
        exit(1)

    if CONF_SECTION not in config.sections():
        if conf_arg is None:
            # Configuration file may be missing if default
            # configuration is up to date
            if params['debug']:
                print_debug(u'Configuration not found, using default values.')
            return params
        else:
            # User provided configuration is incorrect
            print_error(u"No [{}] section found in configuration file '{}'.".format(
                CONF_SECTION, conf_name))
            exit(1)

    # All items found in configuration file
    conf_items = dict(config.items(CONF_SECTION)).keys()

    try:
        if 'debug'.lower() in conf_items:
            params['debug'] = config.getint(CONF_SECTION, 'debug')
        if 'zabbix_url'.lower() in conf_items:
            params['zabbix_url'] = config.get(CONF_SECTION, 'zabbix_url')
        if 'zabbix_sender_port'.lower() in conf_items:
            params['zabbix_sender_port'] = config.getint(CONF_SECTION, 'zabbix_sender_port')
        if 'zabbix_user'.lower() in conf_items:
            params['zabbix_user'] = config.get(CONF_SECTION, 'zabbix_user')
        if 'zabbix_pass'.lower() in conf_items:
            params['zabbix_pass'] = config.get(CONF_SECTION, 'zabbix_pass')
        if 'zabbix_group_id'.lower() in conf_items:
            params['zabbix_group_id'] = config.get(CONF_SECTION, 'zabbix_group_id')
        if 'zabbix_trapper_type'.lower() in conf_items:
            params['zabbix_trapper_type'] = config.get(CONF_SECTION, 'zabbix_trapper_type')
        if 'envmon_template_id'.lower() in conf_items:
            params['envmon_template_id'] = config.get(CONF_SECTION, 'envmon_template_id')
        if 'envmon_template_name'.lower() in conf_items:
            params['envmon_template_name'] = config.get(CONF_SECTION, 'envmon_template_name')
        if 'server_url'.lower() in conf_items:
            params['server_url'] = config.get(CONF_SECTION, 'server_url')
        if 'tls_cert'.lower() in conf_items:
            params['tls_cert'] = config.get(CONF_SECTION, 'tls_cert')
        if 'tls_key'.lower() in conf_items:
            params['tls_key'] = config.get(CONF_SECTION, 'tls_key')
        if 'monitoring_client_inst'.lower() in conf_items:
            params['monitoring_client_inst'] = config.get(
                CONF_SECTION, 'monitoring_client_inst')
        if 'monitoring_client_class'.lower() in conf_items:
            params['monitoring_client_class'] = config.get(CONF_SECTION, 'monitoring_client_class')
        if 'monitoring_client_member'.lower() in conf_items:
            params['monitoring_client_member'] = config.get(
                CONF_SECTION, 'monitoring_client_member')
        if 'monitoring_client_subsystem'.lower() in conf_items:
            params['monitoring_client_subsystem'] = config.get(
                CONF_SECTION, 'monitoring_client_subsystem')
        if 'thread_count'.lower() in conf_items:
            params['thread_count'] = config.getint(CONF_SECTION, 'thread_count')
        if 'timeout'.lower() in conf_items:
            params['timeout'] = config.getfloat(CONF_SECTION, 'timeout')
        if 'servers'.lower() in conf_items:
            params['servers'] = config.get(CONF_SECTION, 'servers')
    except ValueError as e:
        print_error(u"Incorrect value found in configuration file '{}'.\nDetail: {}".format(
            conf_name, e))
        exit(1)

    if params['debug']:
        print_debug(u"Configuration loaded from '{}'.".format(conf_name))

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
    except Exception as e:
        if params['debug'] > 1:
            print_debug(u"get_template_name: {}".format(e))
        return None


def check_template(params, host_id, parent_templates):
    """Check if EnvMon Template is added to Host and add the Host if
    necessary.
    """
    parent_template_ids = [item['templateid'] for item in parent_templates]
    if params['envmon_template_id'] not in parent_template_ids:
        # Add template to host
        if params['debug']:
            print_debug(u"Adding EnvMon Template to HostId '{}' to Zabbix.".format(host_id))
        try:
            result = params['zapi'].host.update(
                hostid=host_id,
                templates=[
                    {
                        'templateid': params['envmon_template_id']
                    }
                ],
            )
            if not result['hostids'][0] == host_id:
                return None
        except Exception as e:
            if params['debug'] > 1:
                print_debug(u"check_template: {}".format(e))
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
            selectApplications=['name', 'applicationid'],
        )
        if not host:
            return None
        return host[0]
    except Exception as e:
        if params['debug'] > 1:
            print_debug(u"get_host: {}".format(e))
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
        return result['hostids'][0]
    except Exception as e:
        if params['debug'] > 1:
            print_debug(u"add_host: {}".format(e))
        return None


def check_host(params, host_name, host_visible_name):
    """Check if Host is added to Zabbix and add the Host if
    necessary.
    """
    host_data = get_host(params, host_name)
    if host_data is None:
        if params['debug']:
            print_debug(u"Adding Host '{}' to Zabbix.".format(host_name))
        if add_host(params, host_name, host_visible_name):
            host_data = get_host(params, host_name)
    return host_data


def add_app(params, host_id, app):
    """Add Application to Zabbix."""
    try:
        result = params['zapi'].application.create(
            hostid=host_id,
            name=app,
        )
        return result['applicationids']
    except Exception as e:
        if params['debug'] > 1:
            print_debug(u"add_app: {}".format(e))
        return None


def check_app(params, host_id, host_apps, app):
    """Check if Application is already added to the host, and add that
    application if necessary.
    Return updated dict host_apps.
    """
    if app not in host_apps.keys():
        if params['debug']:
            print_debug(u"Adding Application: '{}' for host_id '{}'.".format(app, host_id))
        result = add_app(params, host_id, app)
        try:
            host_apps[app] = result[0]
        except Exception as e:
            if params['debug'] > 1:
                print_debug(u"addHostApp: {}".format(e))
            return None
    return host_apps


def add_item(params, host_id, item, app):
    """Add Item to Zabbix."""
    try:
        apps = []
        if app:
            apps = [app]
        if params['api_client_version'] == '1':
            result = params['zapi'].item.create(
                hostid=host_id,
                name=item['name'] if 'name' in item else item['key'],
                key_=item['key'],
                type=params['zabbix_trapper_type'],
                value_type=item['type'],
                units=item['units'],
                history=item['history'],
                description=item['description'],
                applications=apps,
            )
        else:
            # api_client_version=2 --> Zabbix version 3.4+
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
                applications=apps,
            )
        return result['itemids']
    except Exception as e:
        if params['debug'] > 1:
            print_debug(u"add_item: {}".format(e))
        return None


def check_server_items(params, host_id, host_items):
    """Check if Server Items are already added to the Host, and adds
    missing Items.
    """
    for item in SERVER_HEALTH_ITEMS:
        if item['key'] not in host_items:
            if params['debug']:
                print_debug(u"Adding item: '{}' for host_id '{}'.".format(item['key'], host_id))
            if add_item(params, host_id, item, None) is None:
                return None
    return True


def check_service_items(params, host_id, host_items, service_name, service_key, app_id):
    """Check if Service Items are already added to the Host, and adds
    missing Items.
    """
    for const_item in SERVICE_HEALTH_ITEMS:
        item = const_item.copy()
        item['name'] = u"{}[{}]".format(service_name, item['key'])
        item['key'] = u"{}[{}]".format(service_key, item['key'])
        if item['key'] not in host_items:
            if params['debug']:
                print_debug(u"Adding item: '{}' for host_id '{}'.".format(item['key'], host_id))
            if add_item(params, host_id, item, app_id) is None:
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
    return u"{}/{}/{}/{}/{}/{}".format(
        x_road_instance, member_class, member_code, subsystem_code, service_code, service_version)


def get_metric(params, node, server):
    """Convert XML metric to ZabbixMetric.
       Return Zabbix packet elements.
    """
    if params is None or node is None or server is None:
        return None

    p = []
    nsp = '{' + NS['m'] + '}'

    if node.tag == nsp + 'stringMetric' or node.tag == nsp + 'numericMetric':
        try:
            name = node.find('./m:name', NS).text
            # Some names may have '/' character which is forbidden by
            # Zabbix
            name = name.replace('/', '')
            p.append(ZabbixMetric(server, name, node.find('./m:value', NS).text))
            return p
        except AttributeError:
            if params['debug'] > 1:
                print_debug(u"get_metric: Incorrect node: {}".format(ElementTree.tostring(node)))
            return None
    elif node.tag == nsp + 'histogramMetric':
        try:
            name = node.find('./m:name', NS).text
            p.append(ZabbixMetric(server, name + '_updated', node.find('./m:updated', NS).text))
            p.append(ZabbixMetric(server, name + '_min', node.find('./m:min', NS).text))
            p.append(ZabbixMetric(server, name + '_max', node.find('./m:max', NS).text))
            p.append(ZabbixMetric(server, name + '_mean', node.find('./m:mean', NS).text))
            p.append(ZabbixMetric(server, name + '_median', node.find('./m:median', NS).text))
            p.append(ZabbixMetric(server, name + '_stddev', node.find('./m:stddev', NS).text))
            return p
        except AttributeError:
            if params['debug'] > 1:
                print_debug(u"get_metric: Incorrect node: {}".format(ElementTree.tostring(node)))
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

    p = []

    try:
        name = node.find('./m:name', NS).text
        data = ''
        for pack in node.findall('./m:stringMetric', NS):
            package_name = pack.find('./m:name', NS).text
            if 'xroad' in package_name or 'xtee' in package_name:
                data += u"{}: {}\n".format(package_name, pack.find('./m:value', NS).text)
        p.append(ZabbixMetric(server, name, data))
        return p
    except AttributeError:
        if params['debug'] > 1:
            print_debug(u"get_x_road_packages: Incorrect node: {}".format(
                ElementTree.tostring(node)))
        return None


def get_certificates(params, node, server):
    """Convert XML Certificates metric to ZabbixMetric
    Return Zabbix packet elements.
    """
    if params is None or node is None or server is None:
        return None

    p = []

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
            data += u"sha1Hash: {}\nnotBefore: {}\nnotAfter: {}\ncertificateType: {}\n" \
                    u"active: {}\n\n".format(
                        sha1_hash.find('./m:value', NS).text,
                        not_before_value, not_after_value, certificate_type_value, active_value)
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
        p.append(ZabbixMetric(server, name, data))

        # Adding Certificates_validity metric
        current_time = time.gmtime()
        if current_time < max_not_before or current_time > min_not_after:
            # Some certificate is not yet valid or already expired
            p.append(ZabbixMetric(server, name + '_validity', '0'))
        else:
            p.append(ZabbixMetric(server, name + '_validity', str(
                calendar.timegm(min_not_after) - calendar.timegm(current_time))))
        return p
    except AttributeError:
        if params['debug'] > 1:
            print_debug(u"get_certificates: Incorrect node: {}".format(ElementTree.tostring(node)))
        return None


def host_mon(params, server_data):
    """Query Host monitoring data (Health or EnvMon) and save to Zabbix.
    """
    # Examples of server_data:
    # INST/GOV/00000000/00000000_1/xrd0.ss.dns
    # INST/GOV/00000001/00000001_1/xrd1.ss.dns
    # INST/COM/00000002/00000002_1/xrd2.ss.dns
    # Server name part is "greedy" match to allow server names to have
    # "/" character
    m = re.match('^(.+?)/(.+?)/(.+?)/(.+)/(.+?)$', server_data)

    if m is None or m.lastindex != 5:
        print_error(u"Incorrect server string '{}'!".format(server_data))
        return

    host_visible_name = m.group(0)
    host_name = re.sub('[^0-9a-zA-Z-]+', '.', host_visible_name)

    if params['debug']:
        print_debug(u"Processing Host '{}'.".format(host_name))

    # Check if Host is added to Zabbix and adds the Host if necessary
    host_data = check_host(params, host_name, host_visible_name)
    if host_data is None:
        print_error(u"Cannot add Host '{}' to Zabbix!".format(host_name))
        return

    # Check if Host is disabled (status == 1)
    if host_data['status'] == 1:
        print_error(u"Host '{}' is disabled.".format(host_name))
        return

    host_apps = {}
    # Getting dict of Applications already added to Host
    for item in host_data['applications']:
        host_apps[item['name']] = item['applicationid']

    # Getting list of Items already added to Host
    host_items = [item['key_'] for item in host_data['items']]

    if params['envmon']:
        # Check if Host has envmon template in "parentTemplates"
        if check_template(params, host_data['hostid'], host_data['parentTemplates']) is None:
            print_error(u"Cannot add EnvMon Template to Host '{}'!".format(host_name))
            return
    else:
        # Adding missing Server Items
        if check_server_items(params, host_data['hostid'], host_items) is None:
            print_error(u"Cannot add some of the Items for Host '{}'!".format(host_name))
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
        monitor_subsystem=params['monitoring_client_subsystem'], instance=m.group(1),
        member_class=m.group(2), member_code=m.group(3), server_code=m.group(4), uuid=uuid.uuid4()
    )

    cert = None
    if params['tls_cert'] and params['tls_key']:
        cert = (params['tls_cert'], params['tls_key'])

    headers = {'Content-type': 'text/xml;charset=UTF-8'}

    try:
        response = requests.post(
            params['server_url'], data=body, headers=headers, timeout=params['timeout'],
            verify=False, cert=cert)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print_error(
            u"Cannot get response for '{}' ({}: {})!".format(
                host_visible_name, type(e).__name__, e))
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
    except Exception as e:
        print_error(u"Cannot parse response of '{}' ({}: {})!".format(
            host_visible_name, type(e).__name__, e))
        if params['debug'] > 1:
            print_debug(u"host_mon -> Response: {}".format(response.content))
        return

    # Packet of Zabbix metrics
    packet = []

    if params['envmon']:
        # Host metrics
        for item in ENVMON_METRICS:
            metric = None
            metric_element = metrics.find(
                ".//m:{}[m:name='{}']".format(item['type'], item['name']), NS)
            if metric_element is not None:
                metric = get_metric(params, metric_element, host_name)
            if metric is not None:
                packet += metric
            else:
                print_error(u"Metric '{}' for Host '{}' is not available!".format(
                    item['name'], host_name))

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
            print_error(u"MetricSet 'Packages' for Host '{}' is not available!".format(host_name))

        # Finding proxy version
        proxy_version = metrics.find(".//m:stringMetric[m:name='proxyVersion']", NS)
        proxy_version_value = proxy_version.find('./m:value', NS).text
        # Workaround: Some develop versions contain extra quotes.
        proxy_version_value = proxy_version_value.strip("'")

        # Certificates metrics are not supported before '6.16.0-1'
        if LooseVersion(proxy_version_value) > LooseVersion('6.16.0-1'):
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
                print_error(u"MetricSet 'Certificates' for Host '{}' is not available!".format(
                    host_name))
    else:
        # Host metrics
        for item in SERVER_HEALTH_ITEMS:
            metric_path = './om:{}'.format(item['key'])
            metric_key = item['key']
            try:
                packet.append(ZabbixMetric(host_name, metric_key, metrics.find(
                    metric_path, NS).text))
            except AttributeError:
                print_error(
                    u"Metric '{}' for Host '{}' is not available!".format(metric_key, host_name))

        for service_events in metrics.findall('om:servicesEvents/om:serviceEvents', NS):
            service_name = get_service_name(service_events.find('./om:service', NS))
            service_key = re.sub('[^0-9a-zA-Z-]+', '.', service_name)

            # Check if Application is added
            host_apps = check_app(params, host_data['hostid'], host_apps, service_name)
            if host_apps is None:
                print_error(
                    u"Cannot add Application '{}' to Host '{}'!".format(service_name, host_name))
                return

            # Check if Service Items are added
            if check_service_items(
                    params, host_data['hostid'], host_items, service_name, service_key,
                    host_apps[service_name]) is None:
                print_error(u"Cannot add some of the service '{}' Items to Host '{}'!".format(
                    service_name, host_name))
                return

            # Service metrics
            for item in SERVICE_HEALTH_ITEMS:
                metric_path = ".//om:{}".format(item['key'])
                metric_key = "{}[{}]".format(service_key, item['key'])
                try:
                    packet.append(ZabbixMetric(host_name, metric_key, service_events.find(
                        metric_path, NS).text))
                except AttributeError:
                    pass

    # Pushing metrics to Zabbix
    sender = ZabbixSender(zabbix_server=urlparse.urlsplit(params['zabbix_url']).hostname,
                          zabbix_port=params['zabbix_sender_port'])
    try:
        if params['debug']:
            if params['envmon']:
                print_debug(u"Saving Environment metrics for Host '{}'.".format(host_name))
            else:
                print_debug(u"Saving Health metrics for Host '{}'.".format(host_name))

        send_result = sender.send(packet)
        if params['debug']:
            print_debug(send_result)
    except Exception as e:
        print_error(u"Cannot save metrics for Host '{}'!\n{}".format(host_name, e))


def worker(params):
    while True:
        # Checking periodically if it is the time to gracefully shutdown
        # the worker.
        try:
            item = params['work_queue'].get(True, 0.1)
        except queue.Empty:
            if params['shutdown'].is_set():
                return
            else:
                continue
        try:
            # Calling main processing function
            host_mon(params, item)
        except Exception as e:
            print_error(u"Unexpected error: {}: {}".format(type(e).__name__, e))
        finally:
            params['work_queue'].task_done()


def main():
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
            print_debug(u'Collecting Environmental Monitoring.')
    else:
        if params['debug']:
            print_debug(u'Collecting Health Monitoring.')

    # Create ZabbixAPI class instance
    try:
        params['zapi'] = ZabbixAPI(
            url=params['zabbix_url'], user=params['zabbix_user'], password=params['zabbix_pass'])
    except Exception as e:
        print_error(u"Cannot connect to Zabbix.\nURL: {}\nDetail: {}".format(
            params['zabbix_url'], e))
        exit(1)

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
    # api_client_version=1 --> 3.0<=Zabbix version<3.4
    params['api_client_version'] = '1'
    if LooseVersion(params['api_version']) >= LooseVersion('3.4'):
        # api_client_version=2 --> 3.4<=Zabbix version
        params['api_client_version'] = '2'

    if params['debug']:
        print(u"Connected to Zabbix API version {} (using client version {})".format(
            params['api_version'], params['api_client_version']))

    # Check if EnvMon Template exists
    if params['envmon'] and not get_template_name(
            params, params['envmon_template_id']) == params['envmon_template_name']:
        print_error(u"EnvMon Template (id='{}', name='{}') not found in Zabbix!".format(
            params['envmon_template_id'], params['envmon_template_name']))
        exit(1)

    # Working queue (list of servers to load the data from)
    params['work_queue'] = queue.Queue()

    # Event used to signal threads to shut down.
    params['shutdown'] = threading.Event()

    # Create and start new threads
    threads = []
    for i in range(params['thread_count']):
        t = threading.Thread(target=worker, args=(params,))
        t.daemon = True
        t.start()
        threads.append(t)

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
    params['work_queue'].join()

    # Set shutdown event and wait until all daemon processes finish
    params['shutdown'].set()
    for t in threads:
        t.join()

    if params['debug']:
        print_debug(u'Main program: Exiting.')


if __name__ == '__main__':
    main()
