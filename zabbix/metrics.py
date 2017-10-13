#!/usr/bin/python

"""X-Road Health and Environment monitoring collector for Zabbix."""

# TODO: Better exception handling instead of general "Exception".

# pip install py-zabbix
from pyzabbix import ZabbixMetric, ZabbixSender, ZabbixAPI
import argparse
import re
import requests
import sys
import threading
import time
import uuid
import six
import xml.etree.ElementTree as ET
import six.moves.urllib.parse as urlparse
from six.moves import queue as Queue
from six.moves import configparser as ConfigParser

# Collect Envinronmental Monitoring data instead of Health data
ENVMON = False

##############################
### Default Configurations ###
##############################

# Dict containing configuration
CONF = {}

# Debug levels: 0 = Errors only; 1 = Simple debug; 2 = Detailed debug.

CONF['DEBUG'] = 2

# Zabbix configuration
CONF['ZABBIX_URL'] = 'http://localhost/'
CONF['ZABBIX_SENDER_PORT'] = 10051
CONF['ZABBIX_USER'] = 'zabbix_user'
CONF['ZABBIX_PASS'] = 'zabbix_pass'
CONF['ZABBIX_GROUP_ID'] = '2' # "Linux servers"
CONF['ZABBIX_TRAPPER_TYPE'] = '2' # "Zabbix trapper"

# EnvMon template configuration
CONF['ENVMON_TEMPLATE_ID'] = '10242'
CONF['ENVMON_TEMPLATE_NAME'] = 'X-Road Environmental monitoring'

# Security server URL used by Central monitoring:
CONF['SERVER_URL'] = 'http://xtee9.ci.kit'

# Path to TLS certificate and key in case when Security Server requires TLS authentication
CONF['TLS_CERT'] = ''
CONF['TLS_KEY'] = ''

# Client subsystem that performs health data collection:
CONF['MONITORING_CLIENT_INST'] = 'XTEE-CI-XM'
CONF['MONITORING_CLIENT_CLASS'] = 'GOV'
CONF['MONITORING_CLIENT_MEMBER'] = '00000001'
CONF['MONITORING_CLIENT_SUBSYSTEM'] = 'Central monitoring client'

# How many threads to use for data quering.
CONF['THREAD_COUNT'] = 2

# Timeout for http requests
CONF['TIMEOUT'] = 15.0

# List of servers to collect data from
CONF['SERVERS'] = u''

########################
### Global Constants ###
########################

# Default configuration file
CONF_FILE = 'metrics.cfg'

# Configuration section used
CONF_SECTION = 'metrics'

# Namespace of monitoring service
NS = {'m': 'http://x-road.eu/xsd/monitoring', 'om': 'http://x-road.eu/xsd/op-monitoring.xsd', 'id': 'http://x-road.eu/xsd/identifiers'}

# Zabbix value types:
#     "0": Numeric (float)
#     "1": Character
#     "2": Log
#     "3": Numeric (unsigned)
#     "4": Text
# Definitions of Server Items
SERVER_HEALTH_ITEMS = [
    {'key': 'monitoringStartupTimestamp', 'type': '3', 'units': 'ms', 'history': '7', 'description': 'The Unix timestamp in milliseconds when the monitoring system was started.'},
    {'key': 'statisticsPeriodSeconds', 'type': '3', 'units': 's', 'history': '7', 'description': 'Duration of the statistics period in seconds.'},
]
# Definitions of Service Items
SERVICE_HEALTH_ITEMS = [
    {'key': 'successfulRequestCount', 'type': '3', 'units': None, 'history': '7', 'description': 'The number of successful requests occurred during the last period.'},
    {'key': 'unsuccessfulRequestCount', 'type': '3', 'units': None, 'history': '7', 'description': 'The number of unsuccessful requests occurred during the last period.'},

    {'key': 'requestMinDuration', 'type': '3', 'units': 'ms', 'history': '7', 'description': 'The minimum duration of the request in milliseconds.'},
    {'key': 'requestAverageDuration', 'type': '0', 'units': 'ms', 'history': '7', 'description': 'The average duration of the request in milliseconds.'},
    {'key': 'requestMaxDuration', 'type': '3', 'units': 'ms', 'history': '7', 'description': 'The maximum duration of the request in milliseconds.'},
    {'key': 'requestDurationStdDev', 'type': '0', 'units': 'ms', 'history': '7', 'description': 'The standard deviation of the duration of the requests.'},
    {'key': 'requestMinSoapSize', 'type': '3', 'units': 'B', 'history': '7', 'description': 'The minimum SOAP message size of the request in bytes.'},
    {'key': 'requestAverageSoapSize', 'type': '0', 'units': 'B', 'history': '7', 'description': 'The average SOAP message size of the request in bytes.'},
    {'key': 'requestMaxSoapSize', 'type': '3', 'units': 'B', 'history': '7', 'description': 'The maximum SOAP message size of the request in bytes.'},
    {'key': 'requestSoapSizeStdDev', 'type': '0', 'units': 'B', 'history': '7', 'description': 'The standard deviation of the SOAP message size of the request.'},
    {'key': 'responseMinSoapSize', 'type': '3', 'units': 'B', 'history': '7', 'description': 'The minimum SOAP message size of the response in bytes.'},
    {'key': 'responseAverageSoapSize', 'type': '0', 'units': 'B', 'history': '7', 'description': 'The average SOAP message size of the response in bytes.'},
    {'key': 'responseMaxSoapSize', 'type': '3', 'units': 'B', 'history': '7', 'description': 'The maximum SOAP message size of the response in bytes.'},
    {'key': 'responseSoapSizeStdDev', 'type': '0', 'units': 'B', 'history': '7', 'description': 'The standard deviation of the SOAP message size of the response.'},
]


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
    {'name': 'TotalSwapSpace', 'type': 'numericMetric'},
]

HEALTH_REQUEST_TEMPLATE = u"""<SOAP-ENV:Envelope
       xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"
       xmlns:id="http://x-road.eu/xsd/identifiers"
       xmlns:xrd="http://x-road.eu/xsd/xroad.xsd"
       xmlns:om="http://x-road.eu/xsd/op-monitoring.xsd">
    <SOAP-ENV:Header>
        <xrd:client id:objectType="SUBSYSTEM">
            <id:xRoadInstance>{monitorInst}</id:xRoadInstance>
            <id:memberClass>{monitorClass}</id:memberClass>
            <id:memberCode>{monitorMember}</id:memberCode>
            <id:subsystemCode>{monitorSubsystem}</id:subsystemCode>
        </xrd:client>
        <xrd:service id:objectType="SERVICE">
            <id:xRoadInstance>{inst}</id:xRoadInstance>
            <id:memberClass>{memberClass}</id:memberClass>
            <id:memberCode>{member}</id:memberCode>
            <id:serviceCode>getSecurityServerHealthData</id:serviceCode>
        </xrd:service>
        <xrd:securityServer id:objectType="SERVER">
            <id:xRoadInstance>{inst}</id:xRoadInstance>
            <id:memberClass>{memberClass}</id:memberClass>
            <id:memberCode>{member}</id:memberCode>
            <id:serverCode>{server}</id:serverCode>
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
            <id:xRoadInstance>{monitorInst}</id:xRoadInstance>
            <id:memberClass>{monitorClass}</id:memberClass>
            <id:memberCode>{monitorMember}</id:memberCode>
            <id:subsystemCode>{monitorSubsystem}</id:subsystemCode>
        </xrd:client>
        <xrd:service id:objectType="SERVICE">
            <id:xRoadInstance>{inst}</id:xRoadInstance>
            <id:memberClass>{memberClass}</id:memberClass>
            <id:memberCode>{member}</id:memberCode>
            <id:serviceCode>getSecurityServerMetrics</id:serviceCode>
        </xrd:service>
        <xrd:securityServer id:objectType="SERVER">
            <id:xRoadInstance>{inst}</id:xRoadInstance>
            <id:memberClass>{memberClass}</id:memberClass>
            <id:memberCode>{member}</id:memberCode>
            <id:serverCode>{server}</id:serverCode>
        </xrd:securityServer>
        <xrd:id>{uuid}</xrd:id>
        <xrd:protocolVersion>4.0</xrd:protocolVersion>
    </SOAP-ENV:Header>
    <SOAP-ENV:Body>
        <m:getSecurityServerMetrics/>
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
            <id:xRoadInstance>{monitorInst}</id:xRoadInstance>
            <id:memberClass>{monitorClass}</id:memberClass>
            <id:memberCode>{monitorMember}</id:memberCode>
        </xrd:client>
        <xrd:service id:objectType="SERVICE">
            <id:xRoadInstance>{inst}</id:xRoadInstance>
            <id:memberClass>{memberClass}</id:memberClass>
            <id:memberCode>{member}</id:memberCode>
            <id:serviceCode>getSecurityServerHealthData</id:serviceCode>
        </xrd:service>
        <xrd:securityServer id:objectType="SERVER">
            <id:xRoadInstance>{inst}</id:xRoadInstance>
            <id:memberClass>{memberClass}</id:memberClass>
            <id:memberCode>{member}</id:memberCode>
            <id:serverCode>{server}</id:serverCode>
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
            <id:xRoadInstance>{monitorInst}</id:xRoadInstance>
            <id:memberClass>{monitorClass}</id:memberClass>
            <id:memberCode>{monitorMember}</id:memberCode>
        </xrd:client>
        <xrd:service id:objectType="SERVICE">
            <id:xRoadInstance>{inst}</id:xRoadInstance>
            <id:memberClass>{memberClass}</id:memberClass>
            <id:memberCode>{member}</id:memberCode>
            <id:serviceCode>getSecurityServerMetrics</id:serviceCode>
        </xrd:service>
        <xrd:securityServer id:objectType="SERVER">
            <id:xRoadInstance>{inst}</id:xRoadInstance>
            <id:memberClass>{memberClass}</id:memberClass>
            <id:memberCode>{member}</id:memberCode>
            <id:serverCode>{server}</id:serverCode>
        </xrd:securityServer>
        <xrd:id>{uuid}</xrd:id>
        <xrd:protocolVersion>4.0</xrd:protocolVersion>
    </SOAP-ENV:Header>
    <SOAP-ENV:Body>
        <m:getSecurityServerMetrics/>
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

def loadConf(confArg):
    """ Load configuration from file."""
    config = ConfigParser.RawConfigParser()
    confName = CONF_FILE
    if confArg:
        # Using configuration file provided by user
        confName = confArg

    try:
        config.read(confName)
    except ConfigParser.Error as e:
        print_error(u"Cannot load configuration '{}'\nDetail: {}\n".format(confName, e))
        exit(1)

    if CONF_SECTION not in config.sections():
        if confArg is None:
            # Default configuration file may be missing if program configuration is up to date
            if CONF['DEBUG']: print_debug(u'Configuration not found, using default values.')
            return
        else:
            # User provided configuration is incorrect
            print_error(u"No [{}] section found in configuration file '{}'.\n".format(CONF_SECTION, confName))
            exit(1)

    # All items found in configuration file
    confItems = dict(config.items(CONF_SECTION)).keys()

    try:
        if 'DEBUG'.lower() in confItems: CONF['DEBUG'] = config.getint(CONF_SECTION, 'DEBUG')
        if 'ZABBIX_URL'.lower() in confItems: CONF['ZABBIX_URL'] = config.get(CONF_SECTION, 'ZABBIX_URL')
        if 'ZABBIX_SENDER_PORT'.lower() in confItems: CONF['ZABBIX_SENDER_PORT'] = config.getint(CONF_SECTION, 'ZABBIX_SENDER_PORT')
        if 'ZABBIX_USER'.lower() in confItems: CONF['ZABBIX_USER'] = config.get(CONF_SECTION, 'ZABBIX_USER')
        if 'ZABBIX_PASS'.lower() in confItems: CONF['ZABBIX_PASS'] = config.get(CONF_SECTION, 'ZABBIX_PASS')
        if 'ZABBIX_GROUP_ID'.lower() in confItems: CONF['ZABBIX_GROUP_ID'] = config.get(CONF_SECTION, 'ZABBIX_GROUP_ID')
        if 'ZABBIX_TRAPPER_TYPE'.lower() in confItems: CONF['ZABBIX_TRAPPER_TYPE'] = config.get(CONF_SECTION, 'ZABBIX_TRAPPER_TYPE')
        if 'ENVMON_TEMPLATE_ID'.lower() in confItems: CONF['ENVMON_TEMPLATE_ID'] = config.get(CONF_SECTION, 'ENVMON_TEMPLATE_ID')
        if 'ENVMON_TEMPLATE_NAME'.lower() in confItems: CONF['ENVMON_TEMPLATE_NAME'] = config.get(CONF_SECTION, 'ENVMON_TEMPLATE_NAME')
        if 'SERVER_URL'.lower() in confItems: CONF['SERVER_URL'] = config.get(CONF_SECTION, 'SERVER_URL')
        if 'TLS_CERT'.lower() in confItems: CONF['TLS_CERT'] = config.get(CONF_SECTION, 'TLS_CERT')
        if 'TLS_KEY'.lower() in confItems: CONF['TLS_KEY'] = config.get(CONF_SECTION, 'TLS_KEY')
        if 'MONITORING_CLIENT_INST'.lower() in confItems: CONF['MONITORING_CLIENT_INST'] = config.get(CONF_SECTION, 'MONITORING_CLIENT_INST')
        if 'MONITORING_CLIENT_CLASS'.lower() in confItems: CONF['MONITORING_CLIENT_CLASS'] = config.get(CONF_SECTION, 'MONITORING_CLIENT_CLASS')
        if 'MONITORING_CLIENT_MEMBER'.lower() in confItems: CONF['MONITORING_CLIENT_MEMBER'] = config.get(CONF_SECTION, 'MONITORING_CLIENT_MEMBER')
        if 'MONITORING_CLIENT_SUBSYSTEM'.lower() in confItems: CONF['MONITORING_CLIENT_SUBSYSTEM'] = config.get(CONF_SECTION, 'MONITORING_CLIENT_SUBSYSTEM')
        if 'THREAD_COUNT'.lower() in confItems: CONF['THREAD_COUNT'] = config.getint(CONF_SECTION, 'THREAD_COUNT')
        if 'TIMEOUT'.lower() in confItems: CONF['TIMEOUT'] = config.getfloat(CONF_SECTION, 'TIMEOUT')
        if 'SERVERS'.lower() in confItems: CONF['SERVERS'] = config.get(CONF_SECTION, 'SERVERS')
    except ValueError as e:
        print_error(u"Incorrect value found in configuration file '{}'.\nDetail: {}\n".format(confName, e))
        exit(1)

    if CONF['DEBUG']: print_debug(u"Configuration loaded from '{}'.".format(confName))

def getTemplateName(templateId):
    """Query Template name from Zabbix."""
    try:
        template = zapi.template.get(
            templateids = templateId,
            output = ['templateid', 'host'],
        )
        if not template:
            return None
        return template[0]['host']
    except Exception as e:
        if CONF['DEBUG'] > 1: print_debug(u"getTemplateName: {}".format(e))
        return None

def checkTemplate(hostId, parentTemplates):
    """Check if EnvMon Template is added to Host and add the Host if neccessary.""" 
    parentTemplateIds = [item['templateid'] for item in parentTemplates]
    if CONF['ENVMON_TEMPLATE_ID'] not in parentTemplateIds:
        # Add template to host
        if CONF['DEBUG']: print_debug(u"Adding EnvMon Template to HostId '{}' to Zabbix.".format(hostId))
        try:
            result = zapi.host.update(
                hostid = hostId,
                templates = [
                    {
                        'templateid': CONF['ENVMON_TEMPLATE_ID']
                    }
                ],
            )
            if not result['hostids'][0] == hostId:
                return None
        except Exception as e:
            if CONF['DEBUG'] > 1: print_debug(u"checkTemplate: {}".format(e))
            return None
    return True

def getHost(hostName):
    """Query Host data from Zabbix."""
    try:
        host = zapi.host.get(
            filter = {'host': [hostName]},
            output = ['hostid', 'host', 'name', 'status'],
            selectItems = ['key_'],
            selectParentTemplates = ['templateid'],
            selectApplications = ['name', 'applicationid'],
        )
        if not host:
            return None
        return host[0]
    except Exception as e:
        if CONF['DEBUG'] > 1: print_debug(u"getHost: {}".format(e))
        return None

def addHost(hostName, hostVisibleName):
    """Add Host to Zabbix."""
    try:
        result = zapi.host.create(
            host = hostName,
            name = hostVisibleName,
            description = hostVisibleName,
            interfaces = [
                {
                    'type': 1,
                    'main': 1,
                    'useip': 1,
                    'ip': '127.0.0.1',
                    'dns': '',
                    'port': '10050'
                }
            ],
            groups = [
                {
                    'groupid': CONF['ZABBIX_GROUP_ID']
                }
            ],
            templates = [
                {
                    'templateid': CONF['ENVMON_TEMPLATE_ID']
                }
            ] if ENVMON else []
        )
        return result['hostids'][0]
    except Exception as e:
        if CONF['DEBUG'] > 1: print_debug(u"addHost: {}".format(e))
        return None

def checkHost(hostName, hostVisibleName):
    """Check if Host is added to Zabbix and add the Host if neccessary."""
    hostData = getHost(hostName)
    if hostData is None:
        if CONF['DEBUG']: print_debug(u"Adding Host '{}' to Zabbix.".format(hostName))
        if addHost(hostName, hostVisibleName):
            hostData = getHost(hostName)
    return hostData

def addApp(hostId, app):
    """Add Application to Zabbix."""
    try:
        result = zapi.application.create(
            hostid = hostId,
            name = app,
        )
        return result['applicationids']
    except Exception as e:
        if CONF['DEBUG'] > 1: print_debug(u"addApp: {}".format(e))
        return None

def checkApp(hostId, hostApps, app):
    """Check if Application is already added to the host, and add that application if neccessary.
       Return updated dict hostApps.
    """
    if app not in hostApps.keys():
        if CONF['DEBUG']: print_debug(u"Adding Application: '{}' for hostId '{}'.".format(app, hostId))
        result = addApp(hostId, app)
        try:
            hostApps[app] = result[0]
        except Exception as e:
            if CONF['DEBUG'] > 1: print_debug(u"addHostApp: {}".format(e))
            return None
    return hostApps

def addItem(hostId, item, app):
    """Add Item to Zabbix."""
    try:
        apps = []
        if app:
            apps = [app]
        result = zapi.item.create(
            hostid = hostId,
            name = item['name'] if 'name' in item else item['key'],
            key_ = item['key'],
            type = CONF['ZABBIX_TRAPPER_TYPE'],
            value_type = item['type'],
            units = item['units'],
            history = item['history'],
            description = item['description'],
            applications = apps,
        )
        return result['itemids']
    except Exception as e:
        if CONF['DEBUG'] > 1: print_debug(u"addItem: {}".format(e))
        return None

def checkServerItems(hostId, hostItems):
    """Check if Server Items are already added to the Host, and adds missing Items."""
    for item in SERVER_HEALTH_ITEMS:
        if item['key'] not in hostItems:
            if CONF['DEBUG']: print_debug(u"Adding item: '{}' for hostId '{}'.".format(item['key'], hostId))
            if addItem(hostId, item, None) is None:
                return None
    return True

def checkServiceItems(hostId, hostItems, serviceName, serviceKey, appId):
    """Check if Service Items are already added to the Host, and adds missing Items."""
    for constItem in SERVICE_HEALTH_ITEMS:
        item=constItem.copy()
        item['name'] = u"{}[{}]".format(serviceName, item['key'])
        item['key'] = u"{}[{}]".format(serviceKey, item['key'])
        if item['key'] not in hostItems:
            if CONF['DEBUG']: print_debug(u"Adding item: '{}' for hostId '{}'.".format(item['key'], hostId))
            if addItem(hostId, item, appId) is None:
                return None
    return True

def getServiceName(service):
    """Get service name from XML element."""
    elem = service.find('./id:xRoadInstance', NS)
    xRoadInstance = elem.text if elem is not None else ''
    elem = service.find('./id:memberClass', NS)
    memberClass = elem.text if elem is not None else ''
    elem = service.find('./id:memberCode', NS)
    memberCode = elem.text if elem is not None else ''
    elem = service.find('./id:subsystemCode', NS)
    subsystemCode = elem.text if elem is not None else ''
    elem = service.find('./id:serviceCode', NS)
    serviceCode = elem.text if elem is not None else ''
    elem = service.find('./id:serviceVersion', NS)
    serviceVersion = elem.text if elem is not None else ''
    return u"{}/{}/{}/{}/{}/{}".format(xRoadInstance, memberClass, memberCode, subsystemCode, serviceCode, serviceVersion)

def getMetric(node, server):
    """Convert XML metric to ZabbixMetric.
       Return Zabbix packet elements.
    """
    p = []
    nsp = '{' + NS['m'] + '}'

    if node.tag == nsp+'stringMetric' or node.tag == nsp+'numericMetric':
        try:
            name = node.find('./m:name', NS).text
            # Some names may have '/' character which is forbiden by zabbix
            name=name.replace('/', '')
            p.append(ZabbixMetric(server, name, node.find('./m:value', NS).text))
            return p
        except AttributeError:
            if CONF['DEBUG'] > 1: print_debug(u"getMetric: Incorect node: {}".format(ET.tostring(node)))
            return None
    elif node.tag == nsp+'histogramMetric':
        try:
            name = node.find('./m:name', NS).text
            p.append(ZabbixMetric(server, name+'_updated', node.find('./m:updated', NS).text))
            p.append(ZabbixMetric(server, name+'_min', node.find('./m:min', NS).text))
            p.append(ZabbixMetric(server, name+'_max', node.find('./m:max', NS).text))
            p.append(ZabbixMetric(server, name+'_mean', node.find('./m:mean', NS).text))
            p.append(ZabbixMetric(server, name+'_median', node.find('./m:median', NS).text))
            p.append(ZabbixMetric(server, name+'_stddev', node.find('./m:stddev', NS).text))
            return p
        except AttributeError:
            if CONF['DEBUG'] > 1: print_debug(u"getMetric: Incorect node: {}".format(ET.tostring(node)))
            return None
    else:
        return None

def getXRoadPackages(node, server):
    """Convert XML Packages metric to ZabbixMetric (includes only X-Road packages)
       Return Zabbix packet elements.
    """
    p=[]

    try:
        name = node.find('./m:name', NS).text
        data = ''
        for pack in node.findall('./m:stringMetric', NS):
            packname = pack.find('./m:name', NS).text
            if 'xroad' in packname or 'xtee' in packname:
                data += u"{}: {}\n".format(packname, pack.find('./m:value', NS).text)
        p.append(ZabbixMetric(server, name, data))
        return p
    except AttributeError:
        if CONF['DEBUG'] > 1: print_debug(u"getXRoadPackages: Incorect node: {}".format(ET.tostring(node)))
        return None

def hostMon(serverData):
    """Query Host monitoring data (Health or EnvMon) and save to Zabbix."""
    # Examples of serverData:
    # XTEE-CI-XM/GOV/00000000/00000000_1/xtee8.ci.kit
    # XTEE-CI-XM/GOV/00000001/00000001_1/xtee9.ci.kit
    # XTEE-CI-XM/COM/00000002/00000002_1/xtee10.ci.kit
    # Server name part is "greedy" match to allow server names to have "/" character
    m = re.match('^(.+?)/(.+?)/(.+?)/(.+)/(.+?)$', serverData)

    if m is None or m.lastindex != 5:
        print_error(u"Incorrect server string '{}'!\n".format(serverData))
        return

    hostVisibleName = m.group(0)
    hostName = re.sub('[^0-9a-zA-Z\.-]+', '.', hostVisibleName)

    if CONF['DEBUG']: print_debug(u"Processing Host '{}'.".format(hostName))

    # Check if Host is added to Zabbix and adds the Host if neccessary
    hostData = checkHost(hostName, hostVisibleName)
    if hostData is None:
        print_error(u"Cannot add Host '{}' to Zabbix!\n".format(hostName))
        return

    # Check if Host is disabled (status == 1)
    if hostData['status'] == 1:
        print_error(u"Host '{}' is disabled.\n".format(hostName))
        return

    hostApps = {}
    # Getting dict of Applications already added to Host
    for item in hostData['applications']:
        hostApps[item['name']] = item['applicationid']

    # Getting list of Items already added to Host
    hostItems=[item['key_'] for item in hostData['items']]

    if ENVMON:
        # Check if Host has envmon template in "parentTemplates"
        if checkTemplate(hostData['hostid'], hostData['parentTemplates']) is None:
            print_error(u"Cannot add EnvMon Template to Host '{}'!\n".format(hostName))
            return
    else:
        # Adding missing Server Items
        if checkServerItems(hostData['hostid'], hostItems) is None:
            print_error(u"Cannot add some of the Items for Host '{}'!\n".format(hostName))
            return

    # Request body
    if CONF['MONITORING_CLIENT_SUBSYSTEM']:
        body_template = ENVMON_REQUEST_TEMPLATE if ENVMON else HEALTH_REQUEST_TEMPLATE
    else:
        body_template = ENVMON_REQUEST_MEMBER_TEMPLATE if ENVMON else HEALTH_REQUEST_MEMBER_TEMPLATE
    body = body_template.format(
        monitorInst = CONF['MONITORING_CLIENT_INST'], monitorClass = CONF['MONITORING_CLIENT_CLASS'],
        monitorMember = CONF['MONITORING_CLIENT_MEMBER'], monitorSubsystem = CONF['MONITORING_CLIENT_SUBSYSTEM'],
        inst = m.group(1), memberClass = m.group(2), member = m.group(3), server = m.group(4), uuid = uuid.uuid4(),
    )

    cert = None
    if CONF['TLS_CERT'] and CONF['TLS_KEY']:
        cert = (CONF['TLS_CERT'], CONF['TLS_KEY'])

    headers = {'Content-type': 'text/xml;charset=UTF-8'}

    try:
        response = requests.post(CONF['SERVER_URL'], data=body, headers=headers, timeout=CONF['TIMEOUT'], verify=False, cert=cert)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print_error(u"Cannot get response for '{}' ({}: {})!\n".format(hostVisibleName, type(e).__name__, e))
        return

    try:
        # Skipping multipart headers
        envel = re.search('<SOAP-ENV:Envelope.+<\/SOAP-ENV:Envelope>', response.text, re.DOTALL)
        root = ET.fromstring(envel.group(0).encode('utf-8'))    # ET.fromstring wants encoded bytes as input (PY2)
        metrics = root.find('.//m:getSecurityServerMetricsResponse/m:metricSet' if ENVMON else './/om:getSecurityServerHealthDataResponse', NS)
        if metrics is None:
            raise
    except Exception:
        print_error(u"Cannot parse response of '{}'!\n".format(hostVisibleName))
        if CONF['DEBUG'] > 1: print_debug(u"hostMon -> Response: {}".format(response.content))
        return

    # Packet of Zabbix metrics
    packet = []

    if ENVMON:
        # Host metrics
        for item in ENVMON_METRICS:
            metric = getMetric(metrics.find(".//m:{}[m:name='{}']".format(item['type'], item['name']), NS), hostName)
            if metric is not None:
                packet += metric
            else:
                print_error(u"Metric '{}' for Host '{}' is not available!\n".format(item['name'], hostName))
    
        # It might not be a good idea to store full Package list in zabbix.
        # As a compromise we filter only xroad packages.
        metric = getXRoadPackages(metrics.find(".//m:metricSet[m:name='Packages']", NS), hostName)
        if metric is not None:
            packet += metric
        else:
            print_error(u"MetricSet 'Packages' for Host '{}' is not available!\n".format(hostName))
    else:
        # Host metrics
        for item in SERVER_HEALTH_ITEMS:
            metricPath = './om:{}'.format(item['key'])
            metricKey = item['key']
            try:
                packet.append(ZabbixMetric(hostName, metricKey, metrics.find(metricPath, NS).text))
            except AttributeError:
                print_error(u"Metric '{}' for Host '{}' is not available!\n".format(metricKey, hostName))
    
        for serviceEvents in metrics.findall('om:servicesEvents/om:serviceEvents', NS):
            serviceName = getServiceName(serviceEvents.find('./om:service', NS))
            serviceKey = re.sub('[^0-9a-zA-Z\.-]+', '.', serviceName)
    
            # Check if Application is added
            hostApps = checkApp(hostData['hostid'], hostApps, serviceName)
            if hostApps is None:
                print_error(u"Cannot add Application '{}' to Host '{}'!\n".format(serviceName, hostName))
                return
    
            # Check if Service Items are added
            if checkServiceItems(hostData['hostid'], hostItems, serviceName, serviceKey, hostApps[serviceName]) is None:
                print_error(u"Cannot add some of the service '{}' Items to Host '{}'!\n".format(serviceName, hostName))
                return
    
            # Service metrics
            for item in SERVICE_HEALTH_ITEMS:
                metricPath = ".//om:{}".format(item['key'])
                metricKey = "{}[{}]".format(serviceKey, item['key'])
                try:
                    packet.append(ZabbixMetric(hostName, metricKey, serviceEvents.find(metricPath, NS).text))
                except AttributeError:
                    None

    # Pushing metrics to zabbix
    sender = ZabbixSender(zabbix_server=urlparse.urlsplit(CONF['ZABBIX_URL']).hostname, zabbix_port=CONF['ZABBIX_SENDER_PORT'])
    try:
        if CONF['DEBUG']: print_debug(u"Saving Health metrics for Host '{}'.".format(hostName))
        sender.send(packet)
    except Exception as e:
        print_error(u"Cannot save Health metrics for Host '{}'!\n{}\n".format(hostName, e))

def worker():
    while True:
        item = workQueue.get()
        try:
            # Calling main processing function
            hostMon(item)
        except Exception as e:
            print_error(u"Unexpected error: {}: {}\n".format(type(e).__name__, e))
        finally:
            workQueue.task_done()


# Main programm
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='X-Road Health and Environment monitoring collector for Zabbix.')
    parser.add_argument('-c', '--config', help='Configuration file location')
    parser.add_argument('--env', action='store_true', help='Collect Environment data instead of Health data')
    args = parser.parse_args()

    loadConf(args.config)

    if args.env:
        # Starting EnvMon instead on Health data collection
        ENVMON = True

    if ENVMON:
        if CONF['DEBUG']: print_debug(u'Collecting Envinronmental Monitoring.')
    else:
        if CONF['DEBUG']: print_debug(u'Collecting Health Monitoring.')

    # Create ZabbixAPI class instance
    try: 
        zapi = ZabbixAPI(url = CONF['ZABBIX_URL'], user = CONF['ZABBIX_USER'], password = CONF['ZABBIX_PASS'])
    except Exception as e:
        print_error(u"Cannot connect to Zabbix.\nURL: {}\nDetail: {}\n".format(CONF['ZABBIX_URL'], e))
        exit(1)

    # Check if EnvMon Template exists
    if ENVMON and not getTemplateName(CONF['ENVMON_TEMPLATE_ID']) == CONF['ENVMON_TEMPLATE_NAME']:
        print_error(u"EnvMon Template (id='{}', name='{}') not found in Zabbix!\n".format(CONF['ENVMON_TEMPLATE_ID'], CONF['ENVMON_TEMPLATE_NAME']))
        exit(1)

    # Working queue (list of servers to load the data from)
    workQueue = Queue.Queue()
    for i in range(CONF['THREAD_COUNT']):
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()

    # Populate the queue
    if CONF['SERVERS']:
        # Using list of servers from configuration file
        for line in CONF['SERVERS'].splitlines():
            workQueue.put(line)
    else:
        # Fill workqueue with stdin values
        for line in sys.stdin:
            workQueue.put(line)

    # block until all tasks are done
    workQueue.join()

    if CONF['DEBUG']: print_debug(u'Main programm: Exiting.')
