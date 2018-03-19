#!/usr/bin/python

from pyzabbix import ZabbixAPI
import argparse
import time
import sys
from six.moves import configparser


parser = argparse.ArgumentParser(
    description='Checks when hosts in Zabbix were updated.',
    epilog='Command line arguments override configuration file.')
parser.add_argument('-c', '--config', help='Configuration file location')
parser.add_argument('--url', help='Zabbix URL')
parser.add_argument('--user', help='Zabbix user')
parser.add_argument('--password', help='Zabbix password')
parser.add_argument('--instance', help='X-Road instance filter')
parser.add_argument('-s', help='Output only percentage of hosts that were not '
                               'updated in the last S seconds', type=int)
args = parser.parse_args()

url = ''
user = ''
password = ''
instance = ''
if args.config:
    config = configparser.RawConfigParser()
    config.read(args.config)
    conf_items = dict(config.items('zabbix')).keys()
    if 'url' in conf_items:
        url = config.get('zabbix', 'url')
    if 'user' in conf_items:
        user = config.get('zabbix', 'user')
    if 'password' in conf_items:
        password = config.get('zabbix', 'password')
    if 'instance' in conf_items:
        instance = config.get('zabbix', 'instance')
if args.url:
    url = args.url
if args.user:
    user = args.user
if args.password:
    password = args.password
if args.instance:
    instance = args.instance

if not url or not user or not password:
    sys.stderr.write('ERROR: Zabbix configuration missing.\n')
    exit(1)

zapi = ZabbixAPI(url=url, user=user, password=password)

hosts = None
if instance:
    hosts = zapi.host.get(
        output=['hostid', 'host'],
        selectItems=['key_'],
        filter={'status': '0'},
        startSearch=True,
        search={'host': instance + '.'}
    )
else:
    hosts = zapi.host.get(
        output=['hostid', 'host'],
        selectItems=['key_'],
        filter={'status': '0'}
    )

updated_hosts = 0
total_hosts = 0
for host in hosts:
    # Checking only hosts that have proxyVersion metric
    if {u'key_': u'proxyVersion'} in host['items']:
        total_hosts += 1
        items = zapi.item.get(
            output=['lastvalue', 'lastclock'],
            hostids=[host['hostid']],
            search={'key_': 'proxyVersion'}
        )

        if items and items[0]:
            item = items[0]
            if item['lastclock'] and item['lastclock'] <> '0':
                last_update = int(time.time() - float(item['lastclock']))
                if not args.s:
                    print('host: {}; last data was {} seconds ago'.format(
                        host['host'], last_update))
                elif args.s >= last_update:
                    updated_hosts += 1
            elif not args.s:
                print('host: {}; NO LAST DATA'.format(host['host']))

if args.s and total_hosts:
    print(str(int(100 * updated_hosts / total_hosts)))
elif args.s:
    print('0')
