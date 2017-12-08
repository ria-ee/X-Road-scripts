#!/usr/bin/python

import argparse
import calendar
import psycopg2
import re
import time


parser = argparse.ArgumentParser(
    description='Get time of last successful X-Road message.',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog='Status returns number of seconds since last successful X-Road message.'
)
parser.add_argument('-s', help='Output status only', action="store_true")
args = parser.parse_args()

with open('/etc/xroad/db.properties', 'r') as dbConf:
    for line in dbConf:
        # Example: op-monitor.hibernate.connection.url = jdbc:postgresql://127.0.0.1:5432/op-monitor
        m = re.match('^op-monitor.hibernate.connection.url\s*=\s*jdbc:postgresql://(.+):(.+)/(.+)$', line)
        if m:
            host = m.group(1)
            port = m.group(2)
            dbname = m.group(3)

        # Example: op-monitor.hibernate.connection.username = opmonitor
        m = re.match('^op-monitor.hibernate.connection.username\s*=\s*(.+)$', line)
        if m:
            user = m.group(1)

        # Example: op-monitor.hibernate.connection.username = opmonitor
        m = re.match('^op-monitor.hibernate.connection.password\s*=\s*(.+)$', line)
        if m:
            password = m.group(1)

conn = psycopg2.connect("host={} port={} dbname={} user={} password={}".format(host, port, dbname, user, password))
cur = conn.cursor()

cur.execute("""select to_timestamp(max(monitoring_data_ts)) at time zone 'UTC'
    from operational_data
    where succeeded;""")
rec = cur.fetchone()

if rec[0] is not None:
    if args.s:
        t = time.strptime(str(rec[0]), '%Y-%m-%d %H:%M:%S')
        print(int(time.time()) - calendar.timegm(t))
    else:
        print(rec[0])

cur.close()
conn.close()
