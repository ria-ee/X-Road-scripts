#!/usr/bin/python

import psycopg2
import re

with open('/etc/xroad/db.properties', 'r') as dbConf:
    for line in dbConf:
        # Example:
        # serverconf.hibernate.connection.url = jdbc:postgresql://127.0.0.1:5432/serverconf
        m = re.match(
            '^serverconf.hibernate.connection.url\s*=\s*jdbc:postgresql://(.+):(.+)/(.+)$', line)
        if m:
            host = m.group(1)
            port = m.group(2)
            dbname = m.group(3)

        # Example: serverconf.hibernate.connection.username = serverconf
        m = re.match('^serverconf.hibernate.connection.username\s*=\s*(.+)$', line)
        if m:
            user = m.group(1)

        # Example: serverconf.hibernate.connection.password = serverconf
        m = re.match('^serverconf.hibernate.connection.password\s*=\s*(.+)$', line)
        if m:
            password = m.group(1)

conn = psycopg2.connect(
    "host={} port={} dbname={} user={} password={}".format(host, port, dbname, user, password))
cur = conn.cursor()

# Old schema has accessright.servicecode column
cur.execute("""select column_name from information_schema.columns
    where table_name='accessright' and column_name='servicecode';""")

if cur.rowcount:
    # Work with version <=6.21
    cur.execute("""select ar.servicecode, ar.rightsgiven,
        ci.xroadinstance p_xroadinstance, ci.memberclass p_memberclass,
        ci.membercode p_membercode, ci.subsystemcode p_subsystemcode,
        si.xroadinstance c_xroadinstance, si.memberclass c_memberclass,
        si.membercode c_membercode, si.subsystemcode c_subsystemcode,
        si.type c_type, si.groupcode c_groupcode
        from accessright ar
        join client c on c.id=ar.client_id
        join identifier ci on ci.id=c.identifier
        join identifier si on si.id=ar.subjectid;""")
else:
    # Works with version >= 6.22
    cur.execute("""select ep.servicecode, ar.rightsgiven,
        ci.xroadinstance p_xroadinstance, ci.memberclass p_memberclass,
        ci.membercode p_membercode, ci.subsystemcode p_subsystemcode,
        si.xroadinstance c_xroadinstance, si.memberclass c_memberclass,
        si.membercode c_membercode, si.subsystemcode c_subsystemcode,
        si.type c_type, si.groupcode c_groupcode
        from accessright ar
        join client c on c.id=ar.client_id
        join identifier ci on ci.id=c.identifier
        join identifier si on si.id=ar.subjectid
        join endpoint ep on ep.id=ar.endpoint_id;""")

print('service, rightgiventime, producer, consumer, globalgroup, localgroup')
for rec in cur:
    if rec[10] == 'SUBSYSTEM':
        line = '{},{},{},{},,'.format(rec[0], rec[1], '/'.join(rec[2:6]), '/'.join(rec[6:10]))
        print(line)
    elif rec[10] == 'GLOBALGROUP':
        line = '{},{},{},,{},'.format(
            rec[0], rec[1], '/'.join(rec[2:6]), '{}/{}'.format(rec[6], rec[11]))
        print(line)
    elif rec[10] == 'LOCALGROUP':
        line = '{},{},{},,,{}'.format(rec[0], rec[1], '/'.join(rec[2:6]), rec[11])
        print(line)

cur.close()
conn.close()
