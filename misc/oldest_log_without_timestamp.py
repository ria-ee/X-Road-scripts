#!/usr/bin/python

import psycopg2
import re

with open('/etc/xroad/db.properties', 'r') as dbConf:
    for line in dbConf:
        # Example: messagelog.hibernate.connection.url = jdbc:postgresql://127.0.0.1:5432/messagelog
        m = re.match('^messagelog.hibernate.connection.url\s*=\s*jdbc:postgresql://(.+):(.+)/(.+)$', line)
        if m:
            host = m.group(1)
            port = m.group(2)
            dbname = m.group(3)

        # Example: messagelog.hibernate.connection.username = messagelog
        m = re.match('^messagelog.hibernate.connection.username\s*=\s*(.+)$', line)
        if m:
            user = m.group(1)

        # Example: messagelog.hibernate.connection.password = messagelog
        m = re.match('^messagelog.hibernate.connection.password\s*=\s*(.+)$', line)
        if m:
            password = m.group(1)

conn = psycopg2.connect("host={} port={} dbname={} user={} password={}".format(host, port, dbname, user, password))
cur = conn.cursor()

cur.execute("""select to_timestamp(min( time )::float/1000) at time zone 'UTC'
    from logrecord
    where discriminator::text = 'm'::text AND signaturehash IS NOT NULL;""")
rec = cur.fetchone()
if rec[0] is not None:
    print(rec[0])

cur.close()
conn.close()
