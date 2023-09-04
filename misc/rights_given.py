#!/usr/bin/python3

"""List service access rights."""

import re
import psycopg2


def main():
    """Main function"""
    with open('/etc/xroad/db.properties', 'r', encoding='utf-8') as db_conf:
        for line in db_conf:
            # Example:
            # serverconf.hibernate.connection.url = jdbc:postgresql://127.0.0.1:5432/serverconf
            match = re.match(
                r'^serverconf.hibernate.connection.url\s*=\s*jdbc:postgresql://(.+):(.+)/(.+)$',
                line)
            if match:
                host = match.group(1)
                port = match.group(2)
                dbname = match.group(3)

            # Example: serverconf.hibernate.connection.username = serverconf
            match = re.match(r'^serverconf.hibernate.connection.username\s*=\s*(.+)$', line)
            if match:
                user = match.group(1)

            # Example: serverconf.hibernate.connection.password = serverconf
            match = re.match(r'^serverconf.hibernate.connection.password\s*=\s*(.+)$', line)
            if match:
                password = match.group(1)

    conn = psycopg2.connect(
        f'host={host} port={port} dbname={dbname} user={user} password={password}')
    cur = conn.cursor()

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
            print(f"{rec[0]},{rec[1]},{'/'.join(rec[2:6])},{'/'.join(rec[6:10])},,")
        elif rec[10] == 'GLOBALGROUP':
            print(f"{rec[0]},{rec[1]},{'/'.join(rec[2:6])},,{rec[6]}/{rec[11]},")
        elif rec[10] == 'LOCALGROUP':
            print(f"{rec[0]},{rec[1]},{'/'.join(rec[2:6])},,,{rec[11]}")

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
