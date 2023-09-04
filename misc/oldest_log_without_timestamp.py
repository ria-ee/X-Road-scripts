#!/usr/bin/python3

"""Get time of oldest X-Road message without timestamp."""

import argparse
import calendar
import re
import time
import psycopg2


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Get time of oldest X-Road message without timestamp.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Status returns number of seconds since X-Road messages are awaiting for timestamp.'
    )
    parser.add_argument('-s', help='Output status only', action="store_true")
    args = parser.parse_args()

    with open('/etc/xroad/db.properties', 'r', encoding='utf-8') as db_conf:
        for line in db_conf:
            # Example:
            # messagelog.hibernate.connection.url = jdbc:postgresql://127.0.0.1:5432/messagelog
            match = re.match(
                r'^messagelog.hibernate.connection.url\s*=\s*jdbc:postgresql://(.+):(.+)/(.+)$',
                line)
            if match:
                host = match.group(1)
                port = match.group(2)
                dbname = match.group(3)

            # Example: messagelog.hibernate.connection.username = messagelog
            match = re.match(r'^messagelog.hibernate.connection.username\s*=\s*(.+)$', line)
            if match:
                user = match.group(1)

            # Example: messagelog.hibernate.connection.password = messagelog
            match = re.match(r'^messagelog.hibernate.connection.password\s*=\s*(.+)$', line)
            if match:
                password = match.group(1)

    conn = psycopg2.connect(
        f'host={host} port={port} dbname={dbname} user={user} password={password}')
    cur = conn.cursor()

    cur.execute("""select to_timestamp(min( time )::float/1000) at time zone 'UTC'
        from logrecord
        where discriminator::text = 'm'::text AND signaturehash IS NOT NULL;""")
    rec = cur.fetchone()

    if rec[0] is not None:
        if args.s:
            log_time_str = str(rec[0]).split('.', maxsplit=1)[0]
            log_time = time.strptime(log_time_str, '%Y-%m-%d %H:%M:%S')
            print(int(time.time()) - calendar.timegm(log_time))
        else:
            print(rec[0])
    elif args.s:
        print(0)

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
