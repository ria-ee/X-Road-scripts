#!/usr/bin/python3

"""Get time of last successful X-Road message."""

import argparse
import calendar
import re
import time
import psycopg2


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Get time of last successful X-Road message.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Status returns number of seconds since last successful X-Road message.'
    )
    parser.add_argument('-s', help='Output status only', action="store_true")
    args = parser.parse_args()

    with open('/etc/xroad/db.properties', 'r', encoding='utf-8') as db_conf:
        for line in db_conf:
            # Example:
            # op-monitor.hibernate.connection.url = jdbc:postgresql://127.0.0.1:5432/op-monitor
            match = re.match(
                r'^op-monitor.hibernate.connection.url\s*=\s*jdbc:postgresql://(.+):(.+)/(.+)$',
                line)
            if match:
                host = match.group(1)
                port = match.group(2)
                dbname = match.group(3)

            # Example: op-monitor.hibernate.connection.username = opmonitor
            match = re.match(r'^op-monitor.hibernate.connection.username\s*=\s*(.+)$', line)
            if match:
                user = match.group(1)

            # Example: op-monitor.hibernate.connection.username = opmonitor
            match = re.match(r'^op-monitor.hibernate.connection.password\s*=\s*(.+)$', line)
            if match:
                password = match.group(1)

    conn = psycopg2.connect(
        f'host={host} port={port} dbname={dbname} user={user} password={password}')
    cur = conn.cursor()

    cur.execute("""select to_timestamp(max(monitoring_data_ts)) at time zone 'UTC'
        from operational_data
        where succeeded;""")
    rec = cur.fetchone()

    if rec[0] is not None:
        max_time_str = str(rec[0])
        if args.s:
            max_time = time.strptime(max_time_str, '%Y-%m-%d %H:%M:%S')
            print(int(time.time()) - calendar.timegm(max_time))
        else:
            print(max_time_str)

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
