#!/bin/bash

# IP or URI of Security Server
SECURITYSERVER="xrd0.ss.dns"

# A cache file is used when central server does not reply
SERVERS_CACHE="/var/tmp/xtee_servers_xrd0.ss.dns.txt"

DATA=`../xrdinfo/xrd_servers.py -s ${SECURITYSERVER}`

if [[ -z ${DATA} ]]; then
    DATA=`cat ${SERVERS_CACHE}`
    if [[ -z ${DATA} ]]; then
        echo "ERROR: Server list not available"
        exit 1
    fi
else
    echo "${DATA}" > ${SERVERS_CACHE}
fi

# Collecting Health data
echo "${DATA}" | python metrics.py -c metrics.cfg
# Collecting Environment data
echo "${DATA}" | python metrics.py --env -c metrics.cfg
