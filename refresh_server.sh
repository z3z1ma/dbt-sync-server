#!/bin/bash

# RUN SERVER
dbt_sync_server --inject-rpc &

# CAPTURE PIDs
echo $! >/tmp/dbt_sync_server.pid
sleep 2.5
ps aux | grep dbt-rpc | grep -v grep | awk '{print $2}' >/tmp/dbt_rpc_server.pid

# SET CRON VARS
dbt_rpc_id=`cat /tmp/dbt_rpc_server.pid`
every_x_minutes=5

# REPARSE DBT PROJECT EVERY X MINUTES
crontab -l | grep -v '__dbt reparsed__' | crontab
(crontab -l ; echo "1/$every_x_minutes * * * * kill -HUP $dbt_rpc_id ; echo '__dbt reparsed__'") | crontab

# USE THIS /tmp/dbt_rpc_server.pid TO REPARSE MANUALLY
echo $dbt_rpc_id
