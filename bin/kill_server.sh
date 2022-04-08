#!/bin/bash

# SET VARS
dbt_rpc_id=`cat /tmp/dbt_rpc_server.pid`
dbt_ss_id=`cat /tmp/dbt_sync_server.pid`

# REPARSE DBT PROJECT EVERY X MINUTES
crontab -l | grep -v '__dbt reparsed__' | crontab
kill $dbt_rpc_id ; kill $dbt_ss_id
