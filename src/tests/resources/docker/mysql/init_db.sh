#!/bin/bash
echo 'Initialising database'
mysql -pabc123 < "/docker-entrypoint-initdb.d/init_script.s"
echo 'Initialising database ended'
