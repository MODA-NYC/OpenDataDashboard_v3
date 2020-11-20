#!/bin/bash

export {http,https,ftp}_proxy="http://bcpxy.nycnet:8080"

/home/yulia/ODD/dashboard_v3.py --noauth_local_webserver
echo "Dashboard crontab job finished"
