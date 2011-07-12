#!/bin/bash

IFS=","

for i in `xe sr-list sm-config:rolling_upgrade_mode=true --minimal`
do
	xe sr-param-remove uuid=$i param-name=sm-config param-key=rolling_upgrade_mode
done

/opt/xensource/debug/vcli smpath=internal cmd=rolling-upgrade-finished
 
