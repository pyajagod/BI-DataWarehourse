#!/bin/bash

APP=gmall
hive=/opt/module/hive/bin/hive

if [ -n "$1" ]; then
	do_date=$1
else
	do_date=`date -d '-1 day' +%F`
fi

sql="
use gmall;
insert overwrite table ${APP}.dwd_base_event_log partition(dt='$do_date')
select
    base_analizer(line,'mid') as mid_id,
    base_analizer(line,'uid') as user_id,
    base_analizer(line,'vc') as version_code,
    base_analizer(line,'vn') as version_name,
    base_analizer(line,'l') as lang,
    base_analizer(line,'sr') as source,
    base_analizer(line,'os') as os,
    base_analizer(line,'ar') as area,
    base_analizer(line,'md') as model,
    base_analizer(line,'ba') as brand,
    base_analizer(line,'sv') as sdk_version,
    base_analizer(line,'g') as gmail,
    base_analizer(line,'hw') as height_width,
    base_analizer(line,'t') as app_time,
    base_analizer(line,'nw') as network,
    base_analizer(line,'ln') as lng,
    base_analizer(line,'la') as lat,
    event_name,
    event_json,
    base_analizer(line,'st') as server_time
from ${APP}.ods_event_log lateral view flat_analizer(base_analizer(line,'et')) tmp_flat as event_name,event_json
where dt='$do_date' and base_analizer(line,'et')<>'';"

$hive -e "$sql";
