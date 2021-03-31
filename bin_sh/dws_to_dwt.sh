#!/bin/bash

APP=gmall
hive=/opt/module/hive/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    do_date=$1
else 
    do_date=`date -d "-1 day" +%F`
fi

sql="
insert overwrite table ${APP}.dwt_uv_topic
select
    nvl(new.mid_id,old.mid_id),
    nvl(new.user_id,old.user_id),
    nvl(new.version_code,old.version_code),
    nvl(new.version_name,old.version_name),
    nvl(new.lang,old.lang),
    nvl(new.source,old.source),
    nvl(new.os,old.os),
    nvl(new.area,old.area),
    nvl(new.model,old.model),
    nvl(new.brand,old.brand),
    nvl(new.sdk_version,old.sdk_version),
    nvl(new.gmail,old.gmail),
    nvl(new.height_width,old.height_width),
    nvl(new.app_time,old.app_time),
    nvl(new.network,old.network),
    nvl(new.lng,old.lng),
    nvl(new.lat,old.lat),
    nvl(old.login_date_first,'$do_date'),
    if(new.login_count>0,'$do_date',old.login_date_last),
    nvl(new.login_count,0),
    nvl(new.login_count,0)+nvl(old.login_count,0)
from
(
    select
        *
    from ${APP}.dwt_uv_topic
)old
full outer join
(
    select
        *
    from ${APP}.dws_uv_detail_daycount
    where dt='$do_date'
)new
on old.mid_id=new.mid_id;

insert overwrite table ${APP}.dwt_user_topic
select
    nvl(new.user_id,old.user_id),
    if(old.login_date_first is null and new.login_count>0,'$do_date',old.login_date_first),
    if(new.login_count>0,'$do_date',old.login_date_last),
    nvl(old.login_count,0)+if(new.login_count>0,1,0),
    nvl(new.login_last_30d_count,0),
    if(old.order_date_first is null and new.order_count>0,'$do_date',old.order_date_first),
    if(new.order_count>0,'$do_date',old.order_date_last),
    nvl(old.order_count,0)+nvl(new.order_count,0),
    nvl(old.order_amount,0)+nvl(new.order_amount,0),
    nvl(new.order_last_30d_count,0),
    nvl(new.order_last_30d_amount,0),
    if(old.payment_date_first is null and new.payment_count>0,'$do_date',old.payment_date_first),
    if(new.payment_count>0,'$do_date',old.payment_date_last),
    nvl(old.payment_count,0)+nvl(new.payment_count,0),
    nvl(old.payment_amount,0)+nvl(new.payment_amount,0),
    nvl(new.payment_last_30d_count,0),
    nvl(new.payment_last_30d_amount,0)
from
(
    select 
        *
    from ${APP}.dwt_user_topic
)old
full outer join
(
    select
        user_id,
        sum(if(dt='$do_date',login_count,0)) login_count,
        sum(if(dt='$do_date',order_count,0)) order_count,
        sum(if(dt='$do_date',order_amount,0)) order_amount,
        sum(if(dt='$do_date',payment_count,0)) payment_count,
        sum(if(dt='$do_date',payment_amount,0)) payment_amount,
        sum(if(order_count>0,1,0)) login_last_30d_count,
        sum(order_count) order_last_30d_count,
        sum(order_amount) order_last_30d_amount,
        sum(payment_count) payment_last_30d_count,
        sum(payment_amount) payment_last_30d_amount
    from ${APP}.dws_user_action_daycount
    where dt>=date_add( '$do_date',-30)
    group by user_id
)new
on old.user_id=new.user_id;

with
sku_act as
(
select 
    sku_id,
    sum(if(dt='$do_date', order_count,0 )) order_count,
    sum(if(dt='$do_date',order_num ,0 ))  order_num, 
    sum(if(dt='$do_date',order_amount,0 )) order_amount ,
    sum(if(dt='$do_date',payment_count,0 )) payment_count,
    sum(if(dt='$do_date',payment_num,0 )) payment_num,
    sum(if(dt='$do_date',payment_amount,0 )) payment_amount,
    sum(if(dt='$do_date',refund_count,0 )) refund_count,
    sum(if(dt='$do_date',refund_num,0 )) refund_num,
    sum(if(dt='$do_date',refund_amount,0 )) refund_amount,  
    sum(if(dt='$do_date',cart_count,0 )) cart_count,
    sum(if(dt='$do_date',cart_num,0 )) cart_num,
    sum(if(dt='$do_date',favor_count,0 )) favor_count,
    sum(if(dt='$do_date',appraise_good_count,0 )) appraise_good_count,  
    sum(if(dt='$do_date',appraise_mid_count,0 ) ) appraise_mid_count ,
    sum(if(dt='$do_date',appraise_bad_count,0 )) appraise_bad_count,  
    sum(if(dt='$do_date',appraise_default_count,0 )) appraise_default_count,
    sum( order_count  ) order_count30 ,
    sum( order_num  )  order_num30,
    sum(order_amount ) order_amount30,
    sum(payment_count ) payment_count30,
    sum(payment_num ) payment_num30,
    sum(payment_amount ) payment_amount30,
    sum(refund_count  ) refund_count30,
    sum(refund_num ) refund_num30,
    sum(refund_amount ) refund_amount30,
    sum(cart_count  ) cart_count30,
    sum(cart_num ) cart_num30,
    sum(favor_count ) favor_count30,
    sum(appraise_good_count ) appraise_good_count30,
    sum(appraise_mid_count  ) appraise_mid_count30,
    sum(appraise_bad_count ) appraise_bad_count30,
    sum(appraise_default_count )  appraise_default_count30 
from ${APP}.dws_sku_action_daycount
where dt>=date_add ( '$do_date',-30)
group by sku_id
),
sku_topic
as 
(
select
    sku_id,
    spu_id,
    order_last_30d_count,
    order_last_30d_num,
    order_last_30d_amount,
    order_count,
    order_num,
    order_amount  ,
    payment_last_30d_count,
    payment_last_30d_num,
    payment_last_30d_amount,
    payment_count,
    payment_num,
    payment_amount,
    refund_last_30d_count,
    refund_last_30d_num,
    refund_last_30d_amount ,
    refund_count  ,
    refund_num ,
    refund_amount  ,
    cart_last_30d_count  ,
    cart_last_30d_num  ,
    cart_count  ,
    cart_num  ,
    favor_last_30d_count  ,
    favor_count  ,
    appraise_last_30d_good_count  ,
    appraise_last_30d_mid_count  ,
    appraise_last_30d_bad_count  ,
    appraise_last_30d_default_count  ,
    appraise_good_count  ,
    appraise_mid_count  ,
    appraise_bad_count  ,
    appraise_default_count 
from ${APP}.dwt_sku_topic
)
insert overwrite table ${APP}.dwt_sku_topic
select 
    nvl(sku_act.sku_id,sku_topic.sku_id) ,
    sku_info.spu_id,
    nvl (sku_act.order_count30,0)      ,
    nvl (sku_act.order_num30,0)   ,
    nvl (sku_act.order_amount30,0)   ,
    nvl(sku_topic.order_count,0)+ nvl (sku_act.order_count,0) ,
    nvl(sku_topic.order_num,0)+ nvl (sku_act.order_num,0)   ,
    nvl(sku_topic.order_amount,0)+ nvl (sku_act.order_amount,0),
    nvl (sku_act.payment_count30,0),
    nvl (sku_act.payment_num30,0),
    nvl (sku_act.payment_amount30,0),
    nvl(sku_topic.payment_count,0)+ nvl (sku_act.payment_count,0) ,
    nvl(sku_topic.payment_num,0)+ nvl (sku_act.payment_count,0)  ,
    nvl(sku_topic.payment_amount,0)+ nvl (sku_act.payment_count,0)  ,
    nvl (refund_count30,0),
    nvl (sku_act.refund_num30,0),
    nvl (sku_act.refund_amount30,0),
    nvl(sku_topic.refund_count,0)+ nvl (sku_act.refund_count,0),
    nvl(sku_topic.refund_num,0)+ nvl (sku_act.refund_num,0),
    nvl(sku_topic.refund_amount,0)+ nvl (sku_act.refund_amount,0),
    nvl(sku_act.cart_count30,0)  ,
    nvl(sku_act.cart_num30,0)  ,
    nvl(sku_topic.cart_count  ,0)+ nvl (sku_act.cart_count,0),
    nvl( sku_topic.cart_num  ,0)+ nvl (sku_act.cart_num,0),
    nvl(sku_act.favor_count30 ,0)  ,
    nvl (sku_topic.favor_count  ,0)+ nvl (sku_act.favor_count,0),
    nvl (sku_act.appraise_good_count30 ,0)  ,
    nvl (sku_act.appraise_mid_count30 ,0)  ,
    nvl (sku_act.appraise_bad_count30 ,0)  ,
    nvl (sku_act.appraise_default_count30 ,0)  ,
    nvl (sku_topic.appraise_good_count  ,0)+ nvl (sku_act.appraise_good_count,0)  ,
    nvl (sku_topic.appraise_mid_count   ,0)+ nvl (sku_act.appraise_mid_count,0) ,
    nvl (sku_topic.appraise_bad_count  ,0)+ nvl (sku_act.appraise_bad_count,0)  ,
    nvl (sku_topic.appraise_default_count  ,0)+ nvl (sku_act.appraise_default_count,0) 
from sku_act
full outer join sku_topic
on sku_act.sku_id =sku_topic.sku_id
left join
(select * from ${APP}.dwd_dim_sku_info where dt='$do_date') sku_info
on nvl(sku_topic.sku_id,sku_act.sku_id)= sku_info.id;

insert overwrite table ${APP}.dwt_coupon_topic
select
    nvl(new.coupon_id,old.coupon_id),
    nvl(new.get_count,0),
    nvl(new.using_count,0),
    nvl(new.used_count,0),
    nvl(old.get_count,0)+nvl(new.get_count,0),
    nvl(old.using_count,0)+nvl(new.using_count,0),
    nvl(old.used_count,0)+nvl(new.used_count,0)
from
(
    select
        *
    from ${APP}.dwt_coupon_topic
)old
full outer join
(
    select
        coupon_id,
        get_count,
        using_count,
        used_count
    from ${APP}.dws_coupon_use_daycount
    where dt='$do_date'
)new
on old.coupon_id=new.coupon_id;

insert overwrite table ${APP}.dwt_activity_topic
select
    nvl(new.id,old.id),
    nvl(new.activity_name,old.activity_name),
    nvl(new.order_count,0),
    nvl(new.payment_count,0),
    nvl(old.order_count,0)+nvl(new.order_count,0),
    nvl(old.payment_count,0)+nvl(new.payment_count,0)
from
(
    select
        *
    from ${APP}.dwt_activity_topic
)old
full outer join
(
    select
        id,
        activity_name,
        order_count,
        payment_count
    from ${APP}.dws_activity_info_daycount
    where dt='$do_date'
)new
on old.id=new.id;
"

$hive -e "$sql"

