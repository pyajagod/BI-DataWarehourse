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
insert overwrite table ${APP}.dws_uv_detail_daycount partition(dt='$do_date')
select  
    mid_id,
    concat_ws('|', collect_set(user_id)) user_id,
    concat_ws('|', collect_set(version_code)) version_code,
    concat_ws('|', collect_set(version_name)) version_name,
    concat_ws('|', collect_set(lang))lang,
    concat_ws('|', collect_set(source)) source,
    concat_ws('|', collect_set(os)) os,
    concat_ws('|', collect_set(area)) area, 
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(sdk_version)) sdk_version,
    concat_ws('|', collect_set(gmail)) gmail,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    concat_ws('|', collect_set(lng)) lng,
    concat_ws('|', collect_set(lat)) lat,
    count(*) login_count
from ${APP}.dwd_start_log
where dt='$do_date'
group by mid_id;


with
tmp_login as
(
    select
        user_id,
        count(*) login_count
    from ${APP}.dwd_start_log
    where dt='$do_date'
    and user_id is not null
    group by user_id
),
tmp_cart as
(
    select
        user_id,
        count(*) cart_count,
        sum(cart_price*sku_num) cart_amount
    from ${APP}.dwd_fact_cart_info
    where dt='$do_date'
    and user_id is not null
    and date_format(create_time,'yyyy-MM-dd')='$do_date'
    group by user_id
),
tmp_order as
(
    select
        user_id,
        count(*) order_count,
        sum(final_total_amount) order_amount
    from ${APP}.dwd_fact_order_info
    where dt='$do_date'
    group by user_id
) ,
tmp_payment as
(
    select
        user_id,
        count(*) payment_count,
        sum(payment_amount) payment_amount
    from ${APP}.dwd_fact_payment_info
    where dt='$do_date'
    group by user_id
)

insert overwrite table ${APP}.dws_user_action_daycount partition(dt='$do_date')
select
    user_actions.user_id,
    sum(user_actions.login_count),
    sum(user_actions.cart_count),
    sum(user_actions.cart_amount),
    sum(user_actions.order_count),
    sum(user_actions.order_amount),
    sum(user_actions.payment_count),
    sum(user_actions.payment_amount)
from 
(
    select
        user_id,
        login_count,
        0 cart_count,
        0 cart_amount,
        0 order_count,
        0 order_amount,
        0 payment_count,
        0 payment_amount
    from 
    tmp_login
    union all
    select
        user_id,
        0 login_count,
        cart_count,
        cart_amount,
        0 order_count,
        0 order_amount,
        0 payment_count,
        0 payment_amount
    from 
    tmp_cart
    union all
    select
        user_id,
        0 login_count,
        0 cart_count,
        0 cart_amount,
        order_count,
        order_amount,
        0 payment_count,
        0 payment_amount
    from tmp_order
    union all
    select
        user_id,
        0 login_count,
        0 cart_count,
        0 cart_amount,
        0 order_count,
        0 order_amount,
        payment_count,
        payment_amount
    from tmp_payment
 ) user_actions
group by user_id;


with 
tmp_order as
(
    select
        sku_id,
        count(*) order_count,
        sum(sku_num) order_num,
        sum(total_amount) order_amount
    from ${APP}.dwd_fact_order_detail
    where dt='$do_date'
    group by sku_id
),
tmp_payment as
(
    select
        sku_id,
        count(*) payment_count,
        sum(sku_num) payment_num,
        sum(total_amount) payment_amount
    from ${APP}.dwd_fact_order_detail
    where dt='$do_date'
    and order_id in
    (
        select
            id
        from ${APP}.dwd_fact_order_info
        where (dt='$do_date' or dt=date_add('$do_date',-1))
        and date_format(payment_time,'yyyy-MM-dd')='$do_date'
    )
    group by sku_id
),
tmp_refund as
(
    select
        sku_id,
        count(*) refund_count,
        sum(refund_num) refund_num,
        sum(refund_amount) refund_amount
    from ${APP}.dwd_fact_order_refund_info
    where dt='$do_date'
    group by sku_id
),
tmp_cart as
(
    select
        sku_id,
        count(*) cart_count,
        sum(sku_num) cart_num
    from ${APP}.dwd_fact_cart_info
    where dt='$do_date'
    and date_format(create_time,'yyyy-MM-dd')='$do_date'
    group by sku_id
),
tmp_favor as
(
    select
        sku_id,
        count(*) favor_count
    from ${APP}.dwd_fact_favor_info
    where dt='$do_date'
    and date_format(create_time,'yyyy-MM-dd')='$do_date'
    group by sku_id
),
tmp_appraise as
(
    select
        sku_id,
        sum(if(appraise='1201',1,0)) appraise_good_count,
        sum(if(appraise='1202',1,0)) appraise_mid_count,
        sum(if(appraise='1203',1,0)) appraise_bad_count,
        sum(if(appraise='1204',1,0)) appraise_default_count
    from ${APP}.dwd_fact_comment_info
    where dt='$do_date'
    group by sku_id
)

insert overwrite table ${APP}.dws_sku_action_daycount partition(dt='$do_date')
select
    sku_id,
    sum(order_count),
    sum(order_num),
    sum(order_amount),
    sum(payment_count),
    sum(payment_num),
    sum(payment_amount),
    sum(refund_count),
    sum(refund_num),
    sum(refund_amount),
    sum(cart_count),
    sum(cart_num),
    sum(favor_count),
    sum(appraise_good_count),
    sum(appraise_mid_count),
    sum(appraise_bad_count),
    sum(appraise_default_count)
from
(
    select
        sku_id,
        order_count,
        order_num,
        order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 cart_num,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_order
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        payment_count,
        payment_num,
        payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 cart_num,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_payment
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        refund_count,
        refund_num,
        refund_amount,
        0 cart_count,
        0 cart_num,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count        
    from tmp_refund
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        cart_count,
        cart_num,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_cart
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 cart_num,
        favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_favor
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 cart_num,
        0 favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from tmp_appraise
)tmp
group by sku_id;


insert overwrite table ${APP}.dws_coupon_use_daycount partition(dt='$do_date')
select
    cu.coupon_id,
    ci.coupon_name,
    ci.coupon_type,
    ci.condition_amount,
    ci.condition_num,
    ci.activity_id,
    ci.benefit_amount,
    ci.benefit_discount,
    ci.create_time,
    ci.range_type,
    ci.spu_id,
    ci.tm_id,
    ci.category3_id,
    ci.limit_num,
    cu.get_count,
    cu.using_count,
    cu.used_count
from 
(
    select
        coupon_id,
        sum(if(date_format(get_time,'yyyy-MM-dd')='$do_date',1,0)) get_count,
        sum(if(date_format(using_time,'yyyy-MM-dd')='$do_date',1,0)) using_count,
        sum(if(date_format(used_time,'yyyy-MM-dd')='$do_date',1,0)) used_count
    from ${APP}.dwd_fact_coupon_use
    where dt='$do_date'
    group by coupon_id
)cu
left join
(
    select
        *
    from ${APP}.dwd_dim_coupon_info
    where dt='$do_date'
)ci on cu.coupon_id=ci.id;

insert overwrite table ${APP}.dws_activity_info_daycount partition(dt='$do_date')
select
    oi.activity_id,
    ai.activity_name,
    ai.activity_type,
    ai.start_time,
    ai.end_time,
    ai.create_time,
    oi.order_count,
    oi.payment_count
from
(
    select
        activity_id,
        sum(if(date_format(create_time,'yyyy-MM-dd')='$do_date',1,0)) order_count,
        sum(if(date_format(payment_time,'yyyy-MM-dd')='$do_date',1,0)) payment_count
    from ${APP}.dwd_fact_order_info
    where (dt='$do_date' or dt=date_add('$do_date',-1))
    and activity_id is not null
    group by activity_id
)oi
join
(
    select
        *
    from ${APP}.dwd_dim_activity_info
    where dt='$do_date'
)ai
on oi.activity_id=ai.id;

insert overwrite table ${APP}.dws_sale_detail_daycount partition(dt='$do_date')
select
    op.user_id,
    op.sku_id,
    ui.gender,
    months_between('$do_date', ui.birthday)/12  age, 
    ui.user_level,
    si.price,
    si.sku_name,
    si.tm_id,
    si.category3_id,
    si.category2_id,
    si.category1_id,
    si.category3_name,
    si.category2_name,
    si.category1_name,
    si.spu_id,
    op.sku_num,
    op.order_count,
    op.order_amount 
from
(
    select
        user_id,
        sku_id,
        sum(sku_num) sku_num,
        count(*) order_count,
        sum(total_amount) order_amount
    from ${APP}.dwd_fact_order_detail
    where dt='$do_date'
    group by user_id, sku_id
)op
join
(
    select
        *
    from ${APP}.dwd_dim_user_info_his
    where end_date='9999-99-99'
)ui on op.user_id = ui.id
join
(
    select
        *
    from ${APP}.dwd_dim_sku_info
    where dt='$do_date'
)si on op.sku_id = si.id;

"

$hive -e "$sql"

