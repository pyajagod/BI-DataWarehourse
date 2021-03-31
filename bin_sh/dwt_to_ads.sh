#!/bin/bash

hive=/opt/module/hive/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    do_date=$1
else 
    do_date=`date -d "-1 day" +%F`
fi

sql="use gmall;
insert into table ads_uv_count
select
    '$do_date',
    sum(if(login_date_last='$do_date',1,0)),
    sum(if(login_date_last>=date_add(next_day('$do_date','monday'),-7) and login_date_last<=date_add(next_day('$do_date','monday'),-1) ,1,0)),
    sum(if(date_format(login_date_last,'yyyy-MM')=date_format('$do_date','yyyy-MM'),1,0)),
    if('$do_date'=date_add(next_day('$do_date','monday'),-1),'Y','N'),
    if('$do_date'=last_day('$do_date'),'Y','N')
from dwt_uv_topic;

insert into table ads_new_mid_count
select
    '$do_date',
    count(*)
from dwt_uv_topic
where login_date_first='$do_date';

insert into table ads_silent_count
select
    '$do_date',
    count(*) 
from dwt_uv_topic
where login_date_first=login_date_last
and login_date_last<=date_add('$do_date',-7);

insert into table ads_back_count
select
    '$do_date',
    concat(date_add(next_day('2020-03-10','MO'),-7),'_',date_add(next_day('2020-03-10','MO'),-1)),
    count(*)
from
(
    select
        mid_id
    from dwt_uv_topic
    where login_date_last>=date_add(next_day('$do_date','MO'),-7) 
    and login_date_last<= date_add(next_day('$do_date','MO'),-1)
    and login_date_first<date_add(next_day('$do_date','MO'),-7)
)current_wk
left join
(
    select
        mid_id
    from dws_uv_detail_daycount
    where dt>=date_add(next_day('$do_date','MO'),-7*2) 
    and dt<= date_add(next_day('$do_date','MO'),-7-1) 
    group by mid_id
)last_wk
on current_wk.mid_id=last_wk.mid_id
where last_wk.mid_id is null;


insert into table ads_wastage_count
select
    '$do_date',
    count(*)
from dwt_uv_topic
where login_date_last<=date_add('$do_date',-7);

insert into table ads_user_retention_day_rate
select
    '$do_date',
    date_add('$do_date',-3),
    3,
    sum(if(login_date_first=date_add('$do_date',-3) and login_date_last='$do_date',1,0)),
    sum(if(login_date_first=date_add('$do_date',-3),1,0)),
    sum(if(login_date_first=date_add('$do_date',-3) and login_date_last='$do_date',1,0))/sum(if(login_date_first=date_add('$do_date',-3),1,0))*100
from dwt_uv_topic
union all 
select
    '$do_date',
    date_add('$do_date',-2),
    2,
    sum(if(login_date_first=date_add('$do_date',-2) and login_date_last='$do_date',1,0)),
    sum(if(login_date_first=date_add('$do_date',-2),1,0)),
    sum(if(login_date_first=date_add('$do_date',-2) and login_date_last='$do_date',1,0))/sum(if(login_date_first=date_add('$do_date',-2),1,0))*100
from dwt_uv_topic
union all
select
    '$do_date',
    date_add('$do_date',-1),
    1,
    sum(if(login_date_first=date_add('$do_date',-1) and login_date_last='$do_date',1,0)),
    sum(if(login_date_first=date_add('$do_date',-1),1,0)),
    sum(if(login_date_first=date_add('$do_date',-1) and login_date_last='$do_date',1,0))/sum(if(login_date_first=date_add('$do_date',-1),1,0))*100
from dwt_uv_topic;


insert into table ads_continuity_wk_count
select
    '$do_date',
    concat(date_add(next_day('$do_date','MO'),-7*3),'_',date_add(next_day('$do_date','MO'),-1)),
    count(*)
from
(
    select
        mid_id
    from
    (
        select
            mid_id
        from dws_uv_detail_daycount
        where dt>=date_add(next_day('$do_date','monday'),-7)
        and dt<=date_add(next_day('$do_date','monday'),-1)
        group by mid_id

        union all

        select
            mid_id
        from dws_uv_detail_daycount
        where dt>=date_add(next_day('$do_date','monday'),-7*2)
        and dt<=date_add(next_day('$do_date','monday'),-7-1)
        group by mid_id

        union all

        select
            mid_id
        from dws_uv_detail_daycount
        where dt>=date_add(next_day('$do_date','monday'),-7*3)
        and dt<=date_add(next_day('$do_date','monday'),-7*2-1)
        group by mid_id
    )t1
    group by mid_id
    having count(*)=3
)t2;

insert into table ads_continuity_uv_count
select
    '$do_date',
    concat(date_add('$do_date',-6),'_','$do_date'),
    count(*)
from
(
    select mid_id
    from
    (
        select mid_id      
        from
        (
            select 
                mid_id,
                date_sub(dt,rank) date_dif
            from
            (
                select 
                    mid_id,
                    dt,
                    rank() over(partition by mid_id order by dt) rank
                from dws_uv_detail_daycount
                where dt>=date_add('$do_date',-6) and dt<='$do_date'
            )t1
        )t2 
        group by mid_id,date_dif
        having count(*)>=3
    )t3 
    group by mid_id
)t4;

insert into table ads_user_topic
select
    '$do_date',
    sum(if(login_date_last='$do_date',1,0)),
    sum(if(login_date_first='$do_date',1,0)),
    sum(if(payment_date_first='$do_date',1,0)),
    sum(if(payment_count>0,1,0)),
    count(*),
    sum(if(login_date_last='$do_date',1,0))/count(*),
    sum(if(payment_count>0,1,0))/count(*),
    sum(if(login_date_first='$do_date',1,0))/sum(if(login_date_last='$do_date',1,0))
from dwt_user_topic;

insert into table ads_user_action_convert_day
select 
    '$do_date',
    uv.day_count,
    ua.cart_count,
    ua.cart_count/uv.day_count*100 visitor2cart_convert_ratio,
    ua.order_count,
    ua.order_count/ua.cart_count*100  visitor2order_convert_ratio,
    ua.payment_count,
    ua.payment_count/ua.order_count*100 order2payment_convert_ratio
from  
(
    select 
        '$do_date' dt,
        sum(if(cart_count>0,1,0)) cart_count,
        sum(if(order_count>0,1,0)) order_count,
        sum(if(payment_count>0,1,0)) payment_count
    from dws_user_action_daycount
    where dt='$do_date'
)ua join ads_uv_count uv on uv.dt=ua.dt;


insert into table ads_product_info
select
    '$do_date' dt,
    sku_num,
    spu_num
from
(
    select
        '$do_date' dt,
        count(*) sku_num
    from
        dwt_sku_topic
) tmp_sku_num
join
(
    select
        '$do_date' dt,
        count(*) spu_num
    from
    (
        select
            spu_id
        from
            dwt_sku_topic
        group by
            spu_id
    ) tmp_spu_id
) tmp_spu_num
on tmp_sku_num.dt=tmp_spu_num.dt;


insert into table ads_product_sale_topN
select
    '$do_date',
    sku_id,
    payment_amount
from dws_sku_action_daycount
where dt='$do_date'
order by payment_amount desc
limit 10;


insert into table ads_product_favor_topN
select
    '$do_date',
    sku_id,
    favor_count
from dws_sku_action_daycount
where dt='$do_date'
order by favor_count
limit 10;


insert into table ads_product_cart_topN
select
    '$do_date' dt,
    sku_id,
    cart_num
from dws_sku_action_daycount
where dt='$do_date'
order by cart_num
limit 10;

insert into table ads_product_refund_topN
select
    '$do_date',
    sku_id,
    refund_last_30d_count/payment_last_30d_count*100 refund_ratio
from dwt_sku_topic
order by refund_ratio desc
limit 10;

insert into table ads_appraise_bad_topN
select
    '$do_date' dt,
    sku_id,
    appraise_bad_count/(appraise_bad_count+appraise_good_count+appraise_mid_count+appraise_default_count)*100 appraise_bad_ratio
from dws_sku_action_daycount
where dt='$do_date'
order by appraise_bad_ratio desc
limit 10;


insert into table ads_order_daycount
select
    '$do_date',
    sum(order_count),
    sum(order_amount),
    sum(if(order_count>0,1,0))
from dws_user_action_daycount
where dt='$do_date';

insert into table ads_payment_daycount
select
    tmp_payment.dt,
    tmp_payment.payment_count,
    tmp_payment.payment_amount,
    tmp_payment.payment_user_count,
    tmp_skucount.payment_sku_count,
    tmp_time.payment_avg_time
from
(
    select
        '$do_date' dt,
        sum(payment_count) payment_count,
        sum(payment_amount) payment_amount,
        sum(if(payment_count>0,1,0)) payment_user_count
    from dws_user_action_daycount
    where dt='$do_date'
)tmp_payment
join
(
    select
        '$do_date' dt,
        sum(if(payment_count>0,1,0)) payment_sku_count 
    from dws_sku_action_daycount
    where dt='$do_date'
)tmp_skucount on tmp_payment.dt=tmp_skucount.dt
join
(
    select
        '$do_date' dt,
        sum(unix_timestamp(payment_time)-unix_timestamp(create_time))/count(*)/60 payment_avg_time
    from dwd_fact_order_info
    where dt='$do_date'
    and payment_time is not null
)tmp_time on tmp_payment.dt=tmp_time.dt;

insert into table ads_sale_tm_category1_stat_mn
select   
    mn.sku_tm_id,
    mn.sku_category1_id,
    mn.sku_category1_name,
    sum(if(mn.order_count>=1,1,0)) buycount,
    sum(if(mn.order_count>=2,1,0)) buyTwiceLast,
    sum(if(mn.order_count>=2,1,0))/sum( if(mn.order_count>=1,1,0)) buyTwiceLastRatio,
    sum(if(mn.order_count>=3,1,0))  buy3timeLast  ,
    sum(if(mn.order_count>=3,1,0))/sum( if(mn.order_count>=1,1,0)) buy3timeLastRatio ,
    date_format('$do_date' ,'yyyy-MM') stat_mn,
    '$do_date' stat_date
from 
(
    select 
        user_id, 
        sd.sku_tm_id,
        sd.sku_category1_id,
        sd.sku_category1_name,
        sum(order_count) order_count
    from dws_sale_detail_daycount sd 
    where date_format(dt,'yyyy-MM')=date_format('$do_date' ,'yyyy-MM')
    group by user_id, sd.sku_tm_id, sd.sku_category1_id, sd.sku_category1_name
) mn
group by mn.sku_tm_id, mn.sku_category1_id, mn.sku_category1_name;
"

$hive -e "$sql"

