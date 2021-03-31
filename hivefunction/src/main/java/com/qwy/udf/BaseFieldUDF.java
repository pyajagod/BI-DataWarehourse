package com.qwy.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
    public String evaluate(String line, String key){

        // 切割
        String[] logs = line.split("\\|");

        //合法性判断
        if (logs.length != 2 || StringUtils.isBlank(logs[1].trim())){
            return "";
        }

        JSONObject json = new JSONObject(logs[1].trim());

        String result = "";

        //根据传过来的key取值
        if ("st".equals(key)){
            //返回服务器时间
            return logs[0].trim();
        }else if("et".equals(key)){
            if(json.has("et")){
                result = json.getString("et");
            }
        }else {
            //获取cm对应的value值
            JSONObject cm = json.getJSONObject("cm");

            if (cm.has(key)){
                result = cm.getString(key);
            }
        }

        return result;
    }

    public static void main(String[] args) {
        String line = "1583834132231|{\"cm\":{\"ln\":\"-71.5\",\"sv\":\"V2.8.1\",\"os\":\"8.1.6\",\"g\":\"NB67IOC2@gmail.com\",\"mid\":\"0\",\"nw\":\"3G\",\"l\":\"en\",\"vc\":\"4\",\"hw\":\"640*1136\",\"ar\":\"MX\",\"uid\":\"0\",\"t\":\"1583805844190\",\"la\":\"-19.4\",\"md\":\"HTC-19\",\"vn\":\"1.2.5\",\"ba\":\"HTC\",\"sr\":\"L\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1583798163948\",\"en\":\"display\",\"kv\":{\"goodsid\":\"0\",\"action\":\"1\",\"extend1\":\"1\",\"place\":\"2\",\"category\":\"40\"}},{\"ett\":\"1583816084263\",\"en\":\"newsdetail\",\"kv\":{\"entry\":\"3\",\"goodsid\":\"1\",\"news_staytime\":\"24\",\"loading_time\":\"24\",\"action\":\"2\",\"showtype\":\"2\",\"category\":\"74\",\"type1\":\"102\"}},{\"ett\":\"1583788468371\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"36\",\"action\":\"3\",\"extend1\":\"\",\"type\":\"2\",\"type1\":\"\",\"loading_way\":\"2\"}},{\"ett\":\"1583770481936\",\"en\":\"ad\",\"kv\":{\"activityId\":\"1\",\"displayMills\":\"44908\",\"entry\":\"3\",\"action\":\"4\",\"contentType\":\"0\"}},{\"ett\":\"1583833963975\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1583766544612\",\"action\":\"3\",\"type\":\"4\",\"content\":\"\"}},{\"ett\":\"1583807752239\",\"en\":\"error\",\"kv\":{\"errorDetail\":\"java.lang.NullPointerException\\\\n    at cn.lift.appIn.web.AbstractBaseController.validInbound(AbstractBaseController.java:72)\\\\n at cn.lift.dfdf.web.AbstractBaseController.validInbound\",\"errorBrief\":\"at cn.lift.appIn.control.CommandUtil.getInfo(CommandUtil.java:67)\"}},{\"ett\":\"1583749111002\",\"en\":\"comment\",\"kv\":{\"p_comment_id\":2,\"addtime\":\"1583828330480\",\"praise_count\":726,\"other_id\":5,\"comment_id\":3,\"reply_count\":121,\"userid\":2,\"content\":\"败矩描荤嘘稍癣藩乌护牡区念\"}},{\"ett\":\"1583781286094\",\"en\":\"favorites\",\"kv\":{\"course_id\":6,\"id\":0,\"add_time\":\"1583768356410\",\"userid\":2}}]}\t2020-03-10";

        String result = new BaseFieldUDF().evaluate(line, "et");

        System.out.println(result);
    }
}
