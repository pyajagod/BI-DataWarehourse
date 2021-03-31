package com.qwy.flume.interceptor;

import org.apache.commons.lang.math.NumberUtils;

public class LogUtils {
    //验证启动日志
    public static boolean validateStart(String log){

        if (log == null){
            return false;
        }

        //判断数据是否是 “{” 开头， “}” 结尾
        if (!log.trim().startsWith("{") || !log.trim().endsWith("}")) {
            return false;
        }

        return true;
    }

    //验证事件日志
    public static boolean validateEvent(String log){
        //服务器时间 | 日志内容

        if (log == null){
            return false;
        }

        //切割数据
        String[] logContents = log.split("\\|");

        //数据长度判断
        if (logContents.length != 2){
            return false;
        }

        //校验服务器时间(长度必须为13位，且必须全是数字)
        if (logContents[0].length() != 13 || !NumberUtils.isDigits(logContents[0])){
            return false;
        }

        //校验日志格式
        if (!logContents[1].trim().startsWith("{") || !logContents[1].trim().endsWith("}")){
            return false;
        }

        return true;
    }
}
