package com.qwy.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //清洗数据 ETL

        //获取日志
        byte[] body = event.getBody();
        String log = new String(body, Charset.forName("UTF-8"));

        //区分类型
        if (log.contains("start")){
            //验证启动日志的逻辑
            if (LogUtils.validateStart(log)){
                return event;
            }
        }else {
            //验证事件日志的逻辑
            if (LogUtils.validateEvent(log)){
                return event;
            }
        }

        return null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        //多events处理
        ArrayList<Event> interceptors = new ArrayList<>();

        //取出校验合格的数据
        for (Event event : events){
            Event intercept1 = intercept(event);
            if (intercept1 != null){
                interceptors.add(intercept1);
            }
        }
        return interceptors;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
