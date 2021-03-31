package com.qwy.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //定义UDTF返回值类型与名称
        List<String> fieldName = new ArrayList<>();
        List<ObjectInspector> filedType = new ArrayList<>();

        fieldName.add("event_name");
        fieldName.add("event_json");

        filedType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        filedType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, filedType);
    }

    @Override
    public void process(Object[] objects) throws HiveException {

        //传入的是json array => UDF 传入et取出
        String input = objects[0].toString();

        //合法校验
        if (StringUtils.isBlank(input)){
            return;
        }else {
            JSONArray ja = new JSONArray(input);

            if (ja == null){
                return;
            }

            // 循环遍历Array中每一个元素，封装成返回的事件名称与事件内容
            int len = ja.length();
            for (int i = 0; i < len; i++) {

                String[] result = new String[2];

                try {
                    result[0] = ja.getJSONObject(i).getString("en");
                    result[1] = ja.getJSONObject(i).getString("kv");
                }catch (JSONException e) {
                    continue;
                }

                forward(result);
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
