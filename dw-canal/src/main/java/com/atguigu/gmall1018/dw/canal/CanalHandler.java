package com.atguigu.gmall1018.dw.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.google.common.base.CaseFormat;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall1018.dw.common.constant.GmallConstant;
import com.atguigu.gmall1018.dw.util.MyKafkaSender;

import java.util.List;

public class CanalHandler {

    public static  void handle(String tableName,CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList){
        if("order_info".equals(tableName)&&eventType.equals(CanalEntry.EventType.INSERT)&&rowDatasList!=null&&rowDatasList.size()>0){
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();//修改后的列集
                JSONObject jsonObject=new JSONObject();
                for (CanalEntry.Column column : afterColumnsList) {
                    System.out.println(column.getName()+"::::"+column.getValue());
                    String propertiesName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    jsonObject.put(propertiesName,column.getValue());

                }
                MyKafkaSender.send(GmallConstant.TOPIC_ORDER,jsonObject.toJSONString());

            }

        }

    }
}
