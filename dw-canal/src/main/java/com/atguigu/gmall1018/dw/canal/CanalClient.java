package com.atguigu.gmall1018.dw.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {


    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop1", 11111), "example", "", "");
        while (true){
            canalConnector.connect();  //连接
            canalConnector.subscribe("gmall1018.order_info");  // 订阅表
            Message message = canalConnector.get(100);//抓取100条命令
            if(message.getEntries().size()==0){
                try {
                    System.out.println("没有数据，休息5秒");
                    Thread.sleep(5000);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //需要entry storevalue (序列化的内容)
                    if( entry.getEntryType()!= CanalEntry.EntryType.ROWDATA  ){  //只要行变化内容
                        continue;
                    }
                    CanalEntry.RowChange rowChange=null;
                    try {
                         rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                    //需要rowchange  eventtype(insert,update)   rowdatalist   表名
                    String tableName = entry.getHeader().getTableName();//表名
                    CanalEntry.EventType eventType = rowChange.getEventType();//操作类型
                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList(); //行集

                    CanalHandler.handle(tableName,eventType,rowDatasList);
                }
            }

        }


    }
}
