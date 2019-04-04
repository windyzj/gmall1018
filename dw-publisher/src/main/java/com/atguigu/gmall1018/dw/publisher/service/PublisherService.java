package com.atguigu.gmall1018.dw.publisher.service;

import java.util.Map;

public interface PublisherService {

     public int getDauTotal(String date);

     public Map getDauHourCount(String date);

     public Double getOrderAmount(String date);

     public Map getOrderAmountHour(String date);

     public Map getSaleDetail(String date ,String keyword,int startPage ,int size,String aggFieldName ,int aggsSize );
}
