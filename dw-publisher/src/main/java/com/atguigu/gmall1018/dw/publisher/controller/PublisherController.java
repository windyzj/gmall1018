package com.atguigu.gmall1018.dw.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1018.dw.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController   //==> controller + responsebody
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    @RequestMapping(value = "realtime-total" ,method = RequestMethod.GET)
   public String getTotal(@RequestParam("date") String date){
        List totalList=new ArrayList();
        int dauTotal = publisherService.getDauTotal(date);
        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);

        totalList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增用户");
        newMidMap.put("value",2000);

        totalList.add(newMidMap);

        return JSON.toJSONString(totalList);
    }


   // http://publisher:8070/realtime-hour?id=dau&&date=2019-02-01
    @GetMapping("realtime-hour")
    public  String getRealtimeHour(@RequestParam("id") String id,@RequestParam("date") String date   ){
        if("dau".equals(id)){
            //求今天的数据
            Map dauHourCountTDMap = publisherService.getDauHourCount(date);
            //昨天
            String yesterday = getYesterday(date);
            Map dauHourCountYDMap = publisherService.getDauHourCount(yesterday);
            //合并
            Map dauMap=new HashMap();
            dauMap.put("today",dauHourCountTDMap);
            dauMap.put("yesterday",dauHourCountYDMap);
            return  JSON.toJSONString(dauMap);
        }


        return   null;
    }

    private String getYesterday(String date){
        Date today=null;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {

              today = simpleDateFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date yesterday = DateUtils.addDays(today, -1);
        String yesterdayString = simpleDateFormat.format(yesterday);
        return yesterdayString;
    }


}
