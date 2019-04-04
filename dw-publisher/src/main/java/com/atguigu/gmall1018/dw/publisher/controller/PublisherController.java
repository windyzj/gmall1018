package com.atguigu.gmall1018.dw.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1018.dw.publisher.bean.Option;
import com.atguigu.gmall1018.dw.publisher.bean.Stat;
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

        Double orderAmount = publisherService.getOrderAmount(date);
        Map orderAmountMap=new HashMap();
        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","订单金额");
        orderAmountMap.put("value",orderAmount);

        totalList.add(orderAmountMap);

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
        }else if("order_amount".equals(id)){
            //求今天的数据
            Map orderAmountHourTDMap = publisherService.getOrderAmountHour(date);
            //昨天
            String yesterday = getYesterday(date);
            Map orderAmountHourYDMap = publisherService.getOrderAmountHour(yesterday);
            //合并
            Map orderAmountHourMap=new HashMap();
            orderAmountHourMap.put("today",orderAmountHourTDMap);
            orderAmountHourMap.put("yesterday",orderAmountHourYDMap);
            return  JSON.toJSONString(orderAmountHourMap);
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


    @GetMapping("sale_detail")
    public  String getSaleDetail(@RequestParam("date") String date,@RequestParam("startpage") int startpage, @RequestParam("size") int size , @RequestParam("keyword") String keyword ){
            //根据条件查询 数据
        Map genderMap = publisherService.getSaleDetail(date, keyword, startpage, size, "user_gender", 2);
        //总数
        int total = (int)genderMap.get("total");
        //求性别占比
        Map genderAggsMap =(Map) genderMap.get("aggs");


        Long maleCount = (Long)genderAggsMap.get("M");
        Long femaleCount = (Long)genderAggsMap.get("F");
        double maleRate = Math.round(maleCount * 1000D / total) / 10D;
        double femaleRate = Math.round(femaleCount * 1000D / total) / 10D;



        List<Option> genderOptions=new ArrayList<>();
        genderOptions.add( new Option("男",maleRate)) ;
        genderOptions.add( new Option("女",femaleRate)) ;

        Stat genderStat = new Stat();
        genderStat.setOptions(genderOptions);
        genderStat.setTitle("用户性别占比");


        //年龄占比
            //取数
        Map ageMap = publisherService.getSaleDetail(date, keyword, startpage, size, "user_age", 100);
        Map ageAggMap =(Map) ageMap.get("aggs");

        int age_20=0;
        int age20_30=0;
        int age30_=0;
        //求每个年龄段总值
        for (Object obj : ageAggMap.entrySet()) {
            Map.Entry entry = (Map.Entry) obj;
            String ageStr =(String) entry.getKey();
            int age = Integer.parseInt(ageStr);
            Long count  =(Long) entry.getValue();


            if(age<20){
                age_20+=count;
            }else if (age>=20&&age<=30){
                age20_30+=count;
            }else{
                age30_+=count;
            }

        }
        //每个年龄段占比
        double age_20_rate = Math.round(age_20 * 1000D / total) / 10D;
        double age20_30_rate = Math.round(age20_30 * 1000D / total) / 10D;
        double age30_rate = Math.round(age30_ * 1000D / total) / 10D;
        //制作选项
        List<Option> aggeOptions=new ArrayList<>();
        aggeOptions.add( new Option("20岁以下",age_20_rate)) ;
        aggeOptions.add( new Option("20岁到30岁",age20_30_rate)) ;
        aggeOptions.add( new Option("30岁以上",age30_rate)) ;


        Stat ageStat = new Stat();
        ageStat.setOptions(aggeOptions);
        ageStat.setTitle("用户年龄占比");

        List<Stat> statList=new ArrayList<>();
        statList.add(genderStat);
        statList.add(ageStat);

        //封装最终结果
        Map saleMap=new HashMap();
        saleMap.put("total",total);
        saleMap.put("stat",statList);
        saleMap.put("detail",genderMap.get("detail"));

        return  JSON.toJSONString(saleMap);
    }



}
