package com.atguigu.gmall1018.dw.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall1018.dw.common.constant.GmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController  //= controller+ responsebody
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;

    @PostMapping("log")
    public String doLog(@RequestParam("log") String log){

        long ts = System.currentTimeMillis();
        JSONObject jsonObject = JSON.parseObject(log);
        jsonObject.put("ts",ts);
        if("startup".equals(jsonObject.getString("type")) ){
            kafkaTemplate.send(GmallConstant.TOPIC_STARTUP,jsonObject.toJSONString());
        }else{
            kafkaTemplate.send(GmallConstant.TOPIC_EVENT,jsonObject.toJSONString());
        }
        logger.info(jsonObject.toJSONString());
        return "success";
    }
}
