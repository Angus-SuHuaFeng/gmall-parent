package com.angus.gmall.publisher.controller;

import com.angus.gmall.publisher.service.EsService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author ：Angus
 * @date ：Created in 2022/2/26 14:04
 * @description：
 */
@RestController
public class PublisherController {

    @Autowired
    EsService esService;
    /*
        访问路径: http://publisher:8070/realtime-total?date=2019-02-01
        响应数据: [{"id":"dau","name":"新增日活","value":1200},
                    {"id":"new_mid","name":"新增设备","value":233} ]
     */
    @RequestMapping("/realtime-total")
    public Object realtimeTotal(@RequestParam("date") String date){
        List<Map<String ,Object>> rsList = new ArrayList<>();
        // 日活统计
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal = esService.getDauTotal(date);
        if (dauTotal!=null){
            dauMap.put("value",dauTotal);
        }else {
            dauMap.put("value",0L);
        }
        rsList.add(dauMap);
        // 新增设备统计     未实现
        HashMap<String, Object> midMap = new HashMap<>();
        midMap.put("id","new_mid");
        midMap.put("name","新增设备");
        midMap.put("value",10);
        rsList.add(midMap);
        return rsList;
    }

    /*
        访问路径: http://publisher:8070/realtime-hour?id=dau&date=2019-02-01
        响应数据: {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
                    "today":{"12":38,"13":1233,"17":123,"19":688 }}
     */
    @RequestMapping("realtime-hour")
    public Object realtimeHour(@RequestParam(value = "id", defaultValue = "dau") String id, @RequestParam("date") String date){
        Map<String, Map<String ,Long>> rsMap = new HashMap<>();
        Map<String, Long> todayMap = esService.getDauHour(date);
        String yd = getYD(date);
        Map<String, Long> ydMap = esService.getDauHour(yd);
        rsMap.put("yesterday",ydMap);
        rsMap.put("today", todayMap);
        return rsMap;
    }

    private String getYD(String today) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday = null;
        try {
            Date td = sdf.parse(today);
            Date yd = DateUtils.addDays(td, -1);
            yesterday = sdf.format(yd);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转变失败");
        }
        return yesterday;
    }
}
