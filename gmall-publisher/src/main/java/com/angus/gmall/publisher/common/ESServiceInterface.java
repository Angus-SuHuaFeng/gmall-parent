package com.angus.gmall.publisher.common;

import java.util.List;
import java.util.Map;

/**
 * @author ：Angus
 * @date ：Created in 2022/2/26 14:05
 * @description：
 */
public interface ESServiceInterface {
    // 日活统计
    public Long getDauTotal(String date);
    // 新增设备统计
    public Long getMidTotal(String date);
    // 分时查询（查询某天某时段的日活数）
    public Map<String, Long> getDauHour(String date);
}
