package com.angus.gmall.publisher.service;

import com.angus.gmall.publisher.common.ESServiceInterface;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ：Angus
 * @date ：Created in 2022/2/26 14:05
 * @description： 从ES中查询数据
 */

// 表示当前对象的创建交给Spring容器管理
@Service
public class EsService implements ESServiceInterface {
    // 将ES的客户端操作对象注入到Service中
    @Autowired
    JestClient jestClient;
    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchAllQueryBuilder());
        String query = sourceBuilder.toString();
        String index = "gmall_dau_info_"+date+"-query";
        Search search = new Search.Builder(query)
                .addIndex(index)
                .build();
        Long total = 0L;
        try {
            SearchResult searchResult = jestClient.execute(search);
            total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败");
        }
        return total;
    }

    // 新增设备，后续实现
    @Override
    public Long getMidTotal(String date) {

        return null;
    }
    // 分时查询
    @Override
    public Map<String, Long> getDauHour(String date) {
        Map<String, Long> hourMap = new HashMap<>();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // ES聚合查询
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("groupBy_hr", ValueType.LONG).field("hr.keyword").size(24);
        sourceBuilder.aggregation(termsAggregationBuilder);
        String query = sourceBuilder.toString();
        String index = "gmall_dau_info_"+date+"-query";
        Search search = new Search.Builder(query)
                .addIndex(index)
                .build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            TermsAggregation groupBy_hr = searchResult.getAggregations().getTermsAggregation("groupBy_hr");
            if (groupBy_hr!=null){
                List<TermsAggregation.Entry> buckets = groupBy_hr.getBuckets();
                for (TermsAggregation.Entry  bucket : buckets) {
                    String hr = bucket.getKey();
                    Long count = bucket.getCount();
                    hourMap.put(hr, count);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询异常");
        }
        return hourMap;
    }


}
