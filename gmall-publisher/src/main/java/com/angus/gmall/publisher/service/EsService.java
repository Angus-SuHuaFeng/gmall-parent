package com.angus.gmall.publisher.service;

import com.angus.gmall.publisher.common.ESServiceInterface;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
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
        return null;
    }


}
