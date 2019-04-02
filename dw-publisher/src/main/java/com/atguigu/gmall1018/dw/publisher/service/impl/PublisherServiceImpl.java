package com.atguigu.gmall1018.dw.publisher.service.impl;

import com.atguigu.gmall1018.dw.common.constant.GmallConstant;
import com.atguigu.gmall1018.dw.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    JestClient jestClient;

    @Override
    public int getDauTotal(String date) {
        int total=0;
        String query="{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"logDate\": \""+date+"\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        //利用构造工具组合dsl
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);
        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult searchResult = jestClient.execute(search);
              total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }

    @Override
    public Map getDauHourCount(String date) {
        Map dauHourMap=new HashMap();


        //利用构造工具组合dsl
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤部分
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合部分
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        searchSourceBuilder.aggregation(termsBuilder);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_DEFAULT_TYPE).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            //遍历得到每个小时的累计值
            for (TermsAggregation.Entry bucket : buckets) {
                dauHourMap.put( bucket.getKey(),bucket.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dauHourMap;
    }
}
