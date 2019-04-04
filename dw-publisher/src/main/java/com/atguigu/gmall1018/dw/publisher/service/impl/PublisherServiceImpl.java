package com.atguigu.gmall1018.dw.publisher.service.impl;

import com.atguigu.gmall1018.dw.common.constant.GmallConstant;
import com.atguigu.gmall1018.dw.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
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


    /***
     * 订单总金额
     * @param date
     * @return
     */
    @Override
    public Double getOrderAmount(String date) {
        Double orderAmount=0D;
        // 查询 1 过滤  2 聚合
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        searchSourceBuilder.aggregation(sumBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            orderAmount = searchResult.getAggregations().getSumAggregation("sum_totalamount").getSum();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return orderAmount;
    }

    /***
     * 订单分时金额
     * @param date
     * @return
     */
    @Override
    public Map getOrderAmountHour(String date) {
        Map orderAmountMap=new HashMap();
        // 查询 1 过滤  2 聚合
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        //根据小时进行分组
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_createHour").field("createHour").size(24);
        //按金额进行sum
        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalamount").field("totalAmount");
        //sum操作作为term(分组）的子聚合
        termsBuilder.subAggregation(sumBuilder);

        searchSourceBuilder.aggregation(termsBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_ORDER).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                String hour = bucket.getKey();//小时
                //每小时金额
                Double hourAmount = bucket.getSumAggregation("sum_totalamount").getSum();
                orderAmountMap.put(hour,hourAmount);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return orderAmountMap;
    }

    @Override
    public Map getSaleDetail(String date, String keyword, int startPage, int pageSize, String aggFieldName, int aggsSize) {
        Map saleMap=new HashMap();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //条件  ： 1过滤 2匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date)); //过滤
        //文字匹配
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));

        searchSourceBuilder.query(boolQueryBuilder);

        //聚合操作
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_" + aggFieldName).field(aggFieldName).size(aggsSize);
        searchSourceBuilder.aggregation(termsBuilder);

        //分页
        searchSourceBuilder.from((startPage-1)*pageSize);
        searchSourceBuilder.size(pageSize);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            //取出明细数据
            List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
            List<HashMap> detailList= new ArrayList<>();
            for (SearchResult.Hit<HashMap, Void> hit : hits) {
                HashMap detailMap = hit.source;
                detailList.add(detailMap);
            }
            saleMap.put("detail",detailList);
            // 取出聚合结果
            Map aggsMap=new HashMap();
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_" + aggFieldName).getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                aggsMap.put(bucket.getKey(),bucket.getCount());
            }
            saleMap.put("aggs",aggsMap);

            //获取总数
            saleMap.put("total",searchResult.getTotal()) ;


        } catch (IOException e) {
            e.printStackTrace();
        }
        return saleMap;
    }


}
