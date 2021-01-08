package com.oay.service;

import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.oay.constant.Const;
import com.oay.entity.User;
import com.oay.mapper.UserMapper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/*********************************************************
 * @Package: com.oay.service
 * @ClassName: ElasticsearchServiceImpl.java
 * @Description： Elasticsearch实现类
 * -----------------------------------
 * @author：ouay
 * @Version：v1.0
 * @Date: 2021-01-06
 *********************************************************/
@Service
public class ElasticsearchServiceImpl implements ElasticsearchService {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Autowired
    private UserMapper userMapper;

    @Override
    public Boolean BulkInsert() {

        int pageInfo = 1;
        int pageMax = 100000;
        boolean flag = false;
        int Retry = 0;

        QueryWrapper<User> wrapper = new QueryWrapper<>();
        wrapper.ge("`delete`", "0");

        Integer count = userMapper.selectCount(wrapper);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("2m");

        while (count / pageMax >= pageInfo) {
            if (Retry == 3) {
                return false;
            }
            Page<User> page = new Page<>(pageInfo, pageMax);
            IPage<User> userIPage = userMapper.selectPage(page, wrapper);

            for (long i = 0; i < userIPage.getSize(); i++) {
                bulkRequest.add(
                        new IndexRequest(Const.ELASTICSEARCH_INDEX)
                                .id("" + userIPage.getRecords().get((int) i).getId())
                                .source(JSON.toJSONString(userIPage.getRecords().get((int) i)), XContentType.JSON)
                );
            }
            try {
                BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                pageInfo = pageInfo + 1;
                flag = !bulk.hasFailures();
            } catch (Exception e) {
                Retry = Retry + 1;
                e.printStackTrace();
            }
        }
        return flag;
    }

    @Override
    public Boolean deleteBulk() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("`delete`", "1");
        List<User> users = userMapper.selectByMap(map);
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout(new TimeValue(5, TimeUnit.SECONDS));
        for (User user : users) {
            bulkRequest.add(new DeleteRequest(Const.ELASTICSEARCH_INDEX, "" + user.getId()));
        }
        try {
            BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            //  false代表成功，成功返回true
            return !bulk.hasFailures();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Boolean deleteBulk(String id) {
        DeleteRequest deleteRequest = new DeleteRequest(Const.ELASTICSEARCH_INDEX, id);
        deleteRequest.timeout(new TimeValue(1, TimeUnit.SECONDS));
        try {
            DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
            if ("OK".equals(String.valueOf(response.status()))) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public List<Map<String, Object>> searchPage(String name, String keyword, int pageNo, int pageSize) {
        //  索引
        SearchRequest searchRequest = new SearchRequest(Const.ELASTICSEARCH_INDEX);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //  匹配最细粒度搜索，可以不用设置 ==analyzer==
        sourceBuilder.query(QueryBuilders.matchQuery(name, keyword).analyzer("ik_max_word"));
        //  分页起始页
        sourceBuilder.from(pageNo);
        //  每页的条数
        sourceBuilder.size(pageSize);
        //  高亮
        sourceBuilder.highlighter(
                new HighlightBuilder()
                        //  高亮字段
                        .field(name)
                        //  高亮前置标签
                        .preTags("<span class='key' style='color:red'>")
                        //  高亮后置标签
                        .postTags("</span>")
                        //  片段大小，默认为100,可以不用设置
                        .fragmenter("100")
                        //  是否多处高亮
                        .requireFieldMatch(false)
        );
        searchRequest.source(sourceBuilder);
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            //  是否存在命中
            for (SearchHit hit : response.getHits().getHits()) {
                //  高亮字段
                Map<String, HighlightField> fields = hit.getHighlightFields();
                //  获取高亮字段
                HighlightField field = fields.get(name);
                //  响应信息
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                //  高亮字段是否为空
                if (field != null) {
                    Text[] texts = field.fragments();
                    StringBuilder n_text = new StringBuilder();
                    for (Text text : texts) {
                        //  拼接高亮字段
                        n_text.append(text);
                    }
                    //  替换高亮字段
                    sourceAsMap.put(name, n_text);
                }
                list.add(sourceAsMap);
            }
            return list;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }
}
