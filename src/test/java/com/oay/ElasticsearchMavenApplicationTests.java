package com.oay;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.oay.constant.Const;
import com.oay.entity.User;
import com.oay.mapper.UserMapper;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SpringBootTest
class ElasticsearchMavenApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    @Autowired
    private UserMapper userMapper;

    @Test
    void contextLoads() throws IOException {

        SearchRequest searchRequest = new SearchRequest("db1");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("id", "111"));
        sourceBuilder.from(0);
        sourceBuilder.size(10);
        sourceBuilder.highlighter(new HighlightBuilder().field("email").preTags("<span class='key' style='color:red'>").postTags("</span>").fragmenter("100"));
        searchRequest.source(sourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        List<Map<String, Object>> list = new ArrayList<>();

        for (SearchHit hit : response.getHits().getHits()) {

            Map<String, HighlightField> fields = hit.getHighlightFields();

            HighlightField name = fields.get("email");

            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            if (name != null) {
                Text[] texts = name.fragments();
                StringBuilder n_text = new StringBuilder();
                for (Text text : texts) {
                    n_text.append(text);
                }
                sourceAsMap.put("email", n_text);
            }
            list.add(sourceAsMap);
        }

        list.forEach(System.out::println);

    }

}
