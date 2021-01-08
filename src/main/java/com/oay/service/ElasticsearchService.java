package com.oay.service;

import java.util.List;
import java.util.Map;

/*********************************************************
 * @Package: com.oay.service
 * @ClassName: ElasticsearchService.java
 * @Description： Elasticsearch服务类
 * -----------------------------------
 * @author：ouay
 * @Version：v1.0
 * @Date: 2021-01-06
 *********************************************************/
public interface ElasticsearchService {

    //  批量插入
    Boolean BulkInsert();

    //  分页查询
    List<Map<String, Object>> searchPage(String name, String keyword, int pageNo, int pageSize);

    //  删除索引
    Boolean deleteBulk();

    Boolean deleteBulk(String id);

}
