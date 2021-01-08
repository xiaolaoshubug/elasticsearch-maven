package com.oay.controller;

import com.oay.service.ElasticsearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/*********************************************************
 * @Package: com.oay.controller
 * @ClassName: UserController.java
 * @Description：描述
 * -----------------------------------
 * @author：ouay
 * @Version：v1.0
 * @Date: 2021-01-07
 *********************************************************/
@RestController
public class UserController {

    @Autowired
    private ElasticsearchService elasticsearchService;

    @GetMapping("/index")
    public boolean createIndex() {
        return elasticsearchService.BulkInsert();
    }


    @GetMapping("/search/{name}/{keyword}/{pageNo}/{pageSize}")
    public List<Map<String, Object>> searchPage(
            @PathVariable("name") String name,
            @PathVariable("keyword") String keyword,
            @PathVariable("pageNo") int pageNo,
            @PathVariable("pageSize") int pageSize) {
        if (pageNo <= 0) {
            pageNo = 0;
        }
        if (pageSize <= pageNo) {
            pageSize = 10;
        }
        return elasticsearchService.searchPage(name, keyword, pageNo, pageSize);
    }

    @GetMapping("/deleteBulk")
    public Boolean deleteBulk() {
        return elasticsearchService.deleteBulk();
    }

    @GetMapping("/deleteBulk/{id}")
    public Boolean deleteBulk(@PathVariable("id") String id) {
        return elasticsearchService.deleteBulk(id);
    }

}
