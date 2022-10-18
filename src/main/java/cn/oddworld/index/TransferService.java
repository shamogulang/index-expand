package cn.oddworld.index;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;

@Component
public class TransferService {

    Logger log = LoggerFactory.getLogger(TransferService.class);
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    public void doReindex(String indexName){
        long startTime = System.currentTimeMillis();
        long cnt = 0L;
        String   scrollId = null;
        log.info("starting transfer, indexName = {}", indexName);
        boolean scrollNext = true;
        do {
            try {
                final SearchResponse response = scrollAll(scrollId, indexName);
                final SearchHits searchHits = response.getHits();
                if (searchHits.getHits().length == 0) {
                    // 数据已经全部迁移完毕
                    scrollNext = false;
                    log.info("{} reindex success, docs cnt = {}, fly time = {}ms,transferring work done...", indexName, cnt, System.currentTimeMillis() -startTime);
                }else {

                    // todo reindex docs

                    final int length = searchHits.getHits().length;
                    cnt = cnt + length;
                    scrollId = response.getScrollId();
                }
            }catch (IOException e){
                log.info("scroll error, for details:", e);
            }
        }while (scrollNext);
    }

    public SearchResponse scrollAll(String scrollId, String indexName) throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(1000);
        SearchRequest searchRequest = new SearchRequest(new String[]{indexName}, searchSourceBuilder);
        if(StringUtils.isEmpty(scrollId)){
            searchRequest.scroll("5m");
            return restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        }else {
            SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
            searchScrollRequest.scroll("5m");
            return restHighLevelClient.scroll(searchScrollRequest, RequestOptions.DEFAULT);
        }
    }
}
