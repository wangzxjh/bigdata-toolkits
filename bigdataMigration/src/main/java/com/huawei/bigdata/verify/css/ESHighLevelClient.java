package com.huawei.bigdata.verify.css;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESHighLevelClient {

  private Logger logger = LoggerFactory.getLogger(ESHighLevelClient.class);
  private String host;
  private String username;
  private String password;

  private int port;
  private int connectTimeout;
  private int connectionRequestTimeout;
  private int socketTimeout;
  private boolean certification;
  private int retryCnt;
  private RestHighLevelClient client;

  public ESHighLevelClient() {
    this.port = 9200;
    this.connectTimeout = 30000;
    this.connectionRequestTimeout = 30000;
    this.socketTimeout = 30000;
    this.certification = false;
    this.retryCnt = 3;
  }

  public static ESHighLevelClient create() {
    return new ESHighLevelClient();
  }

  public ESHighLevelClient host(String host) {
    this.host = host;
    return this;
  }

  public ESHighLevelClient port(int port) {
    this.port = port;
    return this;
  }

  public ESHighLevelClient username(String username) {
    this.username = username;
    this.certification = true;
    return this;
  }

  public ESHighLevelClient password(String password) {
    this.password = password;
    this.certification = true;
    return this;
  }

  public ESHighLevelClient connectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  public ESHighLevelClient connectionRequestTimeout(int connectionRequestTimeout) {
    this.connectionRequestTimeout = connectionRequestTimeout;
    return this;
  }

  public ESHighLevelClient socketTimeout(int socketTimeout) {
    this.socketTimeout = socketTimeout;
    return this;
  }

  //create high client
  public ESHighLevelClient build() throws IOException {
    RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"))
        .setRequestConfigCallback(
            config -> config.setConnectTimeout(connectTimeout)
                .setConnectionRequestTimeout(connectionRequestTimeout)
                .setSocketTimeout(socketTimeout))
        .setHttpClientConfigCallback(
            httpClientBuilder -> {
              final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
              if (this.certification) {
                credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
              httpClientBuilder.setMaxConnTotal(100);
              httpClientBuilder.setMaxConnPerRoute(50);
              List<Header> headers = new ArrayList<>(2);
              headers.add(new BasicHeader("Connection", "keep-alive"));
              headers.add(new BasicHeader("Keep-Alive", "720"));
              httpClientBuilder.setDefaultHeaders(headers);
              httpClientBuilder.setKeepAliveStrategy(CustomConnectionKeepAliveStrategy.INSTANCE);
              return httpClientBuilder;
            }
        );
    this.client = new RestHighLevelClient(builder);
    logger.info("es rest client build success {} ", client);

    ClusterHealthRequest request = new ClusterHealthRequest();
    ClusterHealthResponse response = this.client.cluster().health(request, RequestOptions.DEFAULT);
    logger.info("es rest client health response {} ", response);
    return this;
  }

  public CountResponse count() throws Exception {
    CountRequest countRequest = new CountRequest();
    return this.client.count(countRequest, RequestOptions.DEFAULT);
  }


  public void createIndex(String index, Map<String, String> config) throws Exception {
    try {
      CreateIndexRequest request = new CreateIndexRequest(index);
      request.settings(Settings.builder()
          .put("index.number_of_shards", 1)
          .put("index.number_of_replicas", 1));

      Map<String, Object> properties = new HashMap<>();
      for (Map.Entry<String, String> entry : config.entrySet()) {
        String propertyName = entry.getKey();
        String type = entry.getValue();
        Map<String, Object> property = new HashMap<>();
        property.put("type", type);
        properties.put(propertyName, property);
      }
      Map<String, Object> mapping = new HashMap<>();
      mapping.put("properties", properties);
      //request.mapping("_doc", mapping);
      logger.info("create index success，mapping:  " + mapping);
      // 同步方式发送请求
      CreateIndexResponse createIndexResponse = this.client.indices()
          .create(request, RequestOptions.DEFAULT);

      boolean acknowledged = createIndexResponse.isAcknowledged();
      boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
      logger.info("create index success，acknowledged = " + acknowledged);
      logger.info("create index success，shardsAcknowledged = " + shardsAcknowledged);
      logger.info("create index success, index name {}.", index);
    } catch (IOException e) {
      String message = String
          .format("es high level client (%s) create index failed, message %s.", index,
              e.getMessage());
      logger.error(message, e);
      throw new Exception("create index failed, message" + message);
    }
  }

  public SearchResponse queryCondition(String indexName, Map<String, String> queryTerms,
      String property, long startTime, long stopTime, int from, int size) throws Exception {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
        .must(QueryBuilders.rangeQuery(property).gte(startTime).lte(stopTime));
    if (queryTerms != null) {
      for (Map.Entry<String, String> entry : queryTerms.entrySet()) {
        String name = entry.getKey();
        String value = entry.getValue();
        boolQueryBuilder.must(QueryBuilders.termQuery(name, value));
      }
    }
    searchSourceBuilder.query(boolQueryBuilder)
        .from(from)
        .size(size)
        .timeout(new TimeValue(60, TimeUnit.SECONDS))
        .sort(new ScoreSortBuilder().order(SortOrder.DESC));

    SearchRequest searchRequest = new SearchRequest(indexName);
    searchRequest.source(searchSourceBuilder);
    try {
      SearchResponse searchResponse = this.client.search(searchRequest, RequestOptions.DEFAULT);
      logger.info("response query condition {} ", searchRequest);
      return searchResponse;
    } catch (Exception e) {
      logger.error("search doc failed", e);
      throw new Exception("search document failed, message" + e.getMessage());
    }
  }

  public static class CustomConnectionKeepAliveStrategy extends DefaultConnectionKeepAliveStrategy {

    public static final CustomConnectionKeepAliveStrategy INSTANCE = new CustomConnectionKeepAliveStrategy();

    private CustomConnectionKeepAliveStrategy() {
      super();
    }

    /**
     * 最大keep alive的时间（分钟） 这里默认为10分钟，可以根据实际情况设置。可以观察客户端机器状态为TIME_WAIT的TCP连接数，如果太多，可以增大此值。
     */
    private final long MAX_KEEP_ALIVE_MINUTES = 10;

    @Override
    public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
      long keepAliveDuration = super.getKeepAliveDuration(response, context);
      // <0 为无限期keepalive
      // 将无限期替换成一个默认的时间
      if (keepAliveDuration < 0) {
        return TimeUnit.MINUTES.toMillis(MAX_KEEP_ALIVE_MINUTES);
      }
      return keepAliveDuration;
    }
  }
}
