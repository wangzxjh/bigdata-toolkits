package com.huawei.bigdata.verify.css;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.huawei.bigdata.verify.css.EsVerifyConf.EsConf;
import com.huawei.bigdata.verify.css.EsVerifyConf.VerifyConf;
import com.huawei.bigdata.verify.mysql.MysqlVerify;
import com.huawei.bigdata.verify.mysql.MysqlVerifyConf;
import java.io.IOException;
import java.util.Collections;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticVerify {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticVerify.class);

  public static void main(String[] args) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    EsVerifyConf conf = null;
    try {
      conf = mapper
          .readValue(EsVerifyConf.class.getResourceAsStream("/EsVerify.yml"),
              EsVerifyConf.class);
      LOG.info("Elastic verify conf:\n {}", mapper.writeValueAsString(conf));
    } catch (IOException e) {
      LOG.error("read Elastic conf failed", e);
      return;
    }

    for (VerifyConf verifyConf : conf.getConfs()) {
      RestClient sourceClient = getEsClient(verifyConf.getSource());
      RestClient targetClient = getEsClient(verifyConf.getTarget());
      Request request = new Request("GET", "_cat/indices?v&s=docs.count");
      Response response = sourceClient.performRequest(request);
      LOG.info("respone,{}", response);
    }
  }

  private static RestClient getEsClient(EsConf source) {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials("USER NAME", "PASSWORD"));
    // 单击所创建的Elasticsearch实例ID，在基本信息页面获取公网地址，即为HOST。
    RestClient restClient = RestClient.builder(new HttpHost("HOST", 9200))
        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
          @Override
          public HttpAsyncClientBuilder customizeHttpClient(
              HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
          }
        }).build();
    return restClient;
  }
}
