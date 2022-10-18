package cn.oddworld.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.elasticsearch.rest.RestClientBuilderCustomizer;
import org.springframework.boot.autoconfigure.elasticsearch.rest.RestClientProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;


@Configuration
public class ElasticSearchConfiguration {

    @Value("${elasticsearch.http.maxConnect:2000}")
    private int maxConnTotal;

    @Value("${elasticsearch.http.maxConnerPerRoute:2000}")
    private int maxConnerPerRoute;

    @Value("${elasticsearch.http.keepAliveTime:60}")
    private long keepAliveTime;

    @Value("${group.elasticsearch.http.connectTimeout:60}")
    private long connectTimeout;

    @Value("${group.elasticsearch.http.socketTimeout:60}")
    private long socketTimeout;

    @Bean
    public RestHighLevelClient restClientBuilder(RestClientProperties properties,
                                                 ObjectProvider<RestClientBuilderCustomizer> builderCustomizers){
        HttpHost[] hosts = properties.getUris().stream().map(HttpHost::create).toArray(HttpHost[]::new);
        RestClientBuilder builder = RestClient.builder(hosts);
        PropertyMapper map = PropertyMapper.get();
        map.from(properties::getUsername).whenHasText().to((username) -> {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            Credentials credentials = new UsernamePasswordCredentials(properties.getUsername(),
                    properties.getPassword());
            credentialsProvider.setCredentials(AuthScope.ANY, credentials);
            builder.setHttpClientConfigCallback(
                    (httpClientBuilder) -> {
                        httpClientBuilder.setMaxConnTotal(maxConnTotal);
                        httpClientBuilder.setMaxConnPerRoute(maxConnerPerRoute);
                        httpClientBuilder.setKeepAliveStrategy(((httpResponse, httpContext) -> Duration.ofSeconds(keepAliveTime).toMillis()));
                        httpClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom()
                                .setSoKeepAlive(true)
                                .build());
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    });
        });
        builder.setRequestConfigCallback((requestConfigBuilder) -> {
            map.from(Duration.ofSeconds(connectTimeout)).whenNonNull().asInt(Duration::toMillis)
                    .to(requestConfigBuilder::setConnectTimeout);
            map.from(Duration.ofSeconds(socketTimeout)).whenNonNull().asInt(Duration::toMillis)
                    .to(requestConfigBuilder::setSocketTimeout);
            return requestConfigBuilder;
        });
        builderCustomizers.orderedStream().forEach((customizer) -> customizer.customize(builder));
        return new RestHighLevelClient(builder);
    }

}
