package com.elasticm2m.frameworks.http;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HttpTransformBolt extends ElasticBaseRichBolt {

    private String endpoint;
    CloseableHttpClient httpclient;
    private String authorizationHeader;
    private ContentType contentType = ContentType.APPLICATION_JSON;

    @Inject
    public void setEndpoint(@Named("endpoint") String endpoint) {
        this.endpoint = endpoint;
    }

    @Inject
    public void setAuthorizationHeader(@Named("authorization-header") String authorizationHeader) {
        this.authorizationHeader = authorizationHeader;
    }

    @Inject
    public void setContentType(@Named("content-type") String contentType) {
        this.contentType = ContentType.create(contentType);
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        try {
            super.prepare(conf, topologyContext, collector);
            HttpClientBuilder builder = HttpClients.custom();
            builder.setRetryHandler(new DefaultHttpRequestRetryHandler(2, false));
            httpclient = builder.build();
        } catch (Throwable e) {
            logger.error("Unable to prepare service", e);
            throw e;
        }
    }


    @Override
    public void execute(Tuple tuple) {
        try {
            Object body = tuple.getValue(1);


            String content = getContent(body);
            logger.debug("Calling HTTP");
            if(content != null){
                List<Object> values = new Values();
                values.add(tuple.getValue(0));
                values.add(content);
                values.add(tuple.getValue(2));
                collector.emit(tuple, values);
            }
            else{
                logger.warn("no content, skipping");
            }

        } catch (Throwable e) {
            logger.error("Unable to process tuple", e);
            logger.error("Bad Tuple: {}", tuple.getValue(1).toString());
           // collector.fail(tuple);
        }
        finally{
            collector.ack(tuple);
        }
    }

    public String getContent(Object body) throws IOException {

        HttpPost httpPost = new HttpPost(endpoint);
        httpPost.setEntity(toEntity(body));
        if (StringUtils.isNotBlank(authorizationHeader)) {
            httpPost.addHeader("Authorization", authorizationHeader);
        }
        CloseableHttpResponse response = httpclient.execute(httpPost);
        if(response.getStatusLine().getStatusCode() != HttpStatus.SC_OK){
            logger.warn("Non-zero status {}, for body {}", response.getStatusLine().getStatusCode(), body.toString());
            response.close();
            return null;
        }

        HttpEntity responseEntity = response.getEntity();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(responseEntity.getContent(), out);

        String output =  new String(out.toByteArray());
        response.close();
        return output;
    }

    HttpEntity toEntity(Object body) {
        HttpEntity result = null;
        if (body instanceof String) {
            result = new ByteArrayEntity(((String) body).getBytes(), contentType);
        } else if (body instanceof byte[]) {
            result = new ByteArrayEntity((byte[]) body, contentType);
        } else {
            throw new RuntimeException("Unsupported body object");
        }
        return result;
    }

}
