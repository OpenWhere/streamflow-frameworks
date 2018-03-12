package com.elasticm2m.frameworks.aws.lambda;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClient;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

public class LambdaWriter extends ElasticBaseRichBolt {

    private AWSLambda lambda;
    private String functionName;
    private AWSCredentialsProvider credentialsProvider;

    @Override
    public void execute(Tuple tuple) {
        try {
            Object body = tuple.getValue(1);
            InvokeRequest request = new InvokeRequest();
            request.setFunctionName(functionName);
            request.setPayload(toString(body));

            InvokeResult result = lambda.invoke(request);
            collector.ack(tuple);
        } catch (Throwable e) {
            logger.error("Unable to process tuple", e);
            collector.fail(tuple);
        }
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(conf, topologyContext, collector);
        logger.info("Lambda Writer: Lambda Function Name = " + functionName);
        credentialsProvider = new DefaultAWSCredentialsProviderChain();
        if (credentialsProvider == null) {
            lambda = new AWSLambdaClient();
        } else {
            lambda = new AWSLambdaClient(credentialsProvider);
        }
    }

    @Inject
    public void setFunctionName(@Named("lambda-function-name") String functionName) {
        this.functionName = functionName;
    }

    String toString(Object body) {
        String result = null;
        if (body instanceof String) {
            result = (String) body;
        } else if (body instanceof byte[]) {
            result = new String((byte[]) body, Charset.forName("UTF-8"));
        } else if (body instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) body;
            byte[] bytes;
            if (buffer.hasArray()) {
                bytes = buffer.array();
            } else {
                bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
            }
            return new String(bytes, Charset.defaultCharset());
        } else {
            throw new RuntimeException("Unsupported body object");
        }
        return result;
    }

}