package com.elasticm2m.frameworks.aws.s3;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.ByteArrayInputStream;
import java.util.Map;

/**
 * Created by singram on 8/30/15.
 */
public class S3Writer extends ElasticBaseRichBolt {
    private AWSCredentialsProvider credentialsProvider;
    private String bucketName;
    private AmazonS3 s3Service;
    private String folderName;
    private String pathKeyProperty;
    private String[] paths;
    private String key2Property;
    private String[] key2Paths;
    private boolean logTuple = false;

    @Inject
    public void setBucketName(@Named("s3-bucket-name") String bucketName) {
        this.bucketName = bucketName;
    }

    @Inject(optional = true)
    public void setFolderName(@Named("s3-default-folder") String folderName) {
        this.folderName = folderName;
    }

    @Inject
    public void setPathKeyProperty(@Named("path-key-property") String pathKeyProperty) {
        this.pathKeyProperty = pathKeyProperty;
    }

    @Inject(optional = true)
    public void setkey2Property(@Named("path2-property") String key2Property) {
        this.key2Property = key2Property;
    }

    @Inject
    public void setLogTupple(@Named("log-tuple") Boolean logTuple) {
        this.logTuple = logTuple;
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(conf, topologyContext, collector);

        logger.info("S3 Writer: Bucket Name = " + bucketName);

        credentialsProvider = new DefaultAWSCredentialsProviderChain();

        s3Service = new AmazonS3Client(credentialsProvider);

        try {
            s3Service.getBucketLocation(bucketName);
        } catch (AmazonClientException e) {
            logger.error("Bucket [" + bucketName + " does not exist");
            throw new IllegalStateException("Bucket [" + bucketName + " does not exist");
        }

        // create Json paths for property keys
        paths = pathKeyProperty.split("\\.");
        if ( null != key2Property ) key2Paths = key2Property.split("\\.");
    }

    protected JsonNode pathFor(JsonNode node, String p[], int idex) {
        if (idex < p.length) {
            return pathFor(node.path(p[idex]), p, ++idex); // recurse
        }
        else {
            return node;
        }
    }

    protected JsonNode pathFor(JsonNode node, String p[]) {
        return pathFor(node, p, 0);
    }

    protected String createKey(JsonNode root) {
        StringBuilder builder = new StringBuilder();
        if ( null != folderName ) {
            builder.append(folderName);
            if ( !folderName.endsWith("/") ) builder.append("/");
        }
        String path = pathFor(root, paths).asText();
        if ( path.endsWith("/") ) path = path.substring(0, path.length() - 1);
        builder.append(path);
        if (null != key2Property)
        {
            path = pathFor(root, key2Paths).asText();
            builder.append("/").append( path );
        }
        return builder.toString();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String body = tuple.getString(1);
            byte[] data = body.getBytes();
            ObjectMetadata md = new ObjectMetadata();
            md.setContentLength( data.length );
            ObjectMapper mapper = new ObjectMapper();
            PutObjectRequest request = new PutObjectRequest(
                    bucketName,
                    createKey( mapper.readValue(body, JsonNode.class) ),
                    new ByteArrayInputStream(data),
                    md);
            s3Service.putObject(request);

            if (logTuple) {
                logger.info(body);
            } else {
                logger.debug("Published record to S3");
            }

            collector.ack(tuple);
        } catch (Throwable ex) {
            logger.error("Error writing the entity to S3:", ex);
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
