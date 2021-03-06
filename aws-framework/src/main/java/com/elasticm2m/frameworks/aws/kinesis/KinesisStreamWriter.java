package com.elasticm2m.frameworks.aws.kinesis;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.nio.ByteBuffer;
import java.util.Map;

public class KinesisStreamWriter extends ElasticBaseRichBolt {

    private AWSCredentialsProvider credentialsProvider;
    private String streamName;
    private String partitionKey;
    private AmazonKinesis kinesis;
    private boolean logTuple = false;
    private String regionName;

    @Inject
    public void setStreamName(@Named("kinesis-stream-name") String streamName) {
        this.streamName = streamName;
    }

    @Inject(optional = true)
    public void setRegionName(@Named("kinesis-region-name") String regionName) { this.regionName = regionName; }

    @Inject
    public void setPartitionKey(@Named("kinesis-partition-key") String partitionKey) {
        this.partitionKey = partitionKey;
    }

    @Inject
    public void setLogTupple(@Named("log-tuple") boolean logTuple) {
        this.logTuple = logTuple;
    }

    /*
    @Inject(optional = true)
    public void setCredentialsProvider(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }
    */

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(conf, topologyContext, collector);

        logger.info("Kinesis Writer: Stream Name = " + streamName
                + ", Partition Key = " + partitionKey
                + ", Region Name = " + regionName);

        credentialsProvider = new DefaultAWSCredentialsProviderChain();

        if (credentialsProvider == null) {
            kinesis = new AmazonKinesisAsyncClient();
            kinesis.setRegion(Region.getRegion(Regions.fromName(regionName)));
        } else {
            kinesis = new AmazonKinesisClient(credentialsProvider);
            kinesis.setRegion(Region.getRegion(Regions.fromName(regionName)));
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String body = tuple.getString(1);
            PutRecordRequest request = new PutRecordRequest()
                    .withStreamName(streamName)
                    .withPartitionKey(partitionKey)
                    .withData(ByteBuffer.wrap(body.getBytes()));
            kinesis.putRecord(request);

            if (logTuple) {
                logger.info(body);
            } else {
                logger.debug("Published record to kinesis");
            }

            collector.ack(tuple);
        } catch (Throwable ex) {
            logger.error("Error writing the entity to Kinesis:", ex);
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
