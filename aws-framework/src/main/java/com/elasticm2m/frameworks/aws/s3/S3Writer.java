package com.elasticm2m.frameworks.aws.s3;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayInputStream;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by singram on 8/30/15.
 */
public class S3Writer extends ElasticBaseRichBolt {
    private AWSCredentialsProvider credentialsProvider;
    private String bucketName;
    private AmazonS3 s3Service;
    private String folderName;
    protected LinkedBlockingQueue<Tuple> queue;
    private ExecutorService executorService;
    protected Integer batchSize = 500;
    protected Integer minBatchSize = 25;
    protected Integer sleepTime = 5;
    protected ObjectMapper objectMapper;
    private NumberFormat format2, format3, format4;

    @Inject
    public void setBucketName(@Named("s3-bucket-name") String bucketName) {
        this.bucketName = bucketName;
    }

    @Inject(optional = true)
    public void setFolderName(@Named("s3-default-folder") String folderName) {
        this.folderName = folderName;
    }

    @Inject
    public void setBatchSize(@Named("bolt_s3_batch") Integer b) { this.batchSize = b; }

    @Inject
    public void setMinBatchSize(@Named("bolt_s3_min_batch") Integer b) { this.minBatchSize = b; }

    @Inject
    public void setSleepTime(@Named("bolt_s3_sleep") Integer b) { this.sleepTime = b; }

    protected static NumberFormat makeFormat(int digits) {
        NumberFormat retval = NumberFormat.getIntegerInstance();
        retval.setMinimumIntegerDigits( digits );
        retval.setGroupingUsed(false);
        return retval;
    }

    /**
     * Create a name with format: year_dayOfYear_hourOfDay_minuteOfHour_secondOfMinute_millisOfSecond
     *
     * @param dt DateTime to use
     * @return String
     */
    public String createFileKey(DateTime dt) {
        StringBuilder builder = new StringBuilder();
        builder.append(folderName).append("_")
                .append(format4.format(dt.getYear())).append("_")
                .append(format2.format(dt.getDayOfYear())).append("_")
                .append(format2.format(dt.getHourOfDay())).append("_")
                .append(format2.format(dt.getMinuteOfHour())).append("_")
                .append(format2.format(dt.getSecondOfMinute())).append("_")
                .append(format3.format(dt.getMillisOfSecond()));
        return builder.toString();
    }

    protected Runnable getTask() {
        return () -> {
            logger.info("Checking for S3 Records");

            while(!Thread.currentThread().isInterrupted()) {
                try {
                    if (queue.size() >= minBatchSize) {
                        logger.info("Writing Records to S3");
                        Tuple tuple = queue.poll();
                        List<Tuple> tupleList = new ArrayList<>();
                        while (tuple != null && tupleList.size() < batchSize) {
                            tupleList.add(tuple);
                            tuple = queue.poll();
                        }

                        logger.info("Batch Size: " + tupleList.size() + " Queue Size: " + queue.size());
                        // Make tuples into a Json array
                        StringJoiner sj = new StringJoiner(", ", "[", "]");
                        tupleList.forEach(t -> sj.add(t.getValue(1).toString()));
                        String body = sj.toString();
                        byte[] data = body.getBytes();
                        ObjectMetadata md = new ObjectMetadata();
                        md.setContentLength( data.length );
                        PutObjectRequest request = new PutObjectRequest(
                                bucketName,
                                createFileKey( DateTime.now(DateTimeZone.UTC) ),
                                new ByteArrayInputStream(data),
                                md).withCannedAcl(CannedAccessControlList.BucketOwnerFullControl);
                        s3Service.putObject(request);
                        tupleList.forEach(collector::ack);
                    } else {
                        Utils.sleep(sleepTime * 1000);
                    }
               } catch (Exception e) {
                    logger.error("Unexpected exception ", e);
                }
            }
            logger.warn("Writing thread is exiting");
        };
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

        queue = new LinkedBlockingQueue<>(10000);

        objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.INDENT_OUTPUT, false);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        format2 = makeFormat(2);
        format3 = makeFormat(3);
        format4 = makeFormat(4);

        executorService = Executors.newSingleThreadExecutor();
        executorService.submit( getTask() );
    }

    @Override
    public void execute(Tuple tuple)  {
        queue.offer(tuple);
    }

    @Override
    public void cleanup() {
        logger.info("S3 Bulk Writer Stopped");
        try {
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            logger.error("Shutdown interrupted");
        }
        finally {
            if (!executorService.isTerminated()) {
                logger.info("cancel non-finished tasks");
            }
            executorService.shutdownNow();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
