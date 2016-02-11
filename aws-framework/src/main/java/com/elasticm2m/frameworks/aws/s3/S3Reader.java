package com.elasticm2m.frameworks.aws.s3;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichSpout;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * Created by singram on 9/8/15.
 */
public class S3Reader extends ElasticBaseRichSpout {

    private AWSCredentialsProvider credentialsProvider;
    private String bucketName;
    private ScheduledExecutorService executorService;
    private AmazonS3 s3Service;
    private boolean isGzip;
    private boolean isJson;
    private boolean isContinuous;
    private String fromDate;
    private String toDate;
    private Integer stableSeconds;
    private DateTime fromDateTime, toDateTime;
    private int objectIndex;
    private ObjectListing metadata;
    private List<S3ObjectSummary> s3Objects;
    private LinkedBlockingDeque<JsonNode> jsonQueue;
    private LinkedBlockingDeque<byte[]> byteQueue;
    private ObjectMapper objectMapper;

    @Inject
    public void setBucketName(@Named("s3-bucket-name") String bucketName) {
        this.bucketName = bucketName;
    }

    @Inject
    public void setFromDate(@Named("s3-from-date") String fromDate) {
        this.fromDate = fromDate;
    }

    @Inject(optional = true)
    public void setToDate(@Named("s3-to-date") String toDate) {
        this.toDate = toDate;
    }

    @Inject
    public void setIsContinuous(@Named("is-continuous") boolean isContinuous) {
        this.isContinuous = isContinuous;
    }

    @Inject
    public void setIsJson(@Named("is-json") boolean isJson) {
        this.isJson = isJson;
    }

    @Inject
    public void setStableSeconds(@Named("stable-seconds") Integer stableSeconds) {
        this.stableSeconds = stableSeconds;
    }

    @Inject(optional = true)
    public void setIsGzip(@Named("is-gzip") boolean isGzip) {
        this.isGzip = isGzip;
    }

    private boolean isWithinTimeBox(S3ObjectSummary s) {
        DateTime date = new DateTime( s.getLastModified().getTime() );
        if ( !isContinuous && null != toDateTime ) {
            return !date.isBefore(fromDateTime) && date.isBefore(toDateTime);
        } else {
            return !date.isBefore(fromDateTime);
        }
    }

    private List<S3ObjectSummary> filterListing(ObjectListing ol) {
         return ol.getObjectSummaries().stream()
                .filter(this::isWithinTimeBox)
                .sorted(comparing(S3ObjectSummary::getLastModified))
                .collect(toList());
    }

    private void getNextListing() {
        try {
            if (null != metadata && metadata.isTruncated()) {
                metadata = s3Service.listNextBatchOfObjects(metadata);
                s3Objects = filterListing(metadata);
                objectIndex = 0;
            } else {
                // start over
                logger.info("Starting bucket scan");
                metadata = s3Service.listObjects(bucketName);
                s3Objects = filterListing(metadata);
                objectIndex = 0;
            }
        } catch (Throwable t) {
            metadata = null;
            logger.error("Error in getNextListing", t);
        }
    }

    private JsonNode readJsonData(InputStream is) throws IOException {
        return objectMapper.readValue(is, JsonNode.class);
    }

    private byte[] readData(InputStream is, long len) throws IOException {
        byte[] data = new byte[ (int)len ];
        ByteStreams.readFully(is, data);
        return data;
    }

    private void getNextObject() {
        if ( objectIndex >= s3Objects.size() ) getNextListing();

        if (objectIndex < s3Objects.size() ) {
            S3ObjectSummary summary = s3Objects.get(objectIndex++);
            if ( DateTime.now().minusSeconds(stableSeconds).isBefore( new DateTime(summary.getLastModified().getTime()) ) ) {
                logger.info("Waiting {} seconds for S3 Object to stabilize", stableSeconds);
                Utils.sleep(stableSeconds * 1000);
            }
            logger.info("Processing S3 object {}", summary.getKey());
            DateTime lastModified = new DateTime(summary.getLastModified().getTime(), DateTimeZone.UTC);
            if (lastModified.isAfter(fromDateTime)) fromDateTime = lastModified;
            S3Object s3Object = s3Service.getObject(new GetObjectRequest(bucketName, summary.getKey()));
            int sz = isJson ? jsonQueue.size() : byteQueue.size();
            try (InputStream is = s3Object.getObjectContent()) {
                if (isJson) {
                    JsonNode data = readJsonData(isGzip ? new GZIPInputStream(is) : is);
                    if (data.isArray()) {
                        data.forEach(jsonQueue::offer);
                        logger.info("Found {} records in {}, new queue size is {}", jsonQueue.size() - sz, summary.getKey(), jsonQueue.size());
                    } else {
                        jsonQueue.offer(data);
                        logger.info("Found {} records in {}, new queue size is {}", jsonQueue.size() - sz, summary.getKey(), jsonQueue.size());
                    }
                } else {
                    byteQueue.offer(readData(isGzip ? new GZIPInputStream(is) : is, s3Object.getObjectMetadata().getContentLength()));
                    logger.info("Found {} records in {}, new queue size is {}", byteQueue.size() - sz, summary.getKey(), byteQueue.size());
                }
            } catch (IOException e) {
                logger.error("Error in getNextObject()", e);
            }
        }
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

    protected Runnable getTask() {
        return () -> {
            try {
                if (isJson) {
                    if (jsonQueue.size() <= 100) getNextObject();
                } else {
                    if (byteQueue.size() <= 100) getNextObject();
                }
            } catch (Throwable t) {
                logger.error("Error in load task", t);
            }
        };
    }

    @Override
    public void open(Map stormConf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        super.open(stormConf, topologyContext, collector);

        if (isJson) {
            jsonQueue = new LinkedBlockingDeque<>(10000);
            objectMapper = new ObjectMapper();
        }
        else
            byteQueue = new LinkedBlockingDeque<>(5000);
        credentialsProvider = new DefaultAWSCredentialsProviderChain();
        s3Service = new AmazonS3Client(credentialsProvider);
        try {
            s3Service.getBucketLocation(bucketName);
        } catch (AmazonClientException e) {
            logger.error("Bucket [" + bucketName + " does not exist");
            throw new IllegalStateException("Bucket [" + bucketName + " does not exist");
        }
        if (!isContinuous)
            logger.info("Opening S3 Reader from bucket [{}], from date={} and toDate={}", bucketName, fromDate, toDate);
        else
            logger.info("Opening S3 Reader from bucket [{}], from date={}", bucketName, fromDate);
        fromDateTime = ISODateTimeFormat.dateTimeNoMillis().withZoneUTC().parseDateTime(fromDate);
        if (!isContinuous) toDateTime = ISODateTimeFormat.dateTimeNoMillis().withZoneUTC().parseDateTime(toDate);
        getNextListing();
        getNextObject();
        executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate( getTask(), 0, 100, TimeUnit.MILLISECONDS );
    }

    public List<Object> getJsonTuple(JsonNode node) {
        List<Object> values = new Values();

        String id = pathFor(node, new String[] { "id" }).asText();
        if ( null == id ) id = pathFor(node, new String[] {"properties", "id"}).asText();
        try {
            String jsonString = objectMapper.writeValueAsString(node);
            values.add( id );
            values.add( jsonString );
            values.add( new HashMap<>() );

            return values;
        } catch (JsonProcessingException e) {
            logger.error("Error converting JsonNode to Tuple", e);
        }
        return null;
    }

    public List<Object> getBinaryTuple(byte[] data) {
        List<Object> values = new Values();

        values.add( 1 );
        values.add( data );
        values.add( new HashMap<>() );

        return values;
    }

    @Override
    public void nextTuple() {
        if (isJson) {
            JsonNode node = jsonQueue.poll();
            if ( null == node ) {
                Utils.sleep(50);
            } else {
                List<Object> tuple = getJsonTuple(node);
                if (null != tuple ) collector.emit(tuple);
            }
        } else {
            byte[] data = byteQueue.poll();
            if ( null == data ) {
                Utils.sleep(50);
            } else {
                List<Object> tuple = getBinaryTuple(data);
                if (null != tuple ) collector.emit(tuple);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        executorService.shutdown();
        try {
            executorService.awaitTermination(2, TimeUnit.MINUTES);
        } catch (InterruptedException e) {

        } finally {
            if ( !executorService.isTerminated() ) executorService.shutdownNow();
        }
    }
}
