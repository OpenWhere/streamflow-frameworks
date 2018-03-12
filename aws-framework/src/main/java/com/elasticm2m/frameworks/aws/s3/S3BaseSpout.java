package com.elasticm2m.frameworks.aws.s3;

import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichSpout;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * Created by singram on 10/27/17
 * Abstract base class encapsulating S3 monitoring
 */
public abstract class S3BaseSpout extends ElasticBaseRichSpout {
    private String bucketName;
    private String roleArn;
    private ScheduledExecutorService executorService;
    private AmazonS3 s3Service;
    private boolean isContinuous;
    private String fromDate;
    private String toDate;
    private Integer stableSeconds;
    private DateTime fromDateTime, toDateTime;
    private int objectIndex;
    private ObjectListing metadata;
    private List<S3ObjectSummary> s3Objects;

    protected boolean isGzip;

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

    @Inject(optional = true)
    public void setRoleArn(@Named("s3-role-arn") String roleArn) {
        this.roleArn = roleArn;
    }

    @Inject
    public void setIsContinuous(@Named("is-continuous") boolean isContinuous) {
        this.isContinuous = isContinuous;
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
            return date.isAfter(fromDateTime);
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
                metadata = s3Service.listObjects(bucketName);
                s3Objects = filterListing(metadata);
                if (s3Objects.size() > 0){
                    logger.info("Starting Bucket scan and new objects found");
                }
                objectIndex = 0;
            }
        } catch (Throwable t) {
            metadata = null;
            logger.error("Error in getNextListing", t);
        }
    }

    public abstract boolean roomInQueue();
    public abstract List<Object> getNextTuple();
    public abstract void processS3Object(S3ObjectSummary summary, S3Object s3Object);

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
            logger.info("From date is now {}", fromDateTime);
            S3Object s3Object = s3Service.getObject(new GetObjectRequest(bucketName, summary.getKey()));
            processS3Object(summary, s3Object);
        }
    }

    protected Runnable getTask() {
        return () -> {
            try {
                if ( roomInQueue() ) getNextObject();
            } catch (Throwable t) {
                logger.error("Error in load task", t);
            }
        };
    }

    protected List<Object> makeTuple(Object data) {
        List<Object> values = new Values();

        values.add(1);
        values.add(data);
        values.add(new HashMap<>());

        return values;
    }

    @Override
    public void nextTuple() {
        List<Object> tuple = getNextTuple();
        if (null != tuple) collector.emit(tuple);
    }

    public void s3start() {
        AWSCredentialsProvider credentialsProvider;
        if(StringUtils.isNotBlank(this.roleArn)){
            logger.info("assuming Role with arn " + this.roleArn);
            STSAssumeRoleSessionCredentialsProvider.Builder stsBuilder =
                    new STSAssumeRoleSessionCredentialsProvider.Builder(
                            this.roleArn, "storm");
            credentialsProvider = stsBuilder.withLongLivedCredentialsProvider(new DefaultAWSCredentialsProviderChain()).build();
        }
        else{
            credentialsProvider = new DefaultAWSCredentialsProviderChain();
        }

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


    public void s3Stop() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(2, TimeUnit.MINUTES);
        } catch (InterruptedException e) {

        } finally {
            if ( !executorService.isTerminated() ) executorService.shutdownNow();
        }
    }
}
