package com.elasticm2m.frameworks.aws.s3;

import backtype.storm.utils.Utils;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.zip.GZIPInputStream;

/**
 * Created by singram on 10/26/17
 * Extends S3Reader to handle binary files
 */
public class S3BinaryReader extends S3Reader {
    private LinkedBlockingDeque<byte[]> byteQueue;

    private byte[] readData(InputStream is, long len) throws IOException {
        byte[] data = new byte[(int) len];
        ByteStreams.readFully(is, data);
        return data;
    }

    @Override
    public void processS3Object(S3ObjectSummary summary, S3Object s3Object) {
        int sz = byteQueue.size();
        try (InputStream is = s3Object.getObjectContent()) {
            byteQueue.offer(readData(isGzip ? new GZIPInputStream(is) : is, s3Object.getObjectMetadata().getContentLength()));
            logger.info("Found {} records in {}, new queue size is {}", byteQueue.size() - sz, summary.getKey(), byteQueue.size());
        } catch (IOException e) {
            logger.error("Error in S3BinaryReader.processS3Object()", e);
        }
    }

    @Override
    public void nextTuple() {
        List<Object> tuple = null;
        byte[] data = byteQueue.poll();
        if ( null == data ) {
            Utils.sleep(50);
        } else {
            tuple = makeTuple(data);
        }
        if (null != tuple) collector.emit(tuple);
    }

    @Override
    public boolean roomInQueue() {
        return byteQueue.size() <= 100;
    }

    @Override
    public void initialize() {
        byteQueue = new LinkedBlockingDeque<>(5000);
    }
}

