package com.elasticm2m.frameworks.aws.s3;

import backtype.storm.utils.Utils;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.zip.GZIPInputStream;

/**
 * Created by singram on 10/26/17
 * Extends S3Reader to handle csv files
 */
public class S3CsvReader extends S3Reader {
    private LinkedBlockingDeque<String> lineQueue;
    private boolean isFirstLineExcluded;

    @Inject(optional = true)
    public void setIsFirstLineExcluded(@Named("is-first-line-excluded") boolean excludeFirstLine) {
        this.isFirstLineExcluded = excludeFirstLine;
    }

    @Override
    public void processS3Object(S3ObjectSummary summary, S3Object s3Object) {
        try (InputStream is = s3Object.getObjectContent()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(isGzip ? new GZIPInputStream(is) : is));
            if (isFirstLineExcluded) {
                reader.readLine();
            }
            long sent = reader.lines().map(l -> lineQueue.offer(l)).count();
            logger.info("Found {} lines in {}, new queue size is {}", sent, summary.getKey(), lineQueue.size());
        } catch (IOException e) {
            logger.error("Error in getNextObject()", e);
        }
    }

    @Override
    public void nextTuple() {
        List<Object> tuple = null;
        String line = lineQueue.poll();
        if ( null == line ) {
            Utils.sleep(50);
        } else {
            tuple = makeTuple(line);
        }
        if (null != tuple) collector.emit(tuple);
    }

    @Override
    public boolean roomInQueue() {
        return lineQueue.size() <= 100;
    }

    @Override
    public void initialize() {
        lineQueue = new LinkedBlockingDeque<>(10000);
    }
}
