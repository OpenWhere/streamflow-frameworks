package com.blacklocus.metrics;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.codahale.metrics.Counting;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import java.util.concurrent.TimeUnit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: aaslinger
 * Date: 12/7/16
 * Time: 10:26 AM
 * <p>
 * Developed By OpenWhere, Inc.
 */
public class ContinualCloudWatchReporter extends com.blacklocus.metrics.CloudWatchReporter{


    private String typeDimName = CloudWatchReporter.METRIC_TYPE_DIMENSION;
    private String dimensions;
    private final Map<Counting, Long> lastPolledCounts = new HashMap<>();

    public ContinualCloudWatchReporter(MetricRegistry registry,
                              String metricNamespace,
                              MetricFilter metricFilter,
                              AmazonCloudWatchAsync cloudWatch) {

        super(registry, metricNamespace, metricFilter, cloudWatch);
    }


    @Override
    void reportCounter(Map.Entry<String, ? extends Counting> entry, String typeDimValue, List<MetricDatum> data) {
        Counting metric = entry.getValue();
        final long diff = diffLast(metric);

        DemuxedKey key = new DemuxedKey(appendGlobalDimensions(entry.getKey()));
        Iterables.addAll(data, key.newDatums(typeDimName, typeDimValue, new Function<MetricDatum, MetricDatum>() {
            @Override
            public MetricDatum apply(MetricDatum datum) {
                return datum.withValue((double) diff).withUnit(StandardUnit.Count);
            }
        }));


    }

    /*This stuff is unfortunately private so copy-pasted*/
    private long diffLast(Counting metric) {
        long count = metric.getCount();

        Long lastCount = lastPolledCounts.get(metric);
        lastPolledCounts.put(metric, count);

        if (lastCount == null) {
            lastCount = 0L;
        }
        return count - lastCount;
    }


    public CloudWatchReporter withTypeDimName(String typeDimName) {
        this.typeDimName = typeDimName;
        return this;
    }

    private String appendGlobalDimensions(String metric) {
        if (StringUtils.isBlank(StringUtils.trim(dimensions))) {
            return metric;
        } else {
            return metric + CloudWatchReporter.NAME_TOKEN_DELIMITER + dimensions;
        }
    }

    public CloudWatchReporter withDimensions(String dimensions) {
        this.dimensions = dimensions;
        return this;
    }
}
