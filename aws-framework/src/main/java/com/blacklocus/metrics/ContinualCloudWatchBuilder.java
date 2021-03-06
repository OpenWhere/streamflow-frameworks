package com.blacklocus.metrics;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;

import java.util.SortedMap;


/**
 * A fluent style builder for a {@link ContinualCloudWatchReporter}. Unfortunately this couldn't override
 * the existing builder due to private variables
 */
public class ContinualCloudWatchBuilder{

    // Defaults are resolved in build() so that 1) it is precisely clear what the caller set,
    // and 2) so that partial constructions can copy() before setting member variables with
    // things that should not be part of the copy such as a base builder for similar CloudWatchReporters.

    private MetricRegistry registry;
    private String namespace;
    private AmazonCloudWatchAsync client;
    private MetricFilter filter;
    private String dimensions;
    private Boolean timestampLocal;

    private String typeDimName;
    private String typeDimValGauge;
    private String typeDimValCounterCount;
    private String typeDimValMeterCount;
    private String typeDimValHistoSamples;
    private String typeDimValHistoStats;
    private String typeDimValTimerSamples;
    private String typeDimValTimerStats;

    private Predicate<MetricDatum> reporterFilter;

    /**
     * @param registry of metrics for CloudWatchReporter to submit
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withRegistry(MetricRegistry registry) {
        this.registry = registry;
        return this;
    }

    /**
     * @param namespace metric namespace to use when submitting metrics to CloudWatch
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withNamespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    /**
     * @param client CloudWatch client
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withClient(AmazonCloudWatchAsync client) {
        this.client = client;
        return this;
    }

    /**
     * @param filter which returns true for metrics that should be sent to CloudWatch
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withFilter(MetricFilter filter) {
        this.filter = filter;
        return this;
    }

    /**
     * @param dimensions global dimensions in the form name=value that should be appended to all metrics submitted to
     *                   CloudWatch
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withDimensions(String dimensions) {
        this.dimensions = dimensions;
        return this;
    }

    /**
     * @param timestampLocal whether or not to explicitly timestamp metric data to now (true), or leave it null so that
     *                       CloudWatch will timestamp it on receipt (false)
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withTimestampLocal(Boolean timestampLocal) {
        this.timestampLocal = timestampLocal;
        return this;
    }


    /**
     * @param typeDimName name of the "metric type" dimension added to CloudWatch submissions.
     *                    Defaults to <b>{@value Constants#DEF_DIM_NAME_TYPE}</b> when using the CloudWatchReporterBuilder
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withTypeDimName(String typeDimName) {
        this.typeDimName = typeDimName;
        return this;
    }

    /**
     * @param typeDimValGauge value of the "metric type" dimension added to CloudWatch submissions of {@link Gauge}s.
     *                        Defaults to <b>{@value Constants#DEF_DIM_VAL_GAUGE}</b>
     *                        when using the CloudWatchReporterBuilder
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withTypeDimValGauge(String typeDimValGauge) {
        this.typeDimValGauge = typeDimValGauge;
        return this;
    }

    /**
     * @param typeDimValCounterCount value of the "metric type" dimension added to CloudWatch submissions of
     *                               {@link Counter#getCount()}.
     *                               Defaults to <b>{@value Constants#DEF_DIM_VAL_COUNTER_COUNT}</b> when using the CloudWatchReporterBuilder
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withTypeDimValCounterCount(String typeDimValCounterCount) {
        this.typeDimValCounterCount = typeDimValCounterCount;
        return this;
    }

    /**
     * @param typeDimValMeterCount value of the "metric type" dimension added to CloudWatch submissions of
     *                             {@link Meter#getCount()}.
     *                             Defaults to <b>{@value Constants#DEF_DIM_VAL_METER_COUNT}</b> when using the CloudWatchReporterBuilder
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withTypeDimValMeterCount(String typeDimValMeterCount) {
        this.typeDimValMeterCount = typeDimValMeterCount;
        return this;
    }

    /**
     * @param typeDimValHistoSamples value of the "metric type" dimension added to CloudWatch submissions of
     *                               {@link Histogram#getCount()}.
     *                               Defaults to <b>{@value Constants#DEF_DIM_VAL_HISTO_SAMPLES}</b> when using the CloudWatchReporterBuilder
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withTypeDimValHistoSamples(String typeDimValHistoSamples) {
        this.typeDimValHistoSamples = typeDimValHistoSamples;
        return this;
    }

    /**
     * @param typeDimValHistoStats value of the "metric type" dimension added to CloudWatch submissions of
     *                             {@link Histogram#getSnapshot()}.
     *                             Defaults to <b>{@value Constants#DEF_DIM_VAL_HISTO_STATS}</b> when using the CloudWatchReporterBuilder
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withTypeDimValHistoStats(String typeDimValHistoStats) {
        this.typeDimValHistoStats = typeDimValHistoStats;
        return this;
    }

    /**
     * @param typeDimValTimerSamples value of the "metric type" dimension added to CloudWatch submissions of
     *                               {@link Timer#getCount()}.
     *                               Defaults to <b>{@value Constants#DEF_DIM_VAL_TIMER_SAMPLES}</b> when using the CloudWatchReporterBuilder
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withTypeDimValTimerSamples(String typeDimValTimerSamples) {
        this.typeDimValTimerSamples = typeDimValTimerSamples;
        return this;
    }

    /**
     * @param typeDimValTimerStats value of the "metric type" dimension added to CloudWatch submissions of
     *                             {@link Timer#getSnapshot()}.
     *                             Defaults to <b>{@value Constants#DEF_DIM_VAL_TIMER_STATS}</b> when using the CloudWatchReporterBuilder
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withTypeDimValTimerStats(String typeDimValTimerStats) {
        this.typeDimValTimerStats = typeDimValTimerStats;
        return this;
    }

    /**
     * This filter is applied right before submission to CloudWatch. This filter can access decoded metric name elements
     * such as {@link MetricDatum#getDimensions()}. true means to keep and submit the metric. false means to exclude it.
     * <p>
     * Different from {@link MetricFilter} in that
     * MetricFilter must operate on the encoded, single-string name (see {@link MetricFilter#matches(String, Metric)}),
     * and this filter is applied before {@link ScheduledReporter#report(SortedMap, SortedMap, SortedMap, SortedMap, SortedMap)} so that
     * filtered metrics never reach that method in CloudWatchReporter.
     * <p>
     * Defaults to {@link Predicates#alwaysTrue()} - i.e. do not remove any metrics from the submission due to this
     * particular filter.
     *
     * @param reporterFilter to replace 'alwaysTrue()'
     * @return this (for chaining)
     */
    public ContinualCloudWatchBuilder withReporterFilter(Predicate<MetricDatum> reporterFilter) {
        this.reporterFilter = reporterFilter;
        return this;
    }


    /**
     * @return a shallow copy of this builder
     */
    public ContinualCloudWatchBuilder copy() {
        return new ContinualCloudWatchBuilder()
                .withRegistry(registry)
                .withNamespace(namespace)
                .withClient(client)
                .withFilter(filter)
                .withDimensions(dimensions)
                .withTimestampLocal(timestampLocal)
                .withTypeDimName(typeDimName)
                .withTypeDimValGauge(typeDimValGauge)
                .withTypeDimValCounterCount(typeDimValCounterCount)
                .withTypeDimValMeterCount(typeDimValMeterCount)
                .withTypeDimValHistoSamples(typeDimValHistoSamples)
                .withTypeDimValHistoStats(typeDimValHistoStats)
                .withTypeDimValTimerSamples(typeDimValTimerSamples)
                .withTypeDimValTimerStats(typeDimValTimerStats)
                .withReporterFilter(reporterFilter);
    }

    /**
     * @return a new CloudWatchReporter instance based on the state of this builder
     */
    public CloudWatchReporter build() {
        Preconditions.checkState(!Strings.isNullOrEmpty(namespace), "Metric namespace is required.");

        String resolvedNamespace = namespace;

        // Use specified or fall back to default. Don't secretly modify the fields of this builder
        // in case the caller wants to re-use it to build other reporters, or something.

        MetricRegistry resolvedRegistry = null != registry ? registry : new MetricRegistry();
        MetricFilter resolvedFilter = null != filter ? filter : MetricFilter.ALL;
        AmazonCloudWatchAsync resolvedCloudWatchClient = null != client ? client : new AmazonCloudWatchAsyncClient();
        String resolvedDimensions = null != dimensions ? dimensions : null;
        Boolean resolvedTimestampLocal = null != timestampLocal ? timestampLocal : false;

        String resolvedTypeDimName = null != typeDimName ? typeDimName : Constants.DEF_DIM_NAME_TYPE;
        String resolvedTypeDimValGauge = null != typeDimValGauge ? typeDimValGauge : Constants.DEF_DIM_VAL_GAUGE;
        String resolvedTypeDimValCounterCount = null != typeDimValCounterCount ? typeDimValCounterCount : Constants.DEF_DIM_VAL_COUNTER_COUNT;
        String resolvedTypeDimValMeterCount = null != typeDimValMeterCount ? typeDimValMeterCount : Constants.DEF_DIM_VAL_METER_COUNT;
        String resolvedTypeDimValHistoSamples = null != typeDimValHistoSamples ? typeDimValHistoSamples : Constants.DEF_DIM_VAL_HISTO_SAMPLES;
        String resolvedTypeDimValHistoStats = null != typeDimValHistoStats ? typeDimValHistoStats : Constants.DEF_DIM_VAL_HISTO_STATS;
        String resolvedTypeDimValTimerSamples = null != typeDimValTimerSamples ? typeDimValTimerSamples : Constants.DEF_DIM_VAL_TIMER_SAMPLES;
        String resolvedTypeDimValTimerStats = null != typeDimValTimerStats ? typeDimValTimerStats : Constants.DEF_DIM_VAL_TIMER_STATS;

        Predicate<MetricDatum> resolvedReporterFilter = null != reporterFilter ? reporterFilter : Predicates.<MetricDatum>alwaysTrue();

        return new ContinualCloudWatchReporter(
                resolvedRegistry,
                resolvedNamespace,
                resolvedFilter,
                resolvedCloudWatchClient)
                .withDimensions(resolvedDimensions)
                .withTimestampLocal(resolvedTimestampLocal)
                .withTypeDimName(resolvedTypeDimName)
                .withTypeDimValGauge(resolvedTypeDimValGauge)
                .withTypeDimValCounterCount(resolvedTypeDimValCounterCount)
                .withTypeDimValMeterCount(resolvedTypeDimValMeterCount)
                .withTypeDimValHistoSamples(resolvedTypeDimValHistoSamples)
                .withTypeDimValHistoStats(resolvedTypeDimValHistoStats)
                .withTypeDimValTimerSamples(resolvedTypeDimValTimerSamples)
                .withTypeDimValTimerStats(resolvedTypeDimValTimerStats)
                .withReporterFilter(resolvedReporterFilter);
    }
}