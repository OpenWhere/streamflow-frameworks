package com.elasticm2m.frameworks.aws.cloudwatch;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClient;
import com.blacklocus.metrics.CloudWatchReporterBuilder;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: aaslinger
 * Date: 4/25/16
 * Time: 10:57 AM
 * <p>
 * Developed By OpenWhere, Inc.
 */
public class CloudWatchReporter extends ElasticBaseRichBolt {


    private AWSCredentialsProvider credentialsProvider;
    private AmazonCloudWatchAsyncClient client;
    private final MetricRegistry registry = new MetricRegistry();
    private String environment;
    private String name;
    private String regionName;
    private String namespace;
    private String metricName;
    private Integer reportFrequency;
    private ScheduledReporter reporter;

    @Inject
    public void setEnvironment(@Named("cloudwatch-environment") String environment) {
        this.environment = environment;
    }

    @Inject(optional = true)
    public void setName(@Named("cloudwatch-name") String name) {
        this.name = name;
    }

    @Inject(optional = true)
    public void setNamespace(@Named("cloudwatch-namespace") String namespace) {
        this.namespace = namespace;
    }

    @Inject(optional = true)
    public void setRegionName(@Named("cloudwatch-region-name") String regionName) { this.regionName = regionName; }

    @Inject
    public void setStableSeconds(@Named("report-frequency") Integer reportFrequency) {
        this.reportFrequency = reportFrequency;
    }

    private String buildMetricName(Map conf){
        String topologyName = (String) conf.get(Config.TOPOLOGY_NAME);
        return String.format("%s Topology=%s Region=%s", this.name, topologyName, regionName);
    }

    @Override
    public void cleanup() {
        super.cleanup();
        reporter.close();
    }

    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(conf, topologyContext, collector);

        logger.info("CloudWatch Reporter: Namespace = " + this.namespace);

        credentialsProvider = new DefaultAWSCredentialsProviderChain();

        client = new AmazonCloudWatchAsyncClient(credentialsProvider);
        client.setRegion(Region.getRegion(Regions.fromName(regionName)));

        metricName = buildMetricName(conf);

        reporter = new CloudWatchReporterBuilder()
                .withNamespace(this.namespace)
                .withClient(client)
                .withRegistry(registry)
                .build();
        reporter.start(reportFrequency, TimeUnit.MINUTES);
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            collector.emit(tuple, tuple.getValues());
            registry.meter(metricName).mark();
        }
        catch(Throwable t){
            logger.error("Error logging {}", t);
        }
        finally {
            collector.ack(tuple);
        }
    }
}
