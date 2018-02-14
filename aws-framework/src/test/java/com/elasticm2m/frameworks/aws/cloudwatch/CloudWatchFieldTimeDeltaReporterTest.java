package com.elasticm2m.frameworks.aws.cloudwatch;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.blacklocus.metrics.CloudWatchReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CloudWatchFieldTimeDeltaReporterTest {

    private static final Logger LOG = LoggerFactory.getLogger(CloudWatchFieldTimeDeltaReporterTest.class);

    private static final String SAMPLE_JSON = "{\n" +
            "    \"id\":\"bcde7f74-1a0a-5b38-a890-1c7d6cdd6e06\",\n" +
            "    \"type\":\"Feature\",\n" +
            "    \"geometry\":{\n" +
            "        \"type\":\"Point\",\n" +
            "        \"coordinates\":[\n" +
            "            0,\n" +
            "            0\n" +
            "        ]\n" +
            "    },\n" +
            "    \"properties\":{\n" +
            "        \"source\":\"gdelt-gkg2.0\",\n" +
            "        \"sourceId\":\"20180119170000-T2753\",\n" +
            "        \"url\":\"http://www.bukla.si/index.php?action=landscape&cat_id=7&author_id=5752&osCsid=q63rprrf8fmgm5j5i4ihgkq121\",\n" +
            "        \"image_url\":null,\n" +
            "        \"timestamp\":\"2018-01-19T17:00:00Z\",\n" +
            "        \"meta\":{\n" +
            "            \"ingest_time\":\"2018-01-19T17:01:00Z\"\n" +
            "        },\n" +
            "        \"source_language\":\"slv\"\n" +
            "    }\n" +
            "}";

    @Mock
    private CloudWatchReporter cloudwatchReporter;
    @Mock
    private MetricRegistry metricRegistry;
    @Mock
    private Histogram histogram;
    @Mock
    private Tuple tuple;
    @Mock
    private TopologyContext topologyContext;
    @Mock
    private OutputCollector collector;

    private CloudWatchFieldTimeDeltaReporter bolt;

    @Before
    public void setUp() throws Exception {

        when(metricRegistry.histogram("my_name Topology=my_topology Region=my_region")).thenReturn(histogram);

        bolt = new CloudWatchFieldTimeDeltaReporter(){
            @Override
            CloudWatchReporter getCloudwatchReporter() {
                return cloudwatchReporter;
            }

            @Override
            MetricRegistry getMetricRegistry() {
                return metricRegistry;
            }
        };
        Map conf = Collections.singletonMap(Config.TOPOLOGY_NAME, "my_topology");
        bolt.setComponentLabel("my_component");
        bolt.setName("my_name");
        bolt.setNamespace("my_namespace");
        bolt.setRegionName("my_region");
        bolt.setLogger(LOG);
        bolt.prepare(conf, topologyContext, collector);
    }

    @Test
    public void testValidJsonStartAndEnd() {

        when(tuple.getValue(1)).thenReturn(SAMPLE_JSON);

        bolt.setStartProperty("/properties/timestamp");
        bolt.setEndProperty("/properties/meta/ingest_time");
        bolt.execute(tuple);

        verify(histogram).update(60L);
        verify(collector).emit(eq(tuple), any());
        verify(collector).ack(tuple);
    }

    @Test
    public void testValidJsonStartAndEndInMinutes() {

        when(tuple.getValue(1)).thenReturn(SAMPLE_JSON);

        bolt.setStartProperty("/properties/timestamp");
        bolt.setEndProperty("/properties/meta/ingest_time");
        bolt.setMetricTimeUnit("MINUTES");
        bolt.execute(tuple);

        verify(histogram).update(1L);
        verify(collector).emit(eq(tuple), any());
        verify(collector).ack(tuple);
    }

    @Test
    public void testValidStart() {

        when(tuple.getValue(1)).thenReturn(SAMPLE_JSON);

        bolt.setStartProperty("/properties/timestamp");
        bolt.execute(tuple);

        verify(histogram).update(anyLong());
        verify(collector).emit(eq(tuple), any());
        verify(collector).ack(tuple);
    }

    @Test
    public void invalidJsonStillEmitsAndAcks() {

        when(tuple.getValue(1)).thenReturn("THIS IS NOT JSON");

        bolt.setStartProperty("/properties/timestamp");
        bolt.execute(tuple);

        verify(histogram, never()).update(anyLong());
        verify(collector).emit(eq(tuple), any());
        verify(collector).ack(tuple);
    }

}