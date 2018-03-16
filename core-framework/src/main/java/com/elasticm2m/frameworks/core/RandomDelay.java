package com.elasticm2m.frameworks.core;

import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

/**
 * Test bolt for adding random delays in tuple processing
 */
public class RandomDelay extends ElasticBaseRichBolt {

    private int minSleepMillis = 100;
    private int maxSleepMillis = 500;

    private Random random;
    private int bound;
    private int minimum;


    @Inject
    public void setMinSleepMillis(@Named("min-sleep-millis") int minSleepMillis) {
        this.minSleepMillis = minSleepMillis;
    }

    @Inject
    public void setMaxSleepMillis(@Named("max-sleep-millis") int maxSleepMillis) {
        this.maxSleepMillis = maxSleepMillis;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(stormConf, topologyContext, collector);
        this.random = new Random();
        int maximum = Math.max(Math.abs(minSleepMillis), Math.abs(maxSleepMillis));
        this.minimum = Math.min(Math.abs(minSleepMillis), Math.abs(maxSleepMillis));
        this.bound = (maximum - minimum) + 1;
        logger.info("random delay bolt bounds {}-{} millis", minimum, maximum);
    }


    @Override
    public void execute(Tuple tuple) {
        try {
            long randomMillis = minimum + random.nextInt(bound);
            logger.debug("delaying {} millis", randomMillis);
            Utils.sleep(randomMillis);
        } finally {
            collector.emit(tuple, new ArrayList<>(tuple.getValues()));
            collector.ack(tuple);
        }
    }
}
