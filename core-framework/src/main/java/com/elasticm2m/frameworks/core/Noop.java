package com.elasticm2m.frameworks.core;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import com.elasticm2m.frameworks.common.base.ElasticBaseRichBolt;

public class Noop extends ElasticBaseRichBolt {

    @Override
    public void execute(Tuple tuple) {
        logger.debug("Noop consuming tuple");
        collector.ack(tuple);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }
}
