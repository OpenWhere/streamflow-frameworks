package com.elasticm2m.frameworks.aws.s3;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.zip.GZIPInputStream;

/**
 * Created by singram on 9/8/15.
 *
 * Base spout reads only JSON files,
 * either as a JSON array of objects or a newline delimited
 * list of JSON objects (such as from Kinesis)
 * Emits JSON objects
 */
public class S3Reader extends S3BaseSpout {
    private boolean isJson;
    private boolean isDelimited;
    private LinkedBlockingDeque<JsonNode> jsonQueue;
    private ObjectMapper objectMapper;

    @Inject
    public void setIsDelimited(@Named("is-delimited") boolean isDelimited) {
        this.isDelimited = isDelimited;
    }

    @Inject(optional = true)
    public void setIsJson(@Named("is-json") boolean isJson) {
        this.isJson = isJson; // no longer used, keeping to not break existing deployments
    }

    private JsonNode readJsonData(InputStream is) throws IOException {
        if (isDelimited) {
            Scanner scanner = new Scanner(is).useDelimiter("\\n");
            ArrayNode listNode = objectMapper.createArrayNode();
            while (scanner.hasNext()) {
                JsonNode node = objectMapper.readValue(scanner.next(), JsonNode.class);
                listNode.add(node);
            }
            return listNode;
        } else {
            return objectMapper.readValue(is, JsonNode.class);
        }
    }

    @Override
    public boolean roomInQueue() {
        return jsonQueue.size() <= 100;
    }

    @Override
    public void processS3Object(S3ObjectSummary summary, S3Object s3Object) {
        int sz = jsonQueue.size();
        try (InputStream is = s3Object.getObjectContent()) {
            JsonNode data = readJsonData(isGzip ? new GZIPInputStream(is) : is);
            if (data.isArray()) {
                data.forEach(jsonQueue::offer);
            } else {
                jsonQueue.offer(data);
            }
            logger.info("Found {} records in {}, new queue size is {}", jsonQueue.size() - sz, summary.getKey(), jsonQueue.size());

        } catch (IOException e) {
            logger.error("Error in S3Reader.processS3Object()", e);
        }
    }

    private JsonNode pathFor(JsonNode node, String p[], int idex) {
        if (idex < p.length) {
            return pathFor(node.path(p[idex]), p, ++idex); // recurse
        } else {
            return node;
        }
    }

    private JsonNode pathFor(JsonNode node, String p[]) {
        return pathFor(node, p, 0);
    }

    @Override
    public void open(Map stormConf, TopologyContext topologyContext, SpoutOutputCollector collector) {
        super.open(stormConf, topologyContext, collector);

        jsonQueue = new LinkedBlockingDeque<>(10000);
        objectMapper = new ObjectMapper();

        s3start();
    }

    public List<Object> getJsonTuple(JsonNode node) {
        List<Object> values = new Values();

        String id = pathFor(node, new String[]{"id"}).asText();
        if (null == id) id = pathFor(node, new String[]{"properties", "id"}).asText();
        try {
            String jsonString = objectMapper.writeValueAsString(node);
            values.add(id);
            values.add(jsonString);
            values.add(new HashMap<>());

            return values;
        } catch (JsonProcessingException e) {
            logger.error("Error converting JsonNode to Tuple", e);
        }
        return null;
    }

    @Override
    public List<Object> getNextTuple() {
        List<Object> tuple = null;
        JsonNode node = jsonQueue.poll();
        if (null == node) {
            Utils.sleep(50);
        } else {
            tuple = getJsonTuple(node);
        }
        return tuple;
    }

    @Override
    public void close() {
        super.close();
        s3Stop();
    }
}
