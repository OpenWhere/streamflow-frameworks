package com.elasticm2m.frameworks.aws.dynamodb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;

/**
 * Created by singram on 8/27/15.
 */
public class DynamoDBWriterTest {

    @Test
    public void testPathFor() throws Exception {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("reciperun.json");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readValue(stream, JsonNode.class);
        DynamoDBWriter test = new DynamoDBWriter();
        String paths[] = "properties.meta.ingest_time".split("\\.");
        Assert.assertEquals("Bad ingest_time", "2015-08-25T20:30:10.751Z", test.pathFor(root, paths).asText());
    }
}