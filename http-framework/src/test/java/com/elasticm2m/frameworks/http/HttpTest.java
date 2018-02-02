package com.elasticm2m.frameworks.http;

import org.apache.storm.utils.Utils;
import com.github.nkzawa.socketio.client.IO;
import com.github.nkzawa.socketio.client.Socket;
import org.apache.http.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: aaslinger
 * Date: 4/20/17
 * Time: 11:19 AM
 * <p>
 * Developed By OpenWhere, Inc.
 */
public class HttpTest {

    @Test
     public void testContentSuccess() throws Exception {
        String body = "{\"foo\":\"test\"}";
        HttpTransformBolt bolt = new HttpTransformBolt();
        bolt.httpclient = Mockito.mock(CloseableHttpClient.class);
        bolt.setEndpoint("http://foo");
        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        HttpEntity entity = Mockito.mock(HttpEntity.class);
        Mockito.when(entity.getContent()).thenReturn(new ByteArrayInputStream("{}".getBytes("UTF-8")));
        Mockito.when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "FINE!")) ;
        Mockito.when(bolt.httpclient.execute(Mockito.any())).thenReturn(response);
        Mockito.when(response.getEntity()).thenReturn(entity);
        String content = bolt.getContent(body);
        assertEquals("{}", content);

    }

    @Test
    public void testContentFail() throws Exception {
        String body = "{\"foo\":\"test\"}";
        HttpTransformBolt bolt = new HttpTransformBolt();
        bolt.httpclient = Mockito.mock(CloseableHttpClient.class);
        bolt.setEndpoint("http://foo");
        bolt.setLogger(LoggerFactory.getLogger(HttpTransformBolt.class));
        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        Mockito.when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_INTERNAL_SERVER_ERROR, "FINE!")) ;
        Mockito.when(bolt.httpclient.execute(Mockito.any())).thenReturn(response);
        String content = bolt.getContent(body);
        assertEquals(null, content);

    }
}
