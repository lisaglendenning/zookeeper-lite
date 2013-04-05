package org.apache.zookeeper.protocol;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import org.apache.zookeeper.data.OpCreateSessionAction;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;

@RunWith(JUnit4.class)
public class OpCreateSessionActionTest {
    
    public static final String TAG = Records.CONNECT_TAG;
    
    public static ConnectRequest defaultRequest() {
        byte[] passwd = {0};
        return new ConnectRequest(0, 1, 30, 1, passwd);
    }
    
    public static ConnectResponse defaultResponse(ConnectRequest request) {
        return new ConnectResponse(
                request.getProtocolVersion(),
                request.getTimeOut(),
                request.getSessionId(),
                request.getPasswd());
    }
    
    @Test
    public void testRequestSerialization() throws IOException {
        OpCreateSessionAction.Request input = OpCreateSessionAction.Request.create()
                .setRequest(defaultRequest()).setReadOnly(false).setWraps(false);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        input.encode(outStream);
        outStream.close();
        
        ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
        OpCreateSessionAction.Request output = OpCreateSessionAction.Request.create().decode(inStream);
        inStream.close();
        assertEquals(input, output);
    }

    @Test
    public void testRequestCompatibility() throws IOException {
        ConnectRequest input = defaultRequest();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        Records.serialize(input, outStream, Records.CONNECT_TAG);
        outStream.close();

        ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
        OpCreateSessionAction.Request output = OpCreateSessionAction.Request.create().decode(inStream);
        inStream.close();
        
        assertEquals(input, output.record());
        assertTrue(output.wraps());
    }
    
    @Test
    public void testResponseSerialization() throws IOException {
        OpCreateSessionAction.Response input = OpCreateSessionAction.Response.create()
                .setResponse(defaultResponse(defaultRequest())).setReadOnly(false).setWraps(false);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        input.encode(outStream);
        outStream.close();
        
        ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
        OpCreateSessionAction.Response output = OpCreateSessionAction.Response.create().decode(inStream);
        inStream.close();
        assertEquals(input, output);
    }

    @Test
    public void testResponseCompatibility() throws IOException {
        OpCreateSessionAction.Response input = OpCreateSessionAction.Response.create()
                .setResponse(defaultResponse(defaultRequest())).setWraps(true).setReadOnly(false);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        input.encode(outStream);
        outStream.close();

        ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
        ConnectResponse output = Records.deserialize(new ConnectResponse(), inStream, Records.CONNECT_TAG);
        assertEquals(inStream.available(), 0);
        inStream.close();
        
        assertEquals(input.record(), output);
    }
}
