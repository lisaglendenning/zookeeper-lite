package org.apache.zookeeper.netty;

import static org.junit.Assert.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelStateHandlerAdapter;
import io.netty.channel.embedded.EmbeddedByteChannel;
import io.netty.channel.embedded.EmbeddedMessageChannel;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.EventSink;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.Xid;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.data.OpCreateSessionAction;
import org.apache.zookeeper.data.OpPingAction;
import org.apache.zookeeper.data.OpResult;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.data.Operations;
import org.apache.zookeeper.event.ConnectionEventValue;
import org.apache.zookeeper.event.ConnectionSessionStateEvent;
import org.apache.zookeeper.event.ConnectionStateEvent;
import org.apache.zookeeper.event.SessionConnectionStateEvent;
import org.apache.zookeeper.netty.client.AnonymousClientConnection;
import org.apache.zookeeper.netty.client.ClientConnection;
import org.apache.zookeeper.netty.protocol.LoggingDecoder;
import org.apache.zookeeper.netty.server.ServerConnection;
import org.apache.zookeeper.protocol.FourLetterCommand;
import org.apache.zookeeper.protocol.OpCreateSessionActionTest;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.EventfulEventBus;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.Subscribe;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;

@RunWith(JUnit4.class)
public class ServerConnectionITest extends TestEmbeddedChannels {

    @Rule
    public Timeout globalTimeout = new Timeout(10000); 

    protected static final Logger logger = LoggerFactory.getLogger(ServerConnectionITest.class);

    public static class Module extends AbstractModule {

        public static Injector injector;
        
        public static void createInjector() {
            injector = Guice.createInjector(
                    Module.get());
        }

        public static Module get() {
            return new Module();
        }
        
        @Override
        protected void configure() {
            bind(Eventful.class).to(EventfulEventBus.class);
        }

        @Provides
        public Xid xid() {
            return Xid.create();
        }
        
        @Provides
        public Zxid zxid() {
            return Zxid.create();
        }
    }

    public static class ChannelSink extends ChannelStateHandlerAdapter {
        protected BlockingQueue<Object> events;
        
        public ChannelSink() {
            events = new LinkedBlockingQueue<Object>();
        }

        @Override
        public void inboundBufferUpdated(ChannelHandlerContext ctx)
                throws Exception {
            ctx.fireInboundBufferUpdated();
        }
        
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
            events.add(event);
        }
    }

    public static class EventfulSink extends EventSink {
        
        @Subscribe
        public void handleEvent(Object event) throws InterruptedException {
        	logger.debug("{}", event);
            put(event);
        }
    }

    public static String echoCommand(FourLetterCommand event) {
        return event.word();
    }

    public static OpCreateSessionAction.Request defaultRequest() {
        return OpCreateSessionAction.Request.create(OpCreateSessionActionTest.defaultRequest(), false, false);
    }

    public static OpCreateSessionAction.Response defaultResponse(OpCreateSessionAction.Request request) {
        return OpCreateSessionAction.Response.create(OpCreateSessionActionTest.defaultResponse(request.record()), 
                request.readOnly(), request.wraps());
    }
    
    @BeforeClass
    public static void createInjector() {
        Module.createInjector();
    }

    @Test
    public void testEchoAllFourLetterCommands() throws InterruptedException {
        Injector injector = Module.injector;
        EmbeddedByteChannel serverChannel = new EmbeddedByteChannel(new LoggingDecoder());
        ServerConnection serverHandler = injector.getInstance(ServerConnection.Factory.class).get(serverChannel);
        EmbeddedMessageChannel clientChannel = new EmbeddedMessageChannel(new LoggingDecoder());
        AnonymousClientConnection clientHandler = injector.getInstance(AnonymousClientConnection.Factory.class).get(clientChannel);
        
        for (FourLetterCommand command: FourLetterCommand.values()) {
            ByteBuf clientMsg = writeOutboundAndRead(clientChannel, command);
            FourLetterCommand event = writeInboundAndRead(serverChannel, clientMsg);
            assertEquals(command, event);
            
            String response = echoCommand(event);
            ByteBuf serverMsg = writeOutboundAndRead(serverChannel, response);
            String output = writeInboundAndRead(clientChannel, serverMsg);
            assertEquals(response, output);
        }
        boolean finished = serverChannel.finish();
        assertFalse(finished);
        finished = clientChannel.finish();
        assertFalse(finished);
    }

    @Test
    public void testCreateSession() throws Exception {
        Injector injector = Module.injector;
        ChannelSink serverSink = new ChannelSink();
        EmbeddedByteChannel serverChannel = new EmbeddedByteChannel(new LoggingDecoder(), serverSink);
        ServerConnection serverHandler = injector.getInstance(ServerConnection.Factory.class).get(serverChannel);

        ChannelSink clientSink = new ChannelSink();
        EmbeddedByteChannel clientChannel = new EmbeddedByteChannel(new LoggingDecoder(), clientSink);
        ClientConnection clientHandler = injector.getInstance(ClientConnection.Factory.class).get(clientChannel);
        
        EmbeddedByteChannel anonymousChannel = new EmbeddedByteChannel(new LoggingDecoder());
        AnonymousClientConnection anonymousHandler = injector.getInstance(AnonymousClientConnection.Factory.class).get(anonymousChannel);

        OpCreateSessionAction.Request createRequest = defaultRequest();
        ByteBuf clientMsg = writeOutboundAndRead(clientChannel, createRequest);
        
        OpCreateSessionAction.Request createInput = writeInboundAndRead(serverChannel, clientMsg);
        assertEquals(createRequest, createInput);
        
        // this request shouldn't get through
        FourLetterCommand command = FourLetterCommand.values()[0];
        clientMsg = writeOutboundAndRead(anonymousChannel, command);
        writeInbound(serverChannel, clientMsg);
        
        OpCreateSessionAction.Response createResponse = defaultResponse(createInput);
        ByteBuf serverMsg = writeOutboundAndRead(serverChannel, createResponse);

        Operation.Result createOutput = writeInboundAndRead(clientChannel, serverMsg);
        assertEquals(createRequest, createOutput.request());
        assertEquals(createResponse, createOutput.response());
        
        boolean finished = serverChannel.finish();
        assertFalse(finished);
        finished = clientChannel.finish();
        assertFalse(finished);
        finished = anonymousChannel.finish();
        assertFalse(finished);
    }
    
    @Test
    public void testCreatePingCloseSession() throws Exception {
        Injector injector = Module.injector;
        
        EventfulSink eventSink = new EventfulSink();
        EmbeddedByteChannel serverChannel = new EmbeddedByteChannel(new LoggingDecoder());
        ServerConnection server = injector.getInstance(ServerConnection.Factory.class).get(serverChannel);
        server.register(eventSink);
        
        EmbeddedByteChannel clientChannel = new EmbeddedByteChannel(new LoggingDecoder());
        ClientConnection clientHandler = injector.getInstance(ClientConnection.Factory.class).get(clientChannel);
        
        OpCreateSessionAction.Request createRequest = defaultRequest();
        ByteBuf clientMsg = writeOutboundAndRead(clientChannel, createRequest);

        OpCreateSessionAction.Request createInput = writeInboundAndRead(serverChannel, clientMsg);
        assertEquals(createRequest, createInput);

        SessionConnection.State sessionConnectionState = eventSink.take(ConnectionSessionStateEvent.class).event();
        assertEquals(SessionConnection.State.CONNECTING, sessionConnectionState);

        OpCreateSessionAction.Response createResponse = defaultResponse(createInput);
        ByteBuf serverMsg = writeOutboundAndRead(serverChannel, createResponse);

        Operation.Result createOutput = writeInboundAndRead(clientChannel, serverMsg);
        assertEquals(createRequest, createOutput.request());
        assertEquals(createResponse, createOutput.response());
        
        // ping
        OpPingAction.Request pingRequest = OpPingAction.Request.create();
        clientMsg = writeOutboundAndRead(clientChannel, pingRequest);
        Operation.CallRequest pingRequestInput = writeInboundAndRead(serverChannel, clientMsg);
        assertEquals(pingRequestInput.operation(), pingRequest.operation());
        assertEquals(pingRequestInput.xid(), pingRequest.xid());
        OpPingAction.Response pingResponse = OpPingAction.Response.create();
        serverMsg = writeOutboundAndRead(serverChannel, pingResponse);
        Operation.CallReply pingResponseOutput = writeInboundAndRead(clientChannel, serverMsg);
        assertEquals(pingResponseOutput.operation(), pingResponse.operation());
        assertEquals(pingResponseOutput.xid(), pingRequestInput.xid());
        
        // close
        Operation.Request closeRequest = Operations.Requests.create(Operation.CLOSE_SESSION);
        clientMsg = writeOutboundAndRead(clientChannel, closeRequest);
        Operation.CallRequest closeRequestInput = writeInboundAndRead(serverChannel, clientMsg);
        assertEquals(Operation.CLOSE_SESSION, closeRequestInput.operation());
        Operation.Response closeResponse = Operations.Responses.create(Operation.CLOSE_SESSION);
        Operation.Result closeResponseInput = OpResult.create(closeRequestInput, closeResponse);
        serverMsg = writeOutboundAndRead(serverChannel, closeResponseInput);
        Operation.CallResult closeResponseOutput = writeInboundAndRead(clientChannel, serverMsg);
        assertEquals(closeRequest.operation(), closeResponseOutput.operation());
        assertEquals(closeRequestInput, closeResponseOutput.request());
        assertTrue(closeResponseOutput.response() instanceof Operation.ResponseValue);
        assertEquals(closeResponse, ((Operation.ResponseValue)closeResponseOutput.response()).response());
    }
}
