package org.apache.zookeeper.netty.protocol.server;

import static org.junit.Assert.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelStateHandlerAdapter;
import io.netty.channel.embedded.EmbeddedByteChannel;
import io.netty.channel.embedded.EmbeddedMessageChannel;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ConnectionEventValue;
import org.apache.zookeeper.Xid;
import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.netty.protocol.LoggingDecoder;
import org.apache.zookeeper.netty.protocol.TestEmbeddedChannels;
import org.apache.zookeeper.netty.protocol.client.AnonymousClientConnection;
import org.apache.zookeeper.netty.protocol.client.ClientConnection;
import org.apache.zookeeper.netty.protocol.server.ServerConnection;
import org.apache.zookeeper.protocol.FourLetterCommand;
import org.apache.zookeeper.protocol.OpCreateSessionAction;
import org.apache.zookeeper.protocol.OpCreateSessionActionTest;
import org.apache.zookeeper.protocol.OpPingAction;
import org.apache.zookeeper.protocol.OpResult;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Operations;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.EventfulEventBus;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
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

    //@Rule
    //public Timeout globalTimeout = new Timeout(10000); 

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

    public static class EventSink<T> {
        
        public static <T> EventSink<T> create() {
            return new EventSink<T>();
        }
        
        public BlockingQueue<T> queue = new LinkedBlockingQueue<T>();
        
        @Subscribe
        public void handleEvent(T event) {
            queue.add(event);
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
        ServerConnection serverHandler = injector.getInstance(ServerConnection.class);
        EmbeddedByteChannel serverChannel = new EmbeddedByteChannel(new LoggingDecoder());
        serverHandler.attach(serverChannel);
        AnonymousClientConnection clientHandler = injector.getInstance(AnonymousClientConnection.class);
        EmbeddedMessageChannel clientChannel = new EmbeddedMessageChannel(new LoggingDecoder());
        clientHandler.attach(clientChannel);
        
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
        ServerConnection serverHandler = injector.getInstance(ServerConnection.class);
        EmbeddedByteChannel serverChannel = new EmbeddedByteChannel(new LoggingDecoder(), serverSink);
        serverHandler.attach(serverChannel);

        ChannelSink clientSink = new ChannelSink();
        ClientConnection clientHandler = injector.getInstance(ClientConnection.class);
        EmbeddedByteChannel clientChannel = new EmbeddedByteChannel(new LoggingDecoder(), clientSink);
        clientHandler.attach(clientChannel);
        
        AnonymousClientConnection anonymousHandler = injector.getInstance(AnonymousClientConnection.class);
        EmbeddedByteChannel anonymousChannel = new EmbeddedByteChannel(new LoggingDecoder());
        anonymousHandler.attach(anonymousChannel);

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
        
        EventSink<ConnectionEventValue> eventSink = EventSink.create();
        EmbeddedByteChannel serverChannel = new EmbeddedByteChannel(new LoggingDecoder());
        ServerConnection server = injector.getInstance(ServerConnection.class);
        server.register(eventSink);
        server.attach(serverChannel); 
        
        ClientConnection clientHandler = injector.getInstance(ClientConnection.class);
        EmbeddedByteChannel clientChannel = new EmbeddedByteChannel(new LoggingDecoder());
        clientHandler.attach(clientChannel);
        
        OpCreateSessionAction.Request createRequest = defaultRequest();
        ByteBuf clientMsg = writeOutboundAndRead(clientChannel, createRequest);

        OpCreateSessionAction.Request createInput = writeInboundAndRead(serverChannel, clientMsg);
        assertEquals(createRequest, createInput);

        assertFalse(eventSink.queue.isEmpty());
        ConnectionEventValue<Connection.State> openEvent = eventSink.queue.take();
        assertEquals(server, openEvent.connection());
        assertEquals(Connection.State.OPENING, openEvent.event());
        assertFalse(eventSink.queue.isEmpty());
        openEvent = eventSink.queue.take();
        assertEquals(Connection.State.OPENED, openEvent.event());
        assertFalse(eventSink.queue.isEmpty());

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
