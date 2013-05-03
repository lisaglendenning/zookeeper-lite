package edu.uw.zookeeper.client;

import java.util.concurrent.RejectedExecutionException;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.protocol.OpAction;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.TaskExecutor;

public class SessionClient implements Reference<ClientProtocolConnection>, TaskExecutor<Operation.Request, Operation.SessionReply> {

    public static SessionClient newInstance(
            ClientProtocolConnection client) {
        return new SessionClient(AssignXidProcessor.newInstance(), client);
    }
    
    public static SessionClient newInstance(
            Processor<Operation.Request, Operation.SessionRequest> processor,
            ClientProtocolConnection client) {
        return new SessionClient(processor, client);
    }
    
    protected final Processor<Operation.Request, Operation.SessionRequest> processor;
    protected final ClientProtocolConnection client;
    protected volatile Session session;
    
    protected SessionClient(
            Processor<Operation.Request, Operation.SessionRequest> processor,
            ClientProtocolConnection client) {
        this.processor = processor;
        this.client = client;
        this.session = Session.uninitialized();
    }
    
    public Session session() {
        return session;
    }
    
    public void connect() {
        try {
            this.session = client.connect().get();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public void disconnect() {
        switch (client.state()) {
        case CONNECTING:
        case CONNECTED:
            try {
                submit(OpAction.Request.create(OpCode.CLOSE_SESSION)).get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            break;
        default:
            break;
        }
    }

    @Override
    public ClientProtocolConnection get() {
        return client;
    }

    @Override
    public ListenableFuture<Operation.SessionReply> submit(Operation.Request request) {                   
        Operation.SessionRequest message;
        try {
            message = processor.apply(request);
        } catch (Exception e) {
            throw new RejectedExecutionException(e);
        }
        return client.submit(message);
    }
}