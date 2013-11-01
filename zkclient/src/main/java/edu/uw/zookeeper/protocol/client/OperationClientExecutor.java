package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.SessionListener;


public class OperationClientExecutor<C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>>
    extends PendingQueueClientExecutor<Operation.Request, Message.ServerResponse<?>, PendingQueueClientExecutor.RequestTask<Operation.Request, Message.ServerResponse<?>>, C> {

    public static <C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>> OperationClientExecutor<C> newInstance(
            ConnectMessage.Request request,
            C connection,
            ScheduledExecutorService executor) {
        return newInstance(
                request,
                AssignXidProcessor.newInstance(),
                connection,
                executor);
    }

    public static <C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>> OperationClientExecutor<C> newInstance(
            ConnectMessage.Request request,
            AssignXidProcessor xids,
            C connection,
            ScheduledExecutorService executor) {
        return newInstance(
                ConnectTask.connect(connection, request),
                xids,
                connection,
                TimeValue.milliseconds(request.getTimeOut()),
                executor);
    }

    public static <C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>> OperationClientExecutor<C> newInstance(
            ListenableFuture<ConnectMessage.Response> session,
            AssignXidProcessor xids,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        return new OperationClientExecutor<C>(
                xids,
                session,
                connection,
                timeOut,
                executor,
                new StrongConcurrentSet<SessionListener>());
    }
    
    protected final AssignXidProcessor xids;
    
    protected OperationClientExecutor(
            AssignXidProcessor xids,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            IConcurrentSet<SessionListener> listeners) {
        super(session, connection, timeOut, executor, listeners);
        this.xids = xids;
    }

    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            Operation.Request request, Promise<Message.ServerResponse<?>> promise) {
        RequestTask<Operation.Request, Message.ServerResponse<?>> task = 
                RequestTask.of(request, LoggingPromise.create(logger(), promise));
        if (! send(task)) {
            task.cancel(true);
        }
        return task;
    }

    @Override
    protected boolean apply(RequestTask<Operation.Request, Message.ServerResponse<?>> input) {
        if (! input.isDone()) {
            if (state() != State.TERMINATED) {
                // Assign xids here so we can properly track message request -> response
                Message.ClientRequest<?> message = (Message.ClientRequest<?>) xids.apply(input.task());
                write(message, input.promise());
            } else {
                input.cancel(true);
            }
        }
        
        return (state() != State.TERMINATED);
    }
}
