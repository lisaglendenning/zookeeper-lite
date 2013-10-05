package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.ScheduledExecutorService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;


public class OperationClientExecutor<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>>
    extends AbstractConnectionClientExecutor<Operation.Request, PromiseTask<Operation.Request, Message.ServerResponse<?>>, C> {

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> OperationClientExecutor<C> newInstance(
            ConnectMessage.Request request,
            C connection,
            ScheduledExecutorService executor) {
        return newInstance(
                request,
                AssignXidProcessor.newInstance(),
                connection,
                executor);
    }

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> OperationClientExecutor<C> newInstance(
            ConnectMessage.Request request,
            AssignXidProcessor xids,
            C connection,
            ScheduledExecutorService executor) {
        return newInstance(
                ConnectTask.create(connection, request),
                xids,
                connection,
                TimeValue.milliseconds(request.getTimeOut()),
                executor);
    }

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> OperationClientExecutor<C> newInstance(
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
                executor);
    }
    
    protected final AssignXidProcessor xids;
    
    protected OperationClientExecutor(
            AssignXidProcessor xids,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        super(session, connection, timeOut, executor);
        this.xids = xids;
    }

    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            Operation.Request request, Promise<Message.ServerResponse<?>> promise) {
        PromiseTask<Operation.Request, Message.ServerResponse<?>> task = 
                PromiseTask.of(request, LoggingPromise.create(logger, promise));
        if (! send(task)) {
            task.cancel(true);
        }
        return task;
    }

    @Override
    protected boolean apply(PromiseTask<Operation.Request, Message.ServerResponse<?>> input) {
        if (! input.isDone()) {
            if (state() != State.TERMINATED) {
                // Assign xids here so we can properly track message request -> response
                Message.ClientRequest<?> message = (Message.ClientRequest<?>) xids.apply(input.task());
                PendingTask task = new PendingTask(message.xid(), this, input);
                if (task.xid() != OpCodeXid.PING.xid()) {
                    // task needs to be in the queue before calling write
                    pending.add(task);
                }
                try {
                    ListenableFuture<? extends Message.ClientRequest<?>> writeFuture = connection.write(message);
                    Futures.addCallback(writeFuture, task, executor());
                } catch (Throwable t) {
                    task.onFailure(t);
                }
            } else {
                input.cancel(true);
            }
        }
        
        return (state() != State.TERMINATED);
    }
}
