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


public class MessageClientExecutor<C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>>
    extends PendingQueueClientExecutor<Message.ClientRequest<?>, Message.ServerResponse<?>, PendingQueueClientExecutor.RequestTask<Message.ClientRequest<?>, Message.ServerResponse<?>>, C> {

    public static <C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>> MessageClientExecutor<C> newInstance(
            ConnectMessage.Request request,
            C connection,
            ScheduledExecutorService executor) {
        return newInstance(
                ConnectTask.connect(connection, request),
                connection,
                TimeValue.milliseconds(request.getTimeOut()),
                executor);
    }

    public static <C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>> MessageClientExecutor<C> newInstance(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        return new MessageClientExecutor<C>(
                session,
                connection,
                timeOut,
                executor,
                new StrongConcurrentSet<SessionListener>());
    }
    
    protected MessageClientExecutor(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            IConcurrentSet<SessionListener> listeners) {
        super(session, connection, timeOut, executor, listeners);
    }

    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            Message.ClientRequest<?> request, Promise<Message.ServerResponse<?>> promise) {
        RequestTask<Message.ClientRequest<?>, Message.ServerResponse<?>> task = 
                RequestTask.<Message.ClientRequest<?>, Message.ServerResponse<?>>of(request, LoggingPromise.create(logger, promise));
        if (! send(task)) {
            task.cancel(true);
        }
        return task;
    }

    @Override
    protected boolean apply(RequestTask<Message.ClientRequest<?>, Message.ServerResponse<?>> input) {
        if (! input.isDone()) {
            write(input.task(), input.promise());
        }
        return true;
    }
}
