package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.ScheduledExecutorService;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;


public class MessageClientExecutor<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>>
    extends PendingQueueClientExecutor<Message.ClientRequest<?>, Message.ServerResponse<?>, PendingQueueClientExecutor.RequestTask<Message.ClientRequest<?>, Message.ServerResponse<?>>, C> {

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> MessageClientExecutor<C> newInstance(
            ConnectMessage.Request request,
            C connection,
            ScheduledExecutorService executor) {
        return newInstance(
                ConnectTask.create(connection, request),
                connection,
                TimeValue.milliseconds(request.getTimeOut()),
                executor);
    }

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> MessageClientExecutor<C> newInstance(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        return new MessageClientExecutor<C>(
                session,
                connection,
                timeOut,
                executor);
    }
    
    protected MessageClientExecutor(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        super(session, connection, timeOut, executor);
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
            if (state() != State.TERMINATED) {
                write(input.task(), input.promise());
            } else {
                input.cancel(true);
            }
        }
        
        return (state() != State.TERMINATED);
    }
}
