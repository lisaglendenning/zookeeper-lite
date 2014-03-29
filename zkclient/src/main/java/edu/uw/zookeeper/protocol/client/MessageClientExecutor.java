package edu.uw.zookeeper.protocol.client;

import java.lang.ref.SoftReference;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;


public class MessageClientExecutor<C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>>
    extends PendingQueueClientExecutor<Message.ClientRequest<?>, Message.ServerResponse<?>, PendingQueueClientExecutor.RequestTask<Message.ClientRequest<?>, Message.ServerResponse<?>>, C, PendingQueueClientExecutor.PendingPromiseTask> {

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
                LogManager.getLogger(MessageClientExecutor.class),
                session,
                connection,
                timeOut,
                executor);
    }
    
    protected final MessageActor actor;
    
    protected MessageClientExecutor(
            Logger logger,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService scheduler) {
        super(logger, session, connection, timeOut, scheduler);
        this.actor = new MessageActor(logger);
    }

    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            Message.ClientRequest<?> request, Promise<Message.ServerResponse<?>> promise) {
        RequestTask<Message.ClientRequest<?>, Message.ServerResponse<?>> task = 
                RequestTask.<Message.ClientRequest<?>, Message.ServerResponse<?>>of(request, LoggingPromise.create(logger(), promise));
        if (! send(task)) {
            task.cancel(true);
        }
        return task;
    }

    @Override
    public void onSuccess(PendingPromiseTask result) {
        try {
            Message.ServerResponse<?> response = result.get();
            result.getPromise().set(response);
        } catch (ExecutionException e) {
            result.getPromise().setException(e.getCause());
            onFailure(e.getCause());
        } catch (CancellationException e) {
            result.getPromise().cancel(true);
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    protected MessageActor actor() {
        return actor;
    }

    @Override
    protected Logger logger() {
        return actor.logger();
    }

    protected class MessageActor extends ForwardingActor {

        protected MessageActor(
                Logger logger) {
            super(connection, logger);
        }

        @Override
        protected boolean apply(RequestTask<Message.ClientRequest<?>, Message.ServerResponse<?>> input) {
            if (! input.isDone()) {
                PendingPromiseTask task = PendingPromiseTask.create(
                        input.promise(),
                        new SoftReference<Message.ClientRequest<?>>(input.task()), 
                        SettableFuturePromise.<Message.ServerResponse<?>>create());
                if (! pending.send(task)) {
                    input.cancel(true);
                }
            }
            return true;
        }
    }
}
