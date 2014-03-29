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

public class OperationClientExecutor<C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>>
    extends PendingQueueClientExecutor<Operation.Request, Message.ServerResponse<?>, PendingQueueClientExecutor.RequestTask<Operation.Request, Message.ServerResponse<?>>, C, PendingQueueClientExecutor.PendingPromiseTask> {

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
                LogManager.getLogger(OperationClientExecutor.class),
                session,
                connection,
                timeOut,
                executor);
    }

    protected final OperationActor actor;
    protected final AssignXidProcessor xids;
    
    protected OperationClientExecutor(
            AssignXidProcessor xids,
            Logger logger,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService scheduler) {
        super(logger, session, connection, timeOut, scheduler);
        this.xids = xids;
        this.actor = new OperationActor(logger);
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
    protected OperationActor actor() {
        return actor;
    }

    @Override
    protected Logger logger() {
        return actor.logger();
    }

    protected class OperationActor extends ForwardingActor {

        protected OperationActor(
                Logger logger) {
            super(connection, logger);
        }

        @Override
        protected boolean apply(RequestTask<Operation.Request, Message.ServerResponse<?>> input) {
            if (! input.isDone()) {
                // Assign xids here so we can properly track message request -> response
                Message.ClientRequest<?> message = (Message.ClientRequest<?>) xids.apply(input.task());
                PendingPromiseTask task = PendingPromiseTask.create(
                        input.promise(),
                        new SoftReference<Message.ClientRequest<?>>(message), 
                        SettableFuturePromise.<Message.ServerResponse<?>>create());
                if (! pending.send(task)) {
                    input.cancel(true);
                }
            }
            return true;
        }
    }
}
