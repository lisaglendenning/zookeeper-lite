package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;

import edu.uw.zookeeper.RequestExecutorService;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.ZxidCounter;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.event.SessionStateEvent;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.FilteredProcessor;
import edu.uw.zookeeper.util.FilteredProcessors;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.OptionalProcessor;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.ProcessorBridge;
import edu.uw.zookeeper.util.SettableTask;

public class RequestExecutor extends ForwardingEventful implements
        RequestExecutorService, Callable<ListenableFuture<Operation.Result>> {

    public static class Factory implements RequestExecutorService.Factory {

        public static Factory create(Provider<Eventful> eventfulFactory,
                ExecutorService executor, SessionManager sessions, ZxidCounter zxid) {
            return new Factory(eventfulFactory, executor, sessions, zxid);
        }

        protected final Map<Long, RequestExecutorService> executors;
        protected final Provider<Eventful> eventfulFactory;
        protected final ExecutorService executor;
        protected final ZxidCounter zxid;
        protected final SessionManager sessions;
        protected final RequestExecutorService anonymousExecutor;

        @Inject
        protected Factory(Provider<Eventful> eventfulFactory,
                ExecutorService executor, SessionManager sessions, ZxidCounter zxid) {
            this(eventfulFactory, executor, sessions, zxid, Collections
                    .synchronizedMap(Maps
                            .<Long, RequestExecutorService> newHashMap()));
        }

        protected Factory(Provider<Eventful> eventfulFactory,
                ExecutorService executor, SessionManager sessions, ZxidCounter zxid,
                Map<Long, RequestExecutorService> executors) {
            this.executors = executors;
            this.eventfulFactory = eventfulFactory;
            this.executor = executor;
            this.zxid = zxid;
            this.sessions = sessions;
            this.anonymousExecutor = newExecutor();
            sessions.register(this);
        }

        protected Map<Long, RequestExecutorService> executors() {
            return executors;
        }

        public ZxidCounter zxid() {
            return zxid;
        }

        public SessionManager sessions() {
            return sessions;
        }

        public ExecutorService executor() {
            return executor;
        }

        public RequestExecutorService get(Session session) {
            return get(session.id());
        }

        @Override
        public RequestExecutorService get() {
            return anonymousExecutor;
        }

        @Override
        public RequestExecutorService get(long sessionId) {
            RequestExecutorService executor;
            synchronized (executors()) {
                executor = executors().get(sessionId);
                if (executor == null && sessions.get(sessionId) != null) {
                    executor = newExecutor(sessionId);
                    executors().put(sessionId, executor);
                }
            }
            return executor;
        }

        @Subscribe
        public void handleEvent(SessionStateEvent event) {
            Session session = event.session();
            RequestExecutorService executor = get(session);
            switch (event.event()) {
            case SESSION_CLOSED: {
                executors().remove(session.id());
                break;
            }
            case SESSION_EXPIRED: {
                // here is where the session closing is actually initiated
                // there's no point sending the close response
                // to the client because it doesn't have the context for
                // the message (no xid!)
                if (executor != null) {
                    Operation.Request request = Operations.Requests
                            .create(Operation.CLOSE_SESSION);
                    try {
                        executor.submit(request);
                    } catch (IllegalStateException e) {
                    }
                }
                break;
            }
            default:
                break;
            }
        }

        protected RequestExecutorService newExecutor() {
            return RequestExecutor.create(eventfulFactory, executor(),
                    getResponseProcessor(getAnonymousProcessor()));
        }

        protected RequestExecutorService newExecutor(long sessionId) {
            return RequestExecutor.create(eventfulFactory, executor(),
                    getResponseProcessor(getSessionProcessor(sessionId)));
        }

        protected Processor<Operation.Request, Operation.Response> getAnonymousProcessor() {
            @SuppressWarnings("unchecked")
            Processor<Operation.Request, Operation.Response> requestProcessor = FilteredProcessors
                    .create(OpCreateSessionProcessor.create(sessions),
                            FilteredProcessor.create(
                                    OpRequestProcessor.NotEqualsFilter
                                            .create(Operation.CREATE_SESSION),
                                    OpRequestErrorProcessor.create()));
            return requestProcessor;
        }

        protected Processor<Operation.Request, Operation.Response> getSessionProcessor(
                long sessionId) {
            @SuppressWarnings("unchecked")
            Processor<Operation.Request, Operation.Response> requestProcessor = FilteredProcessors
                    .create(FilteredProcessor.create(
                            OpRequestProcessor.EqualsFilter
                                    .create(Operation.CREATE_SESSION),
                            OpRequestErrorProcessor.create()),
                            OpCloseSessionProcessor.create(sessionId, sessions),
                            FilteredProcessor.create(
                                    OpRequestProcessor.NotEqualsFilter
                                            .create(Operation.CLOSE_SESSION),
                                    OpRequestProcessor.create()));
            return requestProcessor;
        }

        protected Processor<Operation.Request, Operation.Result> getResponseProcessor(
                Processor<Operation.Request, Operation.Response> requestProcessor) {
            requestProcessor = OpErrorProcessor.create(requestProcessor);
            requestProcessor = ProcessorBridge.create(requestProcessor,
                    OptionalProcessor.create(AssignZxidProcessor.create(zxid)));
            Processor<Operation.Request, Operation.Result> processor = OpResultProcessor
                    .create(requestProcessor);
            return processor;
        }
    }

    public static RequestExecutorService create(
            Provider<Eventful> eventfulFactory, ExecutorService executor,
            Processor<Operation.Request, Operation.Result> processor) {
        return new RequestExecutor(eventfulFactory, executor, processor);
    }

    protected final Logger logger = LoggerFactory
            .getLogger(RequestExecutor.class);
    protected final AtomicBoolean scheduled;
    protected final ExecutorService executor;
    protected final Processor<Operation.Request, Operation.Result> processor;
    protected final BlockingQueue<SettableTask<Operation.Request, Operation.Result>> requests;

    @Inject
    protected RequestExecutor(Provider<Eventful> eventfulFactory,
            ExecutorService executor,
            Processor<Operation.Request, Operation.Result> processor) {
        this(
                eventfulFactory,
                executor,
                processor,
                new LinkedBlockingQueue<SettableTask<Operation.Request, Operation.Result>>());
    }

    protected RequestExecutor(
            Provider<Eventful> eventfulFactory,
            ExecutorService executor,
            Processor<Operation.Request, Operation.Result> processor,
            BlockingQueue<SettableTask<Operation.Request, Operation.Result>> requests) {
        super(eventfulFactory.get());
        this.executor = executor;
        this.processor = processor;
        this.requests = requests;
        this.scheduled = new AtomicBoolean(false);
    }

    protected ExecutorService executor() {
        return executor;
    }

    protected Processor<Operation.Request, Operation.Result> processor() {
        return processor;
    }

    protected BlockingQueue<SettableTask<Operation.Request, Operation.Result>> requests() {
        return requests;
    }

    @Override
    public ListenableFuture<Operation.Result> submit(Operation.Request request) {
        checkNotNull(request);
        logger.debug("Submitting request {}", request);
        SettableTask<Operation.Request, Operation.Result> task = newTask(request);
        requests().add(task);
        schedule();
        return task.future();
    }

    public ListenableFuture<Operation.Result> call() throws Exception {
        scheduled.compareAndSet(true, false);

        SettableTask<Operation.Request, Operation.Result> task = null;
        ListenableFuture<Operation.Result> future = null;
        synchronized (this) {
            task = requests().peek();
            if (task != null) {
                future = apply(task);
                if (future.isDone()) {
                    requests().take();
                } else {
                    future = null;
                }
            }
        }

        if (!requests().isEmpty()) {
            schedule();
        }

        return future;
    }

    protected ListenableFuture<Operation.Result> apply(
            SettableTask<Operation.Request, Operation.Result> task) {
        logger.debug("Applying task {}", task);

        Operation.Result result = null;
        Operation.Request request = task.task();
        SettableFuture<Operation.Result> future = task.future();
        try {
            result = processor().apply(request);
        } catch (Throwable t) {
            future.setException(t);
        }
        if (result != null) {
            future.set(result);
        }
        return future;
    }

    protected SettableTask<Operation.Request, Operation.Result> newTask(
            Operation.Request request) {
        return SettableTask.create(request);
    }

    protected void schedule() {
        if (scheduled.compareAndSet(false, true)) {
            executor().submit(this);
        }
    }
}
