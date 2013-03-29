package org.apache.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.UncheckedExecutionException;

public class Pipeline<T> extends ListHelper<Pipe<T>> implements
        PipeProcessor<T> {
    protected final Logger logger = LoggerFactory.getLogger(Pipeline.class);

    protected final List<Pipe<T>> pipes;
    protected final CallableMonitor<Void> monitor;

    public Pipeline(ListeningExecutorService executor) {
        synchronized (this) {
            this.pipes = Collections.synchronizedList(Lists
                    .<Pipe<T>> newArrayList());
            this.monitor = new CallableMonitor<Void>(executor);
        }
    }

    public Service getService() {
        return monitor;
    }

    @Override
    protected List<Pipe<T>> delegate() {
        return pipes;
    }

    @Override
    public synchronized Optional<T> apply(T input) throws InterruptedException,
            ExecutionException {
        synchronized (getService()) {
            Service.State state = getService().state();
            checkState(state == Service.State.NEW
                    || state == Service.State.STARTING
                    || state == Service.State.RUNNING);
            Pipe<T> first = getFirst();
            first.put(input);
            return Optional.of(input);
        }
    }

    public synchronized Pipe<T> getFirst() {
        checkState(!isEmpty());
        return get(0);
    }

    public synchronized Pipe<T> getLast() {
        checkState(!isEmpty());
        return get(size() - 1);
    }

    @Override
    public synchronized void add(int index, Pipe<T> item) {
        checkPositionIndex(index, size());
        checkArgument(item != null);
        synchronized (getService()) {
            Service.State state = getService().state();
            checkState(state == Service.State.NEW);

            // manage connecting queues

            Optional<BlockingQueue<T>> inputQ = Optional.absent();
            Optional<Pipe<T>> upstream = Optional.absent();
            if (index > 0) {
                upstream = Optional.of(get(index - 1));
                inputQ = upstream.get().getOutputQ();
            }
            Optional<BlockingQueue<T>> outputQ = Optional.absent();
            Optional<Pipe<T>> downstream = Optional.absent();
            if (index < size()) {
                downstream = Optional.of(get(index));
                outputQ = downstream.get().getInputQ();
            }

            if (item.getInputQ().isPresent()) {
                checkArgument(!(inputQ.isPresent()));
                inputQ = item.getInputQ();
            } else {
                if (!(inputQ.isPresent())) {
                    BlockingQueue<T> q = new LinkedBlockingQueue<T>();
                    inputQ = Optional.of(q);
                }
                item.setInputQ(inputQ.get());
            }
            if (item.getOutputQ().isPresent()) {
                checkArgument(!(outputQ.isPresent()));
                outputQ = item.getOutputQ();
            } else {
                if (!(outputQ.isPresent())) {
                    BlockingQueue<T> q = new LinkedBlockingQueue<T>();
                    outputQ = Optional.of(q);
                }
                item.setOutputQ(outputQ.get());
            }

            monitor.add(item);
            super.add(index, item);

            if (upstream.isPresent()) {
                if (upstream.get().getOutputQ() != inputQ) {
                    upstream.get().setOutputQ(inputQ.get());
                }
            }
            if (downstream.isPresent()) {
                if (downstream.get().getInputQ() != outputQ) {
                    upstream.get().setInputQ(outputQ.get());
                }
            }
        }
    }

    public synchronized Pipe<T> add(PipeProcessor<T> arg0) {
        return add(size(), arg0);
    }

    public synchronized Pipe<T> add(int arg0, PipeProcessor<T> arg1) {
        Pipe<T> pipe = new Pipe<T>(arg1);
        add(arg0, pipe);
        return pipe;
    }

    @Override
    public synchronized Pipe<T> remove(int index) {
        Pipe<T> pipe = null;
        synchronized (getService()) {
            Service.State state = getService().state();
            checkState(state == Service.State.NEW
                    || state == Service.State.TERMINATED
                    || state == Service.State.FAILED);
            pipe = super.remove(index);
            Future<Void> future = monitor.remove(pipe);

            // disconnect from upstream
            synchronized (pipe) {
                if (pipe.getInputQ().isPresent()) {
                    pipe.clearInputQ();
                }
            }

            try {
                if (future != null
                        && !(future.isCancelled() || future.isDone())) {
                    // try to stop nicely
                    pipe.stop();
                    try {
                        future.get(Pipe.timeout, Pipe.timeoutUnit);
                    } catch (TimeoutException e) {
                        // try to cancel
                        if (!(future.isCancelled() || future.isDone())) {
                            boolean cancelled = future.cancel(true);
                            if (!cancelled) {
                                logger.info("Failed to cancel {}", pipe);
                            }
                        }
                        // wait some
                        try {
                            future.get(Pipe.timeout, Pipe.timeoutUnit);
                        } catch (TimeoutException e1) {
                            logger.info("Failed to stop {}", pipe);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                throw new UncheckedExecutionException(e);
            } finally {
                Optional<Pipe<T>> downstream = (index < size()) ? Optional
                        .of(get(index)) : Optional.<Pipe<T>> absent();
                Optional<BlockingQueue<T>> outputQ = pipe.getOutputQ();
                // TODO: attempt to drain
                // disconnect from downstream
                if (downstream.isPresent()) {
                    downstream.get().clearInputQ();
                }

                if (outputQ.isPresent() && !(outputQ.get().isEmpty())) {
                    logger.info("Dropping requests: {}", outputQ.get()
                            .toArray());
                }

                // Finally, reconnect the pipeline
                Optional<Pipe<T>> upstream = (index > 0) ? Optional
                        .of(get(index - 1)) : Optional.<Pipe<T>> absent();
                if (upstream.isPresent()) {
                    Optional<BlockingQueue<T>> inputQ = upstream.get()
                            .getOutputQ();
                    if (downstream.isPresent()) {
                        downstream.get().setInputQ(inputQ.get());
                    } else {
                        upstream.get().clearOutputQ();
                        if (inputQ.isPresent() && !(inputQ.get().isEmpty())) {
                            logger.info("Dropping requests at tail: {}", inputQ
                                    .get().toArray());
                        }
                    }
                }
            }
        }

        return pipe;
    }

}
