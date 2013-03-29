package org.apache.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

public class Pipe<T> implements PipeProcessor<T>, Callable<Void> {

    public T POISON = null;

    public static long timeout = 100;
    public static TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;

    protected final Logger logger = LoggerFactory.getLogger(Pipe.class);
    protected PipeProcessor<T> processor = null;
    protected Optional<BlockingQueue<T>> inputQ = Optional.absent();
    protected Optional<BlockingQueue<T>> outputQ = Optional.absent();
    protected AtomicBoolean running = new AtomicBoolean(false);

    public Pipe(PipeProcessor<T> processor, Optional<BlockingQueue<T>> inputQ,
            Optional<BlockingQueue<T>> outputQ) {
        checkArgument(processor != null);
        checkArgument(inputQ != null);
        checkArgument(outputQ != null);
        synchronized (this) {
            this.inputQ = inputQ;
            this.outputQ = outputQ;
            this.processor = processor;
        }
    }

    public Pipe(PipeProcessor<T> processor, Optional<BlockingQueue<T>> inputQ) {
        this(processor, inputQ, Optional.<BlockingQueue<T>> absent());
    }

    public Pipe(PipeProcessor<T> processor) {
        this(processor, Optional.<BlockingQueue<T>> absent(), Optional
                .<BlockingQueue<T>> absent());
    }

    protected Pipe() {
    }

    public synchronized Optional<BlockingQueue<T>> getInputQ() {
        return Optional.fromNullable(inputQ.orNull());
    }

    protected synchronized void setInputQ(Optional<BlockingQueue<T>> inputQ) {
        checkArgument(inputQ != null);
        this.inputQ = inputQ;
        if (inputQ.isPresent()) {
            this.notifyAll();
        }
    }

    public synchronized void setInputQ(BlockingQueue<T> inputQ) {
        setInputQ(Optional.of(inputQ));
    }

    public synchronized void clearInputQ() {
        setInputQ(Optional.<BlockingQueue<T>> absent());
    }

    public synchronized Optional<BlockingQueue<T>> getOutputQ() {
        return Optional.fromNullable(outputQ.orNull());
    }

    protected synchronized void setOutputQ(Optional<BlockingQueue<T>> outputQ) {
        checkArgument(outputQ != null);
        this.outputQ = outputQ;
    }

    public synchronized void setOutputQ(BlockingQueue<T> outputQ) {
        setOutputQ(Optional.of(outputQ));
    }

    public synchronized void clearOutputQ() {
        setOutputQ(Optional.<BlockingQueue<T>> absent());
    }

    public void put(T input) throws InterruptedException {
        checkArgument(input != null);
        BlockingQueue<T> inputQ = null;
        synchronized (this) {
            inputQ = this.inputQ.orNull();
        }
        checkState(inputQ != null);
        inputQ.put(input);
    }

    @Override
    public Optional<T> apply(T input) throws Exception {
        checkArgument(input != null);
        // pass along poison
        Optional<T> output = (input != POISON) ? processor.apply(input)
                : Optional.of(input);
        return output;
    }

    public boolean isRunning() {
        return running.get();
    }

    @Override
    public Void call() throws Exception {
        synchronized (this) {
            running.compareAndSet(false, true);
            this.notifyAll();
        }
        try {
            while (running.get()) {
                // wait until we have an inputQ
                BlockingQueue<T> inputQ = null;
                synchronized (this) {
                    while (!(this.inputQ.isPresent())) {
                        this.wait();
                    }
                    inputQ = this.inputQ.get();
                }
                assert inputQ != null;

                // Pull input
                T input = inputQ.poll(timeout, timeoutUnit);
                if (input == null) {
                    continue;
                }
                logger.trace("{} RECEIVED {}", processor, input);
                Optional<T> output = Optional.absent();
                try {
                    output = apply(input);
                } catch (Exception e) {
                    logger.error("Unexpected exception processing {}", input, e);
                    throw e;
                }

                // Push output
                if (output.isPresent()) {
                    BlockingQueue<T> outputQ = null;
                    synchronized (this) {
                        outputQ = this.outputQ.orNull();
                    }
                    if (outputQ != null) {
                        outputQ.add(output.get());
                    } else {
                        logger.trace("Dropping {}", output);
                    }
                }

                // Stop if poisoned
                if (input == POISON) {
                    running.set(false);
                }
            }
        } finally {
            running.compareAndSet(true, false);
        }
        return null;
    }

    public boolean poison(long timeout, TimeUnit timeoutUnit)
            throws InterruptedException {
        if (!isRunning()) {
            return false;
        }
        Optional<BlockingQueue<T>> prevQ = Optional.absent();
        BlockingQueue<T> poisonQ = null;
        synchronized (this) {
            if (inputQ.isPresent()) {
                poisonQ = inputQ.get();
                prevQ = inputQ;
            } else {
                poisonQ = new SynchronousQueue<T>();
                setInputQ(poisonQ);
            }
        }
        assert (poisonQ != null);

        boolean poisoned = false;
        try {
            poisoned = poisonQ.offer(POISON, timeout, timeoutUnit);
        } finally {
            // remove poisonQ
            if (!(prevQ.isPresent())) {
                setInputQ(prevQ);
            }
        }

        return poisoned;
    }

    public boolean poison() throws InterruptedException {
        return poison(timeout, timeoutUnit);
    }

    public void stop() throws InterruptedException {
        running.compareAndSet(true, false);
    }

}
