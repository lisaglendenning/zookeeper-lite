package org.apache.zookeeper.util;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;

public class SimplePipeProcessor<T> implements PipeProcessor<T> {
    
    public static <T> SimplePipeProcessor<T> create() {
        return new SimplePipeProcessor<T>();
    }

    public static <T> SimplePipeProcessor<T> create(PipeProcessor<T> processor) {
        return new SimplePipeProcessor<T>(processor);
    }

    public static <T> SimplePipeProcessor<T> create(PipeProcessor<T> processor, Predicate<T> filter) {
        return new SimplePipeProcessor<T>(processor, filter);
    }

    protected Optional<Predicate<T>> filter = Optional.absent();
    protected Optional<PipeProcessor<T>> processor = Optional.absent();
    protected Optional<PipeProcessor<T>> nextProcessor = Optional.absent();

    public SimplePipeProcessor() {
        this(Optional.<PipeProcessor<T>> absent(), Optional
                .<Predicate<T>> absent(), Optional.<PipeProcessor<T>> absent());
    }

    public SimplePipeProcessor(PipeProcessor<T> processor) {
        this(Optional.of(processor), Optional.<Predicate<T>> absent(), Optional
                .<PipeProcessor<T>> absent());
    }

    public SimplePipeProcessor(PipeProcessor<T> processor, Predicate<T> filter) {
        this(Optional.of(processor), Optional.of(filter), Optional
                .<PipeProcessor<T>> absent());
    }

    public SimplePipeProcessor(Optional<PipeProcessor<T>> processor,
            Optional<Predicate<T>> filter,
            Optional<PipeProcessor<T>> nextProcessor) {
        checkArgument(processor != null);
        checkArgument(filter != null);
        checkArgument(nextProcessor != null);
        synchronized (this) {
            this.processor = processor;
            this.filter = filter;
            this.nextProcessor = nextProcessor;
        }
    }

    public Optional<PipeProcessor<T>> getNextProcessor() {
        return nextProcessor;
    }

    protected synchronized void setNextProcessor(
            Optional<PipeProcessor<T>> nextProcessor) {
        checkArgument(nextProcessor != null);
        this.nextProcessor = nextProcessor;
    }

    public synchronized void setNextProcessor(PipeProcessor<T> nextProcessor) {
        setNextProcessor(Optional.of(nextProcessor));
    }

    public synchronized void clearNextProcessor() {
        setNextProcessor(Optional.<PipeProcessor<T>> absent());
    }

    @Override
    public Optional<T> apply(T input) throws Exception {
        checkArgument(input != null);
        boolean doProcess = true;
        Predicate<T> filter = null;
        synchronized (this) {
            filter = this.filter.orNull();
        }
        if (filter != null) {
            try {
                doProcess = filter.apply(input);
            } catch (IllegalArgumentException e) {
                throw new ExecutionException(e);
            }
        }

        Optional<T> output = Optional.of(input);
        if (doProcess) {
            PipeProcessor<T> processor = null;
            synchronized (this) {
                processor = this.processor.orNull();
            }
            if (processor != null) {
                output = processor.apply(input);
            }
        }

        if (output.isPresent()) {
            PipeProcessor<T> nextProcessor = null;
            synchronized (this) {
                nextProcessor = this.nextProcessor.orNull();
            }
            if (nextProcessor != null) {
                output = nextProcessor.apply(output.get());
            }
        }

        return output;
    }

}
