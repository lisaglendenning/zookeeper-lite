package edu.uw.zookeeper.common;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import net.engio.mbassy.PubSubSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Queues;

public abstract class Actors {
    
    public static abstract class QueuedActor<T> extends AbstractActor<T> {
        
        protected QueuedActor() {
            super();
        }

        protected QueuedActor(State state) {
            super(state);
        }
        
        @Override
        protected boolean doSend(T message) {
            if (! mailbox().offer(message)) {
                return false;
            }
            if (! schedule() && (state() == State.TERMINATED)) {
                mailbox().remove(message);
                return false;
            }
            return true;
        }

        @Override
        protected boolean schedule() {
            if (! mailbox().isEmpty()) {
                return super.schedule();
            } else {
                return false;
            }
        }

        @Override
        protected void doRun() throws Exception {
            T next;
            while ((next = mailbox().poll()) != null) {
                logger().debug("Applying {} ({})", next, this);
                if (! apply(next) || (state() == State.TERMINATED)) {
                    break;
                }
            }
        }

        @Override
        protected void runExit() {
            if (state.compareAndSet(State.RUNNING, State.WAITING)) {
                schedule();
            }
        }

        @Override
        protected void doStop() {
            mailbox().clear();
        }

        protected abstract boolean apply(T input) throws Exception;

        protected abstract Queue<T> mailbox();

    }
    
    public static abstract class ExecutedQueuedActor<I> extends QueuedActor<I> {

        protected abstract Executor executor();

        @Override
        protected void doSchedule() {
            executor().execute(this);
        }
    }
    
    public static abstract class PeekingQueuedActor<T> extends QueuedActor<T> {
        
        protected PeekingQueuedActor() {
            super();
        }

        protected PeekingQueuedActor(State state) {
            super(state);
        }
        
        public abstract boolean isReady();
        
        @Override
        protected boolean schedule() {
            if (isReady()) {
                return super.schedule();
            } else {
                return false;
            }
        }

        @Override
        protected void doRun() throws Exception {
            T next;
            while ((next = mailbox().peek()) != null) {
                logger().debug("Applying {} ({})", next, this);
                if (! apply(next) || (state() == State.TERMINATED)) {
                    break;
                }
            }
        }
    }

    public static abstract class ExecutedPeekingQueuedActor<I> extends PeekingQueuedActor<I> {

        protected abstract Executor executor();

        @Override
        protected void doSchedule() {
            executor().execute(this);
        }
    }
    
    public static abstract class CallbackActor<T> extends ExecutedQueuedActor<T> {

        protected CallbackActor() {}
        
        protected synchronized void flush(T input) {
            doRun();
            apply(input);
        }

        @Override
        protected synchronized void doRun() {
            T next;
            while ((next = mailbox().poll()) != null) {
                apply(next);
            }
        }

        @Override
        protected abstract boolean apply(T input);

        @Override
        protected void doStop() {
             doRun();
        }
    }
    
    public static class ActorExecutor extends CallbackActor<Runnable> implements Executor {

        public static ActorExecutor newInstance(Executor executor) {
            return new ActorExecutor(
                    new ConcurrentLinkedQueue<Runnable>(), 
                    executor,
                    LogManager.getLogger(ActorExecutor.class));
        }
        
        protected final Queue<Runnable> mailbox;
        protected final Executor executor;
        protected final Logger logger;
        
        protected ActorExecutor(
                Queue<Runnable> mailbox,
                Executor executor,
                Logger logger) {
            this.mailbox = mailbox;
            this.executor = executor;
            this.logger = logger;
        }
        
        @Override
        public void execute(Runnable command) {
            if (! send(command)) {
                flush(command);
            }
        }

        @Override
        protected boolean apply(Runnable input) {
            input.run();
            return true;
        }

        @Override
        protected Executor executor() {
            return executor;
        }

        @Override
        protected Queue<Runnable> mailbox() {
            return mailbox;
        }

        @Override
        protected Logger logger() {
            return logger;
        }
    }
    
    public static class ActorPublisher<T> extends CallbackActor<T> implements PubSubSupport<T> {

        public static <T> ActorPublisher<T> newPublisher(
                PubSubSupport<T> publisher,
                Executor executor) {
            return newPublisher(
                    publisher,
                    executor,
                    LogManager.getLogger(ActorPublisher.class));
        }

        public static <T> ActorPublisher<T> newPublisher(
                PubSubSupport<T> publisher,
                Executor executor,
                Logger logger) {
            return new ActorPublisher<T>(
                    publisher,
                    executor,
                    Queues.<T>newConcurrentLinkedQueue(),
                    logger);
        }

        protected final Executor executor;
        protected final Queue<T> mailbox;
        protected final PubSubSupport<? super T> publisher;
        protected final Logger logger;
        
        protected ActorPublisher(
                PubSubSupport<? super T> publisher,
                Executor executor, 
                Queue<T> mailbox,
                Logger logger) {
            super();
            this.publisher = publisher;
            this.mailbox = mailbox;
            this.executor = executor;
            this.logger = logger;
        }

        @Override
        public void publish(T event) {
            if (! send(event)) {
                flush(event);
            }
        }
        
        @Override
        public void subscribe(Object listener) {
            publisher.subscribe(listener);
        }

        @Override
        public boolean unsubscribe(Object listener) {
            return publisher.unsubscribe(listener);
        }
        
        @Override
        protected boolean apply(T input) {
            publisher.publish(input);
            return true;
        }

        @Override
        protected Queue<T> mailbox() {
            return mailbox;
        }

        @Override
        protected Executor executor() {
            return executor;
        }

        @Override
        protected Logger logger() {
            return logger;
        }
    }
}
