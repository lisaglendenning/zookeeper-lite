package edu.uw.zookeeper.common;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import net.engio.mbassy.bus.BusRuntime;
import net.engio.mbassy.bus.common.PubSubSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;

public abstract class Actors {

    public static <T extends Actor<?>> T stopWhenDone(T actor, ListenableFuture<?> future) {
        future.addListener(new StopActorListener(actor), SameThreadExecutor.getInstance());
        return actor;
    }
    
    public static abstract class QueuedActor<T> extends AbstractActor<T> {
        
        protected final Queue<T> mailbox;
        
        protected QueuedActor(Queue<T> mailbox, Logger logger) {
            super(logger);
            this.mailbox = mailbox;
        }

        public boolean isReady() {
            return !mailbox.isEmpty();
        }
        
        @Override
        protected boolean doSend(T message) {
            if (!mailbox.offer(message)) {
                return false;
            }
            if (!schedule() && (state() == State.TERMINATED)) {
                mailbox.remove(message);
                return false;
            }
            return true;
        }

        @Override
        protected boolean schedule() {
            if (!isReady()) {
                return false;
            } else {
                return super.schedule();
            }
        }

        @Override
        protected void doRun() throws Exception {
            T next;
            while ((next = next()) != null) {
                logger.debug("Applying {} ({})", next, this);
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
            mailbox.clear();
        }
        
        protected @Nullable T next() {
            return mailbox.poll();
        }

        protected abstract boolean apply(T input) throws Exception;
    }
    
    public static abstract class ExecutedQueuedActor<T> extends QueuedActor<T> {

        protected final Executor executor;
        
        protected ExecutedQueuedActor(Executor executor, Queue<T> mailbox, Logger logger) {
            super(mailbox, logger);
            this.executor = executor;
        }

        @Override
        protected void doSchedule() {
            executor.execute(this);
        }
    }
    
    public static abstract class PeekingQueuedActor<T> extends QueuedActor<T> {

        protected PeekingQueuedActor(Queue<T> mailbox, Logger logger) {
            super(mailbox, logger);
        }

        @Override
        protected @Nullable T next() {
            return mailbox.peek();
        }
    }

    public static abstract class ExecutedPeekingQueuedActor<T> extends PeekingQueuedActor<T> {

        protected final Executor executor;
        
        protected ExecutedPeekingQueuedActor(
                Executor executor, Queue<T> mailbox, Logger logger) {
            super(mailbox, logger);
            this.executor = executor;
        }

        @Override
        protected void doSchedule() {
            executor.execute(this);
        }
    }

    public static final class StopActorListener implements Runnable {
        
        private final Actor<?> actor;
        
        public StopActorListener(Actor<?> actor) {
            this.actor = actor;
        }
        
        @Override
        public void run() {
            actor.stop();
        }
    }
    
    public static abstract class CallbackActor<T> extends ExecutedQueuedActor<T> {

        protected CallbackActor(Executor executor, Queue<T> mailbox, Logger logger) {
            super(executor, mailbox, logger);
        }
        
        @Override
        protected synchronized void doRun() {
            T next;
            while ((next = next()) != null) {
                logger.debug("Applying {} ({})", next, this);
                apply(next);
            }
        }

        @Override
        protected abstract boolean apply(T input);

        @Override
        protected void doStop() {
             doRun();
        }

        protected synchronized void flush(T input) {
            doRun();
            logger.debug("Flushing {} ({})", input, this);
            apply(input);
        }
    }
    
    public static class ActorExecutor extends CallbackActor<Runnable> implements Executor {

        public static ActorExecutor newInstance(Executor executor) {
            return new ActorExecutor(
                    executor,
                    new ConcurrentLinkedQueue<Runnable>(), 
                    LogManager.getLogger(ActorExecutor.class));
        }
        
        protected ActorExecutor(
                Executor executor,
                Queue<Runnable> mailbox,
                Logger logger) {
            super(executor, mailbox, logger);
        }
        
        @Override
        public void execute(Runnable command) {
            if (!send(command)) {
                flush(command);
            }
        }

        @Override
        protected boolean apply(Runnable input) {
            input.run();
            return true;
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

        protected final BusRuntime runtime;
        protected final PubSubSupport<? super T> publisher;
        
        protected ActorPublisher(
                PubSubSupport<? super T> publisher,
                Executor executor, 
                Queue<T> mailbox,
                Logger logger) {
            super(executor, mailbox, logger);
            this.publisher = publisher;
            this.runtime = new BusRuntime(this);
        }

        @Override
        public void publish(T event) {
            if (!send(event)) {
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
        public BusRuntime getRuntime() {
            return runtime;
        }

        @Override
        protected boolean apply(T input) {
            publisher.publish(input);
            return true;
        }
    }
}
