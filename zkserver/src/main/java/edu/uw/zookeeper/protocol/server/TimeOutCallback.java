package edu.uw.zookeeper.protocol.server;

import java.lang.ref.WeakReference;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.FutureCallback;

import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;

public class TimeOutCallback extends TimeOutActor<Object> {

    public static TimeOutCallback create(TimeOutParameters parameters,
            ScheduledExecutorService executor,
            FutureCallback<?> callback) {
        return new TimeOutCallback(parameters, executor, callback);
    }
    
    protected final Logger logger;
    protected final WeakReference<FutureCallback<?>> callback;
    
    public TimeOutCallback(TimeOutParameters parameters,
            ScheduledExecutorService executor,
            FutureCallback<?> callback) {
        super(parameters, executor);
        this.callback = new WeakReference<FutureCallback<?>>(callback);
        this.logger = LogManager.getLogger(getClass());
    }

    @Override
    protected void doRun() {
        if (parameters.remaining() <= 0) {
            FutureCallback<?> callback = this.callback.get();
            if (callback != null) {
                callback.onFailure(new TimeoutException());
            }
        }
    }

    @Override
    protected synchronized void doSchedule() {
        if (callback.get() == null) {
            stop();
        } else {
            super.doSchedule();
        }
    }
    
    @Override
    protected Logger logger() {
        return logger;
    }
}