package org.apache.zookeeper.util;

import com.google.common.util.concurrent.AbstractExecutionThreadService;

public abstract class TaskMonitor extends AbstractExecutionThreadService {

    @SuppressWarnings("serial")
    static class TaskMonitorException extends Exception {
        public TaskMonitorException(Throwable cause) {
            super(cause);
        }

        public TaskMonitorException() {
            super();
        }
    }

    @Override
    protected void startUp() throws Exception {
        try {
            startTasks();
        } catch (Exception e) {
            shutDown();
            throw e;
        }
    }

    @Override
    protected void shutDown() throws Exception {
        stopTasks();
    }

    @Override
    protected void run() throws Exception {
        // terminate when all monitored tasks have terminated
        boolean running = this.monitorTasks();
        while (running) {
            synchronized (this) {
                if (!isRunning()) {
                    break;
                }
                this.wait();
            }
            running = this.monitorTasks();
        }
    }

    protected abstract boolean monitorTasks() throws Exception;

    protected abstract void startTasks() throws Exception;

    protected abstract void stopTasks() throws Exception;

    @Override
    protected void triggerShutdown() {
        this.wake();
    }

    public synchronized void wake() {
        this.notifyAll();
    }
}
