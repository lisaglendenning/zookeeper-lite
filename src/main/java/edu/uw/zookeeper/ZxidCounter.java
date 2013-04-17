package edu.uw.zookeeper;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A zxid is an id assigned to operations by a ZooKeeper server.
 */
public class ZxidCounter extends AtomicLong {

    private static final long serialVersionUID = 1L;

    public static ZxidCounter create() {
        return new ZxidCounter();
    }

    public static ZxidCounter create(long i) {
        return new ZxidCounter(i);
    }

    public ZxidCounter() {
        super(0);
    }

    public ZxidCounter(long i) {
        super(i);
    }
}
