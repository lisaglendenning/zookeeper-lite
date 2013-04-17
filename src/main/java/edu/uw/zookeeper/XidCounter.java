package edu.uw.zookeeper;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An xid is an id assigned to operation requests by a ZooKeeper client.
 */
public class XidCounter extends AtomicInteger {

    private static final long serialVersionUID = 1L;

    public static XidCounter create() {
        return new XidCounter();
    }

    public static XidCounter create(int i) {
        return new XidCounter(i);
    }

    public XidCounter() {
        super(0);
    }

    public XidCounter(int i) {
        super(i);
    }
}
