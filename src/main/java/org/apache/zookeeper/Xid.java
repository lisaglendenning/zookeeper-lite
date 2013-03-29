package org.apache.zookeeper;

import java.util.concurrent.atomic.AtomicInteger;

public class Xid extends AtomicInteger {
    
    private static final long serialVersionUID = 1L;

    public static Xid create() {
        return new Xid();
    }
    
    public static Xid create(int i) {
        return new Xid(i);
    }
    
    public Xid() {
        super();
    }
    
    public Xid(int i) {
        super(i);
    }
}
