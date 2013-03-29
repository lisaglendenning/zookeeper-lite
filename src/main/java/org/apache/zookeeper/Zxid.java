package org.apache.zookeeper;

import java.util.concurrent.atomic.AtomicLong;

public class Zxid extends AtomicLong {
    private static final long serialVersionUID = 1L;
    
    public static Zxid create(long i) {
        return new Zxid(i);
    }

    public static Zxid create() {
        return new Zxid();
    }
    
    public Zxid(long i) {
        super(i);
    }

    public Zxid() {
        super();
    }
}
