package edu.uw.zookeeper.data;

import edu.uw.zookeeper.protocol.Operation;

public interface TxnRequest {
    public interface Header {
        long session();
        long zxid();
        long time();
    }
    
    Header header();
    
    Operation.Request record();
}
