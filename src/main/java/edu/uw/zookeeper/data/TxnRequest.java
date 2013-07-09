package edu.uw.zookeeper.data;

import edu.uw.zookeeper.protocol.proto.Records;


public interface TxnRequest {
    public interface Header {
        long session();
        long zxid();
        long time();
    }
    
    Header header();
    
    Records.Request record();
}
