package edu.uw.zookeeper.client;

import java.util.Random;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class RandomRequestClient {

    protected static final OpCode[] operations = {
        OpCode.CHECK, 
        OpCode.CREATE, 
        OpCode.GET_CHILDREN, 
        OpCode.DELETE, 
        OpCode.EXISTS, 
        OpCode.GET_DATA, 
        OpCode.SET_DATA, 
        OpCode.SYNC };
    
    protected final Random random;
    protected final ZNodeViewCache<?, ?, ?> client;
    
    public RandomRequestClient(Random random, ZNodeViewCache<?, ?, ?> client) {
        this.random = random;
        this.client = client;
    }
    
    public Records.Request next() {
        OpCode opcode = operations[random.nextInt(operations.length)];
        return null;
    }
}
