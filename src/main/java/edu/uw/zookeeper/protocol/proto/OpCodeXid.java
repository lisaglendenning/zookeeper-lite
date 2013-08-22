package edu.uw.zookeeper.protocol.proto;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.protocol.Operation;

// These are hardcoded in various places in zookeeper code...
public enum OpCodeXid implements Operation.Coded, Operation.RequestId {
    // response only
    NOTIFICATION(-1, OpCode.NOTIFICATION), // zxid is -1
    // request and response
    PING(-2, OpCode.PING), // zxid is lastZxid
    // request and response
    AUTH(-4, OpCode.AUTH), // zxid is 0?
    // request and response
    SET_WATCHES(-8, OpCode.SET_WATCHES); // zxid is lastZxid

    private static ImmutableMap<Integer, OpCodeXid> byXid = Maps
            .uniqueIndex(Iterators.forArray(OpCodeXid.values()), 
                    new Function<OpCodeXid, Integer>() {
                        @Override public Integer apply(OpCodeXid input) {
                            return input.xid();
                        }});

    public static boolean has(int xid) {
        return byXid.containsKey(xid);
    }

    public static final OpCodeXid of(int xid) {
        checkArgument(byXid.containsKey(xid));
        return byXid.get(xid);
    }
    
    public static final long NOTIFICATION_ZXID = -1L;
    
    private final int xid;
    private final OpCode opcode;

    private OpCodeXid(int xid, OpCode opcode) {
        this.xid = xid;
        this.opcode = opcode;
    }

    @Override
    public int xid() {
        return xid;
    }

    @Override
    public OpCode opcode() {
        return opcode;
    }
}