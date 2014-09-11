package edu.uw.zookeeper.protocol.proto;

import static com.google.common.base.Preconditions.*;

import org.apache.zookeeper.ZooDefs;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

public enum OpCode {

    NOTIFICATION(ZooDefs.OpCode.notification) {
    },

    CREATE(ZooDefs.OpCode.create) {
    },

    DELETE(ZooDefs.OpCode.delete) {
    },

    EXISTS(ZooDefs.OpCode.exists) {
    },

    GET_DATA(ZooDefs.OpCode.getData) {
    },

    SET_DATA(ZooDefs.OpCode.setData) {
    },

    GET_ACL(ZooDefs.OpCode.getACL) {
    },

    SET_ACL(ZooDefs.OpCode.setACL) {
    },

    GET_CHILDREN(ZooDefs.OpCode.getChildren) {
    },

    SYNC(ZooDefs.OpCode.sync) {
    },

    PING(ZooDefs.OpCode.ping) {
    },

    GET_CHILDREN2(ZooDefs.OpCode.getChildren2) {
    },

    CHECK(ZooDefs.OpCode.check) {
    },

    MULTI(ZooDefs.OpCode.multi) {
    },

    AUTH(ZooDefs.OpCode.auth) {
    },

    SET_WATCHES(ZooDefs.OpCode.setWatches) {
    },

    SASL(ZooDefs.OpCode.sasl) {
    },

    CREATE_SESSION(ZooDefs.OpCode.createSession) {
    },

    CLOSE_SESSION(ZooDefs.OpCode.closeSession) {
    },

    ERROR(ZooDefs.OpCode.error) {
    },

    CREATE2(ZooDefs.OpCode.create2) {
    },

    RECONFIG(ZooDefs.OpCode.reconfig) {
    },

    REMOVE_WATCHES(ZooDefs.OpCode.removeWatches) {
    };

    private static final ImmutableMap<Integer, OpCode> byInt = Maps
            .uniqueIndex(Iterators.forArray(OpCode.values()), 
                    new Function<OpCode, Integer>() {
                        @Override public Integer apply(OpCode input) {
                            return input.intValue();
                        }});

    public static OpCode of(int code) {
        checkArgument(byInt.containsKey(code));
        return byInt.get(code);
    }

    private final int code;

    private OpCode(int code) {
        this.code = code;
    }

    public int intValue() {
        return code;
    }
}
