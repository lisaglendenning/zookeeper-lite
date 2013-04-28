package edu.uw.zookeeper.protocol;

import org.apache.zookeeper.proto.AuthPacket;


public abstract class OpAuth {

    public static class Request extends OpCodeRecord.Request<AuthPacket> 
        implements Operation.XidHeader {
        
        public static OpAuth.Request create() {
            return create(createRecord());
        }

        public static OpAuth.Request create(AuthPacket record) {
            return new OpAuth.Request(record);
        }

        public static AuthPacket createRecord() {
            return Records.Requests.<AuthPacket>create(opCodeXid().opcode());
        }

        private Request(AuthPacket record) {
            super(record);
        }

        @Override
        public OpCode opcode() {
            return opCodeXid().opcode();
        }

        @Override
        public int xid() {
            return opCodeXid().xid();
        }
    }

    public static Records.OpCodeXid opCodeXid() {
        return Records.OpCodeXid.AUTH;
    }
    
    private OpAuth() {}
}
