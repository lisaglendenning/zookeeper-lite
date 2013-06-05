package edu.uw.zookeeper.protocol;

import edu.uw.zookeeper.protocol.proto.IAuthRequest;


public abstract class OpAuth {

    public static class OpAuthRequest extends OpRecord.OpRequest<IAuthRequest> 
        implements Operation.XidHeader {
        
        public static OpAuthRequest newInstance() {
            return newInstance(newRecord());
        }

        public static OpAuthRequest newInstance(IAuthRequest record) {
            return new OpAuthRequest(record);
        }

        public static IAuthRequest newRecord() {
            return (IAuthRequest) Records.Requests.getInstance().get(IAuthRequest.OPCODE);
        }

        private OpAuthRequest(IAuthRequest record) {
            super(record);
        }

        @Override
        public int xid() {
            return asRecord().xid();
        }
    }

    private OpAuth() {}
}
