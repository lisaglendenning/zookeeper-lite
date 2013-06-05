package edu.uw.zookeeper.protocol;

import edu.uw.zookeeper.protocol.proto.IWatcherEvent;


public abstract class OpNotification {

    public static class OpNotificationResponse extends OpRecord.OpResponse<IWatcherEvent> implements Operation.XidHeader {
        public static OpNotificationResponse newInstance() {
            return newInstance(newRecord());
        }

        public static OpNotificationResponse newInstance(IWatcherEvent record) {
            return new OpNotificationResponse(record);
        }

        public static IWatcherEvent newRecord() {
            return (IWatcherEvent) Records.Responses.getInstance().get(IWatcherEvent.OPCODE);
        }

        private OpNotificationResponse(IWatcherEvent record) {
            super(record);
        }

        @Override
        public int xid() {
            return asRecord().xid();
        }
    }

    private OpNotification() {}
}
