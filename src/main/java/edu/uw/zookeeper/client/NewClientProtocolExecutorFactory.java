package edu.uw.zookeeper.client;

import edu.uw.zookeeper.protocol.client.ClientCodecConnection;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.TimeValue;

public class NewClientProtocolExecutorFactory implements Factory<ClientProtocolExecutor> {

    public static NewClientProtocolExecutorFactory newInstance(
            Factory<? extends ClientCodecConnection> codecConnectionFactory,
            TimeValue timeOut,
            Reference<Long> lastZxid) {
        return new NewClientProtocolExecutorFactory(codecConnectionFactory, timeOut,
                lastZxid);
    }
    
    protected final Factory<? extends ClientCodecConnection> codecConnectionFactory;
    protected final Reference<Long> lastZxid;
    protected final TimeValue timeOut;
    
    protected NewClientProtocolExecutorFactory(
            Factory<? extends ClientCodecConnection> codecConnectionFactory,
            TimeValue timeOut,
            Reference<Long> lastZxid) {
        this.codecConnectionFactory = codecConnectionFactory;
        this.lastZxid = lastZxid;
        this.timeOut = timeOut;
    }
    
    @Override
    public ClientProtocolExecutor get() {
        ClientCodecConnection codecConnection = codecConnectionFactory.get();
        return ClientProtocolExecutor.newSession(codecConnection, lastZxid, timeOut);
    }
}
