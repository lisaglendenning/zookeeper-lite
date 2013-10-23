package edu.uw.zookeeper.protocol.server;

import net.engio.mbassy.PubSubSupport;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.server.SessionExecutor;

public interface ServerExecutor<T extends SessionExecutor> extends Iterable<T> {

    TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor();
    
    TaskExecutor<Pair<ConnectMessage.Request, ? extends PubSubSupport<Object>>, ? extends ConnectMessage.Response> connectExecutor();
    
    T sessionExecutor(long sessionId);
}
