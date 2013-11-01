package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;

public interface ServerExecutor<T extends SessionExecutor> extends Iterable<T> {

    TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor();
    
    TaskExecutor<? super ConnectMessage.Request, ? extends ConnectMessage.Response> connectExecutor();
    
    T sessionExecutor(long sessionId);
}
