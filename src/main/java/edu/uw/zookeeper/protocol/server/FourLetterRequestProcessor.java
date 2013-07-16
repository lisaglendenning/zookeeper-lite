package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.util.Processor;

/**
 * TODO
 */
public enum FourLetterRequestProcessor implements Processor<FourLetterRequest, FourLetterResponse> {
    INSTANCE;
    
    public static FourLetterRequestProcessor getInstance() {
        return INSTANCE; 
    }
    
    
    @Override
    public FourLetterResponse apply(FourLetterRequest request) {
        // TODO
        return FourLetterResponse.create(request.word());
    }
}