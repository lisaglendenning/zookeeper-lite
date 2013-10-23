package edu.uw.zookeeper.server;

import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterWord;
import edu.uw.zookeeper.protocol.FourLetterResponse;

public class FourLetterRequestProcessor implements Processor<FourLetterRequest, FourLetterResponse> {

    public static FourLetterRequestProcessor newInstance() {
        ImmutableMap.Builder<FourLetterWord, Processor<FourLetterRequest, FourLetterResponse>> builder = ImmutableMap.builder();
        for (FourLetterWord e: FourLetterWord.values()) {
            builder.put(e, FourLetterCommands.getInstance(e));
        }
        return new FourLetterRequestProcessor(builder.build()); 
    }
    
    private final Map<FourLetterWord, Processor<FourLetterRequest, FourLetterResponse>> commands;
    
    protected FourLetterRequestProcessor(Map<FourLetterWord, Processor<FourLetterRequest, FourLetterResponse>> commands) {
        this.commands = commands;
    }
    
    @Override
    public FourLetterResponse apply(FourLetterRequest request) {
        Processor<FourLetterRequest, FourLetterResponse> command = commands.get(request.first());
        if (command == null) {
            throw new UnsupportedOperationException(String.valueOf(request));
        }
        try {
            return command.apply(request);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
