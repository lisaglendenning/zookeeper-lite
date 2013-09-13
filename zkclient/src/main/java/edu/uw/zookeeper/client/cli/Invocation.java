package edu.uw.zookeeper.client.cli;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Joiner;


public class Invocation<T> {

    private final T command;
    private final Object[] arguments;

    public Invocation(T command, Object[] arguments) {
        super();
        this.command = checkNotNull(command);
        this.arguments = checkNotNull(arguments);
    }
    
    public T getCommand() {
        return command;
    }

    public Object[] getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return Joiner.on(' ').join(getArguments()).toString();
    }
}