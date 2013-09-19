package edu.uw.zookeeper.client.cli;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Joiner;

import edu.uw.zookeeper.common.Pair;


public class Invocation<T> {

    public static <T> Invocation<T> create(Pair<CommandDescriptor, T> command, Object[] arguments) {
        return new Invocation<T>(command, arguments);
    }
    
    private final Pair<CommandDescriptor, T> command;
    private final Object[] arguments;

    public Invocation(Pair<CommandDescriptor, T> command, Object[] arguments) {
        super();
        this.command = checkNotNull(command);
        this.arguments = checkNotNull(arguments);
    }
    
    public Pair<CommandDescriptor, T> getCommand() {
        return command;
    }

    public Object[] getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return Joiner.on(' ').join(getArguments());
    }
}