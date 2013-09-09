package edu.uw.zookeeper.client.console;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import com.google.common.base.Joiner;

public class Invocation {

    private final Map<String, String> environment;
    private final ConsoleCommand command;
    private final Object[] arguments;

    public Invocation(Map<String, String> environment, ConsoleCommand command, Object[] arguments) {
        super();
        this.environment = checkNotNull(environment);
        this.command = checkNotNull(command);
        this.arguments = checkNotNull(arguments);
    }
    
    public Map<String, String> getEnvironment() {
        return environment;
    }

    public ConsoleCommand getCommand() {
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