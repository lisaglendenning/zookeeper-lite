package edu.uw.zookeeper.client.console;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

public class Invocation {

    public static Invocation parse(CharSequence line) {
        Iterable<String> tokens = ConsoleCommand.getTokens(line);
        String name = Iterables.getFirst(tokens, null);
        if (name == null) {
            return null;
        }
        ConsoleCommand command = null;
        for (ConsoleCommand e : ConsoleCommand.values()) {
            if (e.getNames().contains(name)) {
                command = e;
                break;
            }
        }
        checkArgument(command != null, String.format(
                    "Not a command: '%s'", name));
        Object[] arguments = command.parse(tokens);
        return new Invocation(command, arguments);
    }

    private final ConsoleCommand command;
    private final Object[] arguments;

    public Invocation(ConsoleCommand command, Object[] arguments) {
        super();
        this.command = checkNotNull(command);
        this.arguments = checkNotNull(arguments);
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