package edu.uw.zookeeper.client.console;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.List;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.data.ZNodeLabel;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;

public enum ConsoleCommand {
    @CommandDescriptor(names = { "?", "help" }, description = "Print usage")
    HELP,

    @CommandDescriptor(names = { "quit", "exit" }, description = "Exit program")
    EXIT,

    @CommandDescriptor(names = { "ls", "getChildren" }, arguments = {
            @ArgumentDescriptor(type = TokenType.PATH, value = "/"),
            @ArgumentDescriptor(name = "watch", type = TokenType.BOOLEAN, value = "n"),
            @ArgumentDescriptor(name = "stat", type = TokenType.BOOLEAN, value = "n") })
    LS;

    public static Completer getCompleter() {
        // TODO
        return new NullCompleter();
    }

    protected static Splitter SPLITTER = Splitter
            .on(CharMatcher.BREAKING_WHITESPACE).omitEmptyStrings()
            .trimResults();

    public static Iterable<String> getTokens(CharSequence line) {
        return SPLITTER.split(line);
    }

    private ConsoleCommand() {
    }

    public CommandDescriptor getDescriptor() {
        try {
            return getClass().getField(name()).getAnnotation(
                    CommandDescriptor.class);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    public List<String> getNames() {
        ImmutableList<String> names = ImmutableList
                .<String> copyOf(getDescriptor().names());
        if (names.isEmpty()) {
            names = ImmutableList.of(name().toLowerCase());
        }
        return names;
    }

    public ArgumentDescriptor[] getArguments() {
        return getDescriptor().arguments();
    }

    public String getDescription() {
        return getDescriptor().description();
    }

    public Object[] parse(Iterable<String> tokens) {
        ArgumentDescriptor[] descriptors = getArguments();
        Object[] arguments = new Object[descriptors.length + 1];
        Iterator<String> itr = tokens.iterator();
        checkArgument(itr.hasNext());
        arguments[0] = itr.next();
        checkArgument(getNames().contains(arguments[0]));
        for (int i=0; i<descriptors.length; ++i) {
            ArgumentDescriptor descriptor = descriptors[i];
            String token;
            if (itr.hasNext()) {
                token = itr.next();
            } else {
                checkArgument(! descriptor.value().isEmpty(), 
                    String.format("Missing required argument #%d", i+1));
                token = descriptor.value();
            }
            Object argument;
            switch (descriptor.type()) {
            case BOOLEAN:
                argument = Boolean.valueOf(BooleanArgument.fromString(token).booleanValue());
                break;
            case STRING:
                argument = token;
                break;
            case PATH:
                argument = ZNodeLabel.Path.of(token);
                break;
            default:
                argument = null;
            }
            checkArgument(argument != null);
            arguments[i+1] = argument;
        }
        checkArgument(! itr.hasNext(), String.format("Extra arguments after #%d", descriptors.length));
        return arguments;
    }
}
