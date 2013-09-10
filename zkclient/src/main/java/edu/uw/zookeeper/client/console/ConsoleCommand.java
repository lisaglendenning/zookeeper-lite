package edu.uw.zookeeper.client.console;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.uw.zookeeper.data.ZNodeLabel;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;

public enum ConsoleCommand {
    @CommandDescriptor(names = { "?", "help" }, description = "Print usage")
    HELP,

    @CommandDescriptor(names = { "q", "quit", "exit" }, description = "Exit program")
    EXIT,

    @CommandDescriptor(names = { "printenv" }, description = "Print environment")
    PRINTENV,

    @CommandDescriptor(names = { "cd" }, description = "Change working path",
            arguments = {
                @ArgumentDescriptor(token = TokenType.PATH)})
    CD,
    
    @CommandDescriptor(description="Recursive delete", arguments = {
            @ArgumentDescriptor(token = TokenType.PATH)})
    RMR,

    @CommandDescriptor(arguments = {
            @ArgumentDescriptor(token = TokenType.ENUM, type = MultiArgument.class)})
    MULTI,
    
    // TODO: Acl
    @CommandDescriptor(arguments = {
            @ArgumentDescriptor(token = TokenType.PATH),
            @ArgumentDescriptor(name="data", token = TokenType.STRING),
            @ArgumentDescriptor(name="mode", token = TokenType.ENUM, type = ModeArgument.class, value="p"),
            @ArgumentDescriptor(name = "stat", token = TokenType.ENUM, type = BooleanArgument.class, value = "n") })
    CREATE,
    
    @CommandDescriptor(names = { "rm", "delete" }, arguments = {
            @ArgumentDescriptor(token = TokenType.PATH),
            @ArgumentDescriptor(name="version", token = TokenType.INTEGER, value="-1") })
    RM,
    
    @CommandDescriptor(names = { "stat", "exists" }, arguments = {
            @ArgumentDescriptor(token = TokenType.PATH),
            @ArgumentDescriptor(name = "watch", token = TokenType.ENUM, type = BooleanArgument.class, value = "n") })
    EXISTS,

    @CommandDescriptor(names = { "getAcl" }, arguments = {
            @ArgumentDescriptor(token = TokenType.PATH) })
    GETACL,

    @CommandDescriptor(names = { "get", "getData" }, arguments = {
            @ArgumentDescriptor(token = TokenType.PATH),
            @ArgumentDescriptor(name = "watch", token = TokenType.ENUM, type = BooleanArgument.class, value = "n") })
    GET,

    @CommandDescriptor(names = { "ls", "getChildren" }, arguments = {
            @ArgumentDescriptor(token = TokenType.PATH),
            @ArgumentDescriptor(name = "watch", token = TokenType.ENUM, type = BooleanArgument.class, value = "n"),
            @ArgumentDescriptor(name = "stat", token = TokenType.ENUM, type = BooleanArgument.class, value = "n") })
    LS,
    
    // TODO
    // SETACL,
    
    @CommandDescriptor(names={ "set", "setData" }, arguments = {
            @ArgumentDescriptor(token = TokenType.PATH),
            @ArgumentDescriptor(name="data", token = TokenType.STRING),
            @ArgumentDescriptor(name="version", token = TokenType.INTEGER, value="-1") })
    SET,
    
    @CommandDescriptor(arguments = {
            @ArgumentDescriptor(token = TokenType.PATH) })
    SYNC;

    public static Completer getCompleter() {
        // TODO
        return new NullCompleter();
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

    public Object[] parse(Map<String, Object> env, Iterable<String> tokens) {
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
                token = descriptor.value();
            }
            Object argument;
            switch (descriptor.token()) {
            case ENUM:
                if (descriptor.type() == BooleanArgument.class) {
                    argument = Boolean.valueOf(BooleanArgument.fromString(token).booleanValue());
                } else if (descriptor.type() == ModeArgument.class) {
                    argument = ModeArgument.fromString(token).value();
                } else if (descriptor.type() == MultiArgument.class) {
                    argument = MultiArgument.fromString(token);
                } else {
                    throw new AssertionError(String.valueOf(descriptor.type()));
                }
                break;
            case STRING:
                argument = token;
                break;
            case PATH:
                if (token.isEmpty()) {
                    argument = EnvKey.CWD.get(env);
                } else if (token.charAt(0) == ZNodeLabel.SLASH) {
                    argument = ZNodeLabel.Path.of(token);
                } else {
                    argument = ZNodeLabel.Path.joined(EnvKey.CWD.get(env).toString(), token);
                }
                break;
            case INTEGER:
                argument = Integer.valueOf(token);
                break;
            default:
                argument = null;
            }
            checkArgument(argument != null, String.format("Error parsing argument #%d: %s", i+1, token));
            arguments[i+1] = argument;
        }
        checkArgument(! itr.hasNext(), String.format("Extra arguments after #%d", descriptors.length));
        return arguments;
    }
}
