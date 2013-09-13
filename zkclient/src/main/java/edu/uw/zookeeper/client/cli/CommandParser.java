package edu.uw.zookeeper.client.cli;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.AnnotatedElement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;

import com.google.common.base.CaseFormat;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.data.ZNodeLabel;

public class CommandParser implements Function<Iterable<String>, Invocation<?>> {

    public static CommandParser empty(Environment environment) {
        return new CommandParser(environment, 
                new MapMaker().<String, Pair<CommandDescriptor, Object>>makeMap());
    }
    
    protected static CommandDescriptor getCommandDescriptor(Object command) {
        try {
            CommandDescriptor descriptor = null;
            if (command instanceof AnnotatedElement) {
                descriptor = ((AnnotatedElement) command).getAnnotation(CommandDescriptor.class);
            }
            if (descriptor == null) {
                if (command instanceof Enum) {
                    descriptor = getCommandDescriptor(command.getClass().getField(((Enum<?>) command).name()));
                } else {
                    descriptor = getCommandDescriptor(command.getClass());
                }
            }
            return descriptor;
        } catch (Exception e) {
            throw new IllegalArgumentException(String.valueOf(command), e);
        }
    }
    
    protected static Iterable<String> getCommandNames(Pair<CommandDescriptor, Object> descriptor) {
        Iterable<String> names;
        if (descriptor.first().names().length > 0) {
            names = Arrays.asList(descriptor.first().names());
        } else {
            names = ImmutableList.of(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, ((Enum<?>) descriptor.second()).name()));
        }
        return names;
    }
    
    protected final Logger logger = LogManager.getLogger(getClass());
    protected final Environment environment;
    protected final Map<String, Pair<CommandDescriptor, Object>> commands;
    
    protected CommandParser(
            Environment environment,
            Map<String, Pair<CommandDescriptor, Object>> commands) {
        this.environment = environment;
        this.commands = commands;
    }
    
    public Completer getCompleter() {
        // TODO
        return new NullCompleter();
    }
    
    @SuppressWarnings("unchecked")
    public <T> T getCommand(String name) {
        Pair<CommandDescriptor, Object> descriptor = commands.get(name);
        if (descriptor != null) {
            return (T) descriptor.second();
        } else {
            return null;
        }
    }

    public CommandParser withCommand(Object command) {
        Pair<CommandDescriptor, Object> descriptor = Pair.create(getCommandDescriptor(command), command);
        for (String name: getCommandNames(descriptor)) {
            logger.debug("Adding command {} => {}", name, command);
            checkArgument(! commands.containsKey(name));
            commands.put(name, descriptor);
        }
        return this;
    }
    
    public Set<Pair<CommandDescriptor, Object>> getCommands() {
        return ImmutableSet.copyOf(ImmutableSortedMap.copyOf(commands).values());
    }
    
    @Override
    public Invocation<Object> apply(Iterable<String> tokens) {
        String name = Iterables.getFirst(tokens, null);
        if (name == null) {
            return null;
        }
        Object command = getCommand(name);
        checkArgument(command != null, String.format(
                "Not a command: '%s'", name));
        CommandDescriptor cd = getCommandDescriptor(command);
        ArgumentDescriptor[] ads = cd.arguments();
        Object[] arguments = new Object[ads.length + 1];
        Iterator<String> itr = tokens.iterator();
        checkArgument(itr.hasNext());
        arguments[0] = itr.next();
        for (int i=0; i<ads.length; ++i) {
            ArgumentDescriptor ad = ads[i];
            String token;
            if (itr.hasNext()) {
                token = itr.next();
            } else {
                token = ad.value();
            }
            Object argument;
            switch (ad.token()) {
            case ENUM:
                if (ad.type() == BooleanArgument.class) {
                    argument = Boolean.valueOf(BooleanArgument.fromString(token).booleanValue());
                } else if (ad.type() == ModeArgument.class) {
                    argument = ModeArgument.fromString(token).value();
                } else if (ad.type() == MultiArgument.class) {
                    argument = MultiArgument.fromString(token);
                } else {
                    throw new AssertionError(String.valueOf(ad.type()));
                }
                break;
            case STRING:
                argument = token;
                break;
            case PATH:
                if (token.isEmpty()) {
                    argument = environment.get(ShellInvoker.CWD_KEY);
                } else if (token.charAt(0) == ZNodeLabel.SLASH) {
                    argument = ZNodeLabel.Path.validated(token);
                } else {
                    argument = ZNodeLabel.Path.canonicalized(ZNodeLabel.join(environment.get(ShellInvoker.CWD_KEY).toString(), token));
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
        checkArgument(! itr.hasNext(), String.format("Extra arguments after #%d", ads.length));
        return new Invocation<Object>(command, arguments);
    }
}
