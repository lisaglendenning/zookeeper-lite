package edu.uw.zookeeper.client.cli;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.data.ZNodePath;

public class ShellInvoker extends AbstractIdleService implements Invoker<ShellInvoker.Command> {
    
    @Invokes(commands={Command.class})
    public static ShellInvoker create(Shell shell) {
        return new ShellInvoker(shell);
    }

    public static enum Command {
        @CommandDescriptor(names = { "?", "help" }, description = "Print usage")
        HELP,
    
        @CommandDescriptor(names = { "q", "quit", "exit" }, description = "Exit program")
        EXIT,
    
        @CommandDescriptor(names = { "printenv" }, description = "Print environment")
        PRINTENV,
    
        @CommandDescriptor(names = { "cd" }, description = "Change working path",
                arguments = {
                    @ArgumentDescriptor(token = TokenType.PATH)})
        CD;
    }
    
    public static Environment.Key<String> PROMPT_KEY = Environment.Key.create("PROMPT", String.class);
    public static Environment.Key<ZNodePath> CWD_KEY = Environment.Key.create("CWD", ZNodePath.class);

    protected static final String DEFAULT_PROMPT = "%s $ ";
    protected static final ZNodePath DEFAULT_CWD = ZNodePath.root();
    
    protected final Shell shell;
    
    protected ShellInvoker(Shell shell) {
        this.shell = shell;
    }
    
    @Override
    public void invoke(Invocation<Command> input)
            throws Exception {
        switch (input.getCommand().second()) {
        case HELP:
            help(input);
            break;
        case EXIT:
            exit(input);
            break;
        case PRINTENV:
            printEnv(input);
            break;
        case CD:
            cd(input);
            break;
        default:
            throw new IllegalArgumentException(String.valueOf(input));
        }
    }

    @Override
    protected void startUp() throws Exception {
        shell.getEnvironment().put(PROMPT_KEY, DEFAULT_PROMPT);
        shell.getEnvironment().put(CWD_KEY, DEFAULT_CWD);
        
        for (Command command: Command.values()) {
            shell.getCommands().withCommand(command);
        }
    }

    @Override
    protected void shutDown() throws Exception {
    }

    protected void cd(Invocation<Command> invocation) {
        shell.getEnvironment().put(CWD_KEY, (ZNodePath) invocation.getArguments()[1]);
    }
    
    protected void printEnv(Invocation<Command> invocation) throws IOException {
        String output = Joiner.on('\n').withKeyValueSeparator("\t").join(shell.getEnvironment().entrySet());
        shell.getReader().println(output);
    }
    
    protected void exit(Invocation<Command> invocation) {
        if (shell.isRunning()) {
            shell.stopAsync();
        }
    }

    protected void help(Invocation<Command> invocation) throws Exception {
        Joiner joiner = Joiner.on('\t');
        StringBuilder str = new StringBuilder();
        ImmutableSet.Builder<Pair<CommandDescriptor, Object>> commands = ImmutableSet.builder();
        for (Map.Entry<CharSequence, Pair<CommandDescriptor, Object>> e: shell.getCommands().getCommands()) {
            commands.add(e.getValue());
        }
        for (Pair<CommandDescriptor, Object> command: commands.build()) {
            joiner.appendTo(str, getUsage(command)).append('\n');
        }
        String output =  str.toString();
        shell.getReader().println(output);
    }

    protected List<String> getUsage(Pair<CommandDescriptor, Object> command) {
        ImmutableList.Builder<String> tokens = ImmutableList.builder();
        tokens.add(Joiner.on(',')
                    .appendTo(new StringBuilder().append('{'), CommandParser.getCommandNames(command)).append('}')
                    .toString());
        for (ArgumentDescriptor a : command.first().arguments()) {
            StringBuilder argument = new StringBuilder();
            if (!a.name().isEmpty()) {
                argument.append(a.name()).append('=');
            }
            switch (a.token()) {
            case STRING:
                argument.append("\"...\"");
                break;
            case PATH:
                argument.append("/...");
                break;
            case INTEGER:
                argument.append("int");
                break;
            case ENUM:
                Joiner.on(',').appendTo(
                    argument.append('{'), 
                    a.type().getEnumConstants())
                    .append('}').toString();
                break;
            }
            if (!a.value().isEmpty()) {
                argument.append('(').append(a.value()).append(')');
            }
            tokens.add(argument.toString());
        }
        if (!command.first().description().isEmpty()) {
            tokens.add(command.first().description());
        }
        return tokens.build();
    }
}
