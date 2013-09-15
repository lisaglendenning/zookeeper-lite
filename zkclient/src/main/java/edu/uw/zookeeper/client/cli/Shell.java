package edu.uw.zookeeper.client.cli;

import java.io.Flushable;
import java.io.IOException;
import jline.console.ConsoleReader;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.common.RuntimeModule;

public class Shell extends AbstractIdleService implements Flushable {

    public static Shell create(RuntimeModule runtime) throws IOException {
        Environment environment = Environment.empty();
        ConsoleReader reader = new ConsoleReader();
        return new Shell(runtime, environment, CommandParser.empty(environment), reader, TokenParser.defaults());
    }
    
    protected final RuntimeModule runtime;
    protected final Environment environment;
    protected final CommandParser commands;
    protected final ConsoleReader reader;
    protected final TokenParser tokenizer;
    
    protected Shell(
            RuntimeModule runtime,
            Environment environment,
            CommandParser commands,
            ConsoleReader reader,
            TokenParser tokenizer) {
        this.runtime = runtime;
        this.environment = environment;
        this.commands = commands;
        this.reader = reader;
        this.tokenizer = tokenizer;
    }
    
    public RuntimeModule getRuntime() {
        return runtime;
    }
    
    public Environment getEnvironment() {
        return environment;
    }
    
    public CommandParser getCommands() {
        return commands;
    }
    
    public ConsoleReader getReader() {
        return reader;
    }
    
    public TokenParser getTokenizer() {
        return tokenizer;
    }
    
    @Override
    public void flush() throws IOException {
        reader.flush();
    }

    public void println(CharSequence s) throws IOException {
        reader.println(s);
    }

    public void printException(Exception e) throws IOException {
        if (reader.getTerminal().isAnsiSupported()) {
            reader.println(new StringBuilder().append("\u001b[31m").append(e.toString()).append("\u001b[0m").toString());
        } else {
            reader.println(e.toString());
        }
        reader.flush();
    }
    
    public String readLine() throws IOException {
        String prompt = String.format(environment.get(ShellInvoker.PROMPT_KEY), environment.get(ShellInvoker.CWD_KEY));
        if (reader.getTerminal().isAnsiSupported()) {
            prompt = new StringBuilder().append("\u001b[1m").append(prompt).append("\u001b[0m").toString();
        }
        return reader.readLine(prompt);
    }

    @Override
    protected void startUp() throws Exception {
    }
    
    @Override
    protected void shutDown() throws Exception {
        println("Exiting...");
        flush();
        reader.shutdown();
        // called via shutdown hook
        //TerminalFactory.get().restore();
    }
}
