package edu.uw.zookeeper.client.console;

import java.io.IOException;
import java.util.concurrent.Executor;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.protocol.Operation;
import jline.TerminalFactory;
import jline.console.ConsoleReader;

public class ConsoleReaderService extends AbstractExecutionThreadService {

    public static ConsoleReaderService newInstance(ClientExecutor<? super Operation.Request, ?> client) throws IOException {
        ConsoleReader reader = new ConsoleReader();
        reader.addCompleter(ConsoleCommand.getCompleter());
        return new ConsoleReaderService(reader, Commands.invoker(client));
    }

    protected final ConsoleReader reader;
    protected final Invoker invoker;
    protected final Executor executor;

    public ConsoleReaderService(
            ConsoleReader reader,
            Invoker invoker) {
        this.reader = reader;
        this.invoker = invoker;
        this.executor = MoreExecutors.sameThreadExecutor();
    }

    public ConsoleReader getReader() {
        return reader;
    }

    @Override
    protected void startUp() throws Exception {
        reader.setPrompt("> ");
    }

    @Override
    protected void shutDown() throws Exception {
        reader.println("Exiting...");
        reader.flush();
        reader.shutdown();
        TerminalFactory.get().restore();
    }

    @Override
    protected void run() throws Exception {
        String line = null;
        while (isRunning() && ((line = reader.readLine()) != null)) {
            try {
                Invocation invocation = Invocation.parse(line);
                if (invocation == null) {
                    continue;
                }
                Futures.addCallback(invoker.apply(invocation), new InvokeCallback(invocation), executor);
            } catch (IllegalArgumentException e) {
                printException(e);
                continue;
            }
        }
    }
    
    protected void printException(Exception e) throws IOException {
        // TODO: use Ansi.Color.RED
        // but Eclipse doesn't like it?
        reader.println(e.toString());
        reader.flush();
    }
    
    protected class InvokeCallback implements FutureCallback<String> {

        protected final Invocation invocation;
        
        public InvokeCallback(Invocation invocation) {
            this.invocation = invocation;
        }
        
        @Override
        public void onSuccess(String result) {
            try {
                reader.println(result);
                reader.flush();
            } catch (IOException e) {
                onFailure(e);
            }
            
            if (invocation.getCommand() == ConsoleCommand.EXIT) {
                stopAsync();
            }
        }

        @Override
        public void onFailure(Throwable t) {
            if (isRunning()) {
                stopAsync();
            }
        }
    }
}