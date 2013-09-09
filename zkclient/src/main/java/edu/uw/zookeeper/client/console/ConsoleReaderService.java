package edu.uw.zookeeper.client.console;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import jline.TerminalFactory;
import jline.console.ConsoleReader;

public class ConsoleReaderService extends AbstractExecutionThreadService {

    public static ConsoleReaderService newInstance(ClientExecutor<? super Operation.Request, ?> client) throws IOException {
        ConsoleReader reader = new ConsoleReader();
        reader.addCompleter(ConsoleCommand.getCompleter());
        return new ConsoleReaderService(client, reader, Commands.invoker(client));
    }
    
    public static enum EnvKey {
        PROMPT, CWD;
        
        public String get(Map<String, String> env) {
            return env.get(name());
        }

        public String put(Map<String, String> env, String value) {
            return env.put(name(), value);
        }
    }

    protected final ClientExecutor<? super Operation.Request, ?> client;
    protected final ConsoleReader reader;
    protected final Invoker invoker;
    protected final Executor executor;

    public ConsoleReaderService(
            ClientExecutor<? super Operation.Request, ?> client,
            ConsoleReader reader,
            Invoker invoker) {
        this.client = client;
        this.reader = reader;
        this.invoker = invoker;
        this.executor = MoreExecutors.sameThreadExecutor();
    }

    public ConsoleReader getReader() {
        return reader;
    }

    @Override
    protected void startUp() throws Exception {
        NotificationCallback cb = new NotificationCallback();
        addListener(cb, executor);
        client.register(cb);
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
        Map<String, String> env = newEnvironment();
        LineParser parser = LineParser.create();
        String line = null;
        while (isRunning() && ((line = readLine(env)) != null)) {
            try {
                List<String> tokens = parser.apply(line);
                Invocation invocation = parse(env, tokens);
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
    
    protected String readLine(Map<String, String> env) throws IOException {
        String prompt = String.format(EnvKey.PROMPT.get(env), EnvKey.CWD.get(env));
        if (reader.getTerminal().isAnsiSupported()) {
            prompt = new StringBuilder().append("\u001b[1m").append(prompt).append("\u001b[0m").toString();
        }
        return reader.readLine(prompt);
    }
    
    protected Map<String, String> newEnvironment() {
        Map<String, String> env = Maps.newHashMap();
        EnvKey.PROMPT.put(env, "%s $ ");
        EnvKey.CWD.put(env, "/");
        return env;
    }
    
    protected void printException(Exception e) throws IOException {
        if (reader.getTerminal().isAnsiSupported()) {
            reader.println(new StringBuilder().append("\u001b[31m").append(e.toString()).append("\u001b[0m").toString());
        } else {
            reader.println(e.toString());
        }
        reader.flush();
    }
    
    protected Invocation parse(Map<String, String> env, Iterable<String> tokens) {
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
        Object[] arguments = command.parse(env, tokens);
        return new Invocation(env, command, arguments);
    }
    
    protected class NotificationCallback extends Service.Listener {
        
        @Override
        public void stopping(State from) {
            try {
                client.unregister(this);
            } catch (IllegalArgumentException e) {}
        }
        
        @Override
        public void failed(State from, Throwable failure) {
            try {
                client.unregister(this);
            } catch (IllegalArgumentException e) {}
        }
        
        @Subscribe
        public void handleResponse(Operation.ProtocolResponse<?> response) {
            if (response.xid() == OpCodeXid.NOTIFICATION.xid()) {
                WatchEvent event = WatchEvent.fromRecord((IWatcherEvent) response.record());
                try {
                    reader.println(String.valueOf(event));
                } catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
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