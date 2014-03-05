package edu.uw.zookeeper.client.cli;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.client.ConnectionClientExecutorService;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.WatchEvent;
import edu.uw.zookeeper.data.ZNodePath.AbsoluteZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.Records;

public class ClientExecutorInvoker extends AbstractIdleService implements Invoker<ClientExecutorInvoker.Command>, FutureCallback<String> {

    @Invokes(commands={Command.class})
    public static ClientExecutorInvoker create(Shell shell) {
        ConnectionClientExecutorService.Builder client = ConnectionClientExecutorService.builder().setRuntimeModule(shell.getRuntime()).setDefaults();
        return new ClientExecutorInvoker(client, shell);
    }

    public static enum Command {
        @CommandDescriptor(arguments = {
                @ArgumentDescriptor(token = TokenType.ENUM, type = MultiArgument.class)})
        MULTI,
        
        // TODO: Acl
        @CommandDescriptor(arguments = {
                @ArgumentDescriptor(token = TokenType.PATH),
                @ArgumentDescriptor(name="data", token = TokenType.STRING),
                @ArgumentDescriptor(name="mode", token = TokenType.ENUM, type = CreateModeArgument.class, value="p"),
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
    };
    
    public static Environment.Key<ConnectionClientExecutorService.Builder> CLIENT_KEY = Environment.Key.create("CLIENT", ConnectionClientExecutorService.Builder.class);
    public static Environment.Key<List<Invocation<Command>>> MULTI_KEY = Environment.Key.create("MULTI", List.class);
    
    protected static final String MULTI_PROMPT = "(multi)> ";

    protected static final Executor executor = MoreExecutors.sameThreadExecutor();
    
    protected final Shell shell;
    protected final RequestBuilder operator;
    protected final ConnectionClientExecutorService.Builder client;
    
    protected ClientExecutorInvoker(
            ConnectionClientExecutorService.Builder client,
            Shell shell) {
        this.client = client;
        this.shell = shell;
        this.operator = new RequestBuilder();
    }
    
    @Override
    public void invoke(Invocation<Command> input)
            throws Exception {
        if (input.getCommand().second() == Command.MULTI) {
            switch ((MultiArgument) input.getArguments()[1]) {
            case BEGIN:
            {
                List<Invocation<Command>> invocations = Lists.<Invocation<Command>>newLinkedList();
                invocations = shell.getEnvironment().put(MULTI_KEY, invocations);
                checkArgument(invocations == null, "\"multi begin\" cannot follow a \"multi begin\"");
                shell.getEnvironment().put(ShellInvoker.PROMPT_KEY, shell.getEnvironment().get(ShellInvoker.PROMPT_KEY) + MULTI_PROMPT);
                return;
            }
            case END:
            {
                List<Invocation<Command>> invocations = shell.getEnvironment().remove(MULTI_KEY);
                checkArgument(invocations != null, "\"multi end\" missing preceding \"multi begin\"");
                shell.getEnvironment().put(ShellInvoker.PROMPT_KEY, shell.getEnvironment().get(ShellInvoker.PROMPT_KEY).replace(MULTI_PROMPT, ""));
                Object[] arguments = { input.getArguments()[0], invocations };
                input = Invocation.create(input.getCommand(), arguments);
                break;
            }
            case CANCEL:
            {
                List<Invocation<Command>> invocations = shell.getEnvironment().remove(MULTI_KEY);
                checkArgument(invocations != null, "\"multi cancel\" missing preceding \"multi begin\"");
                shell.getEnvironment().put(ShellInvoker.PROMPT_KEY, shell.getEnvironment().get(ShellInvoker.PROMPT_KEY).replace(MULTI_PROMPT, ""));
                return;
            }
            }
        } else if (shell.getEnvironment().contains(MULTI_KEY)) {
            List<Invocation<Command>> invocations = shell.getEnvironment().get(MULTI_KEY);
            invocations.add(input);
            return;
        }

        Records.Request request = operator.apply(input);
        ClientExecutor<Operation.Request, ?, ?> client = shell.getEnvironment().get(CLIENT_KEY).getConnectionClientExecutor();
        Futures.addCallback(new RequestSubmitter(client, request).call(), this);
    }

    @Override
    public void onSuccess(String result) {
        try {
            shell.println(result);
            shell.flush();
        } catch (IOException e) {
            onFailure(e);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        stopAsync();
    }

    @Override
    protected void startUp() throws Exception {
        for (Command command: Command.values()) {
            shell.getCommands().withCommand(command);
        }
        
        shell.getEnvironment().put(CLIENT_KEY, client);
        for (Service e: client.build()) {
            shell.getRuntime().getServiceMonitor().addOnStart(e);
            if (! e.isRunning()) {
                e.startAsync().awaitRunning();
            }
        }
        
        NotificationCallback cb = new NotificationCallback(client.getConnectionClientExecutor());
        addListener(cb, executor);
        client.getConnectionClientExecutor().subscribe(cb);
    }

    @Override
    protected void shutDown() throws Exception {
    }
    
    protected class NotificationCallback extends Service.Listener implements SessionListener {
        
        protected final ClientExecutor<?, ?, SessionListener> client;
        
        public NotificationCallback(ClientExecutor<?, ?, SessionListener> client) {
            this.client = client;
        }
        
        @Override
        public void stopping(State from) {
            client.unsubscribe(this);
        }
        
        @Override
        public void failed(State from, Throwable failure) {
            client.unsubscribe(this);
        }
        
        @Override
        public void handleAutomatonTransition(
                Automaton.Transition<ProtocolState> transition) {
            // TODO Auto-generated method stub
        }

        @Override
        public void handleNotification(
                Operation.ProtocolResponse<IWatcherEvent> notification) {
            WatchEvent event = WatchEvent.fromRecord(notification.record());
            try {
                shell.println(String.valueOf(event));
                shell.flush();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
    
    protected static class RequestSubmitter implements Callable<ListenableFuture<String>>, Function<Operation.ProtocolResponse<?>, String> {

        protected final ClientExecutor<? super Records.Request, ?, ?> client;
        protected final Records.Request request;
        
        public RequestSubmitter(
                ClientExecutor<? super Records.Request, ?, ?> client,
                Records.Request request) {
            this.client = client;
            this.request = request;
        }

        @Override
        public ListenableFuture<String> call() throws Exception {
            ListenableFuture<? extends Operation.ProtocolResponse<?>> response = client.submit(request);
            return Futures.transform(response, this, executor);
        }

        @Override
        public String apply(Operation.ProtocolResponse<?> input) {
            if (input.record() instanceof Operation.Error) {
                return String.format("%s => %s", request, ((Operation.Error) input.record()).error());
            } else {
                switch (request.opcode()) {
                case MULTI:
                {
                    StringBuilder str = new StringBuilder().append("multi =>").append('\n');
                    Iterator<Records.MultiOpRequest> requests = ((IMultiRequest) request).iterator();
                    Iterator<Records.MultiOpResponse> responses = ((IMultiResponse) input.record()).iterator();
                    while (requests.hasNext()) {
                        Records.MultiOpRequest request = requests.next();
                        Records.MultiOpResponse response = responses.next();
                        str.append(new RequestSubmitter(client, request).apply(ProtocolResponseMessage.of(input.xid(), input.zxid(), response))).append('\n');
                    }
                    return str.toString();
                }
                case CREATE:
                    return String.format("created => %s", 
                            ((Records.PathGetter) input.record()).getPath());
                case CREATE2:
                    return String.format("created => %s %s", 
                            ((Records.PathGetter) input.record()).getPath(),
                            Records.toBeanString(((Records.StatGetter) input.record()).getStat()));
                case DELETE:
                    return String.format("deleted => %s", 
                            ((Records.PathGetter) request).getPath());
                case EXISTS:
                    return String.format("stat %s => %s", 
                            ((Records.PathGetter) request).getPath(),
                            Records.toBeanString(((Records.StatGetter) input.record()).getStat()));
                case GET_ACL:
                    return String.format("getAcl %s => %s", 
                            ((Records.PathGetter) request).getPath(),
                            Records.iteratorToBeanString(((Records.AclGetter) input.record()).getAcl().iterator()));
                case GET_CHILDREN:
                    return String.format("ls %s => %s", 
                            ((Records.PathGetter) request).getPath(),
                            ((Records.ChildrenGetter) input.record()).getChildren());
                case GET_CHILDREN2:
                    return String.format("ls %s => %s %s", 
                            ((Records.PathGetter) request).getPath(),
                            ((Records.ChildrenGetter) input.record()).getChildren(),
                            Records.toBeanString(((Records.StatGetter) input.record()).getStat()));
                case GET_DATA:
                    return String.format("get %s => %s", 
                            ((Records.PathGetter) request).getPath(),
                            new String(((Records.DataGetter) input.record()).getData()));
                case SET_DATA:
                    return String.format("set %s => %s", 
                            ((Records.PathGetter) request).getPath(),
                            Records.toBeanString(((Records.StatGetter) input.record()).getStat()));
                case SYNC:
                    return String.format("sync => %s", 
                            ((Records.PathGetter) input.record()).getPath());
                default:
                    return String.format("%s => %s", request, input);
                }
            }
        }
    }

    protected static class RequestBuilder implements Function<Invocation<ClientExecutorInvoker.Command>, Records.Request> {

        @SuppressWarnings("unchecked")
        @Override
        public Records.Request apply(Invocation<ClientExecutorInvoker.Command> input) {
            switch (input.getCommand().second()) {
            case MULTI:
            {
                ImmutableList.Builder<Records.MultiOpRequest> requests = ImmutableList.builder();
                for (Invocation<ClientExecutorInvoker.Command> e: (List<Invocation<ClientExecutorInvoker.Command>>) input.getArguments()[1]) {
                    requests.add((Records.MultiOpRequest) apply(e));
                }
                return new IMultiRequest(requests.build());
            }
            case CREATE:
                return Operations.Requests.create()
                        .setPath((AbsoluteZNodePath) input.getArguments()[1])
                        .setData(((String) input.getArguments()[2]).getBytes())
                        .setMode(((CreateModeArgument) input.getArguments()[3]).value())
                        .setStat(((BooleanArgument) input.getArguments()[4]).booleanValue())
                        .build();
            case EXISTS:
                return Operations.Requests.exists()
                        .setPath((AbsoluteZNodePath) input.getArguments()[1])
                        .setWatch(((BooleanArgument) input.getArguments()[2]).booleanValue())
                        .build();
            case GETACL:
                return Operations.Requests.getAcl()
                        .setPath((AbsoluteZNodePath) input.getArguments()[1])
                        .build();
            case GET:
                return Operations.Requests.getData()
                        .setPath((AbsoluteZNodePath) input.getArguments()[1])
                        .setWatch(((BooleanArgument) input.getArguments()[2]).booleanValue())
                        .build();
            case LS:
                return Operations.Requests.getChildren()
                        .setPath((AbsoluteZNodePath) input.getArguments()[1])
                        .setWatch(((BooleanArgument) input.getArguments()[2]).booleanValue())
                        .setStat(((BooleanArgument) input.getArguments()[3]).booleanValue())
                        .build();
            case RM:
                return Operations.Requests.delete()
                        .setPath((AbsoluteZNodePath) input.getArguments()[1])
                        .setVersion((Integer) input.getArguments()[2])
                        .build();
            case SET:
                return Operations.Requests.setData()
                        .setPath((AbsoluteZNodePath) input.getArguments()[1])
                        .setData(((String) input.getArguments()[2]).getBytes())
                        .setVersion((Integer) input.getArguments()[3])
                        .build();
            case SYNC:
                return Operations.Requests.sync()
                        .setPath((AbsoluteZNodePath) input.getArguments()[1])
                        .build();
            default:
                throw new IllegalArgumentException(String.valueOf(input.getCommand()));
            }
        }
    }
}
