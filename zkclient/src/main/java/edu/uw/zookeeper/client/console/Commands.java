package edu.uw.zookeeper.client.console;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.TreeFetcher;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public abstract class Commands {
    
    public static Invoker invoker(ClientExecutor<? super Operation.Request, ?> client) {
        return TopInvoker.newInstance(client);
    }
    
    public static Invoker invokeCallable(final Callable<String> callable) {
        return new Invoker() {
            @Override
            public ListenableFuture<String> apply(Invocation input)
                    throws Exception {
                return Futures.immediateFuture(callable.call());
            }
        };
    }
    
    public static class TopInvoker implements Invoker {

        public static TopInvoker newInstance(ClientExecutor<? super Operation.Request, ?> client) {
            ImmutableMap.Builder<ConsoleCommand, AsyncFunction<Invocation, String>> invokers = ImmutableMap.builder();
            for (Class<?> cls: Commands.class.getDeclaredClasses()) {
                if (cls == TopInvoker.class) {
                    continue;
                }
                for (Method m: cls.getDeclaredMethods()) {
                    Invokes invokes = m.getAnnotation(Invokes.class);
                    if (invokes == null) {
                        continue;
                    }
                    Invoker invoker;
                    if (Modifier.isStatic(m.getModifiers())) {
                        try {
                            Object[] args;
                            if (m.getParameterTypes().length > 0) {
                                args = new Object[1];
                                args[0] = client;
                            } else {
                                args = new Object[0];
                            }
                            invoker = (Invoker) m.invoke(null, args);
                        } catch (IllegalAccessException e) {
                            throw new AssertionError(e);
                        } catch (InvocationTargetException e) {
                            throw new AssertionError(e);
                        }
                    } else {
                        throw new AssertionError(m.toString());
                    }
                    for (ConsoleCommand command: invokes.commands()) {
                        invokers.put(command, invoker);
                    }
                }
            }
            return new TopInvoker(invokers.build());
        }
        
        protected final Map<ConsoleCommand, AsyncFunction<Invocation, String>> invokers;
        
        protected TopInvoker(
                Map<ConsoleCommand, AsyncFunction<Invocation, String>> invokers) {
            this.invokers = invokers;
        }
        
        @Override
        public ListenableFuture<String> apply(Invocation input)
                throws Exception {
            if (input.getCommand() == ConsoleCommand.MULTI) {
                if (EnvKey.MULTI.contains(input.getEnvironment())) {
                    List<Invocation> invocations = EnvKey.MULTI.remove(input.getEnvironment());
                    EnvKey.PROMPT.put(input.getEnvironment(), "%s $ ");
                    Object[] arguments = { input.getArguments()[0], invocations };
                    input = new Invocation(input.getEnvironment(), input.getCommand(), arguments);
                } else {
                    EnvKey.MULTI.put(input.getEnvironment(), Lists.<Invocation>newLinkedList());
                    EnvKey.PROMPT.put(input.getEnvironment(), "%s (multi) $ ");
                    return Futures.immediateFuture("");
                }
            } else if (EnvKey.MULTI.contains(input.getEnvironment())) {
                List<Invocation> invocations = EnvKey.MULTI.get(input.getEnvironment());
                invocations.add(input);
                return Futures.immediateFuture("");
            }
            AsyncFunction<Invocation, String> invoker = invokers.get(input.getCommand());
            checkArgument(invoker != null, input.getCommand());
            return invoker.apply(input);
        }
    }

    public static class ShellInvoker implements Invoker {

        @Invokes(commands={ConsoleCommand.HELP, ConsoleCommand.EXIT, ConsoleCommand.PRINTENV, ConsoleCommand.CD})
        public static ShellInvoker invoker() {
            return new ShellInvoker();
        }
        
        @Override
        public ListenableFuture<String> apply(Invocation input)
                throws Exception {
            switch (input.getCommand()) {
            case HELP:
                return Futures.immediateFuture(help());
            case EXIT:
                return Futures.immediateFuture(exit());
            case PRINTENV:
                return Futures.immediateFuture(printEnv(input.getEnvironment()));
            case CD:
                return Futures.immediateFuture(cd(input));
            default:
                throw new IllegalArgumentException(String.valueOf(input));
            }
        }
        
        protected String cd(Invocation invocation) {
            EnvKey.CWD.put(
                    invocation.getEnvironment(), invocation.getArguments()[1]);
            return "";
        }
        
        protected String printEnv(Map<String, Object> env) {
            return Joiner.on('\n').withKeyValueSeparator("\t").join(env);
        }
        
        protected String exit() {
            return "";
        }

        protected String help() throws Exception {
            Joiner joiner = Joiner.on('\t');
            StringBuilder str = new StringBuilder();
            for (ConsoleCommand e: ConsoleCommand.values()) {
                joiner.appendTo(str, getUsage(e)).append('\n');
            }
            return str.toString();
        }

        public List<String> getUsage(ConsoleCommand command) {
            ImmutableList.Builder<String> tokens = ImmutableList.builder();
            String names = Joiner.on(',')
                    .appendTo(new StringBuilder().append('{'), command.getNames()).append('}')
                    .toString();
            tokens.add(names);
            for (ArgumentDescriptor a : command.getArguments()) {
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
                    if (a.type() == BooleanArgument.class) {
                        argument.append(BooleanArgument.getUsage());
                    } else if (a.type() == ModeArgument.class) {
                        argument.append(ModeArgument.getUsage());
                    } else {
                        throw new AssertionError(String.valueOf(a.token()));
                    }
                    break;
                }
                if (!a.value().isEmpty()) {
                    argument.append('(').append(a.value()).append(')');
                }
                tokens.add(argument.toString());
            }
            if (!command.getDescription().isEmpty()) {
                tokens.add(command.getDescription());
            }
            return tokens.build();
        }
    }
    
    public static class ClientExecutorInvoker implements Invoker {
        
        @Invokes(commands={
                ConsoleCommand.MULTI,
                ConsoleCommand.CREATE,
                ConsoleCommand.LS, 
                ConsoleCommand.EXISTS, 
                ConsoleCommand.GET, 
                ConsoleCommand.GETACL,
                ConsoleCommand.RM,
                ConsoleCommand.SET,
                ConsoleCommand.SYNC})
        public static ClientExecutorInvoker invoker(ClientExecutor<? super Operation.Request, ?> client) {
            return new ClientExecutorInvoker(client);
        }

        protected static final Executor executor = MoreExecutors.sameThreadExecutor();
        
        protected final ClientExecutor<? super Operation.Request, ?> client;
        protected final RequestBuilder operator;
        
        public ClientExecutorInvoker(
                ClientExecutor<? super Operation.Request, ?> client) {
            this.client = client;
            this.operator = new RequestBuilder();
        }
        
        @Override
        public ListenableFuture<String> apply(Invocation input) throws Exception {
            Records.Request request = operator.apply(input);
            return new RequestSubmitter(request).call();
        }
        
        protected class RequestSubmitter implements Callable<ListenableFuture<String>>, Function<Operation.ProtocolResponse<?>, String> {

            protected final Records.Request request;
            
            public RequestSubmitter(Records.Request request) {
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
                            str.append(new RequestSubmitter(request).apply(ProtocolResponseMessage.of(input.xid(), input.zxid(), response))).append('\n');
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
    }
    
    public static class RequestBuilder implements Function<Invocation, Records.Request> {
        @SuppressWarnings("unchecked")
        @Override
        public Records.Request apply(Invocation input) {
            switch (input.getCommand()) {
            case MULTI:
            {
                ImmutableList.Builder<Records.MultiOpRequest> requests = ImmutableList.builder();
                for (Invocation e: (List<Invocation>) input.getArguments()[1]) {
                    requests.add((Records.MultiOpRequest) apply(e));
                }
                return new IMultiRequest(requests.build());
            }
            case CREATE:
                return Operations.Requests.create()
                        .setPath((ZNodeLabel.Path) input.getArguments()[1])
                        .setData(((String) input.getArguments()[2]).getBytes())
                        .setMode((CreateMode) input.getArguments()[3])
                        .setStat((Boolean) input.getArguments()[4])
                        .build();
            case EXISTS:
                return Operations.Requests.exists()
                        .setPath((ZNodeLabel.Path) input.getArguments()[1])
                        .setWatch((Boolean) input.getArguments()[2])
                        .build();
            case GETACL:
                return Operations.Requests.getAcl()
                        .setPath((ZNodeLabel.Path) input.getArguments()[1])
                        .build();
            case GET:
                return Operations.Requests.getData()
                        .setPath((ZNodeLabel.Path) input.getArguments()[1])
                        .setWatch((Boolean) input.getArguments()[2])
                        .build();
            case LS:
                return Operations.Requests.getChildren()
                        .setPath((ZNodeLabel.Path) input.getArguments()[1])
                        .setWatch((Boolean) input.getArguments()[2])
                        .setStat((Boolean) input.getArguments()[3])
                        .build();
            case RM:
                return Operations.Requests.delete()
                        .setPath((ZNodeLabel.Path) input.getArguments()[1])
                        .setVersion((Integer) input.getArguments()[2])
                        .build();
            case SET:
                return Operations.Requests.setData()
                        .setPath((ZNodeLabel.Path) input.getArguments()[1])
                        .setData(((String) input.getArguments()[2]).getBytes())
                        .setVersion((Integer) input.getArguments()[3])
                        .build();
            case SYNC:
                return Operations.Requests.sync()
                        .setPath((ZNodeLabel.Path) input.getArguments()[1])
                        .build();
            default:
                throw new IllegalArgumentException(String.valueOf(input.getCommand()));
            }
        }
    }
    
    public static class RmrInvoker implements Invoker {

        @Invokes(commands={ConsoleCommand.RMR})
        public static RmrInvoker invoker(ClientExecutor<? super Operation.Request, ?> client) {
            return new RmrInvoker(client);
        }
        
        protected final ClientExecutor<? super Operation.Request, ?> client;
        
        public RmrInvoker(ClientExecutor<? super Operation.Request, ?> client) {
            this.client = client;
        }

        @Override
        public ListenableFuture<String> apply(Invocation input)
                throws Exception {
            ZNodeLabel.Path root = (ZNodeLabel.Path) input.getArguments()[1];
            return Futures.transform(TreeFetcher.<Set<ZNodeLabel.Path>>builder().setClient(client).setResult(new ComputeLeaves()).build().apply(root), new DeleteRoot(root));
        }

        protected static class ComputeLeaves implements Processor<Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Optional<Set<ZNodeLabel.Path>>> {

            protected final Set<ZNodeLabel.Path> leaves;
            
            public ComputeLeaves() {
                this.leaves = Sets.newHashSet();
            }
            
            @Override
            public synchronized Optional<Set<ZNodeLabel.Path>> apply(
                    Optional<Pair<Records.Request, ListenableFuture<? extends Operation.ProtocolResponse<?>>>> input)
                    throws Exception {
                if (input.isPresent()) {
                    Records.Response response = input.get().second().get().record();
                    if (response instanceof Records.ChildrenGetter) {
                        if (((Records.ChildrenGetter) response).getChildren().isEmpty()) {
                            leaves.add(ZNodeLabel.Path.of(((Records.PathGetter) input.get().first()).getPath()));
                        }
                    }
                    return Optional.absent();
                } else {
                    return Optional.of(leaves);
                }
            }
        }
        
        protected class DeleteRoot implements AsyncFunction<Optional<Set<ZNodeLabel.Path>>, String> {

            protected final ZNodeLabel.Path root;
            
            public DeleteRoot(ZNodeLabel.Path root) {
                this.root = root;
            }

            @Override
            public ListenableFuture<String> apply(Optional<Set<ZNodeLabel.Path>> result) {
                if (result.isPresent()) {
                    DeleteLeaves task = new DeleteLeaves(result.get(), SettableFuturePromise.<String>create());
                    task.run();
                    return task;
                } else {
                    // TODO
                    throw new UnsupportedOperationException();
                }
            }
                
            protected class DeleteLeaves extends PromiseTask<Set<ZNodeLabel.Path>, String> implements FutureCallback<ZNodeLabel.Path> {
                
                public DeleteLeaves(Set<ZNodeLabel.Path> task, Promise<String> promise) {
                    super(task, promise);
                }
                
                public synchronized void run() {
                    if (task().isEmpty()) {
                        set("");
                    } else {
                        for (ZNodeLabel.Path p: ImmutableSet.copyOf(task())) {
                            DeleteLeaf operation = new DeleteLeaf(p);
                            operation.run();
                        }
                    }
                }

                @Override
                public synchronized void onSuccess(ZNodeLabel.Path leaf) {
                    task().remove(leaf);
                    ZNodeLabel.Path parent = (ZNodeLabel.Path) leaf.head();
                    if (root.prefixOf(parent)) {
                        boolean empty = true;
                        for (ZNodeLabel.Path p: task()) {
                            if (parent.prefixOf(p)) {
                                empty = false;
                                break;
                            }
                        }
                        if (empty) {
                            task().add(parent);
                            DeleteLeaf operation = new DeleteLeaf(parent);
                            operation.run();
                        }
                    }
                    if (task().isEmpty()) {
                        set("");
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    setException(t);
                }

                protected class DeleteLeaf implements FutureCallback<Operation.ProtocolResponse<?>> {
                    
                    protected final ZNodeLabel.Path leaf;
                    
                    public DeleteLeaf(ZNodeLabel.Path leaf) {
                        this.leaf = leaf;
                    }
                    
                    public void run() {
                        Futures.addCallback(
                                client.submit(Operations.Requests.delete().setPath(leaf).build()), 
                                this);
                    }
    
                    @Override
                    public void onSuccess(Operation.ProtocolResponse<?> result) {
                        if (result.record().opcode() == OpCode.DELETE) {
                            DeleteLeaves.this.onSuccess(leaf);
                        } else {
                            // TODO
                            onFailure(KeeperException.create(((Operation.Error) result.record()).error()));
                        }
                    }
    
                    @Override
                    public void onFailure(Throwable t) {
                        DeleteLeaves.this.onFailure(t);
                    }
                }
            }
        }
    }
    
    private Commands() {}
}
