package edu.uw.zookeeper.client.console;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public abstract class Commands {
    
    public static Invoker invoker(ClientExecutor<? super Operation.Request, ?> client) {
        return MapInvoker.newInstance(client);
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
    
    public static class MapInvoker implements Invoker {

        public static MapInvoker newInstance(ClientExecutor<? super Operation.Request, ?> client) {
            ImmutableMap.Builder<ConsoleCommand, AsyncFunction<Invocation, String>> invokers = ImmutableMap.builder();
            for (Class<?> cls: Commands.class.getDeclaredClasses()) {
                if (cls == MapInvoker.class) {
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
            return new MapInvoker(invokers.build());
        }
        
        protected final Map<ConsoleCommand, AsyncFunction<Invocation, String>> invokers;
        
        protected MapInvoker(
                Map<ConsoleCommand, AsyncFunction<Invocation, String>> invokers) {
            this.invokers = invokers;
        }
        
        @Override
        public ListenableFuture<String> apply(Invocation input)
                throws Exception {
            AsyncFunction<Invocation, String> invoker = invokers.get(input.getCommand());
            checkArgument(invoker != null, input.getCommand());
            return invoker.apply(input);
        }
    }

    public static class HelpCommand implements Callable<String> {

        @Invokes(commands={ConsoleCommand.HELP})
        public static Invoker invoker() {
            return invokeCallable(new HelpCommand());
        }
        
        @Override
        public String call() throws Exception {
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
    
    public static class ExitCommand implements Callable<String> {

        @Invokes(commands={ConsoleCommand.EXIT})
        public static Invoker invoker() {
            return invokeCallable(new ExitCommand());
        }
        
        @Override
        public String call() throws Exception {
            return "";
        }
    }
    
    public static class ClientExecutorInvoker implements Invoker {
        
        @Invokes(commands={
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
        @Override
        public Records.Request apply(Invocation input) {
            switch (input.getCommand()) {
            case CREATE:
                return Operations.Requests.create()
                        .setPath((ZNodeLabel.Path) input.getArguments()[1])
                        .setMode((CreateMode) input.getArguments()[2])
                        .setData(((String) input.getArguments()[3]).getBytes())
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
                        .setVersion((Integer) input.getArguments()[2])
                        .setData(((String) input.getArguments()[3]).getBytes())
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

    private Commands() {}
}
