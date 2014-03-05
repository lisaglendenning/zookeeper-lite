package edu.uw.zookeeper.client.cli;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jline.console.completer.Completer;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public class DispatchingInvoker extends AbstractExecutionThreadService implements Invoker<Object>, Completer {
    
    @Invokes(commands={Object.class})
    public static DispatchingInvoker defaults(Shell shell, Class<?>...types) {
        Class<?>[] withDefaults = new Class<?>[types.length + 3];
        withDefaults[0] = ShellInvoker.class;
        withDefaults[1] = ClientExecutorInvoker.class;
        withDefaults[2] = RmrInvoker.class;
        for (int i=0; i<types.length; ++i) {
            withDefaults[i+3] = types[i];
        }
        return forInvokers(shell, withDefaults);
    }

    public static DispatchingInvoker forInvokers(Shell shell, Class<?>...types) {
        ImmutableMap.Builder<Class<?>, Pair<Invokes, Invoker<Object>>> invokers = ImmutableMap.builder();
        for (Class<?> type: types) {
            Pair<Invokes, Invoker<Object>> invoker = getInvoker(shell, type);
            for (Class<?> c: invoker.first().commands()) {
                invokers.put(c, invoker);
            }
        }
        return new DispatchingInvoker(shell, invokers.build());
    }
    
    @SuppressWarnings("unchecked")
    protected static Pair<Invokes, Invoker<Object>> getInvoker(Shell shell, Class<?> cls) {
        try {
            for (Method m: cls.getDeclaredMethods()) {
                Invokes invokes = m.getAnnotation(Invokes.class);
                if (invokes == null) {
                    continue;
                }
                Invoker<Object> invoker;
                if (Modifier.isStatic(m.getModifiers())) {
                    try {
                        Object[] args;
                        if (m.getParameterTypes().length > 0) {
                            args = new Object[1];
                            args[0] = shell;
                        } else {
                            args = new Object[0];
                        }
                        invoker = (Invoker<Object>) m.invoke(null, args);
                    } catch (IllegalAccessException e) {
                        throw new AssertionError(e);
                    } catch (InvocationTargetException e) {
                        throw new AssertionError(e);
                    }
                } else {
                    throw new IllegalArgumentException(m.toString());
                }
                return Pair.create(invokes, invoker);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(String.valueOf(cls), e);
        }
        return null;
    }

    protected final Logger logger = LogManager.getLogger(getClass());
    protected final Shell shell;
    protected final Map<Class<?>, Pair<Invokes, Invoker<Object>>> invokers;
    
    protected DispatchingInvoker(
            Shell shell,
            Map<Class<?>, Pair<Invokes, Invoker<Object>>> invokers) {
        this.shell = shell;
        this.invokers = invokers;
    }

    @Override
    public void invoke(Invocation<Object> input)
            throws Exception {
        Invoker<Object> invoker = invokers.get(input.getCommand().second().getClass()).second();
        checkArgument(invoker != null, input.getCommand());
        invoker.invoke(input);
    }

    @Override
    public int complete(String buffer, int cursor,
            List<CharSequence> candidates) {
        logger.entry(buffer, cursor, candidates);
        List<Pair<Integer, String>> tokens;
        if (buffer == null) {
            tokens = ImmutableList.of();
        } else {
            tokens = ImmutableList.copyOf(shell.getTokenizer().apply(buffer));
        }

        int argIndex = -1;
        int ret;
        String name;
        Pair<CommandDescriptor, Object> command;
        if (tokens.isEmpty()) {
            ret = cursor;
            name = "";
            command = null;
        } else {
            Pair<Integer, String> token = tokens.get(0);
            ret = token.first();
            name = token.second();
            command = shell.getCommands().getCommand(name);
        }

        ImmutableSortedSet.Builder<String> builder = ImmutableSortedSet.naturalOrder();
        if (command == null) {
            for (Map.Entry<CharSequence, Pair<CommandDescriptor, Object>> e : shell.getCommands().getCommands()) {
                String candidate = String.valueOf(e.getKey());
                if (candidate.startsWith(name)) {
                    builder.add(candidate);
                    command = e.getValue();
                }
            }
        } else {
            String token;
            if (shell.getTokenizer().delimiter().second().matches(buffer.charAt(cursor - 1))) {
                argIndex = tokens.size() - 1;
                ret = cursor;
                token = "";
            } else {
                argIndex = tokens.size() - 2;
                ret = tokens.get(argIndex + 1).first();
                token = tokens.get(argIndex + 1).second();
            }
            ArgumentDescriptor[] arguments = command.first().arguments();
            if ((argIndex < 0) || (argIndex >= arguments.length)) {
                return logger.exit(-1);
            }
            ArgumentDescriptor ad = arguments[argIndex];
            switch (ad.token()) {
            case ENUM:
                for (Object e: ad.type().getEnumConstants()) {
                    String value = e.toString();
                    if (value.startsWith(token)) {
                        builder.add(value);
                    }
                }
                break;
            case PATH:
            {
                ClientExecutor<Operation.Request, ? extends Message.ServerResponse<?>, ?> client = shell.getEnvironment().get(ClientExecutorInvoker.CLIENT_KEY).getConnectionClientExecutor();
                ZNodePath path = ZNodePath.canonicalized(ZNodePath.join(shell.getEnvironment().get(ShellInvoker.CWD_KEY).toString(), token));
                try {
                    Operations.Requests.GetChildren request = Operations.Requests.getChildren();
                    if (token.isEmpty() || token.endsWith(Character.toString(ZNodeLabel.SLASH))) {
                        request.setPath(path);
                    } else {
                        request.setPath((ZNodePath) path.head());
                    }
                    Message.ServerResponse<?> response = client.submit(request.build()).get();
                    logger.trace("{} {} {}", path, token, response);
                    if (response.record() instanceof Records.ChildrenGetter) {
                        if (token.isEmpty() || token.endsWith(Character.toString(ZNodeLabel.SLASH))) {
                            ret = cursor;
                            builder.addAll(((Records.ChildrenGetter) response.record()).getChildren());
                        } else {
                            int lastSlash = token.lastIndexOf(ZNodeLabel.SLASH);
                            String prefix;
                            if (lastSlash >= 0) {
                                ret += lastSlash + 1;
                                prefix = token.substring(lastSlash + 1);
                            } else {
                                prefix = token;
                            }
                            for (String child: ((Records.ChildrenGetter) response.record()).getChildren()) {
                                if (child.startsWith(prefix)) {
                                    builder.add(child);
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
                break;
            }
            default:
                break;
            }
        }
        
        ImmutableSortedSet<String> values = builder.build();
        if (values.isEmpty()) {
            return logger.exit(-1);
        }
        if ((values.size() == 1) && (command != null) && (command.first().arguments().length > argIndex + 1)) {
            values = ImmutableSortedSet.of(Iterables.getOnlyElement(values) + shell.getTokenizer().delimiter().first());
        }
        candidates.addAll(values);
        logger.trace("{}", values);
        return logger.exit(ret);
    }

    @Override
    protected void startUp() throws Exception {
        for (Pair<Invokes, Invoker<Object>> invoker: invokers.values()) {
            shell.getRuntime().getServiceMonitor().addOnStart(invoker.second());
            if (! invoker.second().isRunning()) {
                invoker.second().startAsync().awaitRunning();
            }
        }
        shell.getReader().addCompleter(this);
    }

    @Override
    protected void run() throws Exception {
        String line = null;
        while (isRunning() && shell.isRunning() && ((line = shell.readLine()) != null)) {
            try {
                TokenParser.TokenIterator itr = shell.getTokenizer().apply(line);
                ImmutableList.Builder<String> tokens = ImmutableList.builder();
                while (itr.hasNext()) {
                    tokens.add(itr.next().second());
                }
                Invocation<Object> invocation = shell.getCommands().apply(tokens.build());
                if (invocation == null) {
                    continue;
                }
                invoke(invocation);
            } catch (IllegalArgumentException e) {
                shell.printThrowable(e);
                continue;
            }
        }
    }
    
    @Override
    protected void shutDown() throws Exception {
    }
}
