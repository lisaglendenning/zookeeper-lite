package edu.uw.zookeeper.client.cli;

import static com.google.common.base.Preconditions.checkArgument;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import edu.uw.zookeeper.common.Pair;

public class DispatchingInvoker extends AbstractExecutionThreadService implements Invoker<Object> {
    
    @Invokes(commands={Object.class})
    public static DispatchingInvoker create(Shell shell) {
        return forInvokers(shell, ShellInvoker.class, ClientExecutorInvoker.class, RmrInvoker.class);
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
        Invoker<Object> invoker = invokers.get(input.getCommand().getClass()).second();
        checkArgument(invoker != null, input.getCommand());
        invoker.invoke(input);
    }

    @Override
    protected void startUp() throws Exception {
        for (Pair<Invokes, Invoker<Object>> invoker: invokers.values()) {
            shell.getRuntime().getServiceMonitor().addOnStart(invoker.second());
            if (! invoker.second().isRunning()) {
                invoker.second().startAsync().awaitRunning();
            }
        }
    }

    @Override
    protected void run() throws Exception {
        String line = null;
        while (isRunning() && ((line = shell.readLine()) != null)) {
            try {
                List<String> tokens = shell.getTokenizer().apply(line);
                Invocation<Object> invocation = shell.getCommands().apply(tokens);
                if (invocation == null) {
                    continue;
                }
                invoke(invocation);
            } catch (IllegalArgumentException e) {
                shell.printException(e);
                continue;
            }
        }
    }
    
    @Override
    protected void shutDown() throws Exception {
    }
}
