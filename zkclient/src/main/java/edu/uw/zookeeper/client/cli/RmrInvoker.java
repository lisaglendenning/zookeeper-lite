package edu.uw.zookeeper.client.cli;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.DeleteSubtree;
import edu.uw.zookeeper.data.AbsoluteZNodePath;
import edu.uw.zookeeper.protocol.proto.Records;

public class RmrInvoker extends AbstractIdleService implements Invoker<RmrInvoker.Command> {

    @Invokes(commands={Command.class})
    public static RmrInvoker create(Shell shell) {
        return new RmrInvoker(shell);
    }

    public static enum Command {
        @CommandDescriptor(
                names = {"rmr", "deleteall"}, 
                description="Recursive delete", 
                arguments = {
                        @ArgumentDescriptor(token = TokenType.PATH)})
        RMR;
    }
    
    protected final Shell shell;
    protected final Set<ListenableFuture<DeleteSubtree>> pending;
    
    public RmrInvoker(Shell shell) {
        this.shell = shell;
        this.pending = Collections.synchronizedSet(Sets.<ListenableFuture<DeleteSubtree>>newHashSet());
    }

    @Override
    public void invoke(final Invocation<Command> input)
            throws Exception {
        AbsoluteZNodePath root = (AbsoluteZNodePath) input.getArguments()[1];
        ClientExecutor<? super Records.Request, ?, ?> client = shell.getEnvironment().get(ClientExecutorInvoker.CLIENT_KEY).getConnectionClientExecutor();
        final ListenableFuture<DeleteSubtree> future = DeleteSubtree.forRoot(root, client);
        Futures.addCallback(future, new FutureCallback<DeleteSubtree>(){
            @Override
            public void onSuccess(DeleteSubtree result) {
                pending.remove(future);
                try {
                    shell.println(String.format("%s => OK", input));
                    shell.flush();
                } catch (IOException e) {
                    onFailure(e);
                }
            }
            @Override
            public void onFailure(Throwable t) {
                pending.remove(future);
                try {
                    shell.printThrowable(new RuntimeException(String.format("%s => FAILED (%s)", input, t)));
                } catch (IOException e) {
                }
                if (isRunning()) {
                    stopAsync();
                }
            }});
        pending.add(future);
    }

    @Override
    protected void startUp() throws Exception {
        for (Command command: Command.values()) {
            shell.getCommands().withCommand(command);
        }
    }

    @Override
    protected void shutDown() throws Exception {
        synchronized (pending) {
            for (ListenableFuture<?> e: Iterables.consumingIterable(pending)) {
                e.cancel(true);
            }
        }
    }
}