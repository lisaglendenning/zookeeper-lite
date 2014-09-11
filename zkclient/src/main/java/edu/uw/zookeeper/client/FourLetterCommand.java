package edu.uw.zookeeper.client;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ChainedFutures;
import edu.uw.zookeeper.common.ChainedFutures.ChainedProcessor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.FourLetterWord;
import edu.uw.zookeeper.protocol.Message;

/**
 * Assumes that the server response is a single packet.
 */
public class FourLetterCommand extends ForwardingListenableFuture<String> implements ChainedProcessor<ListenableFuture<?>>, Connection.Listener<Message.ServerAnonymous>, Runnable {

    public static ListenableFuture<String> call(
            FourLetterWord word,
            ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> connection,
            Promise<String> response) {
        return ChainedFutures.run(
                ChainedFutures.process(
                    ChainedFutures.chain(
                            new FourLetterCommand(word, connection, response),
                            Lists.<ListenableFuture<?>>newArrayListWithCapacity(3)),
                    ChainedFutures.<String>castLast()),
                response);
    }
    
    public static ListenableFuture<String> callThenClose(
            final FourLetterWord word,
            final ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> connection,
            final Promise<String> response) {
        ListenableFuture<String> future = call(word, connection, response);
        future.addListener(
                CloseConnection.create(connection), 
                MoreExecutors.directExecutor());
        return future;
    }
    
    protected final FourLetterWord word;
    protected final ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> connection;
    protected final Promise<String> response;
    
    protected FourLetterCommand(
            FourLetterWord word,
            ListenableFuture<? extends Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?>> connection,
            Promise<String> response) {
        this.word = word;
        this.connection = connection;
        this.response = response;
        addListener(this, MoreExecutors.directExecutor());
    }
    
    @Override
    public Optional<? extends ListenableFuture<?>> apply(
            List<ListenableFuture<?>> input) throws Exception {
        if (input.isEmpty()) {
            return Optional.of(connection);
        }
        ListenableFuture<?> last = input.get(input.size()-1);
        if (last == connection) {
            Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?> connection;
            try {
                connection = this.connection.get();
            } catch (ExecutionException e) {
                cancel(false);
                return Optional.absent();
            }
            connection.subscribe(this);
            return Optional.of(connection.write(FourLetterRequest.forWord(word)));
        } else if (last == this) {
            return Optional.absent();
        } else {
            try {
                last.get();
            } catch (ExecutionException e) {
                cancel(false);
                return Optional.absent();
            }
            return Optional.of(this);
        }
    }

    @Override
    public void handleConnectionState(Automaton.Transition<Connection.State> state) {
        switch (state.to()) {
        case CONNECTION_CLOSING:
        case CONNECTION_CLOSED:
            Futures.getUnchecked(connection).unsubscribe(this);
            if (!isDone()) {
                response.setException(new KeeperException.ConnectionLossException());
            }
            break;
        default:
            break;
        }
    }

    @Override
    public void handleConnectionRead(Message.ServerAnonymous message) {
        Futures.getUnchecked(connection).unsubscribe(this);
        if (!isDone()) {
            response.set(((FourLetterResponse) message).stringValue());
        }
    }

    @Override
    public void run() {
        if (isDone()) {
            if (connection.isDone()) {
                if (!connection.isCancelled()) {
                    try {
                        Connection<? super Message.ClientAnonymous,? extends Message.ServerAnonymous,?> connection = this.connection.get();
                        connection.unsubscribe(this);
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                    }
                }
            } else {
                connection.cancel(false);
            }
        }
    }

    @Override
    protected ListenableFuture<String> delegate() {
        return response;
    }
    
    public static class CloseConnection<T extends Connection<?,?,?>> extends ForwardingListenableFuture<T> implements Runnable {

        public static <T extends Connection<?,?,?>> CloseConnection<T> create(ListenableFuture<T> future) {
            return new CloseConnection<T>(future);
        }
        
        protected final ListenableFuture<T> future;
        
        protected CloseConnection(ListenableFuture<T> future) {
            this.future = future;
        }
        
        @Override
        public void run() {
            if (!isCancelled()) {
                try {
                    Connection<?,?,?> c = get();
                    c.close();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                } catch (ExecutionException e) {
                }
            }
        }

        @Override
        protected ListenableFuture<T> delegate() {
            return future;
        }
    }
}