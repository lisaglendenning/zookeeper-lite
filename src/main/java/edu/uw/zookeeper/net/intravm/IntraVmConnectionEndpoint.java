package edu.uw.zookeeper.net.intravm;

import java.util.concurrent.Executor;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.PublisherActor;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class IntraVmConnectionEndpoint extends AbstractActor<Optional<Object>> implements Publisher, Executor {

    public static IntraVmConnectionEndpoint create(
            Publisher publisher,
            Executor executor) {
        return new IntraVmConnectionEndpoint(publisher, executor);
    }
    
    protected final PublisherActor publisher;
    protected final IntraVmSocketAddress address;
    protected final Promise<Void> stopped;
    
    public IntraVmConnectionEndpoint(
            Publisher publisher,
            Executor executor) {
        super(executor, AbstractActor.<Optional<Object>>newQueue(), newState());
        this.address = IntraVmSocketAddress.of(this);
        this.publisher = PublisherActor.newInstance(publisher, executor);
        this.stopped = SettableFuturePromise.create();
    }
    
    public ListenableFuture<Void> stopped() {
        return stopped;
    }
    
    public IntraVmSocketAddress address() {
        return address;
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public void post(Object object) {
        publisher.post(object);
    }

    @Override
    public void register(Object object) {
        publisher.register(object);
    }

    @Override
    public void unregister(Object object) {
        publisher.unregister(object);
    }

    @Override
    protected boolean apply(Optional<Object> input) throws Exception {
        if (input.isPresent()) {
            post(input.get());
            return true;
        } else {
            stop();
            return false;
        }
    }

    @Override
    protected void doStop() {
        super.doStop();

        stopped.set(null);
    }
}