package edu.uw.zookeeper.net.intravm;


import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;

public class IntraVmConnection<I,O> extends AbstractIntraVmConnection<I,O,I,O,IntraVmEndpoint<I,O>,IntraVmConnection<I,O>> {

    public static <I,O> ParameterizedFactory<Pair<? extends IntraVmEndpoint<I,O>, ? extends AbstractIntraVmEndpoint<?,?,?,? super I>>, IntraVmConnection<I,O>> factory() {
        return new ParameterizedFactory<Pair<? extends IntraVmEndpoint<I,O>, ? extends AbstractIntraVmEndpoint<?,?,?,? super I>>, IntraVmConnection<I,O>>() {
            @Override
            public IntraVmConnection<I,O> get(Pair<? extends IntraVmEndpoint<I,O>, ? extends AbstractIntraVmEndpoint<?, ?, ?, ? super I>> value) {
                return newInstance(value.first(), value.second());
            }
        };
    }
    
    public static <I,O> IntraVmConnection<I,O> newInstance(
            IntraVmEndpoint<I,O> local,
            AbstractIntraVmEndpoint<?,?,?,? super I> remote) {
        return new IntraVmConnection<I,O>(local, remote);
    }

    protected IntraVmConnection(
            IntraVmEndpoint<I,O> local,
            AbstractIntraVmEndpoint<?,?,?,? super I> remote) {
        super(local, remote);
    }
}
