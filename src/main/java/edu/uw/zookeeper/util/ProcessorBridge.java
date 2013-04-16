package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.checkNotNull;

public class ProcessorBridge<T, U, V> extends
        Pair<Processor<T, U>, Processor<U, V>> implements Processor<T, V> {

    public static <T, U, V> ProcessorBridge<T, U, V> create(
            Processor<T, U> first, Processor<U, V> second) {
        return new ProcessorBridge<T, U, V>(first, second);
    }

    protected ProcessorBridge(Processor<T, U> first, Processor<U, V> second) {
        super(checkNotNull(first), checkNotNull(second));
    }

    @Override
    public V apply(T input) throws Exception {
        return second().apply(first().apply(input));
    }
}
