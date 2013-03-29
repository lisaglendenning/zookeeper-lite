package org.apache.zookeeper.util;

import com.google.common.base.Optional;

public interface PipeProcessor<T> extends Processor<T,Optional<T>> {
    public Optional<T> apply(T input) throws Exception;
}
