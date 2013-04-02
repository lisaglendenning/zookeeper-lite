package org.apache.zookeeper.util;

import com.google.common.base.Predicate;

public interface FilteringProcessor<T,V> extends Processor<T,V> {
    Predicate<? super T> filter();
}
