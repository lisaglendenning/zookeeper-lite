package edu.uw.zookeeper.common;

import com.google.common.base.Optional;

public class CachingBuilder<T, U extends Builder<T>> extends Factories.Holder<U> implements Builder<T> {

    public static <T, U extends Builder<T>> CachingBuilder<T,U> fromBuilder(U builder) {
        return new CachingBuilder<T,U>(builder);
    }
    
    private Optional<T> result;
    
    public CachingBuilder(U instance) {
        super(instance);
        this.result = Optional.absent();
    }

    /**
     * Not thread-safe
     */
    @Override
    public T build() {
        if (! result.isPresent()) {
            result = Optional.of(get().build());
        }
        return result.get();
    }

}
