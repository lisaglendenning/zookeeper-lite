package edu.uw.zookeeper.util;

import com.google.common.base.Objects;

/**
 * Helper class for implementing a two-element tuple.
 * 
 * @param <U>
 * @param <V>
 */
public abstract class AbstractPair<U,V> {

    protected final U first;
    protected final V second;

    protected AbstractPair(U first, V second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("first", first)
                .add("second", second)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(first, second);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AbstractPair<?,?> other = (AbstractPair<?,?>) obj;
        return Objects.equal(first, other.first)
                && Objects.equal(second, other.second);
    }
}