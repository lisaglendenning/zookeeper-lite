package org.apache.zookeeper.util;

import com.google.common.base.Objects;

public class Pair<U,V> {

    protected final U first;
    protected final V second;
    
    public Pair(U first, V second) {
        this.first = first;
        this.second = second;
    }
    
    public U first() {
        return first;
    }
    
    public V second() {
        return second;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("first", first())
                .add("second", second())
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(first(), second());
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Pair<U,V> other = (Pair<U,V>) obj;
        return Objects.equal(first(), other.first()) 
                && Objects.equal(second(), other.second());
    }
}