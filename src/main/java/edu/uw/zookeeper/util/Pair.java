package edu.uw.zookeeper.util;

import com.google.common.base.Objects;

public class Pair<U,V> {

    public static <U,V> Pair<U,V> create(U first, V second) {
        return new Pair<U,V>(first, second);
    }

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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Pair<?,?> other = (Pair<?,?>) obj;
        return Objects.equal(first(), other.first())
                && Objects.equal(second(), other.second());
    }
}
