package edu.uw.zookeeper.common;

public class Pair<U,V> extends AbstractPair<U,V> {

    public static <U,V> Pair<U,V> create(U first, V second) {
        return new Pair<U,V>(first, second);
    }

    public Pair(U first, V second) {
        super(first, second);
    }

    public U first() {
        return first;
    }

    public V second() {
        return second;
    }
}
