package edu.uw.zookeeper.client;

import java.util.Random;

import com.google.common.collect.ImmutableList;

public class ImmutableRandomFromList<E> extends RandomFromList<E> {

    public static <E> ImmutableRandomFromList<E> create(Random random, Iterable<E> elements) {
        return new ImmutableRandomFromList<E>(random, elements);
    }
    
    public ImmutableRandomFromList(Random random, Iterable<E> elements) {
        super(random, ImmutableList.copyOf(elements));
    }
}