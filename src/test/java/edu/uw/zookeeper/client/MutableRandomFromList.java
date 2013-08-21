package edu.uw.zookeeper.client;

import java.util.Random;

import com.google.common.collect.Lists;

public class MutableRandomFromList<E> extends RandomFromList<E> {

    public static <E> MutableRandomFromList<E> create(Random random, Iterable<E> elements) {
        return new MutableRandomFromList<E>(random, elements);
    }
    
    public MutableRandomFromList(Random random, Iterable<E> elements) {
        super(random, Lists.newArrayList(elements));
    }
    
    public boolean add(E element) {
        return elements.add(element);
    }

    public boolean remove(E element) {
        return elements.remove(element);
    }
}