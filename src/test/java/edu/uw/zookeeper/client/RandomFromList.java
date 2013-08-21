package edu.uw.zookeeper.client;

import java.util.List;
import java.util.Random;

public abstract class RandomFromList<E> implements Generator<E> {

    protected final Random random;
    protected final List<E> elements;
    
    protected RandomFromList(Random random, List<E> elements) {
        this.random = random;
        this.elements = elements;
    }

    @Override
    public E next() {
        int size = elements.size();
        if (size == 0) {
            return null;
        } else {
            int index = random.nextInt(size);
            return elements.get(index);
        }
    }
}
