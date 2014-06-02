package edu.uw.zookeeper.client.random;

import java.util.List;
import java.util.Random;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Generator;

public class RandomFromList<E> extends AbstractPair<Random, List<E>> implements Generator<E> {

    public static <E> RandomFromList<E> create(Random random, List<E> elements) {
        return new RandomFromList<E>(random, elements);
    }
    
    protected RandomFromList(Random random, List<E> elements) {
        super(random, elements);
    }
    
    public Random getRandom() {
        return first;
    }
    
    public List<E> getElements() {
        return second;
    }

    @Override
    public E next() {
        int size = getElements().size();
        if (size == 0) {
            return null;
        } else {
            int index = (size > 1) ? getRandom().nextInt(size) : 0;
            return getElements().get(index);
        }
    }
}
