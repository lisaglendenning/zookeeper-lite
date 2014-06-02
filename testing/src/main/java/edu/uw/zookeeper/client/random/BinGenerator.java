package edu.uw.zookeeper.client.random;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Iterator;
import java.util.Random;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.primitives.Floats;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.Pair;

public final class BinGenerator<V> extends AbstractPair<Random, ImmutableRangeMap<Float, V>> implements Generator<V> {
    
    public static <V> BinGenerator<V> create(
            Random random, Iterator<? extends Pair<Float, ? extends V>> weightedValues) {
        final ImmutableRangeMap.Builder<Float, V> bins = ImmutableRangeMap.builder();
        Float lower = Float.valueOf(0.0f);
        while (weightedValues.hasNext()) {
            Pair<Float, ? extends V> weighted = weightedValues.next();
            Float upper = Float.valueOf(Floats.min(1.0f, lower.floatValue() + weighted.first().floatValue()));
            checkArgument(upper.floatValue() > lower.floatValue());
            Range<Float> range = Range.closedOpen(lower, upper);
            bins.put(range, weighted.second());
            lower = upper;
        }
        checkArgument(Float.compare(lower.floatValue(), 1.0f) == 0);
        return new BinGenerator<V>(random, bins.build());
    }
    
    protected BinGenerator(Random random, ImmutableRangeMap<Float, V> bins) {
        super(random, bins);
    }
    
    public Random getRandom() {
        return first;
    }
    
    public ImmutableRangeMap<Float, V> getBins() {
        return second;
    }
    
    @Override
    public V next() {
        final float next = getRandom().nextFloat();
        return getBins().get(Float.valueOf(next));
    }
}
