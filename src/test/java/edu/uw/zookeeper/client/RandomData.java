package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.*;

import java.util.Random;

import edu.uw.zookeeper.common.Generator;

public class RandomData implements Generator<byte[]> {

    public static RandomData create(Random random, int minLength, int lengthRange) {
        return new RandomData(random, minLength, lengthRange);
    }

    protected final Random random;
    protected final int minLength;
    protected final int lengthRange;
    
    public RandomData(Random random, int minLength, int lengthRange) {
        this.random = checkNotNull(random);
        checkArgument(minLength >= 0);
        checkArgument(lengthRange >= 0);
        this.minLength = minLength;
        this.lengthRange = lengthRange + 1;
    }

    @Override
    public byte[] next() {
        int length = random.nextInt(lengthRange) + minLength;
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }
}