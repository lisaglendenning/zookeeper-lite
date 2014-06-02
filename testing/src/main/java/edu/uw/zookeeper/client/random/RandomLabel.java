package edu.uw.zookeeper.client.random;

import static com.google.common.base.Preconditions.*;

import java.util.Random;

import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.data.ZNodeLabel;

public class RandomLabel implements Generator<ZNodeLabel> {

    public static RandomLabel create(Random random, int minLength, int lengthRange) {
        return new RandomLabel(random, ALPHABET, minLength, lengthRange);
    }
    
    // no easy way to get a dictionary?
    public static final String LOWER_ALPHA = "abcdefghijklmnopqrstuvwxyz";
    public static final String UPPER_ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    public static final String SYMBOLS = "._-";
    public static final char[] ALPHABET = 
            new StringBuilder()
            .append(LOWER_ALPHA)
            .append(UPPER_ALPHA)
            .append(SYMBOLS)
            .toString().toCharArray();
    
    protected final Random random;
    protected final int minLength;
    protected final int lengthRange;
    protected final char[] alphabet;
    
    public RandomLabel(Random random, char[] alphabet, int minLength, int lengthRange) {
        this.random = checkNotNull(random);
        checkArgument(minLength > 0);
        checkArgument(lengthRange >= 0);
        this.minLength = minLength;
        this.lengthRange = lengthRange + 1;
        this.alphabet = alphabet;
    }
    
    @Override
    public ZNodeLabel next() {
        ZNodeLabel next = null;
        do {
            int length = random.nextInt(lengthRange) + minLength;
            char[] chars = new char[length];
            for (int i=0; i<length; ++i) {
                chars[i] = alphabet[random.nextInt(alphabet.length)];
            }
            try {
                next = ZNodeLabel.validated(String.valueOf(chars));
            } catch (IllegalArgumentException e) {
                next = null;
            }
        } while ((next == null) || next.isReserved());
        return next;
    }
}