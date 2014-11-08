package edu.uw.zookeeper.data;

import static org.junit.Assert.*;

import java.util.Comparator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.primitives.UnsignedInteger;

@RunWith(JUnit4.class)
public class SequentialTest {

    @Test
    public void testSequentialCompareTo() {
        Sequential.Overflowed<String> overflowed = Sequential.overflowed("a");
        compareToEquals(overflowed, Sequential.overflowed(overflowed.prefix()));
        Sequential.Sequenced<String> sequenced = Sequential.sequenced(overflowed.prefix(), UnsignedInteger.MAX_VALUE);
        compareToEquals(sequenced, Sequential.sequenced(sequenced.prefix(), sequenced.sequence()));
        compareToLessThan(sequenced, overflowed);
        compareToLessThan(
                Sequential.sequenced("a", UnsignedInteger.ZERO),
                Sequential.sequenced("b", UnsignedInteger.ZERO));
        compareToLessThan(
                Sequential.sequenced("b", UnsignedInteger.ZERO),
                Sequential.sequenced("a", UnsignedInteger.ONE));
        compareToLessThan(
                Sequential.overflowed("a"),
                Sequential.overflowed("b"));
    }

    @Test
    public void testSequentialComparator() {
        Comparator<String> comparator = Sequential.comparator();
        compareTo(comparator, "a", "b");
        compareToLessThan(
                comparator, 
                "a", 
                Sequential.sequenced("a", UnsignedInteger.ZERO).toString());
        compareToLessThan(
                comparator, 
                Sequential.sequenced("a", UnsignedInteger.ZERO).toString(), 
                "b");
        compareToString(
                comparator,
                Sequential.sequenced("b", UnsignedInteger.ZERO),
                Sequential.sequenced("a", UnsignedInteger.ONE));
    }
    
    protected <T extends Comparable<? super T>> void compareToString(Comparator<? super String> comparator, T v1, T v2) {
        assertEquals(v1.compareTo(v2), comparator.compare(v1.toString(), v2.toString()));
        assertEquals(v2.compareTo(v1), comparator.compare(v2.toString(), v1.toString()));
    }
    
    protected <T extends Comparable<? super T>> void compareTo(Comparator<? super T> comparator, T v1, T v2) {
        assertEquals(v1.compareTo(v2), comparator.compare(v1, v2));
        assertEquals(v2.compareTo(v1), comparator.compare(v2, v1));
    }
    
    protected <T> void compareToLessThan(Comparator<? super T> comparator, T v1, T v2) {
        assertTrue(comparator.compare(v1, v2) < 0);
        assertTrue(comparator.compare(v2, v1) > 0);
    }
    
    protected <T extends Comparable<? super T>> void compareToLessThan(final T v1, final T v2) {
        assertTrue(v1.compareTo(v2) < 0);
        assertTrue(v2.compareTo(v1) > 0);
    }
    
    protected <T extends Comparable<? super T>> void compareToEquals(final T v1, final T v2) {
        assertEquals(v1, v2);
        assertEquals(0, v1.compareTo(v2));
        assertEquals(0, v2.compareTo(v1));
    }
}
