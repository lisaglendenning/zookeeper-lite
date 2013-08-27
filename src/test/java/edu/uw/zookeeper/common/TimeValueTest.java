package edu.uw.zookeeper.common;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TimeValueTest {

    @Test
    public void test() {
        TimeValue t = TimeValue.milliseconds(1);
        assertEquals(t, TimeValue.create(1, TimeUnit.MILLISECONDS));
        assertEquals(t, TimeValue.fromString(t.toString()));
    }
}
