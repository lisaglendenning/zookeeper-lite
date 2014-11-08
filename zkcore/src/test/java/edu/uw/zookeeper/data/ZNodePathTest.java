package edu.uw.zookeeper.data;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ZNodePathTest {

    @Test
    public void testCanonicalize() {
        assertEquals("/", ZNodePath.canonicalize("/"));
        assertEquals("/", ZNodePath.canonicalize("//"));
        assertEquals("/", ZNodePath.canonicalize("///"));
        assertEquals("/a", ZNodePath.canonicalize("/a"));
        assertEquals("/a", ZNodePath.canonicalize("/a/"));
        assertEquals("/", ZNodePath.canonicalize("/."));
        assertEquals("/a", ZNodePath.canonicalize("/a/."));
        assertEquals("/", ZNodePath.canonicalize("/a/.."));
    }
}
