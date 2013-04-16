package edu.uw.zookeeper.protocol;

import java.io.IOException;
import java.io.InputStream;

public interface Decoder<T> {
    T decode(InputStream stream) throws IOException;
}
