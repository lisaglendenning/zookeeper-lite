package edu.uw.zookeeper.protocol;

import java.io.IOException;
import java.io.OutputStream;

public interface Encoder<T> {
    OutputStream encode(T input, OutputStream stream) throws IOException;
}
