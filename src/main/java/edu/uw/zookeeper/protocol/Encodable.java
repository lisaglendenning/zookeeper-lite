package edu.uw.zookeeper.protocol;

import java.io.IOException;
import java.io.OutputStream;

public interface Encodable {
    OutputStream encode(OutputStream stream) throws IOException;
}
