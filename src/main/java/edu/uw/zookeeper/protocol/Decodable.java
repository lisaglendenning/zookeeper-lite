package edu.uw.zookeeper.protocol;

import java.io.IOException;
import java.io.InputStream;

public interface Decodable {
    Decodable decode(InputStream stream) throws IOException;
}
