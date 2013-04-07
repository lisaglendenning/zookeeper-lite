package org.apache.zookeeper.util;

import java.util.Map;

public interface Configuration extends Iterable<Map.Entry<String, Object>> {

    Configuration initialize(Arguments arguments);

    <T> T get(String name, T defaultValue);

    void set(String name, Object value);

    void flush() throws Exception;
}
