package org.apache.zookeeper.util;

public interface Configuration {

    <T> void set(String name, T value) throws Exception;

    <T> T get(String name, T defaultValue);

    Configuration initialize(Arguments arguments);
}
