package edu.uw.zookeeper.client.cli;

import com.google.common.util.concurrent.Service;

public interface Invoker<T> extends Service {
    void invoke(Invocation<T> invocation) throws Exception;
}