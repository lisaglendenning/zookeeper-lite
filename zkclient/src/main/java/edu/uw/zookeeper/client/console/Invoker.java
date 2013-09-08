package edu.uw.zookeeper.client.console;

import com.google.common.util.concurrent.AsyncFunction;

public interface Invoker extends AsyncFunction<Invocation, String> {}