package edu.uw.zookeeper.netty.nio;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import edu.uw.zookeeper.util.DefaultsFactory;

public enum NioEventLoopGroupFactory implements DefaultsFactory<ThreadFactory, EventLoopGroup> {

    DEFAULT(NioEventLoopGroup.DEFAULT_POOL_SIZE);
    
    public static NioEventLoopGroupFactory getInstance() {
        return DEFAULT;
    }

    private final int nthreads;
    
    private NioEventLoopGroupFactory(int nthreads) {
        this.nthreads = nthreads;
    }
    
    @Override
    public NioEventLoopGroup get() {
        return new NioEventLoopGroup(nthreads);
    }

    @Override
    public NioEventLoopGroup get(ThreadFactory value) {
        return new NioEventLoopGroup(nthreads, value);
    }
}
