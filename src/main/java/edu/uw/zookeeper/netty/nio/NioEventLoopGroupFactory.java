package edu.uw.zookeeper.netty.nio;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import edu.uw.zookeeper.util.DefaultsFactory;

public enum NioEventLoopGroupFactory implements DefaultsFactory<ThreadFactory, EventLoopGroup> {

    DEFAULT(0), AVAILABLE_PROCESSORS(Math.max(1,
                Runtime.getRuntime().availableProcessors()));
    
    private final int nthreads;
    private final int ioRatio = 75;
    
    private NioEventLoopGroupFactory(int nthreads) {
        this.nthreads = nthreads;
    }
    
    @Override
    public NioEventLoopGroup get() {
        NioEventLoopGroup instance = new NioEventLoopGroup(nthreads);
        instance.setIoRatio(ioRatio);
        return instance;
    }

    @Override
    public NioEventLoopGroup get(ThreadFactory value) {
        NioEventLoopGroup instance =  new NioEventLoopGroup(nthreads, value);
        instance.setIoRatio(ioRatio);
        return instance;
    }
}
