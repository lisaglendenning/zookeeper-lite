package edu.uw.zookeeper.client.random;

import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.data.NodeWatchEvent;
import edu.uw.zookeeper.data.ZNodeCache;
import edu.uw.zookeeper.data.LockableZNodeCache;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.data.ZNodePath;

public class CachedPaths implements Generator<ZNodePath>, ZNodeCache.CacheListener {

    public static CachedPaths fromCache(LockableZNodeCache<?,?,?> cache, Random random) {
        CachedPaths instance = new CachedPaths(random, ImmutableList.<ZNodePath>of());
        cache.events().subscribe(instance);
        cache.lock().readLock().lock();
        try {
            for (ZNodeCache.CacheNode<?,?> e: cache.cache()) {
                instance.add(e.path());
            }
        } finally {
            cache.lock().readLock().unlock();
        }
        return instance;
    }

    protected final Random random;
    protected final List<ZNodePath> elements;
    
    protected CachedPaths(Random random, Iterable<ZNodePath> elements) {
        this.random = random;
        this.elements = Lists.newArrayList(elements);
    }

    public synchronized boolean add(ZNodePath element) {
        if (! elements.contains(element)) {
            return elements.add(element);
        } else {
            return false;
        }
    }

    public synchronized boolean remove(ZNodePath element) {
        return elements.remove(element);
    }

    @Override
    public synchronized ZNodePath next() {
        int size = elements.size();
        if (size == 0) {
            return null;
        } else {
            int index = random.nextInt(size);
            return elements.get(index);
        }
    }

    @Override
    public void handleCacheEvent(Set<NodeWatchEvent> events) {
        for (NodeWatchEvent event: events) {
            switch (event.getEventType()) {
            case NodeCreated:
                add(event.getPath());
                break;
            case NodeDeleted:
                remove(event.getPath());
                break;
            default:
                break;
            }
        }
    }
}
