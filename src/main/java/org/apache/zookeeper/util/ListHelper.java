package org.apache.zookeeper.util;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.ForwardingList;
import com.google.common.collect.Lists;

public abstract class ListHelper<T> extends ForwardingList<T> {

    @Override
    public synchronized boolean add(T arg0) {
        add(size(), arg0);
        return true;
    }

    @Override
    public synchronized boolean addAll(Collection<? extends T> arg0) {
        return addAll(size(), arg0);
    }

    @Override
    public synchronized boolean addAll(int arg0, Collection<? extends T> arg1) {
        int index = arg0;
        for (T e : arg1) {
            add(index, e);
            index++;
        }
        return arg1.size() > 0;
    }

    @Override
    public synchronized T set(int arg0, T arg1) {
        T prev = remove(arg0);
        add(arg0, arg1);
        return prev;
    }

    @Override
    public synchronized void clear() {
        while (!isEmpty()) {
            remove(size() - 1);
        }
    }

    @Override
    public synchronized boolean remove(Object arg0) {
        int index = indexOf(arg0);
        if (index == -1) {
            return false;
        }
        remove(index);
        return true;
    }

    @Override
    public synchronized boolean removeAll(Collection<?> arg0) {
        boolean changed = false;
        for (Object item : arg0) {
            if (remove(item)) {
                changed = true;
            }
        }
        return changed;
    }

    @Override
    public synchronized boolean retainAll(Collection<?> arg0) {
        List<Object> toRemove = Lists.newArrayList();
        for (Object item : this) {
            if (!(arg0.contains(item))) {
                toRemove.add(item);
            }
        }
        return removeAll(toRemove);
    }

}
