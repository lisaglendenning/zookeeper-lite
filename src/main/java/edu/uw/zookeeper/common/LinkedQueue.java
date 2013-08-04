package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.*;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Iterators;

public class LinkedQueue<E> extends AbstractQueue<E> implements Queue<E> {

    public static <E> LinkedQueue<E> create() {
        return new LinkedQueue<E>();
    }
    
    public static interface LinkedIterator<E> extends Iterator<E> {
        E peekNext();

        E peekPrevious();

        boolean hasPrevious();

        E previous();
        
        void add(E value);
    }
    
    protected class Itr implements LinkedIterator<E> {
    
        protected Entry<E> previous;
        protected Entry<E> next;
        protected Entry<E> last;
        
        Itr(Entry<E> next) {
            this.next = next;
            this.previous = null;
            this.last = null;
        }
        
        @Override
        public synchronized E peekNext() {
            return (next == null) ? null : next.value;
        }
    
        @Override
        public synchronized E peekPrevious() {
            if ((previous == null) && (next != null)) {
                next.readLock.lock();
                try {
                    previous = next.previous;
                } finally {
                    next.readLock.unlock();
                }
            }
            return (previous == null) ? null : previous.value;
        }
    
        @Override
        public boolean hasNext() {
            return (peekNext() != null);
        }
    
        @Override
        public boolean hasPrevious() {
            return (peekPrevious() != null);
        }
    
        @Override
        public synchronized E next() {
            E value = peekNext();
            if (value == null) {
                throw new NoSuchElementException();
            }
            Entry<E> newNext;
            next.readLock.lock();
            try {
                newNext = next.next;
            } finally {
                next.readLock.unlock();
            }
            last = next;
            next = newNext;
            previous = null;
            return value;
        }
    
        @Override
        public synchronized E previous() {
            E value = peekPrevious();
            if (value == null) {
                throw new NoSuchElementException();
            }
            last = next = previous;
            previous = null;
            return value;
        }
    
        @Override
        public synchronized void add(E value) {
            checkNotNull(value);
            checkState(this.next != null);
            for (;;) {
                Entry<E> first = next.previous;
                checkState(first != null);
                first.writeLock.lock();
                try {
                    Entry<E> third = previous.next;
                    if (third == next) {
                        third.writeLock.lock();
                        try {
                            insert(value, first, third);
                            break;
                        } finally {
                            third.writeLock.unlock();
                        }
                    } else {
                        continue;
                    }
                } finally {
                    first.writeLock.unlock();
                }
            }
            previous = null;
            last = null;
        }
    
        @Override
        public synchronized void remove() {
            checkState(last != null);
            for (;;) {
                Entry<E> first = last.previous;
                if (first == null) {
                    // already deleted
                    break;
                }
                first.writeLock.lock();
                try {
                    Entry<E> second = first.next;
                    if (second == last) {
                        second.writeLock.lock();
                        try {
                            Entry<E> third = second.next;
                            third.writeLock.lock();
                            try {
                                unlink(first, second, third);
                            } finally {
                                third.writeLock.unlock();
                            }
                        } finally {
                            second.writeLock.unlock();
                        }
                    } else {
                        continue;
                    }
                } finally {
                    first.writeLock.unlock();
                }
            }
            last = null;
            previous = null;
        }
    }

    protected static class Entry<E> {
        
        public static <E> Entry<E> sentinel() {
            return new Entry<E>();
        }

        protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
        protected final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        protected final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

        protected final E value;
        protected volatile Entry<E> next;
        protected volatile Entry<E> previous;

        protected Entry() {
            this.value = null;
            this.next = null;
            this.previous = null;
        }

        protected Entry(E value, Entry<E> previous, Entry<E> next) {
            assert (value != null);
            assert (previous != null);
            assert (next != null);
            this.value = value;
            this.next = next;
            this.previous = previous;
        }
    }
    
    protected final Entry<E> head;
    protected final Entry<E> tail;
    
    public LinkedQueue() {
        this.head = Entry.sentinel();
        this.tail = Entry.sentinel();
        head.next = tail;
        tail.previous = head;
    }

    @Override
    public boolean isEmpty() {
        head.readLock.lock();
        try {
            return (head.next == tail);
        } finally {
            head.readLock.unlock();
        }
    }

    @Override
    public E peek() {
        head.readLock.lock();
        try {
            return head.next.value;
        } finally {
            head.readLock.unlock();
        }
    }

    @Override
    public E poll() {
        head.writeLock.lock();
        try {
            Entry<E> second = head.next;
            if (second != tail) {
                second.writeLock.lock();
                try {
                    Entry<E> third = second.next;
                    third.writeLock.lock();
                    try {
                        unlink(head, second, third);
                    } finally {
                        third.writeLock.unlock();
                    }
                } finally {
                    second.writeLock.unlock();
                }
            }
            return second.value;
        } finally {
            head.writeLock.unlock();
        }
    }

    @Override
    public boolean offer(E value) {
        checkNotNull(value);
        for (;;) {
            Entry<E> first = tail.previous;
            first.writeLock.lock();
            try {
                Entry<E> third = first.next;
                if (third == tail) {
                    third.writeLock.lock();
                    try {
                        insert(value, first, third);
                        break;
                    } finally {
                        third.writeLock.unlock();
                    }
                } else {
                    continue;
                }
            } finally {
                first.writeLock.unlock();
            }
        }
        return true;
    }

    @Override
    public LinkedIterator<E> iterator() {
        head.readLock.lock();
        try {
            return new Itr(head.next);
        } finally {
            head.readLock.unlock();
        }
    }

    @Override
    public int size() {
        return Iterators.size(iterator());
    }

    protected Entry<E> insert(E value, Entry<E> previous, Entry<E> next) {
        assert (previous.lock.isWriteLockedByCurrentThread());
        assert (next.lock.isWriteLockedByCurrentThread());
        assert (previous.next == next);
        assert (next.previous == previous);
        Entry<E> e = new Entry<E>(value, previous, next);
        previous.next = e;
        next.previous = e;
        return e;
    }
    
    protected void unlink(Entry<E> previous, Entry<E> entry, Entry<E> next) {
        assert (entry.lock.isWriteLockedByCurrentThread());
        assert (previous.lock.isWriteLockedByCurrentThread());
        assert (next.lock.isWriteLockedByCurrentThread());
        assert (entry.next == next);
        assert (next.previous == entry);
        assert (entry.previous == previous);
        assert (previous.next == entry);
        previous.next = next;
        next.previous = previous;
        entry.next = null;
        entry.previous = null;
    }
}
