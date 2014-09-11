package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.collect.UnmodifiableListIterator;

@Operational(value=OpCode.MULTI)
public abstract class IMulti<T extends Records.Coded> extends IOperationalRecord<EmptyRecord> implements List<T> {

    private final List<T> delegate;

    protected IMulti(Iterable<T> ops) {
        super(EmptyRecord.getInstance());
        this.delegate = Lists.newArrayList(ops);
    }
    
    protected List<T> delegate() {
        return delegate;
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        for (T record: delegate()) {
            headerOf(record).serialize(archive, tag);
            record.serialize(archive, tag);
        }
        IMultiHeader.ofEnd().serialize(archive, tag);
        archive.endRecord(this, tag);
    }
    
    protected abstract IMultiHeader headerOf(T record);

    @Override
    public void deserialize(InputArchive archive, String tag)
            throws IOException {
        delegate().clear();
        archive.startRecord(tag);
        IMultiHeader h = new IMultiHeader();
        h.deserialize(archive, tag);
        while (!h.getDone()) {
            OpCode opcode = OpCode.of(h.getType());
            T record = recordOf(opcode);
            record.deserialize(archive, tag);
            delegate().add(record);
            h.deserialize(archive, tag);
        }
        archive.endRecord(tag);
    }
    
    protected abstract T recordOf(OpCode opcode);

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(Records.iteratorToBeanString(delegate().iterator())).toString();
    }

    @Override
    public UnmodifiableIterator<T> iterator() {
        return ImmutableList.copyOf(delegate()).iterator() ;
    }

    @Override
    public int size() {
        return delegate().size();
    }

    @Override
    public boolean add(T e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(Object o) {
        return delegate().contains(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate().containsAll(c);
    }

    @Override
    public T get(int index) {
        return delegate().get(index);
    }

    @Override
    public int indexOf(Object o) {
        return delegate().indexOf(o);
    }

    @Override
    public boolean isEmpty() {
        return delegate().isEmpty();
    }

    @Override
    public int lastIndexOf(Object o) {
        return delegate().lastIndexOf(o);
    }

    @Override
    public UnmodifiableListIterator<T> listIterator() {
        return ImmutableList.copyOf(delegate()).listIterator();
    }

    @Override
    public UnmodifiableListIterator<T> listIterator(int index) {
        return ImmutableList.copyOf(delegate()).listIterator(index);
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T remove(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public T set(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ImmutableList<T> subList(int fromIndex, int toIndex) {
        return ImmutableList.copyOf(delegate()).subList(fromIndex, toIndex);
    }

    @Override
    public Object[] toArray() {
        return delegate().toArray();
    }

    @Override
    public <U> U[] toArray(U[] a) {
        return delegate().toArray(a);
    }
}