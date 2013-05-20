package edu.uw.zookeeper.data;

import java.util.Arrays;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import edu.uw.zookeeper.data.ZNodeResponseCacheTrie.ZNodeCache;
import edu.uw.zookeeper.data.ZNodeLabelTrie.Pointer;
import edu.uw.zookeeper.data.ZNodeLabelTrie.SimplePointer;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.protocol.proto.Records;

public class ZNodeDataCacheTrie<T> extends ZNodeResponseCacheTrie<ZNodeDataCacheTrie.ZNodeDataCache<T>> {

    public static interface Deserializer<T> {
        T deserialize(ZNodeLabel.Path path, byte[] bytes);
    }
    
    public static class AnnotationDeserializer<T> implements Deserializer<T> {
        protected final Function<ZNodeLabel.Path, Class<? extends T>> mapper;
        
        protected AnnotationDeserializer(Function<ZNodeLabel.Path, Class<? extends T>> mapper) {
            this.mapper = mapper;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(ZNodeLabel.Path path, byte[] bytes) {
            Class<?> inputType = byte[].class;
            Class<? extends T> outputType = mapper.apply(path);
            Serializers.Serializer serializer = Serializers.getInstance().find(outputType, inputType, outputType);
            try {
                return (T) serializer.method().invoke(null, bytes);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
    
    public static class ZNodeDataCache<T> extends ZNodeCache<ZNodeDataCache<T>> {

        public static <T> ZNodeDataCache<T> root(Deserializer<T> deserializer) {
            return new ZNodeDataCache<T>(Optional.<Pointer<ZNodeDataCache<T>>>absent(), deserializer);
        }
        
        protected final Deserializer<T> deserializer;
        protected final StampedReference.Updater<T> value;

        protected ZNodeDataCache(
                Optional<Pointer<ZNodeDataCache<T>>> parent,
                Deserializer<T> deserializer) {
            super(parent);
            T initialValue = null;
            this.deserializer = deserializer;
            this.value = StampedReference.Updater.newInstance(StampedReference.of(initialValue));
        }
        
        public Deserializer<T> deserializer() {
            return deserializer;
        }
        
        @Override
        public <U extends Records.View> StampedReference<? extends U> update(View view, StampedReference<U> value) {
            StampedReference<? extends U> prev = super.update(view, value);
            if (view == View.DATA && prev.stamp().compareTo(value.stamp()) < 0) {
                byte[] prevData = ((Records.DataHolder) prev.get()).getData();
                byte[] updatedData = ((Records.DataHolder) value.get()).getData();
                if (! Arrays.equals(prevData, updatedData)) {
                    T updatedObj = deserializer().deserialize(path(), updatedData);
                    StampedReference<T> updatedValue = StampedReference.of(value.stamp(), updatedObj);
                    this.value.setIfGreater(updatedValue);
                }
            }
            return prev;
        }
        
        @SuppressWarnings("unchecked")
        public <U extends T> U get() {
            return (U) value.get();
        }

        @Override
        protected ZNodeDataCache<T> newChild(ZNodeLabel.Component label) {
            Pointer<ZNodeDataCache<T>> childPointer = SimplePointer.of(label, this);
            return new ZNodeDataCache<T>(Optional.of(childPointer), deserializer());
        }
    }
    
    protected ZNodeDataCacheTrie(ClientProtocolConnection client, ZNodeDataCache<T> root) {
        super(client, root);
    }
}
