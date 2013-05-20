package edu.uw.zookeeper.data;

import java.util.Arrays;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import edu.uw.zookeeper.data.ZNodeCacheTrie.ZNodeCache;
import edu.uw.zookeeper.data.ZNodeLabelTrie.Pointer;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.DefaultsFactory;

public class ZNodeDataCacheTrie<T> extends ZNodeCacheTrie<ZNodeDataCacheTrie.ZNodeDataCache<T>> {

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
            Serializers.SerializerMethod serializer = Serializers.getInstance().find(outputType, inputType, outputType);
            try {
                return (T) serializer.method().invoke(null, bytes);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
    
    public static class ZNodeDataCache<T> extends ZNodeCache<ZNodeDataCache<T>> {

        public static <T> ZNodeDataCache<T> root(Deserializer<T> deserializer) {
            return new ZNodeDataFactory<T>(deserializer).get();
        }

        public static <T> ZNodeDataCache<T> childOf(ZNodeLabelTrie.Pointer<ZNodeDataCache<T>> parent,
                Deserializer<T> deserializer) {
            return new ZNodeDataFactory<T>(deserializer).get(parent);
        }
        
        public static class ZNodeDataFactory<T> implements DefaultsFactory<ZNodeLabelTrie.Pointer<ZNodeDataCache<T>>, ZNodeDataCache<T>> {

            protected final Deserializer<T> deserializer;
            
            public ZNodeDataFactory(Deserializer<T> deserializer) {
                this.deserializer = deserializer;
            }
            
            public Deserializer<T> deserializer() {
                return deserializer;
            }
            
            @Override
            public ZNodeDataCache<T> get() {
                return new ZNodeDataCache<T>(Optional.<ZNodeLabelTrie.Pointer<ZNodeDataCache<T>>>absent(), this);
            }

            @Override
            public ZNodeDataCache<T> get(ZNodeLabelTrie.Pointer<ZNodeDataCache<T>> value) {
                return new ZNodeDataCache<T>(Optional.of(value), this);
            }
        }
        
        protected final StampedReference.Updater<T> value;

        protected ZNodeDataCache(
                Optional<Pointer<ZNodeDataCache<T>>> parent,
                ZNodeDataFactory<T> factory) {
            super(parent, factory);
            T initialValue = null;
            this.value = StampedReference.Updater.newInstance(StampedReference.of(initialValue));
        }
        
        @Override
        public <U extends Records.View> StampedReference<? extends U> update(View view, StampedReference<U> value) {
            StampedReference<? extends U> prev = super.update(view, value);
            if (view == View.DATA && prev.stamp().compareTo(value.stamp()) < 0) {
                byte[] prevData = ((Records.DataHolder) prev.get()).getData();
                byte[] updatedData = ((Records.DataHolder) value.get()).getData();
                if (! Arrays.equals(prevData, updatedData)) {
                    T updatedObj = ((ZNodeDataFactory<T>)factory).deserializer().deserialize(path(), updatedData);
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
    }
    
    protected ZNodeDataCacheTrie(ClientProtocolConnection client, ZNodeDataCache<T> root) {
        super(client, root);
    }
}
