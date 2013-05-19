package edu.uw.zookeeper.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import com.google.common.base.Optional;
import edu.uw.zookeeper.data.ZNodeCacheTrie.ZNodeCache;
import edu.uw.zookeeper.data.ZNodeLabelTrie.Pointer;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class ZNodeDataCacheTrie extends ZNodeCacheTrie<ZNodeDataCacheTrie.ZNodeDataCache> {

    public static enum Decoder implements DefaultsFactory<ByteBuf, Object> {
        NULL {
            @Override
            public Object get() {
                return null;
            }

            @Override
            public Object get(ByteBuf value) {
                return null;
            }
        },
        BYTEBUF {
            @Override
            public ByteBuf get() {
                return null;
            }

            @Override
            public ByteBuf get(ByteBuf value) {
                return value;
            }
        };
    }
    
    public static class ZNodeDataCache extends ZNodeCache<ZNodeDataCache> {

        public static ZNodeDataCache root(ParameterizedFactory<ZNodeLabel.Path, DefaultsFactory<ByteBuf, ?>> decoders) {
            return new ZNodeDataFactory(decoders).get();
        }

        public static ZNodeDataCache childOf(ZNodeLabelTrie.Pointer<ZNodeDataCache> parent,
                ParameterizedFactory<ZNodeLabel.Path, DefaultsFactory<ByteBuf, ?>> decoders) {
            return new ZNodeDataFactory(decoders).get(parent);
        }
        
        public static class ZNodeDataFactory implements DefaultsFactory<ZNodeLabelTrie.Pointer<ZNodeDataCache>, ZNodeDataCache> {

            protected final ParameterizedFactory<ZNodeLabel.Path, DefaultsFactory<ByteBuf, ?>> decoders;
            
            public ZNodeDataFactory(ParameterizedFactory<ZNodeLabel.Path, DefaultsFactory<ByteBuf, ?>> decoders) {
                this.decoders = decoders;
            }
            
            public ParameterizedFactory<ZNodeLabel.Path, DefaultsFactory<ByteBuf, ?>> decoders() {
                return decoders;
            }
            
            @Override
            public ZNodeDataCache get() {
                return new ZNodeDataCache(Optional.<ZNodeLabelTrie.Pointer<ZNodeDataCache>>absent(), this);
            }

            @Override
            public ZNodeDataCache get(ZNodeLabelTrie.Pointer<ZNodeDataCache> value) {
                return new ZNodeDataCache(Optional.of(value), this);
            }
        }
        
        protected final StampedReference.Updater<Object> value;

        protected ZNodeDataCache(
                Optional<Pointer<ZNodeDataCache>> parent,
                ZNodeDataFactory factory) {
            super(parent, factory);
            Object initialValue = factory.decoders().get(path()).get();
            this.value = StampedReference.Updater.newInstance(StampedReference.of(initialValue));
        }
        
        public <T extends Records.View> StampedReference<? extends T> update(View view, StampedReference<T> value) {
            StampedReference<? extends T> prev = super.update(view, value);
            if (view == View.DATA && prev.stamp().compareTo(value.stamp()) < 0) {
                byte[] prevData = ((Records.DataHolder) prev.get()).getData();
                byte[] updatedData = ((Records.DataHolder) value.get()).getData();
                if (! Arrays.equals(prevData, updatedData)) {
                    ByteBuf buf = Unpooled.wrappedBuffer(updatedData);
                    Object updatedObj = ((ZNodeDataFactory)factory).decoders().get(path()).get(buf);
                    StampedReference<?> updatedValue = StampedReference.of(value.stamp(), updatedObj);
                    this.value.setIfGreater(updatedValue);
                }
            }
            return prev;
        }
        
        @SuppressWarnings("unchecked")
        public <T> T get() {
            return (T) value.get();
        }
    }
    
    protected ZNodeDataCacheTrie(ClientProtocolConnection client, ZNodeDataCache root) {
        super(client, root);
    }
}
