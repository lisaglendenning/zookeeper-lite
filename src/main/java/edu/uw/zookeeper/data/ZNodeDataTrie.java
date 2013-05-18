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

public class ZNodeDataTrie extends ZNodeCacheTrie<ZNodeDataTrie.ZNodeData> {

    public static class ZNodeData extends ZNodeCache<ZNodeData> {

        public static ZNodeData root(ParameterizedFactory<ZNodeLabel.Path, DefaultsFactory<ByteBuf, ?>> decoders) {
            return new ZNodeDataFactory(decoders).get();
        }

        public static ZNodeData childOf(ZNodeLabelTrie.Pointer<ZNodeData> parent,
                ParameterizedFactory<ZNodeLabel.Path, DefaultsFactory<ByteBuf, ?>> decoders) {
            return new ZNodeDataFactory(decoders).get(parent);
        }
        
        public static class ZNodeDataFactory implements DefaultsFactory<ZNodeLabelTrie.Pointer<ZNodeData>, ZNodeData> {

            protected final ParameterizedFactory<ZNodeLabel.Path, DefaultsFactory<ByteBuf, ?>> decoders;
            
            public ZNodeDataFactory(ParameterizedFactory<ZNodeLabel.Path, DefaultsFactory<ByteBuf, ?>> decoders) {
                this.decoders = decoders;
            }
            
            public ParameterizedFactory<ZNodeLabel.Path, DefaultsFactory<ByteBuf, ?>> decoders() {
                return decoders;
            }
            
            @Override
            public ZNodeData get() {
                return new ZNodeData(Optional.<ZNodeLabelTrie.Pointer<ZNodeData>>absent(), this);
            }

            @Override
            public ZNodeData get(ZNodeLabelTrie.Pointer<ZNodeData> value) {
                return new ZNodeData(Optional.of(value), this);
            }
        }
        
        protected final StampedReference.Updater<Object> value;

        protected ZNodeData(
                Optional<Pointer<ZNodeData>> parent,
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
    
    protected ZNodeDataTrie(ClientProtocolConnection client, ZNodeData root) {
        super(client, root);
    }
}
