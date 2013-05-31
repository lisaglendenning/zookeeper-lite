package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Automatons;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Stateful;

public class ProtocolCodec<I extends Message, O extends Message> implements Codec<I, Optional<? extends O>>, Stateful<ProtocolState> {

    public static Automaton<ProtocolState, Message> newAutomaton(Publisher publisher) {
        return Automatons.createSynchronizedEventful(publisher, 
                Automatons.createSimple(ProtocolState.ANONYMOUS));
    }
    
    protected final Automaton<ProtocolState, Message> automaton;
    protected final Encoder<? super I> encoder;
    protected final Decoder<Optional<? extends O>> decoder;

    protected ProtocolCodec(
            Automaton<ProtocolState, Message> automaton,
            Encoder<? super I> encoder,
            Decoder<Optional<? extends O>> decoder) {
        this.automaton = automaton;
        this.encoder = encoder;
        this.decoder = decoder;
    }
    
    @Override
    public ProtocolState state() {
        return automaton.state();
    }

    @Override
    public ByteBuf encode(I input, ByteBufAllocator output) throws IOException {
        ByteBuf out = encoder.encode(input, output);
        automaton.apply(input);
        return out;
    }

    @Override
    public Optional<? extends O> decode(ByteBuf input) throws IOException {
        Optional<? extends O> out =  decoder.decode(input);
        if (out.isPresent()) {
            automaton.apply(out.get());
        }
        return out;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("state", state()).toString();
    }
}
