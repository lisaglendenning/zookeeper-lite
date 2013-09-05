package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Pair;

public class FourLetterRequest extends Pair<FourLetterWord, Optional<ByteBuf>> implements Message.ClientAnonymous {

    public static FourLetterRequest of(FourLetterWord word, Optional<ByteBuf> args) {
        return new FourLetterRequest(word, args);
    }
    
    public static Optional<FourLetterRequest> decode(ByteBuf input) {
        if (checkNotNull(input).readableBytes() >= FourLetterWord.LENGTH) {
            byte[] bytes = new byte[FourLetterWord.LENGTH];
            input.getBytes(input.readerIndex(), bytes);
            if (FourLetterWord.has(bytes)) {
                FourLetterWord command = FourLetterWord.of(bytes);
                Optional<ByteBuf> args;
                switch (command) {
                case STMK:
                    if (input.readableBytes() >= 8) {
                        input.skipBytes(bytes.length);
                        input.readBytes(bytes);
                        args = Optional.of(Unpooled.wrappedBuffer(bytes));
                        break;
                    } else {
                        return Optional.absent();
                    }
                default:
                    input.skipBytes(bytes.length);
                    args = Optional.absent();
                    break;
                }
                return Optional.of(FourLetterRequest.of(command, args));
            }
        }
        return Optional.absent();
    }

    public FourLetterRequest(FourLetterWord word, Optional<ByteBuf> args) {
        super(word, args);
    }

    @Override
    public void encode(ByteBuf output) {
        first.encode(output);
        if (second.isPresent()) {
            output.writeBytes(second.get());
        }
    }
}
