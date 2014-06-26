package edu.uw.zookeeper.protocol;

import io.netty.buffer.ByteBuf;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Pair;

public class FourLetterRequest extends Pair<FourLetterWord, byte[]> implements Message.ClientAnonymous {

    public static FourLetterRequest forWord(FourLetterWord word) {
        return forArgs(word, new byte[0]);
    }
    
    public static FourLetterRequest forArgs(FourLetterWord word, byte[] args) {
        return new FourLetterRequest(word, args);
    }
    
    public static Optional<FourLetterRequest> decode(ByteBuf input) {
        input.markReaderIndex();
        Optional<FourLetterWord> wordOutput = FourLetterWord.decode(input);
        if (wordOutput.isPresent()) {
            FourLetterWord word = wordOutput.get();
            byte[] args;
            switch (word) {
            case STMK:
                if (input.readableBytes() >= 4) {
                    args = new byte[4];
                    input.readBytes(args);
                    break;
                } else {
                    input.resetReaderIndex();
                    return Optional.absent();
                }
            default:
                args = new byte[0];
                break;
            }
            return Optional.of(FourLetterRequest.forArgs(word, args));
        }
        return Optional.absent();
    }

    public FourLetterRequest(FourLetterWord word, byte[] args) {
        super(word, args);
    }

    @Override
    public void encode(ByteBuf output) {
        first.encode(output);
        if (second.length > 0) {
            output.writeBytes(second);
        }
    }
}
