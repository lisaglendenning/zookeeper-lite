package edu.uw.zookeeper.net;

public class StringCodec extends AbstractCodec<String,String,String,String> {

    public static StringCodec defaults() {
        return new StringCodec(new StringEncoder(), new StringDecoder());
    }
    
    protected final Encoder<? super String, String> encoder;
    protected final Decoder<? extends String, String> decoder;
    
    public StringCodec(Encoder<? super String, String> encoder, 
            Decoder<? extends String, String> decoder) {
        this.encoder = encoder;
        this.decoder = decoder;
    }
    
    @Override
    protected Encoder<? super String, String> encoder() {
        return encoder;
    }

    @Override
    protected Decoder<? extends String, String> decoder() {
        return decoder;
    }
}
