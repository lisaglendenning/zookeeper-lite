package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.Create2Response;

@Operational(value=OpCode.CREATE2)
public class ICreate2Response extends ICodedRecord<Create2Response> implements Records.Response, Records.MultiOpResponse, Records.PathGetter, Records.StatGetter {

    public ICreate2Response() {
        this(new Create2Response());
    }
    
    public ICreate2Response(String path, Stat stat) {
        this(new Create2Response(path, stat));
    }
        
    public ICreate2Response(Create2Response record) {
        super(record);
    }

    @Override
    public String getPath() {
        return record.getPath();
    }

    @Override
    public Stat getStat() {
        return record.getStat();
    }
}
