package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.Create2Response;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.CREATE2)
public class ICreate2Response extends IOperationalRecord<Create2Response> implements Operation.Response, Records.MultiOpResponse, Records.PathHolder, Records.StatHolder {

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
        return get().getPath();
    }

    @Override
    public Stat getStat() {
        return get().getStat();
    }
}
