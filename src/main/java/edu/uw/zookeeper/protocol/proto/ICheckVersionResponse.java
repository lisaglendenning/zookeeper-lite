package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetDataResponse;

@Operational(value=OpCode.CHECK)
@Shared
public class ICheckVersionResponse extends ICodedRecord<SetDataResponse> implements Records.Response, Records.StatGetter, Records.MultiOpResponse {

    public ICheckVersionResponse() {
        this(new SetDataResponse());
    }
    
    public ICheckVersionResponse(Stat stat) {
        this(new SetDataResponse(stat));
    }

    public ICheckVersionResponse(SetDataResponse record) {
        super(record);
    }

    @Override
    public Stat getStat() {
        return get().getStat();
    }
}