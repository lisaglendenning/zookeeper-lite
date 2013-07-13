package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetDataResponse;

@Operational(value=OpCode.SET_DATA)
public class ISetDataResponse extends ICodedRecord<SetDataResponse> implements Records.Response, Records.StatGetter, Records.MultiOpResponse {

    public ISetDataResponse() {
        this(new SetDataResponse());
    }
    
    public ISetDataResponse(Stat stat) {
        this(new SetDataResponse(stat));
    }

    public ISetDataResponse(SetDataResponse record) {
        super(record);
    }

    @Override
    public Stat getStat() {
        return get().getStat();
    }
}