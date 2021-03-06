package edu.uw.zookeeper.protocol.proto;

import java.util.List;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetChildren2Response;

@Operational(value=OpCode.GET_CHILDREN2)
public class IGetChildren2Response extends IOperationalRecord<GetChildren2Response> implements Records.Response, Records.ChildrenGetter, Records.StatGetter {

    public IGetChildren2Response() {
        this(new GetChildren2Response());
    }

    public IGetChildren2Response(List<String> children, Stat stat) {
        this(new GetChildren2Response(children, stat));
    }

    public IGetChildren2Response(GetChildren2Response record) {
        super(record);
    }

    @Override
    public Stat getStat() {
        return record.getStat();
    }

    @Override
    public List<String> getChildren() {
        return record.getChildren();
    }
}