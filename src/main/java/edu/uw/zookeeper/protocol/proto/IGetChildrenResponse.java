package edu.uw.zookeeper.protocol.proto;

import java.util.List;

import org.apache.zookeeper.proto.GetChildrenResponse;

@Operational(value=OpCode.GET_CHILDREN)
public class IGetChildrenResponse extends ICodedRecord<GetChildrenResponse> implements Records.Response, Records.ChildrenGetter {

    public IGetChildrenResponse() {
        this(new GetChildrenResponse());
    }

    public IGetChildrenResponse(List<String> children) {
        this(new GetChildrenResponse(children));
    }

    public IGetChildrenResponse(GetChildrenResponse record) {
        super(record);
    }

    @Override
    public List<String> getChildren() {
        return record.getChildren();
    }
}