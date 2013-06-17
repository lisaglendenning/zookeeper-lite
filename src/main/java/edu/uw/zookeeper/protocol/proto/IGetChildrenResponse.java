package edu.uw.zookeeper.protocol.proto;

import java.util.List;

import org.apache.zookeeper.proto.GetChildrenResponse;

import edu.uw.zookeeper.protocol.Operation;
@Operational(opcode=OpCode.GET_CHILDREN)
public class IGetChildrenResponse extends IOperationalRecord<GetChildrenResponse> implements Operation.Response, Records.ChildrenHolder {

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
        return get().getChildren();
    }
}