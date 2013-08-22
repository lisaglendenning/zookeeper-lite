package edu.uw.zookeeper.protocol.proto;

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.GetACLResponse;

@Operational(value=OpCode.GET_ACL)
public class IGetACLResponse extends ICodedRecord<GetACLResponse> implements Records.Response, Records.AclGetter, Records.StatGetter {

    public IGetACLResponse() {
        this(new GetACLResponse());
    }

    public IGetACLResponse(List<ACL> acl, Stat stat) {
        this(new GetACLResponse(acl, stat));
    }

    public IGetACLResponse(GetACLResponse record) {
        super(record);
    }

    @Override
    public Stat getStat() {
        return record.getStat();
    }

    @Override
    public List<ACL> getAcl() {
        return record.getAcl();
    }
}