package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.ReconfigRequest;

@Operational(value=OpCode.RECONFIG)
public class IReconfigRequest extends ICodedRecord<ReconfigRequest> implements Records.Request {

    public IReconfigRequest() {
        this(new ReconfigRequest());
    }

    public IReconfigRequest(String joiningServers, String leavingServers,
            String newMembers, long curConfigId) {
        this(new ReconfigRequest(joiningServers, leavingServers, newMembers, curConfigId));
    }

    public IReconfigRequest(ReconfigRequest record) {
        super(record);
    }

    public String getJoiningServers() {
        return record.getJoiningServers();
    }

    public String getLeavingServers() {
        return record.getLeavingServers();
    }

    public String getNewMembers() {
        return record.getNewMembers();
    }

    public long getCurConfigId() {
        return record.getCurConfigId();
    }
}
