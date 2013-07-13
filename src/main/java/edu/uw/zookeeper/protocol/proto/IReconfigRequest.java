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
        return get().getJoiningServers();
    }

    public String getLeavingServers() {
        return get().getLeavingServers();
    }

    public String getNewMembers() {
        return get().getNewMembers();
    }

    public long getCurConfigId() {
        return get().getCurConfigId();
    }
}
