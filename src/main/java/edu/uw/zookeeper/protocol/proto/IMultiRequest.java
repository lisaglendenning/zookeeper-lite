package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.Record;
import com.google.common.collect.ImmutableList;

@Operational(value=OpCode.MULTI)
public class IMultiRequest extends IMulti<Records.MultiOpRequest> implements Records.Request {

    public IMultiRequest() {
        this(ImmutableList.<Records.MultiOpRequest>of());
    }

    public IMultiRequest(Iterable<Records.MultiOpRequest> ops) {
        super(ops);
    }

    @Override
    protected IMultiHeader headerOf(Records.MultiOpRequest record) {
        return IMultiHeader.ofRequest(record.getOpcode());
    }
    
    @Override
    protected Records.MultiOpRequest recordOf(OpCode opcode) {
        Record record = Records.Requests.getInstance().get(opcode);
        if (! (record instanceof Records.MultiOpRequest)) {
            throw new IllegalArgumentException(opcode.toString());
        }
        return (Records.MultiOpRequest) record; 
    }
}