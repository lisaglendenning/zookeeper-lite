package edu.uw.zookeeper.protocol.proto;

import org.apache.jute.Record;
import com.google.common.collect.ImmutableList;

@Operational(value=OpCode.MULTI)
public class IMultiResponse extends IMulti<Records.MultiOpResponse> implements Records.Response {

    public IMultiResponse() {
        this(ImmutableList.<Records.MultiOpResponse>of());
    }

    public IMultiResponse(Iterable<Records.MultiOpResponse> ops) {
        super(ops);
    }

    @Override
    protected IMultiHeader headerOf(Records.MultiOpResponse record) {
        return (record instanceof IErrorResponse) 
                ? IMultiHeader.ofError(((IErrorResponse) record).getErr())
                : IMultiHeader.ofResponse(record.opcode());
    }

    @Override
    protected Records.MultiOpResponse recordOf(OpCode opcode) {
        Record record = Records.Responses.getInstance().get(opcode);
        if (! (record instanceof Records.MultiOpResponse)) {
            throw new IllegalArgumentException(opcode.toString());
        }
        return (Records.MultiOpResponse) record; 
    }
}
