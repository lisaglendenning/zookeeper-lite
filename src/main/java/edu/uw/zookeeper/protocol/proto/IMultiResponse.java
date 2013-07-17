package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.proto.MultiHeader;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

@Operational(value=OpCode.MULTI)
public class IMultiResponse extends ICodedRecord<EmptyRecord> implements Records.Response, Iterable<Records.MultiOpResponse> {

    private final List<Records.MultiOpResponse> results;
    
    public IMultiResponse() {
        this(ImmutableList.<Records.MultiOpResponse>of());
    }

    public IMultiResponse(Iterable<Records.MultiOpResponse> ops) {
        super(EmptyRecord.getInstance());
        this.results = Lists.newArrayList(ops);
    }

    @Override
    public Iterator<Records.MultiOpResponse> iterator() {
        return results.iterator() ;
    }

    public void add(Records.MultiOpResponse result) {
        results.add(result);
    }

    public int size() {
        return results.size();
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        for (Records.MultiOpResponse result : this) {
            IMultiHeader header = (result instanceof IErrorResponse) 
                    ? IMultiHeader.ofError(((IErrorResponse)result).getErr())
                    : IMultiHeader.ofResponse(result.getOpcode());
            header.serialize(archive, tag);
            result.serialize(archive, tag);
        }
        IMultiHeader.ofEnd().serialize(archive, tag);
        archive.endRecord(this, tag);
    }

    @Override
    public void deserialize(InputArchive archive, String tag)
            throws IOException {
        archive.startRecord(tag);
        MultiHeader h = new MultiHeader();
        h.deserialize(archive, tag);
        while (!h.getDone()) {
            OpCode opcode = OpCode.of(h.getType());
            Record record = Records.Responses.getInstance().get(opcode);
            if (! (record instanceof Records.MultiOpResponse)) {
                throw new IllegalArgumentException();
            }
            record.deserialize(archive, tag);
            add((Records.MultiOpResponse)record);
            h.deserialize(archive, tag);
        }
        archive.endRecord(tag);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(Records.iteratorToBeanString(iterator())).toString();
    }
}
