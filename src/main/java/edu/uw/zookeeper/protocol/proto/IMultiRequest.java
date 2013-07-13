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
public class IMultiRequest extends ICodedRecord<EmptyRecord> implements Records.Request, Iterable<Records.MultiOpRequest> {

    private final List<Records.MultiOpRequest> requests;
    
    public IMultiRequest() {
        this(ImmutableList.<Records.MultiOpRequest>of());
    }

    public IMultiRequest(Iterable<Records.MultiOpRequest> ops) {
        super(EmptyRecord.getInstance());
        this.requests = Lists.newArrayList(ops);
    }

    @Override
    public Iterator<Records.MultiOpRequest> iterator() {
        return requests.iterator() ;
    }

    public void add(Records.MultiOpRequest op) {
        requests.add(op);
    }

    public int size() {
        return requests.size();
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        for (Records.MultiOpRequest request: this) {
            IMultiHeader.ofRequest(request.getOpcode()).serialize(archive, tag);
            request.serialize(archive, tag);
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
            Record record = Records.Requests.getInstance().get(opcode);
            if (! (record instanceof Records.MultiOpRequest)) {
                throw new IllegalArgumentException(h.toString());
            }
            record.deserialize(archive, tag);
            add((Records.MultiOpRequest)record);
            h.deserialize(archive, tag);
        }
        archive.endRecord(tag);
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(Records.iterableToBeanString(this)).toString();
    }
}