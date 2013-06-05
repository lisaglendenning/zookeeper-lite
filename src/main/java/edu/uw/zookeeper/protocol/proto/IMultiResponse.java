package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.Records.MultiOpResponse;

public class IMultiResponse implements Records.ResponseRecord, Iterable<MultiOpResponse> {
    public static final OpCode OPCODE = OpCode.MULTI;

    private final List<MultiOpResponse> results;
    
    public IMultiResponse() {
        this(ImmutableList.<MultiOpResponse>of());
    }

    public IMultiResponse(Iterable<MultiOpResponse> ops) {
        this.results = Lists.newArrayList(ops);
    }

    @Override
    public OpCode opcode() {
        return OPCODE;
    }

    @Override
    public Iterator<MultiOpResponse> iterator() {
        return results.iterator() ;
    }

    public void add(MultiOpResponse result) {
        results.add(result);
    }

    public int size() {
        return results.size();
    }

    @Override
    public void serialize(OutputArchive archive) throws IOException {
        serialize(archive, Records.Responses.TAG);
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        for (MultiOpResponse result : this) {
            IMultiHeader header = (result instanceof IErrorResponse) 
                    ? IMultiHeader.ofError(((IErrorResponse)result).getErr())
                    : IMultiHeader.ofResponse(result.opcode());
            header.serialize(archive, tag);
            result.serialize(archive, tag);
        }
        IMultiHeader.ofEnd().serialize(archive, tag);
        archive.endRecord(this, tag);
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Records.Responses.TAG);
    }

    @Override
    public void deserialize(InputArchive archive, String tag)
            throws IOException {
        archive.startRecord(tag);
        IMultiHeader h = new IMultiHeader();
        h.deserialize(archive, tag);
        while (!h.getDone()) {
            OpCode opcode = OpCode.of(h.getType());
            Records.ResponseRecord record = Records.Responses.getInstance().get(opcode);
            if (! (record instanceof MultiOpResponse)) {
                throw new IllegalArgumentException();
            }
            record.deserialize(archive, tag);
            add((MultiOpResponse)record);
            h.deserialize(archive, tag);
        }
        archive.endRecord(tag);
    }
}
