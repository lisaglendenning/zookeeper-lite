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
import edu.uw.zookeeper.protocol.Records.MultiOpRequest;

public class IMultiRequest implements Records.RequestRecord, Iterable<MultiOpRequest> {
    public static final OpCode OPCODE = OpCode.MULTI;

    private final List<MultiOpRequest> requests;
    
    public IMultiRequest() {
        this(ImmutableList.<MultiOpRequest>of());
    }

    public IMultiRequest(Iterable<MultiOpRequest> ops) {
        this.requests = Lists.newArrayList(ops);
    }

    @Override
    public OpCode opcode() {
        return OPCODE;
    }

    @Override
    public Iterator<MultiOpRequest> iterator() {
        return requests.iterator() ;
    }

    public void add(MultiOpRequest op) {
        requests.add(op);
    }

    public int size() {
        return requests.size();
    }

    @Override
    public void serialize(OutputArchive archive) throws IOException {
        serialize(archive, Records.Requests.TAG);
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Records.Requests.TAG);
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        for (MultiOpRequest request: this) {
            IMultiHeader.ofRequest(request.opcode()).serialize(archive, tag);
            request.serialize(archive, tag);
        }
        IMultiHeader.ofEnd().serialize(archive, tag);
        archive.endRecord(this, tag);
    }

    @Override
    public void deserialize(InputArchive archive, String tag)
            throws IOException {
        archive.startRecord(tag);
        IMultiHeader h = new IMultiHeader();
        h.deserialize(archive, tag);
        while (!h.getDone()) {
            OpCode opcode = OpCode.of(h.getType());
            Records.RequestRecord record = Records.Requests.getInstance().get(opcode);
            if (! (record instanceof MultiOpRequest)) {
                throw new IllegalArgumentException();
            }
            record.deserialize(archive, tag);
            add((MultiOpRequest)record);
            h.deserialize(archive, tag);
        }
        archive.endRecord(tag);
    }
}