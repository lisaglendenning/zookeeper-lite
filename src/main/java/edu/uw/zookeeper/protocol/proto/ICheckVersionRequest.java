package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.CheckVersionRequest;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.Records.MultiOpRequest;
import edu.uw.zookeeper.protocol.Records.PathRecord;
import edu.uw.zookeeper.protocol.Records.RequestRecord;
import edu.uw.zookeeper.protocol.Records.Requests;

public class ICheckVersionRequest extends CheckVersionRequest implements RequestRecord, MultiOpRequest, PathRecord, Records.VersionRecord {
    public static final OpCode OPCODE = OpCode.CHECK;
    
    public ICheckVersionRequest() {
        super();
    }

    public ICheckVersionRequest(String path, int version) {
        super(path, version);
    }

    @Override
    public OpCode opcode() {
        return OPCODE;
    }

    @Override
    public void serialize(OutputArchive archive) throws IOException {
        serialize(archive, Requests.TAG);
    }

    @Override
    public void deserialize(InputArchive archive) throws IOException {
        deserialize(archive, Requests.TAG);
    }
}