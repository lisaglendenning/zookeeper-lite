package edu.uw.zookeeper.protocol.proto;

import java.io.IOException;

import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.proto.SetDataRequest;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.Records.MultiOpRequest;
import edu.uw.zookeeper.protocol.Records.RequestRecord;
import edu.uw.zookeeper.protocol.Records.Requests;

public class ISetDataRequest extends SetDataRequest implements RequestRecord, Records.PathRecord, Records.DataRecord, Records.VersionRecord, MultiOpRequest {
    public static final OpCode OPCODE = OpCode.SET_DATA;
    
    public ISetDataRequest() {
        super();
    }

    public ISetDataRequest(String path, byte[] data, int version) {
        super(path, data, version);
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
