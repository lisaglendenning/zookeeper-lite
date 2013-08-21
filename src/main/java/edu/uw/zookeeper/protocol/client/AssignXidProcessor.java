package edu.uw.zookeeper.protocol.client;

import com.google.common.base.Function;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.proto.Records;

public class AssignXidProcessor implements
        XidGenerator,
        Function<Operation.Request, Message.ClientSession> {

    public static Factory<AssignXidProcessor> factory() {
        return new Factory<AssignXidProcessor>() {
            @Override
            public AssignXidProcessor get() {
                return AssignXidProcessor.newInstance();
            }
        };
    }
    
    public static AssignXidProcessor newInstance() {
        return newInstance(XidIncrementer.fromZero());
    }

    public static AssignXidProcessor newInstance(XidIncrementer xid) {
        return new AssignXidProcessor(xid);
    }

    private final XidIncrementer xids;

    private AssignXidProcessor(XidIncrementer xids) {
        this.xids = xids;
    }

    @Override
    public Message.ClientSession apply(Operation.Request input) {  
        Message.ClientSession output;
        if (input instanceof Message.ClientSession) {
            output = (Message.ClientSession) input;
            if (output instanceof Operation.RequestId) {
                xids.setIfGreater(((Operation.RequestId) output).getXid() + 1);
            }
        } else if (input instanceof Operation.ProtocolRequest<?>) { 
            Operation.ProtocolRequest<?> request = (Operation.ProtocolRequest<?>) input;
            output = ProtocolRequestMessage.of(request.getXid(), request.getRecord()); 
        } else if (input instanceof Records.Request) {
            int xid;
            if (input instanceof Operation.RequestId) {
                xid = ((Operation.RequestId) input).getXid();
            } else {
                xid = next();
            }
            output = ProtocolRequestMessage.of(xid, (Records.Request) input); 
        } else {
            throw new IllegalArgumentException(input.toString());
        }
        return output;
    }

    @Override
    public int get() {
        return xids.get();
    }

    @Override
    public int next() {
        return xids.next();
    }
    
    @Override
    public String toString() {
        return xids.toString();
    }
}
