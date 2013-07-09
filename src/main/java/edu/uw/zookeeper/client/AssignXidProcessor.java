package edu.uw.zookeeper.client;

import com.google.common.base.Function;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionRequestMessage;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Generator;

public class AssignXidProcessor implements
        Generator<Integer>,
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
                xids.setIfGreater(Integer.valueOf(((Operation.RequestId) output).xid() + 1));
            }
        } else if (input instanceof Records.Request) {
            int xid;
            if (input instanceof Operation.RequestId) {
                xid = ((Operation.RequestId) input).xid();
            } else {
                xid = next();
            }
            output = SessionRequestMessage.newInstance(xid, (Records.Request) input); 
        } else {
            throw new IllegalArgumentException(input.toString());
        }
        return output;
    }

    @Override
    public Integer get() {
        return xids.get();
    }

    @Override
    public Integer next() {
        return xids.next();
    }
    
    @Override
    public String toString() {
        return xids.toString();
    }
}
