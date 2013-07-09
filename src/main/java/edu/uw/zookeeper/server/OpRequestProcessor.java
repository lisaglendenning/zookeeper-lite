package edu.uw.zookeeper.server;

import java.util.Set;


import com.google.common.base.Predicate;

import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processor;

public class OpRequestProcessor implements
        Processor<Records.Request, Records.Response> {

    public static class SetFilter implements Predicate<Operation.Coded> {
        public static SetFilter newInstance(Set<OpCode> opcodes) {
            return new SetFilter(opcodes);
        }

        protected final Set<OpCode> opcodes;

        public SetFilter(Set<OpCode> opcodes) {
            this.opcodes = opcodes;
        }

        @Override
        public boolean apply(Operation.Coded input) {
            return (opcodes().contains(input.opcode()));
        }

        public Set<OpCode> opcodes() {
            return opcodes;
        }
    }

    public static class NotEqualsFilter implements Predicate<Operation.Coded> {
        public static NotEqualsFilter newInstance(OpCode opcode) {
            return new NotEqualsFilter(opcode);
        }

        protected final OpCode opcode;

        public NotEqualsFilter(OpCode opcode) {
            this.opcode = opcode;
        }

        @Override
        public boolean apply(Operation.Coded input) {
            return (input.opcode() != opcode());
        }

        public OpCode opcode() {
            return opcode;
        }
    }

    public static class EqualsFilter implements Predicate<Operation.Coded> {
        public static EqualsFilter newInstance(OpCode opcode) {
            return new EqualsFilter(opcode);
        }

        protected final OpCode opcode;

        public EqualsFilter(OpCode opcode) {
            this.opcode = opcode;
        }

        @Override
        public boolean apply(Operation.Coded input) {
            return (input.opcode() == opcode());
        }

        public OpCode opcode() {
            return opcode;
        }
    }

    public static OpRequestProcessor newInstance() {
        return new OpRequestProcessor();
    }

    protected OpRequestProcessor() {
    }

    @Override
    public Records.Response apply(Records.Request request) {
        OpCode opcode = request.opcode();
        Records.Response response;
        switch (opcode) {
        case CREATE_SESSION:
            response = ConnectMessage.Response.Invalid.newInstance();
            break;
        case RECONFIG:
            response = Records.Responses.getInstance().get(OpCode.GET_DATA);
            break;
        default:
            response = Records.Responses.getInstance().get(opcode);
            break;
        }
        return response;
    }
}
