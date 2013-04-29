package edu.uw.zookeeper.server;

import java.util.Set;


import com.google.common.base.Predicate;

import edu.uw.zookeeper.protocol.OpAction;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.OpCodeRecord;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.OpPing;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Processor;

public class OpRequestProcessor implements
        Processor<Operation.Request, Operation.Reply> {

    public static class SetFilter implements Predicate<Operation.Action> {
        public static SetFilter create(Set<OpCode> opcodes) {
            return new SetFilter(opcodes);
        }

        protected final Set<OpCode> opcodes;

        public SetFilter(Set<OpCode> opcodes) {
            this.opcodes = opcodes;
        }

        @Override
        public boolean apply(Operation.Action input) {
            return (opcodes().contains(input.opcode()));
        }

        public Set<OpCode> opcodes() {
            return opcodes;
        }
    }

    public static class NotEqualsFilter implements Predicate<Operation.Action> {
        public static NotEqualsFilter create(OpCode opcode) {
            return new NotEqualsFilter(opcode);
        }

        protected final OpCode opcode;

        public NotEqualsFilter(OpCode opcode) {
            this.opcode = opcode;
        }

        @Override
        public boolean apply(Operation.Action input) {
            return (input.opcode() != opcode());
        }

        public OpCode opcode() {
            return opcode;
        }
    }

    public static class EqualsFilter implements Predicate<Operation.Action> {
        public static EqualsFilter create(OpCode opcode) {
            return new EqualsFilter(opcode);
        }

        protected final OpCode opcode;

        public EqualsFilter(OpCode opcode) {
            this.opcode = opcode;
        }

        @Override
        public boolean apply(Operation.Action input) {
            return (input.opcode() == opcode());
        }

        public OpCode opcode() {
            return opcode;
        }
    }

    public static OpRequestProcessor create() {
        return new OpRequestProcessor();
    }

    protected OpRequestProcessor() {
    }

    @Override
    public Operation.Reply apply(Operation.Request request) throws Exception {
        OpCode opcode = request.opcode();
        Operation.Reply reply;
        switch (opcode) {
        case CREATE_SESSION:
            reply = OpCreateSession.Response.Invalid.create();
            break;
        case PING:
            reply = OpPing.Response.create();
            break;
        case AUTH:
        case SET_WATCHES:
        case DELETE:
        case CLOSE_SESSION:
            reply = OpAction.Response.create(opcode);
            break;
        default:
            reply = OpCodeRecord.Response.create(opcode);
            break;
        }
        return reply;
    }
}
