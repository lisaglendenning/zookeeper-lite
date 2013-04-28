package edu.uw.zookeeper.protocol;

import com.google.common.base.Objects;


public class OpAction implements Operation.Action {

    public static class Request extends OpAction implements Operation.Request {

        public static Request create(OpCode opcode) {
            return new Request(opcode);
        }

        public Request(OpCode opcode) {
            super(opcode);
        }
    }

    public static class Response extends OpAction implements Operation.Response {

        public static Response create(OpCode opcode) {
            return new Response(opcode);
        }

        public Response(OpCode opcode) {
            super(opcode);
        }
    }

    private final OpCode opcode;

    protected OpAction(OpCode opcode) {
        this.opcode = opcode;
    }

    @Override
    public OpCode opcode() {
        return opcode;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("opcode", opcode())
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(opcode());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        OpAction other = (OpAction) obj;
        return Objects.equal(opcode(), other.opcode());
    }
}
