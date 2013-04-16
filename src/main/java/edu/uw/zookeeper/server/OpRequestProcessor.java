package edu.uw.zookeeper.server;

import java.util.Set;


import com.google.common.base.Predicate;

import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.util.Processor;

public class OpRequestProcessor implements
        Processor<Operation.Request, Operation.Response> {

    public static class SetFilter implements Predicate<Operation.Action> {
        public static SetFilter create(Set<Operation> operations) {
            return new SetFilter(operations);
        }

        protected final Set<Operation> operations;

        public SetFilter(Set<Operation> operations) {
            this.operations = operations;
        }

        @Override
        public boolean apply(Operation.Action input) {
            return (operations().contains(input.operation()));
        }

        public Set<Operation> operations() {
            return operations;
        }
    }

    public static class NotEqualsFilter implements Predicate<Operation.Action> {
        public static NotEqualsFilter create(Operation operation) {
            return new NotEqualsFilter(operation);
        }

        protected final Operation operation;

        public NotEqualsFilter(Operation operation) {
            this.operation = operation;
        }

        @Override
        public boolean apply(Operation.Action input) {
            return (input.operation() != operation());
        }

        public Operation operation() {
            return operation;
        }
    }

    public static class EqualsFilter implements Predicate<Operation.Action> {
        public static EqualsFilter create(Operation operation) {
            return new EqualsFilter(operation);
        }

        protected final Operation operation;

        public EqualsFilter(Operation operation) {
            this.operation = operation;
        }

        @Override
        public boolean apply(Operation.Action input) {
            return (input.operation() == operation());
        }

        public Operation operation() {
            return operation;
        }
    }

    public static OpRequestProcessor create() {
        return new OpRequestProcessor();
    }

    protected OpRequestProcessor() {
    }

    @Override
    public Operation.Response apply(Operation.Request request) throws Exception {
        return Operations.Responses.create(request.operation());
    }
}
