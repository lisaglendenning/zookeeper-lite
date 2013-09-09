package edu.uw.zookeeper.client.console;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class LineParser implements Function<String, List<String>> {

    public static LineParser create() {
        return new LineParser();
    }
    
    protected static char QUOTE_CHAR = '"';

    protected static char ESCAPE_CHAR = '\\';

    protected static CharMatcher WHITESPACE = CharMatcher.anyOf(" \t");

    protected static enum State {
        START,
        TOKEN,
        QUOTED,
        ESCAPED;
    }

    protected final Logger logger = LogManager.getLogger(getClass());
    
    public LineParser() {}
    
    @Override
    public ImmutableList<String> apply(String input) {
        logger.entry(input);
        ImmutableList.Builder<String> tokens = ImmutableList.builder();
        LinkedList<State> states = Lists.newLinkedList();
        states.push(State.START);
        StringBuilder tokenBuilder = new StringBuilder();
        for (int i=0; i<=input.length(); ++i) {
            if (i < input.length()) {
                char c = input.charAt(i);
                if (WHITESPACE.matches(c)) {
                    switch (states.peek()) {
                    case START:
                        if (tokenBuilder.length() > 0) {
                            tokens.add(tokenBuilder.toString());
                            tokenBuilder = tokenBuilder.delete(0, tokenBuilder.length());
                        }
                        break;
                    case TOKEN:
                        tokens.add(tokenBuilder.toString());
                        tokenBuilder = tokenBuilder.delete(0, tokenBuilder.length());
                        states.pop();
                        break;
                    case QUOTED:
                        tokenBuilder.append(c);
                        break;
                    case ESCAPED:
                        tokenBuilder.append(c);
                        states.pop(); 
                        break;
                    }
                } else if (QUOTE_CHAR == c) {
                    switch (states.peek()) {
                    case START:
                        states.push(State.QUOTED);
                        break;
                    case TOKEN:
                        throw new IllegalArgumentException("Embedded quote must be escaped");
                    case QUOTED:
                        states.pop(); 
                        break;
                    case ESCAPED:
                        tokenBuilder.append(c);
                        states.pop(); 
                        break;
                    }
                } else if (ESCAPE_CHAR == c) {
                    switch (states.peek()) {
                    case START:
                    case TOKEN:
                    case QUOTED:
                        states.push(State.ESCAPED);
                        break;
                    case ESCAPED:
                        tokenBuilder.append(c);
                        states.pop(); 
                        break;
                    }
                } else {
                    switch (states.peek()) {
                    case START:
                        tokenBuilder.append(c);
                        states.push(State.TOKEN);
                        break;
                    case TOKEN:
                    case QUOTED:
                        tokenBuilder.append(c);
                        break;
                    case ESCAPED:
                        tokenBuilder.append(c);
                        states.pop(); 
                        break;
                    }
                }
            } else {
                switch (states.peek()) {
                case QUOTED:
                    throw new IllegalArgumentException("Incomplete quote");
                case ESCAPED:
                    throw new IllegalArgumentException("Incomplete escape");
                default:
                    // complete the last token
                    if (tokenBuilder.length() > 0) {
                        tokens.add(tokenBuilder.toString());
                        tokenBuilder = tokenBuilder.delete(0, tokenBuilder.length());
                    }
                    break;
                }
            }
        }
        return logger.exit(tokens.build());
    }
}
