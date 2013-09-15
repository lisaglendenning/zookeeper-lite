package edu.uw.zookeeper.client.cli;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.LinkedList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.CharMatcher;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import edu.uw.zookeeper.common.Pair;

public class TokenParser implements Function<CharSequence, Iterator<Pair<Integer, String>>> {

    public static TokenParser defaults() {
        return new TokenParser(SPACE);
    }
    
    protected static char QUOTE_CHAR = '"';

    protected static char ESCAPE_CHAR = '\\';

    protected static Character SPACE = Character.valueOf(' ');

    protected final Logger logger = LogManager.getLogger(getClass());
    protected final Pair<Character, CharMatcher> delimiter;
    
    public TokenParser(Character delimit) {
        this.delimiter = Pair.create(delimit, CharMatcher.is(delimit));
    }
    
    public Pair<Character, CharMatcher> delimiter() {
        return delimiter;
    }
    
    @Override
    public TokenIterator apply(CharSequence input) {
        return new TokenIterator(input);
    }
    
    protected static enum State {
        START,
        TOKEN,
        QUOTED,
        ESCAPED;
    }

    public class TokenIterator extends AbstractIterator<Pair<Integer, String>> {
        
        protected final CharSequence input;
        protected int index;
        
        public TokenIterator(CharSequence input) {
            this.input = checkNotNull(input);
            this.index = 0;
        }
        
        public CharSequence getInput() {
            return input;
        }
        
        public int getIndex() {
            return index;
        }

        @Override
        protected Pair<Integer, String> computeNext() {
            logger.entry(input, index);
            Pair<Integer, String> token = null;
            StringBuilder tokenBuilder = new StringBuilder();
            Integer tokenStart = null;
            LinkedList<State> states = Lists.newLinkedList();
            states.push(State.START);
            while ((token == null) && (index <= input.length())) {
                if (index < input.length()) {
                    char c = input.charAt(index);
                    if (delimiter.second().matches(c)) {
                        switch (states.peek()) {
                        case START:
                            if (tokenBuilder.length() > 0) {
                                token = Pair.create(tokenStart, tokenBuilder.toString());
                            }
                            break;
                        case TOKEN:
                            token = Pair.create(tokenStart, tokenBuilder.toString());
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
                            tokenStart = index;
                            break;
                        case TOKEN:
                            throw new IllegalArgumentException("Embedded quote must be escaped");
                        case QUOTED:
                            token = Pair.create(tokenStart, tokenBuilder.toString());
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
                            tokenStart = index;
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
                            tokenStart = index;
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
                            token = Pair.create(tokenStart, tokenBuilder.toString());
                        }
                        break;
                    }
                }
                
                index++;
            }
            
            if (token == null) {
                return logger.exit(endOfData());
            } else {
                return logger.exit(token);
            }
        }
    }
}
