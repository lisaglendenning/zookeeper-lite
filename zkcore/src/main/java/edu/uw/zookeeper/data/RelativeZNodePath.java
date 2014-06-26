package edu.uw.zookeeper.data;


/**
 * Can be empty.
 */
public final class RelativeZNodePath extends ZNodeLabelVector {
    
    /**
     * @param path to be validated
     * @return path
     */
    public static String validate(String path) {
        return validate(path, 0, path.length());
    }

    public static <T extends CharSequence> T validate(T path, int start, int end) {
        if (start < end) {
            int lastSlash = start-1;
            for (int i=start; i<end; ++i) {
                char c = path.charAt(i);
                if ((c == SLASH) || (i == end - 1)) {
                    if (i > start) {
                        int labelEnd;
                        if (c == SLASH) {
                            if (i == end - 1) {
                                throw new IllegalArgumentException(String.format("Trailing slash at index %d in %s", i, path));
                            }
                            labelEnd = i;
                        } else {
                            labelEnd = i+1;
                        }
                        int labelStart = lastSlash + 1;
                        ZNodeLabel.validate(path, labelStart, labelEnd);
                        for (ZNodeLabel illegal: ILLEGAL_LABELS) {
                            int j;
                            for (j=0; (j<labelEnd-labelStart) && (j<illegal.length()) && (path.charAt(labelStart+j) == illegal.charAt(j)); j++)  {
                            }
                            if (j == labelEnd-labelStart) {
                                throw new IllegalArgumentException(String.format("Illegal label at index %d in %s", labelStart, path));
                            }
                        }
                    }
                    if (c == SLASH) {
                        if (i == start) {
                            throw new IllegalArgumentException(String.format("Leading slash at index %d in %s", i, path));
                        }
                        lastSlash = i;
                    }
                }
            }
            if (lastSlash < start) {
                throw new IllegalArgumentException(String.format("No slash in index range [%d,%d) for %s", start, end, path));
            }
        } else {
            throw new IllegalArgumentException(String.format("Empty index range [%d,%d) for %s", start, end, path));
        }
        return path;
    }

    @Serializes(from=String.class, to=RelativeZNodePath.class)
    public static RelativeZNodePath validated(String path) {
        return fromString(validate(path));
    }
    
    @Serializes(from=String.class, to=RelativeZNodePath.class)
    public static RelativeZNodePath fromString(String path) {
        return new RelativeZNodePath(path);
    }
    
    public static RelativeZNodePath empty() {
        return new RelativeZNodePath("");
    }
    
    private static final ZNodeLabel[] ILLEGAL_LABELS = { ZNodeLabel.self(), ZNodeLabel.parent() };
    
    private RelativeZNodePath(String label) {
        super(label);
    }

    @Override
    public boolean isAbsolute() {
        return false;
    }
    
    @Override
    public RelativeZNodePath join(ZNodeName other) {
        String suffix = other.toString();
        if (suffix.isEmpty()) {
            return this;
        } else if (suffix.charAt(0) == SLASH) {
            throw new IllegalArgumentException(suffix);
        }
        return RelativeZNodePath.fromString(new StringBuilder(length() + suffix.length() + 1).append(toString()).append(SLASH).append(suffix).toString());
    }
}
