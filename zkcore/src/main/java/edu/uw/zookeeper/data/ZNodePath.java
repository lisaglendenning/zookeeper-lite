package edu.uw.zookeeper.data;

public abstract class ZNodePath extends ZNodeLabelVector {

    public static RootZNodePath root() {
        return RootZNodePath.getInstance();
    }

    /**
     * @param path to be validated
     * @return path
     * @throws IllegalArgumentException
     */
    public static String validate(String path) {
        return validate(path, 0, path.length());
    }

    @SuppressWarnings("unchecked")
    public static <T extends CharSequence> T validate(T path, int start, int end) {
        if (start < end) {
            if (path.charAt(start) != SLASH) {
                throw new IllegalArgumentException(String.format("Missing slash at index %d for %s", start, path));
            }
            if ((start + 1) < end) {
                int j = start+1;
                while (j<end && (path.charAt(j) != SLASH)) {
                    j++;
                }
                if (j < end) {
                    RelativeZNodePath.validate(path, start+1, end);
                } else {
                    ZNodeLabel.validate(path, start+1, end);
                }
            }
            return ((start > 0) || (end < path.length() - 1)) ? (T) path.subSequence(start, end) : path;
        } else {
            throw new IllegalArgumentException(String.format("Empty index range [%d,%d) for %s", start, end, path));
        }
    }

    public static ZNodePath validated(String path) {
        return fromString(validate(path));
    }

    public static ZNodePath validated(String path, int start, int end) {
        return fromString(validate(path));
    }
    
    /**
     * Validates, replaces self and parent labels, and trims slashes.
     * 
     * @param path to be canonicalized
     * @return canonicalized path
     */
    public static String canonicalize(final String path) {
        final int length = path.length();
        int lastSlash = -1;
        StringBuilder builder = null;
        for (int i=0; i<length; ++i) {
            char c = path.charAt(i);
            if ((c == SLASH) || (i == length - 1)) {
                if (i > 0) {
                    // neighboring slash?
                    if ((c == SLASH) && (lastSlash == i-1)) {
                        if (builder == null) {
                            builder = new StringBuilder(length)
                                .append(path, 0, lastSlash+1);
                        }
                    } else {
                        ZNodeLabel label = ZNodeLabel.validated(path, lastSlash+1, (c == SLASH) ? i : i+1);
                        if (ZNodeLabel.self().equals(label)) {
                            if (builder == null) {
                                builder = new StringBuilder(length)
                                    .append(path, 0, lastSlash+1);
                            }
                        } else if (ZNodeLabel.parent().equals(label)) {
                            if (builder == null) {
                                builder = new StringBuilder(length)
                                    .append(path, 0, lastSlash+1);
                            }
                            int buildLength = builder.length();
                            assert (builder.charAt(buildLength -1) == SLASH);
                            int parentSlash = -1;
                            for (int j = buildLength-2; j >= 0; j--) {
                                if (builder.charAt(j) == SLASH) {
                                    parentSlash = j;
                                    break;
                                }
                            }
                            if (parentSlash < 0) {
                                throw new IllegalArgumentException(String.format("missing parent at index %d of %s", i, path));
                            }
                            builder.delete(parentSlash + 1, buildLength);
                        } else {
                            if (builder != null) {
                                builder.append(path, lastSlash+1, i+1);
                            }
                        }
                    }
                }
                if (c == SLASH) {
                    lastSlash = i;
                }
            }
        }
        String canonicalized;
        // trailing slash?
        if (builder == null) {
            canonicalized = ((length > 1) && (path.charAt(length - 1) == SLASH)) ? path.substring(0, length - 1) : path;
        } else {
            int buildLength = builder.length();
            if ((buildLength > 1) && (builder.charAt(buildLength - 1) == SLASH)) {
                builder.deleteCharAt(buildLength - 1);
            }
            canonicalized = builder.toString();
        }
        if (canonicalized.isEmpty()) {
            throw new IllegalArgumentException(String.format("empty canonicalized path for %s", path));
        }
        if (canonicalized.charAt(0) != SLASH) {
            throw new IllegalArgumentException(String.format("not absolute path for %s", path));
        }
        return canonicalized;
    }

    public static ZNodePath canonicalized(String path) {
        return fromString(canonicalize(path));
    }

    @Serializes(from=String.class, to=ZNodePath.class)
    public static ZNodePath fromString(String path) {
        assert (path.charAt(0) == SLASH);
        if (path.length() == 1) {
            return RootZNodePath.getInstance();
        } else {
            return AbsoluteZNodePath.fromString(path);
        }
    }
    
    protected ZNodePath(String label) {
        super(label);
    }

    @Override
    public final boolean isAbsolute() {
        return true;
    }

    @Override
    public abstract ZNodePath join(ZNodeName other);

    public abstract boolean isRoot();

    public abstract AbstractZNodeLabel label();
}
