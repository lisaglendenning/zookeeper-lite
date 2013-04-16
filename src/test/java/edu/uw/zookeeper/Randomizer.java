package edu.uw.zookeeper;

import java.util.Random;

public class Randomizer {
    public Random random = new Random();

    public byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }
}
