package com.apm.gateway.util;

/**
 * @author Pragalathan M <pragalathanm@gmail.com>
 */
public class PacketUtils {

    /**
     * Converts a hex string (e.g., "78780501...") to a byte array.
     */
    public static byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
}
