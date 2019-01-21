package org.apache.spark.network.util;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DigestUtils {
    private static final int STREAM_BUFFER_LENGTH = 1024;
    private static final char[] DIGITS_LOWER;
    static {
        DIGITS_LOWER = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    }

    public DigestUtils() {
    }

    private static byte[] digest(MessageDigest digest, InputStream data) throws IOException {
        return updateDigest(digest, data).digest();
    }

    public static MessageDigest updateDigest(MessageDigest digest, InputStream data) throws IOException {
        byte[] buffer = new byte[STREAM_BUFFER_LENGTH];

        for(int read = data.read(buffer, 0, STREAM_BUFFER_LENGTH); read > -1;
            read = data.read(buffer, 0, STREAM_BUFFER_LENGTH)) {
            digest.update(buffer, 0, read);
        }
        return digest;
    }

    public static String md5Hex(InputStream data) throws IOException {
        return new String (encodeHex(digest(getMd5Digest(), data), DIGITS_LOWER));
    }

    public static byte[] md5(InputStream data) throws IOException {
        return digest(getMd5Digest(), data);
    }

    public static MessageDigest getMd5Digest() {
        return getDigest("MD5");
    }

    public static MessageDigest getDigest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException var2) {
            throw new IllegalArgumentException(var2);
        }
    }

    protected static char[] encodeHex(byte[] data, char[] toDigits) {
        int l = data.length;
        char[] out = new char[l << 1];
        int i = 0;
        for(int var5 = 0; i < l; ++i) {
            out[var5++] = toDigits[(240 & data[i]) >>> 4];
            out[var5++] = toDigits[15 & data[i]];
        }
        return out;
    }

    public static String encodeHex(byte[] data) {
        return  new String(encodeHex(data, DIGITS_LOWER));
    }
}
