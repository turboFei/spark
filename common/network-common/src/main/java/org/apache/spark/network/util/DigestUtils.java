package org.apache.spark.network.util;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.zip.CRC32;

public class DigestUtils {
    private static final int STREAM_BUFFER_LENGTH = 2048;
    private static final HashMap<ALGORITHM, Integer> ALOGRITHMS_LENGTH;

    public enum ALGORITHM {
        CRC32, MD5
    }

    public static int getDigestLength(String algorithm) {
       return ALOGRITHMS_LENGTH.get(getAlgorithm(algorithm));
    }

    static {
        ALOGRITHMS_LENGTH = new HashMap<>(2);
        ALOGRITHMS_LENGTH.put(ALGORITHM.MD5, 16);
        ALOGRITHMS_LENGTH.put(ALGORITHM.CRC32, 8);
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
    public static long updateCRC32(CRC32 crc32, InputStream data) throws IOException {
        byte[] buffer = new byte[STREAM_BUFFER_LENGTH];
        for(int read = data.read(buffer, 0, STREAM_BUFFER_LENGTH); read > -1;
            read = data.read(buffer, 0, STREAM_BUFFER_LENGTH)) {
            crc32.update(buffer, 0, read);
        }
        return crc32.getValue();
    }


    public static byte[] md5(InputStream data) throws IOException {
        return digest(getMd5Digest(), data);
    }

    public static byte[] crc32(InputStream data) throws IOException {
        return LongToBytes(updateCRC32(getCRC32(), data));
    }

    public static ALGORITHM getAlgorithm(String algorithm) {
        if (algorithm.toLowerCase().startsWith("md5")) {
            return ALGORITHM.MD5;
        } else {
            return ALGORITHM.CRC32;
        }
    }
    public static byte[] digestWithAlogrithm(String algorithm, InputStream data) throws  IOException {
        switch (getAlgorithm(algorithm)) {
            case MD5:
                return  md5(data);
            default:
                return crc32(data);
        }
    }

    public static MessageDigest getMd5Digest() {
        return getDigest("MD5");
    }

    public static CRC32 getCRC32() {
        return new CRC32();
    }

    public static MessageDigest getDigest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException var2) {
            throw new IllegalArgumentException(var2);
        }
    }

    public static byte[] LongToBytes(long values) {
        byte[] buffer = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = 64 - (i + 1) * 8;
            buffer[i] = (byte) ((values >> offset) & 0xff);
        }
        return buffer;
    }

    public static boolean digestEqual(byte[] digest1, byte[] digest2) {
        if (digest1.length != digest2.length) {
            return  false;
        }
        for (int i = 0; i < digest1.length; i++) {
            if (digest1[i] != digest2[i]) {
                return false;
            }
        }
        return true;
    }

}
