/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.util;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;


public class DigestUtilsSuite {

    @Test
    public void testDigestUtils() {
        DigestUtils.ALGORITHM md5 = DigestUtils.getAlgorithm("md5");
        assertEquals(md5, DigestUtils.ALGORITHM.MD5);

        DigestUtils.ALGORITHM crc32 = DigestUtils.getAlgorithm("crc32");
        assertEquals(crc32, DigestUtils.ALGORITHM.CRC32);

        DigestUtils.ALGORITHM other = DigestUtils.getAlgorithm("other");
        assertEquals(other, DigestUtils.ALGORITHM.CRC32);

        byte[] digest1 = new byte[10];
        byte[] digest2 = new byte[20];
        assertFalse(DigestUtils.digestEqual(digest1, digest2));

        byte[] digest3 = new byte[10];
        for (int i = 0; i < 10; i++) {
            digest1[i] = (byte) i;
            digest3[i] = (byte) i;
        }
        assertTrue(DigestUtils.digestEqual(digest1, digest3));

        String testForDigest = "testForDigestLength";
        String[] codecs = {"crc32", "md5"};
        for (String codec : codecs) {
            try(InputStream in = new ByteArrayInputStream(testForDigest.getBytes())) {
                assertTrue(DigestUtils.digestWithAlogrithm(codec, in).length ==
                        DigestUtils.getDigestLength(codec));
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }

    }

}
