/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc;

import com.facebook.presto.hive.protobuf.CodedInputStream;
import com.facebook.presto.orc.metadata.HiveBloomFilter;
import com.facebook.presto.orc.metadata.RowGroupBloomfilter;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hive.common.util.BloomFilter;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOrcBloomfilters
{
    private static final String TEST_STRING = "ORC_STRING";
    private static final int TEST_INTEGER = 12345;

    @Test
    public void testHiveBloomfilterSerde()
            throws Exception
    {
        BloomFilter bf = new BloomFilter(1_000_000L, 0.05D);

        // String
        bf.addString(TEST_STRING);
        assertTrue(bf.testString(TEST_STRING));
        assertFalse(bf.testString(TEST_STRING + "not"));

        // Integer
        bf.addLong(TEST_INTEGER);
        assertTrue(bf.testLong(TEST_INTEGER));
        assertFalse(bf.testLong(TEST_INTEGER + 1));

        // Extract bitsset
        long[] bs = bf.getBitSet();
        List<Long> bsList = new ArrayList<Long>();
        for (long b : bs) {
            bsList.add(b);
        }
        int numBits = bf.getBitSize();
        int numFuncs = bf.getNumHashFunctions();

        // Re-construct
        HiveBloomFilter hiveBloomFilter = new HiveBloomFilter(bsList, numBits, numFuncs);

        // String
        assertTrue(hiveBloomFilter.testString(TEST_STRING));
        assertFalse(hiveBloomFilter.testString(TEST_STRING + "not"));

        // Integer
        assertTrue(hiveBloomFilter.testLong(TEST_INTEGER));
        assertFalse(hiveBloomFilter.testLong(TEST_INTEGER + 1));
    }

    @Test
    public void testOrcHiveBloomfilterSerde()
            throws Exception
    {
        OrcProto.BloomFilterIndex bfi = OrcProto.BloomFilterIndex.getDefaultInstance();
        OrcProto.BloomFilterIndex.Builder builder = bfi.toBuilder();

        OrcProto.BloomFilter.Builder bfBuilder = OrcProto.BloomFilter.newBuilder();

        // Write value
        BloomFilter bfWrite = new BloomFilter(1000L, 0.05D);
        assertFalse(bfWrite.testString(TEST_STRING));
        assertFalse(bfWrite.testString(TEST_STRING + "not"));
        bfWrite.addString(TEST_STRING);
        assertTrue(bfWrite.testString(TEST_STRING));
        assertFalse(bfWrite.testString(TEST_STRING + "not"));
        for (long l : bfWrite.getBitSet()) {
            bfBuilder.addBitset(l);
        }
        bfBuilder.setNumHashFunctions(bfWrite.getNumHashFunctions());
        OrcProto.BloomFilter bf = bfBuilder.build();
        assertTrue(bf.isInitialized());
        builder.addBloomFilter(bf);

        OrcProto.BloomFilterIndex index = builder.build();
        assertTrue(index.isInitialized());
        assertEquals(1, index.getBloomFilterCount());
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        index.writeTo(os);
        os.flush();
        byte[] bytes = os.toByteArray();

        CodedInputStream input = CodedInputStream.newInstance(bytes);
        OrcProto.BloomFilterIndex bfDeserIdx = OrcProto.BloomFilterIndex.parseFrom(input);
        List<OrcProto.BloomFilter> bloomFilterList = bfDeserIdx.getBloomFilterList();
        assertEquals(1, bloomFilterList.size());

        OrcProto.BloomFilter bloomFilterRead = bloomFilterList.get(0);

        // Validate contents of ORC bloom filter bitset
        int i = 0;
        for (long l : bloomFilterRead.getBitsetList()) {
            assertEquals(l, bfWrite.getBitSet()[i]);
            i++;
        }
        RowGroupBloomfilter rowGroupBloomfilter = new RowGroupBloomfilter(bloomFilterRead);

        // Validate contents of bloom filter bitset
        i = 0;
        for (long l : rowGroupBloomfilter.getBloomfilter().getBitSet()) {
            assertEquals(l, bfWrite.getBitSet()[i]);
            i++;
        }

        BloomFilter rowGroupBloomfilterBloomfilter = rowGroupBloomfilter.getBloomfilter();
        assertEquals(bfWrite.toString(), rowGroupBloomfilterBloomfilter.toString());

        // hash functions
        assertEquals(bfWrite.getNumHashFunctions(), rowGroupBloomfilterBloomfilter.getNumHashFunctions());
        assertEquals(bfWrite.getNumHashFunctions(), bloomFilterRead.getNumHashFunctions());

        // bit size
        assertEquals(bfWrite.getBitSize(), rowGroupBloomfilterBloomfilter.getBitSize());
        assertEquals(bfWrite.getBitSet().length, bloomFilterRead.getBitsetCount());

        // test contents
        assertFalse(rowGroupBloomfilterBloomfilter.testString("robin"));
        rowGroupBloomfilterBloomfilter.addString("robin");
        assertTrue(rowGroupBloomfilterBloomfilter.testString("robin"));

        // test contents
        assertTrue(rowGroupBloomfilterBloomfilter.testString(TEST_STRING));
        assertFalse(rowGroupBloomfilterBloomfilter.testString(TEST_STRING + "bartosz"));
    }
}
