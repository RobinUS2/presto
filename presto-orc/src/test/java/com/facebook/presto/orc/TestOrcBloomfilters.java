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

import com.facebook.presto.orc.metadata.HiveBloomFilter;
import org.apache.hive.common.util.BloomFilter;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

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
}
