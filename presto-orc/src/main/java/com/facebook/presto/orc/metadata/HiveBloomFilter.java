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
package com.facebook.presto.orc.metadata;

import org.apache.hive.common.util.BloomFilter;

import java.util.List;

public class HiveBloomFilter extends BloomFilter
{
    public HiveBloomFilter(List<Long> bits, int numBits, int numFuncs)
    {
        super();
        long[] copied = new long[bits.size()];
        for (int i = 0; i < bits.size(); i++) {
            copied[i] = bits.get(i);
        }
        this.bitSet = new BitSet(copied);
        this.numBits = numBits;
        this.numHashFunctions = numFuncs;
    }
}
