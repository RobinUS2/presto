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

import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hive.common.util.BloomFilter;

public final class RowGroupBloomfilter
{
    private final OrcProto.BloomFilter orcBf;
    private final HiveBloomFilter bf;

    public RowGroupBloomfilter(OrcProto.BloomFilter bf)
    {
        this.orcBf = bf;
        this.bf = new HiveBloomFilter(getOrcBloomfilter().getBitsetList(), getOrcBloomfilter().getBitsetCount(), getOrcBloomfilter().getNumHashFunctions());
    }

    public OrcProto.BloomFilter getOrcBloomfilter()
    {
        return orcBf;
    }

    public BloomFilter getBloomfilter()
    {
        return bf;
    }
}
