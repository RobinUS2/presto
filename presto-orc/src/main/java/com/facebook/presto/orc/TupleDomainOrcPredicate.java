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

import com.facebook.presto.orc.metadata.BooleanStatistics;
import com.facebook.presto.orc.metadata.ColumnStatistics;
import com.facebook.presto.orc.metadata.RangeStatistics;
import com.facebook.presto.orc.metadata.RowGroupBloomfilter;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.EquatableValueSet;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
//import org.apache.hadoop.hive.serde2.io.DateWritable;
//import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.BloomFilter;

import java.math.BigDecimal;
//import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.isLongDecimal;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.Decimals.rescale;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TupleDomainOrcPredicate<C>
        implements OrcPredicate
{
    private final TupleDomain<C> effectivePredicate;
    private final List<ColumnReference<C>> columnReferences;

    private static final Logger log = Logger.get(TupleDomainOrcPredicate.class);

    public TupleDomainOrcPredicate(TupleDomain<C> effectivePredicate, List<ColumnReference<C>> columnReferences)
    {
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
        this.columnReferences = ImmutableList.copyOf(requireNonNull(columnReferences, "columnReferences is null"));
    }

    @Override
    public boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByColumnIndex)
    {
        ImmutableMap.Builder<C, Domain> domains = ImmutableMap.builder();

        for (ColumnReference<C> columnReference : columnReferences) {
            ColumnStatistics columnStatistics = statisticsByColumnIndex.get(columnReference.getOrdinal());

            Domain domain;
            if (columnStatistics == null) {
                // no stats for column
                domain = Domain.all(columnReference.getType());
            }
            else {
                domain = getDomain(columnReference.getType(), numberOfRows, columnStatistics);
            }
            domains.put(columnReference.getColumn(), domain);
        }
        // this is where we create a domain for this stripe, basically a map of what this stripe contains
        TupleDomain<C> stripeDomain = TupleDomain.withColumnDomains(domains.build());

        // Compare effective predicate with the current stripe
        if (!effectivePredicate.overlaps(stripeDomain)) {
            // No overlap, stop here
            return false;
        }

        // check bloomfilters (more expensive so separated from the domain check above)
        boolean allPassedBloomfilters = true;

        // we need to have filters in order to say we checked all
        if (columnReferences.isEmpty()) {
            allPassedBloomfilters = false;
        }

        // check bloomfilters per column
        for (ColumnReference<C> columnReference : columnReferences) {
            ColumnStatistics columnStatistics = statisticsByColumnIndex.get(columnReference.getOrdinal());
            if (columnStatistics == null) {
                allPassedBloomfilters = false;
                log.info("No column stats");
                continue;
            }

            List<RowGroupBloomfilter> bloomfilters = columnStatistics.getBloomfilters();
            if (bloomfilters == null || bloomfilters.isEmpty()) {
                allPassedBloomfilters = false;
                log.info("No bloomfilters");
                continue;
            }

            // @todo refactor logic
            Optional<Map<C, Domain>> domains1 = effectivePredicate.getDomains();
            if (domains1.isPresent()) {
                log.info("found effective predicate domains");
                Map<C, Domain> cDomainMap = domains1.get();
                if (cDomainMap.containsKey(columnReference.getColumn())) {
                    log.info("found effective predicate domains for colujmn");

                    // extract values
                    Domain domain = cDomainMap.get(columnReference.getColumn());
                    ValueSet values = domain.getValues();
                    log.info("values type " + values.getType().getDisplayName());
                    log.info("values class " + values.getClass().getCanonicalName());
                    log.info("values  " + values.toString());
                    Collection<Object> predicateValues = null;
                    if (values instanceof EquatableValueSet) {
                        EquatableValueSet eqValues = (EquatableValueSet) values;
                        if (eqValues.isWhiteList()) {
                            // we can only work with values we know, not excluded blacklists because other rows might contain the data we need
                            predicateValues = values.getDiscreteValues().getValues();
                        }
                    }
                    else if (values instanceof SortedRangeSet) {
                        SortedRangeSet sortedRangeSet = (SortedRangeSet) values;
                        // sorted range set is used for integer comparison (e.g. id = 123 ) where min and max is the same value
                        if (sortedRangeSet.isSingleValue()) {
                            predicateValues = new ArrayList<>();
                            predicateValues.add(sortedRangeSet.getSingleValue());
                        }
                    }

                    // run values against the bloomfilters
                    if (predicateValues != null && !predicateValues.isEmpty()) {
                        for (Object o : predicateValues) {
                            log.info("Equatable value set value=" + String.valueOf(o));

                            for (RowGroupBloomfilter rowGroupBloomfilter : bloomfilters) {
                                BloomFilter bloomfilter = rowGroupBloomfilter.getBloomfilter();
                                log.info("bf = " + bloomfilter.toString());
                                TruthValue truthValue = checkInBloomFilter(bloomfilter, o, true); // @todo replace false with hasnull from orc column stats
                                if (truthValue == TruthValue.YES || truthValue == TruthValue.YES_NO || truthValue == TruthValue.YES_NO_NULL || truthValue == TruthValue.YES_NULL) {
                                    // bloom filter is matched here return true so we select this stripe as it likely contains data which we need to read
                                    return true;
                                }
                            }
                        }
                    }
                    else {
                        // no values checked, treat as failure
                        allPassedBloomfilters = false;
                    }
                }
                else {
                    // no domain found for column, treat as failure
                    allPassedBloomfilters = false;
                }
            }
            else {
                allPassedBloomfilters = false;
                log.info("No predicate domains");
            }
        }

        if (allPassedBloomfilters) {
            // none of the bloomfilters caused a "hit" meaning we should not read
            log.info("Not reading thanks to our bloomfilters :)");
            return false;
        }

        // not enough knowledge in the bloomfilters, let's read it anyway
        return true;
    }

    private TruthValue checkInBloomFilter(BloomFilter bf, Object predObj, boolean hasNull)
    {
        TruthValue result = hasNull ? TruthValue.NO_NULL : TruthValue.NO;

        if (predObj instanceof Long) {
            if (bf.testLong(((Long) predObj).longValue())) {
                result = TruthValue.YES_NO_NULL;
            }
        }
        else if (predObj instanceof Double) {
            if (bf.testDouble(((Double) predObj).doubleValue())) {
                result = TruthValue.YES_NO_NULL;
            }
        }
        else if (predObj instanceof String ||
//                predObj instanceof Text ||
//                predObj instanceof HiveDecimalWritable ||
                predObj instanceof BigDecimal) {
            if (bf.testString(predObj.toString())) {
                result = TruthValue.YES_NO_NULL;
            }
        }
        else if (predObj instanceof Timestamp) {
            if (bf.testLong(((Timestamp) predObj).getTime())) {
                result = TruthValue.YES_NO_NULL;
            }
        }
//        else if (predObj instanceof Date) {
//            if (bf.testLong(DateWritable.dateToDays((Date) predObj))) {
//                result = TruthValue.YES_NO_NULL;
//            }
//        }
        else {
            // @todo enable code below once support for Text HiveDecimalWritable and Date is done
            log.warn("Bloom filter check not supported for type " + predObj);
            return TruthValue.YES;
//            // if the predicate object is null and if hasNull says there are no nulls then return NO
//            if (predObj == null && !hasNull) {
//                result = TruthValue.NO;
//            }
//            else {
//                result = TruthValue.YES_NO_NULL;
//            }
        }

        if (result == TruthValue.YES_NO_NULL && !hasNull) {
            result = TruthValue.YES_NO;
        }

        log.debug("Bloom filter evaluation: " + String.valueOf(predObj) + "=" + result.toString());

        return result;
    }

    @VisibleForTesting
    public static Domain getDomain(Type type, long rowCount, ColumnStatistics columnStatistics)
    {
        if (rowCount == 0) {
            return Domain.none(type);
        }

        if (columnStatistics == null) {
            return Domain.all(type);
        }

        if (columnStatistics.hasNumberOfValues() && columnStatistics.getNumberOfValues() == 0) {
            return Domain.onlyNull(type);
        }

        boolean hasNullValue = columnStatistics.getNumberOfValues() != rowCount;

        if (type.getJavaType() == boolean.class && columnStatistics.getBooleanStatistics() != null) {
            BooleanStatistics booleanStatistics = columnStatistics.getBooleanStatistics();

            boolean hasTrueValues = (booleanStatistics.getTrueValueCount() != 0);
            boolean hasFalseValues = (columnStatistics.getNumberOfValues() != booleanStatistics.getTrueValueCount());
            if (hasTrueValues && hasFalseValues) {
                return Domain.all(BOOLEAN);
            }
            if (hasTrueValues) {
                return Domain.create(ValueSet.of(BOOLEAN, true), hasNullValue);
            }
            if (hasFalseValues) {
                return Domain.create(ValueSet.of(BOOLEAN, false), hasNullValue);
            }
        }
        else if (isShortDecimal(type)) {
            return createDomain(type, hasNullValue, columnStatistics.getDecimalStatistics(), value -> rescale(value, (DecimalType) type).unscaledValue().longValue());
        }
        else if (isLongDecimal(type)) {
            return createDomain(type, hasNullValue, columnStatistics.getDecimalStatistics(), value -> encodeUnscaledValue(rescale(value, (DecimalType) type).unscaledValue()));
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.DATE) && columnStatistics.getDateStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getDateStatistics(), value -> (long) value);
        }
        else if (type.getJavaType() == long.class && columnStatistics.getIntegerStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getIntegerStatistics());
        }
        else if (type.getJavaType() == double.class && columnStatistics.getDoubleStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getDoubleStatistics());
        }
        else if (type.getJavaType() == Slice.class && columnStatistics.getStringStatistics() != null) {
            return createDomain(type, hasNullValue, columnStatistics.getStringStatistics());
        }
        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    private static <T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue, RangeStatistics<T> rangeStatistics)
    {
        return createDomain(type, hasNullValue, rangeStatistics, value -> value);
    }

    private static <F, T extends Comparable<T>> Domain createDomain(Type type, boolean hasNullValue, RangeStatistics<F> rangeStatistics, Function<F, T> function)
    {
        F min = rangeStatistics.getMin();
        F max = rangeStatistics.getMax();

        if (min != null && max != null) {
            return Domain.create(ValueSet.ofRanges(Range.range(type, function.apply(min), true, function.apply(max), true)), hasNullValue);
        }
        if (max != null) {
            return Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(type, function.apply(max))), hasNullValue);
        }
        if (min != null) {
            return Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(type, function.apply(min))), hasNullValue);
        }
        return Domain.create(ValueSet.all(type), hasNullValue);
    }

    public static class ColumnReference<C>
    {
        private final C column;
        private final int ordinal;
        private final Type type;

        public ColumnReference(C column, int ordinal, Type type)
        {
            this.column = requireNonNull(column, "column is null");
            checkArgument(ordinal >= 0, "ordinal is negative");
            this.ordinal = ordinal;
            this.type = requireNonNull(type, "type is null");
        }

        public C getColumn()
        {
            return column;
        }

        public int getOrdinal()
        {
            return ordinal;
        }

        public Type getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("column", column)
                    .add("ordinal", ordinal)
                    .add("type", type)
                    .toString();
        }
    }
}
