/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.partition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.statistics.FullSimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.predicate.PredicateBuilder.fieldIdxToPartitionIdx;
import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;
import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternal;
import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternalRow;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * A special predicate to filter partition only, just like {@link Predicate}.
 *
 * @since 1.3.0
 */
public interface PartitionPredicate extends Serializable {

    /**
     * Test based on the specific partition.
     *
     * @return return true when hit, false when not hit.
     */
    boolean test(BinaryRow partition);

    /**
     * Test based on the statistical information to determine whether a hit is possible.
     *
     * @return return true is likely to hit (there may also be false positives), return false is
     *     absolutely not possible to hit.
     */
    boolean test(
            long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts);

    /**
     * Compared to the multiple method, this approach can accept filtering of partially partitioned
     * fields.
     */
    @Nullable
    static PartitionPredicate fromPredicate(RowType partitionType, Predicate predicate) {
        if (partitionType.getFieldCount() == 0 || predicate == null) {
            return null;
        }

        return new DefaultPartitionPredicate(predicate);
    }

    /** Create {@link PartitionPredicate} from multiple partitions. */
    @Nullable
    static PartitionPredicate fromMultiple(RowType partitionType, List<BinaryRow> partitions) {
        return fromMultiple(partitionType, new HashSet<>(partitions));
    }

    /** Create {@link PartitionPredicate} from multiple partitions. */
    @Nullable
    static PartitionPredicate fromMultiple(RowType partitionType, Set<BinaryRow> partitions) {
        if (partitionType.getFieldCount() == 0 || partitions.isEmpty()) {
            return null;
        }

        return new MultiplePartitionPredicate(
                new RowDataToObjectArrayConverter(partitionType), partitions);
    }

    /** Creates {@link PartitionPredicate} that combines multiple predicates using logical AND. */
    @Nullable
    static PartitionPredicate and(List<PartitionPredicate> predicates) {
        if (predicates.isEmpty()) {
            return null;
        }

        if (predicates.size() == 1) {
            return predicates.get(0);
        }

        return new AndPartitionPredicate(predicates);
    }

    PartitionPredicate ALWAYS_FALSE =
            new PartitionPredicate() {
                @Override
                public boolean test(BinaryRow part) {
                    return false;
                }

                @Override
                public boolean test(
                        long rowCount,
                        InternalRow minValues,
                        InternalRow maxValues,
                        InternalArray nullCounts) {
                    return false;
                }
            };

    PartitionPredicate ALWAYS_TRUE =
            new PartitionPredicate() {
                @Override
                public boolean test(BinaryRow part) {
                    return true;
                }

                @Override
                public boolean test(
                        long rowCount,
                        InternalRow minValues,
                        InternalRow maxValues,
                        InternalArray nullCounts) {
                    return true;
                }
            };

    /** A {@link PartitionPredicate} using {@link Predicate}. */
    class DefaultPartitionPredicate implements PartitionPredicate {

        private static final long serialVersionUID = 1L;

        private final Predicate predicate;

        private DefaultPartitionPredicate(Predicate predicate) {
            this.predicate = predicate;
        }

        @Override
        public boolean test(BinaryRow part) {
            return predicate.test(part);
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            return predicate.test(rowCount, minValues, maxValues, nullCounts);
        }

        public Predicate predicate() {
            return predicate;
        }

        @Override
        public String toString() {
            return predicate.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DefaultPartitionPredicate that = (DefaultPartitionPredicate) o;
            return Objects.equals(predicate, that.predicate);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(predicate);
        }
    }

    /**
     * A {@link PartitionPredicate} optimizing for multiple partitions. Its FieldStats filtering
     * effect may not be as good as {@link DefaultPartitionPredicate}.
     */
    class MultiplePartitionPredicate implements PartitionPredicate {

        private static final long serialVersionUID = 1L;

        private final Set<BinaryRow> partitions;
        private final int fieldNum;
        private final Predicate[] min;
        private final Predicate[] max;

        private MultiplePartitionPredicate(
                RowDataToObjectArrayConverter converter, Set<BinaryRow> partitions) {
            this.partitions = partitions;
            RowType partitionType = converter.partitionType();
            this.fieldNum = partitionType.getFieldCount();
            @SuppressWarnings("unchecked")
            Serializer<Object>[] serializers = new Serializer[fieldNum];
            FullSimpleColStatsCollector[] collectors = new FullSimpleColStatsCollector[fieldNum];
            // 分区字段的 Predicate
            // 比如分区字段为<hour, day>, partitions 为 Set<<'16', '2026-01-15'>, <'16', '2026-01-16'>
            // min[0]: >= '16', max[0]: <= '16'
            // min[1]: >= '2025-01-15', max[1]: <= '2025-01-16'
            min = new Predicate[fieldNum];
            max = new Predicate[fieldNum];
            for (int i = 0; i < fieldNum; i++) {
                serializers[i] = InternalSerializers.create(partitionType.getTypeAt(i));
                collectors[i] = new FullSimpleColStatsCollector();
            }
            for (BinaryRow part : partitions) {
                // 分区值更新到 FullSimpleColStatsCollector
                Object[] fields = converter.convert(part);
                for (int i = 0; i < fields.length; i++) {
                    collectors[i].collect(fields[i], serializers[i]);
                }
            }
            PredicateBuilder builder = new PredicateBuilder(partitionType);
            for (int i = 0; i < collectors.length; i++) {
                SimpleColStats stats = collectors[i].result();
                Long nullCount = stats.nullCount();
                checkArgument(nullCount != null, "nullCount cannot be null!");
                if (nullCount == partitions.size()) {
                    min[i] = builder.isNull(i);
                    max[i] = builder.isNull(i);
                } else {
                    min[i] = builder.greaterOrEqual(i, checkNotNull(stats.min()));
                    max[i] = builder.lessOrEqual(i, checkNotNull(stats.max()));
                    if (nullCount > 0) {
                        min[i] = PredicateBuilder.or(builder.isNull(i), min[i]);
                        max[i] = PredicateBuilder.or(builder.isNull(i), max[i]);
                    }
                }
            }
        }

        @Override
        public boolean test(BinaryRow part) {
            return partitions.contains(part);
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            if (fieldNum == 0) {
                return true;
            }

            for (int i = 0; i < fieldNum; i++) {
                // 比如字段 day，如果 min[0].test 和 max[0].test 有一个不满足，则表示分区值不符合要求，可以直接过滤掉
                if (!min[i].test(rowCount, minValues, maxValues, nullCounts)
                        || !max[i].test(rowCount, minValues, maxValues, nullCounts)) {
                    return false;
                }
            }
            return true;
        }

        public Optional<BinaryRow> extractSinglePartition() {
            return partitions.size() == 1
                    ? Optional.of(partitions.iterator().next())
                    : Optional.empty();
        }

        public Set<BinaryRow> partitions() {
            return partitions;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MultiplePartitionPredicate that = (MultiplePartitionPredicate) o;
            return fieldNum == that.fieldNum
                    && Objects.equals(partitions, that.partitions)
                    && Objects.deepEquals(min, that.min)
                    && Objects.deepEquals(max, that.max);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitions, fieldNum, Arrays.hashCode(min), Arrays.hashCode(max));
        }
    }

    /** AND-combines multiple {@link PartitionPredicate}s. */
    class AndPartitionPredicate implements PartitionPredicate {

        private static final long serialVersionUID = 1L;

        private final List<PartitionPredicate> predicates;

        private AndPartitionPredicate(List<PartitionPredicate> predicates) {
            checkArgument(!predicates.isEmpty());
            this.predicates = Collections.unmodifiableList(new ArrayList<>(predicates));
        }

        @Override
        public boolean test(BinaryRow partition) {
            for (PartitionPredicate predicate : predicates) {
                if (!predicate.test(partition)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean test(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            for (PartitionPredicate predicate : predicates) {
                if (!predicate.test(rowCount, minValues, maxValues, nullCounts)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            if (predicates.size() == 1) {
                return predicates.get(0).toString();
            }
            return "AND" + "(" + predicates + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AndPartitionPredicate that = (AndPartitionPredicate) o;
            return Objects.equals(predicates, that.predicates);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(predicates);
        }
    }

    static Predicate createPartitionPredicate(RowType rowType, Map<String, Object> partition) {
        PredicateBuilder builder = new PredicateBuilder(rowType);
        List<String> fieldNames = rowType.getFieldNames();
        Predicate predicate = null;
        for (Map.Entry<String, Object> entry : partition.entrySet()) {
            Object literal = entry.getValue();
            int idx = fieldNames.indexOf(entry.getKey());
            Predicate predicateTemp =
                    literal == null ? builder.isNull(idx) : builder.equal(idx, literal);
            if (predicate == null) {
                predicate = predicateTemp;
            } else {
                predicate = PredicateBuilder.and(predicate, predicateTemp);
            }
        }
        return predicate;
    }

    static Predicate createPartitionPredicateAskang(
            RowType rowType, Map<String, Object> partitions) {
        PredicateBuilder builder = new PredicateBuilder(rowType);
        List<String> fieldNames = rowType.getFieldNames();
        Predicate predicate = null;
        for (Map.Entry<String, Object> partition : partitions.entrySet()) {
            Object literal = partition.getValue();
            int idx = fieldNames.indexOf(partition.getKey());
            Predicate predicateTmp =
                    literal == null ? builder.isNull(idx) : builder.equal(idx, literal);
            if (predicate == null) {
                predicate = predicateTmp;
            } else {
                predicate = PredicateBuilder.and(predicate, predicateTmp);
            }
        }
        return predicate;
    }

    static Predicate createPartitionPredicate(RowType partitionType, Object[] partition) {
        Preconditions.checkArgument(
                partition.length == partitionType.getFieldCount(),
                "Partition's field count should be equal to partitionType's field count.");

        Map<String, Object> partitionMap = new HashMap<>(partition.length);
        for (int i = 0; i < partition.length; i++) {
            partitionMap.put(partitionType.getFields().get(i).name(), partition[i]);
        }

        return createPartitionPredicate(partitionType, partitionMap);
    }

    static Predicate createPartitionPredicate(RowType partitionType, BinaryRow partition) {
        Preconditions.checkArgument(
                partition.getFieldCount() == partitionType.getFieldCount(),
                "Partition's field count should be equal to partitionType's field count.");
        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(partitionType);
        return createPartitionPredicate(partitionType, converter.convert(partition));
    }

    @Nullable
    static Predicate createPartitionPredicate(
            Map<String, String> spec, RowType rowType, String defaultPartValue) {
        Map<String, Object> internalValues = convertSpecToInternal(spec, rowType, defaultPartValue);
        return createPartitionPredicate(rowType, internalValues);
    }

    static Predicate createPartitionPredicate(
            List<Map<String, String>> partitions, RowType rowType, String defaultPartValue) {
        return PredicateBuilder.or(
                partitions.stream()
                        .map(p -> createPartitionPredicate(p, rowType, defaultPartValue))
                        .toArray(Predicate[]::new));
    }

    static List<BinaryRow> createBinaryPartitions(
            List<Map<String, String>> partitions, RowType partitionType, String defaultPartValue) {
        InternalRowSerializer serializer = new InternalRowSerializer(partitionType);
        List<BinaryRow> result = new ArrayList<>();
        for (Map<String, String> spec : partitions) {
            // spec 的分区 value 封装成 GenericRow
            GenericRow row = convertSpecToInternalRow(spec, partitionType, defaultPartValue);
            result.add(serializer.toBinaryRow(row).copy());
        }
        return result;
    }

    static PartitionPredicate fromMap(
            RowType partitionType, Map<String, String> values, String defaultPartValue) {
        return fromPredicate(
                partitionType, createPartitionPredicate(values, partitionType, defaultPartValue));
    }

    static PartitionPredicate fromMaps(
            RowType partitionType, List<Map<String, String>> values, String defaultPartValue) {
        return fromMultiple(
                partitionType, createBinaryPartitions(values, partitionType, defaultPartValue));
    }

    static Pair<Optional<PartitionPredicate>, List<Predicate>>
            splitPartitionPredicatesAndDataPredicates(
                    Predicate dataPredicates, RowType tableType, List<String> partitionKeys) {
        return splitPartitionPredicatesAndDataPredicates(
                PredicateBuilder.splitAnd(dataPredicates), tableType, partitionKeys);
    }

    static Pair<Optional<PartitionPredicate>, List<Predicate>>
            splitPartitionPredicatesAndDataPredicates(
                    List<Predicate> dataPredicates, RowType tableType, List<String> partitionKeys) {
        if (partitionKeys.isEmpty()) {
            return Pair.of(Optional.empty(), dataPredicates);
        }

        RowType partitionType = tableType.project(partitionKeys);
        // 比如 RowType<id int, hour string, appid string, day string>
        // partitionKeys: List<'hour', 'day'>
        // fieldIdxToPartitionIdx(): partitionIdx = [-1, 0, -1, 1]
        // transformFieldMapping():
        //      FieldRef(1, 'hour', string) => FieldRef(0, 'hour', string)
        //      FieldRef(3, 'day', string) => FieldRef(1, 'day', string)
        // askwang-done: 这里更新了 FieldRef 的 index，在实际读取 Row 时是如何判断的？
        // A: AbstractFileStoreScan#createEntryRowFilter() 的 partitionGetter 只获取分区的值，即只包含
        // Row<'hour' string, 'day' string> 两个字段
        int[] partitionIdx = fieldIdxToPartitionIdx(tableType, partitionKeys);

        List<Predicate> partitionFilters = new ArrayList<>();
        List<Predicate> nonPartitionFilters = new ArrayList<>();
        for (Predicate p : dataPredicates) {
            Optional<Predicate> mapped = transformFieldMapping(p, partitionIdx);
            if (mapped.isPresent()) {
                partitionFilters.add(mapped.get());
            } else {
                nonPartitionFilters.add(p);
            }
        }
        PartitionPredicate partitionPredicate =
                partitionFilters.isEmpty()
                        ? null
                        : PartitionPredicate.fromPredicate(
                                partitionType, PredicateBuilder.and(partitionFilters));
        return Pair.of(Optional.ofNullable(partitionPredicate), nonPartitionFilters);
    }
}
