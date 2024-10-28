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
 * imitations under the License.
 */

package org.apache.flink.table.planner.adaptive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.Int2HashJoinOperatorTestBase;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinFunction;
import org.apache.flink.table.runtime.operators.sort.IntNormalizedKeyComputer;
import org.apache.flink.table.runtime.operators.sort.IntRecordComparator;
import org.apache.flink.table.runtime.util.UniformBinaryRowGenerator;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.runtime.util.JoinUtil.getJoinType;

/** Test for {@link AdaptiveBroadcastJoinOperatorGenerator}. */
class AdaptiveBroadcastJoinOperatorGeneratorTest extends Int2HashJoinOperatorTestBase {

    // ---------------------- hash join -----------------------------------------
    @Test
    void testBuildFirstHashInnerJoin() throws Exception {
        testInnerJoin(true, OperatorType.ShuffleHashJoin, false);
        testInnerJoin(true, OperatorType.ShuffleHashJoin, true);
    }

    @Test
    void testBuildFirstHashLeftOutJoin() throws Exception {
        testLeftOutJoin(true, OperatorType.ShuffleHashJoin, false);
    }

    @Test
    void testBuildFirstHashRightOutJoin() throws Exception {
        testRightOutJoin(true, OperatorType.ShuffleHashJoin, false);
        testRightOutJoin(true, OperatorType.ShuffleHashJoin, true);
    }

    @Test
    void testBuildSecondHashInnerJoin() throws Exception {
        testInnerJoin(false, OperatorType.ShuffleHashJoin, false);
        testInnerJoin(false, OperatorType.ShuffleHashJoin, true);
    }

    @Test
    void testBuildSecondHashLeftOutJoin() throws Exception {
        testLeftOutJoin(false, OperatorType.ShuffleHashJoin, false);
        testLeftOutJoin(false, OperatorType.ShuffleHashJoin, true);
    }

    @Test
    void testBuildSecondHashRightOutJoin() throws Exception {
        testRightOutJoin(false, OperatorType.ShuffleHashJoin, false);
    }

    @Test
    void testSemiHashJoin() throws Exception {
        testSemiJoin(OperatorType.ShuffleHashJoin, false);
        testSemiJoin(OperatorType.ShuffleHashJoin, true);
    }

    @Test
    void testAntiHashJoin() throws Exception {
        testAntiJoin(OperatorType.ShuffleHashJoin, false);
        testAntiJoin(OperatorType.ShuffleHashJoin, true);
    }

    // ---------------------- sort merge join -----------------------------------------
    @Test
    void testInnerSortMergeJoin() throws Exception {
        testInnerJoin(true, OperatorType.SortMergeJoin, false);
    }

    @Test
    void testLeftOutSortMergeJoin() throws Exception {
        testLeftOutJoin(true, OperatorType.SortMergeJoin, false);
    }

    @Test
    void testRightOutSortMergeJoin() throws Exception {
        testRightOutJoin(true, OperatorType.SortMergeJoin, false);
    }

    @Test
    void testSemiSortMergeJoin() throws Exception {
        testSemiJoin(OperatorType.SortMergeJoin, false);
    }

    @Test
    void testAntiSortMergeJoin() throws Exception {
        testAntiJoin(OperatorType.SortMergeJoin, false);
    }

    private void testInnerJoin(
            boolean isBuildLeft, OperatorType originalJoinType, boolean isBroadcast)
            throws Exception {
        int numKeys = 100;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys, buildValsPerKey, false);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys, probeValsPerKey, true);

        buildJoin(
                buildInput,
                probeInput,
                originalJoinType,
                false,
                false,
                isBuildLeft,
                isBroadcast,
                numKeys * buildValsPerKey * probeValsPerKey,
                numKeys,
                165);
    }

    private void testLeftOutJoin(
            boolean isBuildLeft, OperatorType originalJoinType, boolean isBroadcast)
            throws Exception {
        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(
                        isBuildLeft ? numKeys1 : numKeys2, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(
                        isBuildLeft ? numKeys2 : numKeys1, probeValsPerKey, true);

        buildJoin(
                buildInput,
                probeInput,
                originalJoinType,
                true,
                false,
                isBuildLeft,
                isBroadcast,
                numKeys1 * buildValsPerKey * probeValsPerKey,
                numKeys1,
                165);
    }

    private void testRightOutJoin(
            boolean isBuildLeft, OperatorType originalJoinType, boolean isBroadcast)
            throws Exception {
        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        buildJoin(
                buildInput,
                probeInput,
                originalJoinType,
                false,
                true,
                isBuildLeft,
                isBroadcast,
                isBuildLeft ? 280 : 270,
                numKeys2,
                -1);
    }

    private void testSemiJoin(OperatorType originalJoinType, boolean isBroadcast) throws Exception {
        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        if (originalJoinType == OperatorType.SortMergeJoin) {
            numKeys1 = 10;
            numKeys2 = 9;
            buildValsPerKey = 10;
            probeValsPerKey = 3;
        }
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        Object operator = newOperator(FlinkJoinType.SEMI, false, isBroadcast, originalJoinType);
        joinAndAssert(operator, buildInput, probeInput, 90, 9, 45, true);
    }

    private void testAntiJoin(OperatorType originalJoinType, boolean isBroadcast) throws Exception {
        int numKeys1 = 9;
        int numKeys2 = 10;
        int buildValsPerKey = 3;
        int probeValsPerKey = 10;
        if (originalJoinType == OperatorType.SortMergeJoin) {
            numKeys1 = 10;
            numKeys2 = 9;
            buildValsPerKey = 10;
            probeValsPerKey = 3;
        }
        MutableObjectIterator<BinaryRowData> buildInput =
                new UniformBinaryRowGenerator(numKeys1, buildValsPerKey, true);
        MutableObjectIterator<BinaryRowData> probeInput =
                new UniformBinaryRowGenerator(numKeys2, probeValsPerKey, true);

        Object operator = newOperator(FlinkJoinType.ANTI, false, isBroadcast, originalJoinType);
        joinAndAssert(operator, buildInput, probeInput, 10, 1, 45, true);
    }

    public void buildJoin(
            MutableObjectIterator<BinaryRowData> buildInput,
            MutableObjectIterator<BinaryRowData> probeInput,
            OperatorType originalJoinType,
            boolean leftOut,
            boolean rightOut,
            boolean buildLeft,
            boolean isBroadcast,
            int expectOutSize,
            int expectOutKeySize,
            int expectOutVal)
            throws Exception {
        FlinkJoinType flinkJoinType = getJoinType(leftOut, rightOut);
        Object operator = newOperator(flinkJoinType, buildLeft, isBroadcast, originalJoinType);
        joinAndAssert(
                operator,
                buildInput,
                probeInput,
                expectOutSize,
                expectOutKeySize,
                expectOutVal,
                false);
    }

    public Object newOperator(
            FlinkJoinType flinkJoinType,
            boolean buildLeft,
            boolean isBroadcast,
            OperatorType operatorType) {
        GeneratedJoinCondition condFuncCode =
                new GeneratedJoinCondition(
                        Int2HashJoinOperatorTestBase.MyJoinCondition.class.getCanonicalName(),
                        "",
                        new Object[0]) {
                    @Override
                    public JoinCondition newInstance(ClassLoader classLoader) {
                        return new Int2HashJoinOperatorTestBase.MyJoinCondition(new Object[0]);
                    }
                };
        GeneratedProjection leftProjectionCode =
                new GeneratedProjection("", "", new Object[0]) {
                    @Override
                    public Projection newInstance(ClassLoader classLoader) {
                        return new Int2HashJoinOperatorTestBase.MyProjection();
                    }
                };
        GeneratedProjection rightProjectionCode =
                new GeneratedProjection("", "", new Object[0]) {
                    @Override
                    public Projection newInstance(ClassLoader classLoader) {
                        return new Int2HashJoinOperatorTestBase.MyProjection();
                    }
                };
        GeneratedNormalizedKeyComputer computer1 =
                new GeneratedNormalizedKeyComputer("", "") {
                    @Override
                    public NormalizedKeyComputer newInstance(ClassLoader classLoader) {
                        return new IntNormalizedKeyComputer();
                    }
                };
        GeneratedRecordComparator comparator1 =
                new GeneratedRecordComparator("", "", new Object[0]) {
                    @Override
                    public RecordComparator newInstance(ClassLoader classLoader) {
                        return new IntRecordComparator();
                    }
                };

        GeneratedNormalizedKeyComputer computer2 =
                new GeneratedNormalizedKeyComputer("", "") {
                    @Override
                    public NormalizedKeyComputer newInstance(ClassLoader classLoader) {
                        return new IntNormalizedKeyComputer();
                    }
                };
        GeneratedRecordComparator comparator2 =
                new GeneratedRecordComparator("", "", new Object[0]) {
                    @Override
                    public RecordComparator newInstance(ClassLoader classLoader) {
                        return new IntRecordComparator();
                    }
                };
        GeneratedRecordComparator genKeyComparator =
                new GeneratedRecordComparator("", "", new Object[0]) {
                    @Override
                    public RecordComparator newInstance(ClassLoader classLoader) {
                        return new IntRecordComparator();
                    }
                };
        boolean[] filterNulls = new boolean[] {true};

        int maxNumFileHandles =
                ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES.defaultValue();
        boolean compressionEnabled =
                ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED.defaultValue();
        int compressionBlockSize =
                (int)
                        ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE
                                .defaultValue()
                                .getBytes();
        boolean asyncMergeEnabled =
                ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED.defaultValue();

        SortMergeJoinFunction sortMergeJoinFunction =
                new SortMergeJoinFunction(
                        0,
                        flinkJoinType,
                        true,
                        maxNumFileHandles,
                        compressionEnabled,
                        compressionBlockSize,
                        asyncMergeEnabled,
                        condFuncCode,
                        leftProjectionCode,
                        rightProjectionCode,
                        computer1,
                        comparator1,
                        computer2,
                        comparator2,
                        genKeyComparator,
                        filterNulls);

        AdaptiveBroadcastJoinOperatorGenerator adaptiveBroadcastJoinHolder =
                new AdaptiveBroadcastJoinOperatorGenerator(
                        RowType.of(new IntType()),
                        RowType.of(new IntType(), new IntType()),
                        RowType.of(new IntType(), new IntType()),
                        new int[] {0},
                        new int[] {0},
                        leftProjectionCode,
                        rightProjectionCode,
                        20,
                        10000,
                        20,
                        10000,
                        condFuncCode,
                        true,
                        compressionEnabled,
                        compressionBlockSize,
                        sortMergeJoinFunction,
                        flinkJoinType,
                        operatorType,
                        filterNulls,
                        false);

        adaptiveBroadcastJoinHolder.markActualBuildSide(buildLeft ? 1 : 2, isBroadcast);
        return adaptiveBroadcastJoinHolder.genOperatorFactory(
                getClass().getClassLoader(), new Configuration());
    }
}
