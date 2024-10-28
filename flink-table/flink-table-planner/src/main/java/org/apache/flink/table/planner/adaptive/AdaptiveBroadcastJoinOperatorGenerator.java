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

package org.apache.flink.table.planner.adaptive;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.AdaptiveBroadcastJoin;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.LongHashJoinGenerator;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.HashJoinOperator;
import org.apache.flink.table.runtime.operators.join.HashJoinType;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinFunction;
import org.apache.flink.table.runtime.operators.join.SortMergeJoinOperator;
import org.apache.flink.table.types.logical.RowType;

/**
 * Implementation class for {@link AdaptiveBroadcastJoin}. It can selectively generate broadcast
 * hash join, shuffle hash join or shuffle merge join operator based on actual conditions.
 */
public class AdaptiveBroadcastJoinOperatorGenerator implements AdaptiveBroadcastJoin {

    private final RowType keyType;

    private final RowType leftType;

    private final RowType rightType;

    private final int[] leftKeyMapping;

    private final int[] rightKeyMapping;

    private final GeneratedProjection leftProj;

    private final GeneratedProjection rightProj;

    private final int leftRowSize;

    private final long leftRowCount;

    private final int rightRowSize;

    private final long rightRowCount;

    private final GeneratedJoinCondition condFunc;

    private final boolean compressionEnabled;

    private final int compressionBlockSize;

    private final SortMergeJoinFunction sortMergeJoinFunction;

    private final FlinkJoinType joinType;

    private final OperatorType originalJoin;

    private final boolean[] filterNullKeys;

    private final boolean tryDistinctBuildRow;

    private boolean leftIsBuild;

    private boolean isBroadcastJoin;

    public AdaptiveBroadcastJoinOperatorGenerator(
            RowType keyType,
            RowType leftType,
            RowType rightType,
            int[] leftKeyMapping,
            int[] rightKeyMapping,
            GeneratedProjection leftProj,
            GeneratedProjection rightProj,
            int leftRowSize,
            long leftRowCount,
            int rightRowSize,
            long rightRowCount,
            GeneratedJoinCondition condFunc,
            boolean leftIsBuild,
            boolean compressionEnabled,
            int compressionBlockSize,
            SortMergeJoinFunction sortMergeJoinFunction,
            FlinkJoinType joinType,
            OperatorType originalJoin,
            boolean[] filterNullKeys,
            boolean tryDistinctBuildRow) {
        this.keyType = keyType;
        this.leftType = leftType;
        this.rightType = rightType;
        this.leftKeyMapping = leftKeyMapping;
        this.rightKeyMapping = rightKeyMapping;
        this.leftProj = leftProj;
        this.rightProj = rightProj;
        this.leftRowSize = leftRowSize;
        this.leftRowCount = leftRowCount;
        this.rightRowSize = rightRowSize;
        this.rightRowCount = rightRowCount;
        this.condFunc = condFunc;
        this.leftIsBuild = leftIsBuild;
        this.compressionEnabled = compressionEnabled;
        this.compressionBlockSize = compressionBlockSize;
        this.sortMergeJoinFunction = sortMergeJoinFunction;
        this.joinType = joinType;
        this.originalJoin = originalJoin;
        this.filterNullKeys = filterNullKeys;
        this.tryDistinctBuildRow = tryDistinctBuildRow;
    }

    @Override
    public StreamOperatorFactory<?> genOperatorFactory(
            ClassLoader classLoader, ReadableConfig config) {
        StreamOperatorFactory<RowData> operatorFactory;
        sortMergeJoinFunction.resetLeftIsSmaller(leftIsBuild);
        HashJoinType hashJoinType =
                HashJoinType.of(
                        leftIsBuild,
                        joinType.isLeftOuter(),
                        joinType.isRightOuter(),
                        joinType == FlinkJoinType.SEMI,
                        joinType == FlinkJoinType.ANTI);
        if (isBroadcastJoin || originalJoin == OperatorType.ShuffleHashJoin) {
            boolean reverseJoin = !leftIsBuild;
            GeneratedProjection buildProj;
            GeneratedProjection probeProj;
            RowType buildType;
            RowType probeType;
            int buildRowSize;
            long buildRowCount;
            int[] buildKeys;
            long probeRowCount;
            int[] probeKeys;

            if (leftIsBuild) {
                buildProj = leftProj;
                buildType = leftType;
                buildRowSize = leftRowSize;
                buildRowCount = leftRowCount;
                buildKeys = leftKeyMapping;

                probeProj = rightProj;
                probeType = rightType;
                probeRowCount = rightRowCount;
                probeKeys = rightKeyMapping;
            } else {
                buildProj = rightProj;
                buildType = rightType;
                buildRowSize = rightRowSize;
                buildRowCount = rightRowCount;
                buildKeys = rightKeyMapping;

                probeProj = leftProj;
                probeType = leftType;
                probeRowCount = leftRowCount;
                probeKeys = leftKeyMapping;
            }

            if (LongHashJoinGenerator.support(hashJoinType, keyType, filterNullKeys)) {
                operatorFactory =
                        LongHashJoinGenerator.gen(
                                config,
                                classLoader,
                                hashJoinType,
                                keyType,
                                buildType,
                                probeType,
                                buildKeys,
                                probeKeys,
                                buildRowSize,
                                buildRowCount,
                                reverseJoin,
                                condFunc,
                                leftIsBuild,
                                compressionEnabled,
                                compressionBlockSize,
                                sortMergeJoinFunction);

            } else {
                operatorFactory =
                        SimpleOperatorFactory.of(
                                HashJoinOperator.newHashJoinOperator(
                                        hashJoinType,
                                        leftIsBuild,
                                        compressionEnabled,
                                        compressionBlockSize,
                                        condFunc,
                                        reverseJoin,
                                        filterNullKeys,
                                        buildProj,
                                        probeProj,
                                        tryDistinctBuildRow,
                                        buildRowSize,
                                        buildRowCount,
                                        probeRowCount,
                                        keyType,
                                        sortMergeJoinFunction));
            }
        } else {
            operatorFactory =
                    SimpleOperatorFactory.of(new SortMergeJoinOperator(sortMergeJoinFunction));
        }

        return operatorFactory;
    }

    @Override
    public void markActualBuildSide(int side, boolean isBroadcast) {
        isBroadcastJoin = isBroadcast;
        leftIsBuild = (side == 1);
    }

    @Override
    public boolean canBeBuildSide(int side) {
        if (side == 1) {
            return joinType == FlinkJoinType.RIGHT || joinType == FlinkJoinType.INNER;
        } else {
            return joinType == FlinkJoinType.LEFT
                    || joinType == FlinkJoinType.ANTI
                    || joinType == FlinkJoinType.SEMI
                    || joinType == FlinkJoinType.INNER;
        }
    }
}
