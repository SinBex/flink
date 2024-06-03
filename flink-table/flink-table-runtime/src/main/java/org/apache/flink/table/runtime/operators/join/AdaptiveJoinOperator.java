/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file exceBinaryRow in compliance
 * with the License.  You may oBinaryRowain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHBinaryRow WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.types.logical.RowType;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Adaptive join operator. */
public class AdaptiveJoinOperator extends HashJoinOperator {

    private final SortMergeJoinFunction sortMergeJoinFunction;
    private final HashJoinType type;
    private final int originalJoinType;
    private boolean leftIsBuild;
    private boolean isBroadcast;

    AdaptiveJoinOperator(HashJoinParameter parameter, int originalJoinType) {
        super(parameter);
        this.type = parameter.type;
        this.sortMergeJoinFunction = parameter.sortMergeJoinFunction;
        this.originalJoinType = originalJoinType;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (isBroadcast) {
            reApply(leftIsBuild);
        } else {
            if (originalJoinType == 1) {
                this.sortMergeJoinFunction.open(
                        false,
                        this.getContainingTask(),
                        this.getOperatorConfig(),
                        new StreamRecordCollector(output),
                        this.computeMemorySize(),
                        this.getRuntimeContext(),
                        this.getMetricGroup());
            }
        }
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        if (isBroadcast || originalJoinType == 0) {
            super.processElement1(element);
        } else {
            sortMergeJoinFunction.processElement1(element.getValue());
        }
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        if (isBroadcast || originalJoinType == 0) {
            super.processElement2(element);
        } else {
            sortMergeJoinFunction.processElement2(element.getValue());
        }
    }

    @Override
    public InputSelection nextSelection() {
        if (isBroadcast || originalJoinType == 0) {
            return super.nextSelection();
        } else {
            return InputSelection.ALL;
        }
    }

    @Override
    public void endInput(int inputId) throws Exception {
        if (isBroadcast || originalJoinType == 0) {
            super.endInput(inputId);
        } else {
            sortMergeJoinFunction.endInput(inputId);
        }
    }

    @Override
    public void join(RowIterator<BinaryRowData> buildIter, RowData probeRow) throws Exception {
        switch (type) {
            case INNER:
                if (buildIter.advanceNext()) {
                    if (probeRow != null) {
                        innerJoin(buildIter, probeRow);
                    }
                }
                break;
            case BUILD_OUTER:
                if (buildIter.advanceNext()) {
                    if (probeRow != null) {
                        innerJoin(buildIter, probeRow);
                    } else {
                        buildOuterJoin(buildIter);
                    }
                }
                break;
            case PROBE_OUTER:
                if (buildIter.advanceNext()) {
                    if (probeRow != null) {
                        innerJoin(buildIter, probeRow);
                    }
                } else if (probeRow != null) {
                    collect(buildSideNullRow, probeRow);
                }
                break;
            case FULL_OUTER:
                if (buildIter.advanceNext()) {
                    if (probeRow != null) {
                        innerJoin(buildIter, probeRow);
                    } else {
                        buildOuterJoin(buildIter);
                    }
                } else if (probeRow != null) {
                    collect(buildSideNullRow, probeRow);
                }
                break;
            case SEMI:
                checkNotNull(probeRow);
                if (buildIter.advanceNext()) {
                    collector.collect(probeRow);
                }
                break;
            case ANTI:
                checkNotNull(probeRow);
                if (!buildIter.advanceNext()) {
                    collector.collect(probeRow);
                }
                break;
            case BUILD_LEFT_SEMI:
            case BUILD_LEFT_ANTI:
                if (buildIter.advanceNext()) {
                    if (probeRow != null) {
                        while (buildIter.advanceNext()) {}
                    } else {
                        collector.collect(buildIter.getRow());
                        while (buildIter.advanceNext()) {
                            collector.collect(buildIter.getRow());
                        }
                    }
                }
                break;
            default:
                throw new IllegalArgumentException("invalid: " + type);
        }
    }

    @Override
    public void close() throws Exception {
        if (!isBroadcast && originalJoinType == 1) {
            sortMergeJoinFunction.close();
        }
        super.close();
    }

    public static AdaptiveJoinOperator newAdaptiveJoinOperator(
            HashJoinType type,
            boolean leftIsBuild,
            boolean compressionEnable,
            int compressionBlockSize,
            GeneratedJoinCondition condFuncCode,
            boolean reverseJoinFunction,
            boolean[] filterNullKeys,
            GeneratedProjection buildProjectionCode,
            GeneratedProjection probeProjectionCode,
            boolean tryDistinctBuildRow,
            int buildRowSize,
            long buildRowCount,
            long probeRowCount,
            RowType keyType,
            SortMergeJoinFunction sortMergeJoinFunction,
            int originalJobType) {
        HashJoinOperator.HashJoinParameter parameter =
                new HashJoinOperator.HashJoinParameter(
                        type,
                        leftIsBuild,
                        compressionEnable,
                        compressionBlockSize,
                        condFuncCode,
                        reverseJoinFunction,
                        filterNullKeys,
                        buildProjectionCode,
                        probeProjectionCode,
                        tryDistinctBuildRow,
                        buildRowSize,
                        buildRowCount,
                        probeRowCount,
                        keyType,
                        sortMergeJoinFunction);
        return new AdaptiveJoinOperator(parameter, originalJobType);
    }

    @Override
    public boolean hasKeyContext() {
        return super.hasKeyContext();
    }

    public void activateBroadcastJoin(boolean leftIsBuild) {
        isBroadcast = true;
        this.leftIsBuild = leftIsBuild;
    }
}
