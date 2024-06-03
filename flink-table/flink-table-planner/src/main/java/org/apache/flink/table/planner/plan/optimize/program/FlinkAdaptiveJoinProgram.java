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

package org.apache.flink.table.planner.plan.optimize.program;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalAdaptiveJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalHashJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortMergeJoin;
import org.apache.flink.table.planner.plan.utils.DefaultRelShuttle;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.planner.utils.TableConfigUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/** Planner program that tries to inject adaptive join operator. */
public class FlinkAdaptiveJoinProgram implements FlinkOptimizeProgram<BatchOptimizeContext> {

    @Override
    public RelNode optimize(RelNode root, BatchOptimizeContext context) {
        if (!isAdaptiveJoinEnabled(root)) {
            return root;
        }

        DefaultRelShuttle shuttle =
                new DefaultRelShuttle() {
                    @Override
                    public RelNode visit(RelNode rel) {
                        if (!(rel instanceof Join)) {
                            List<RelNode> newInputs = new ArrayList<>();
                            for (RelNode input : rel.getInputs()) {
                                RelNode newInput = input.accept(this);
                                newInputs.add(newInput);
                            }
                            return rel.copy(rel.getTraitSet(), newInputs);
                        }

                        Join join = (Join) rel;
                        RelNode newLeft = join.getLeft().accept(this);
                        RelNode newRight = join.getRight().accept(this);

                        return tryInjectAdaptiveJoin(
                                join.copy(join.getTraitSet(), Arrays.asList(newLeft, newRight)));
                    }
                };
        return shuttle.visit(root);
    }

    private Join tryInjectAdaptiveJoin(Join join) {
        if (!join.getHints().isEmpty()) {
            return join;
        }
        int maybeBroadcastJoinSide = -1;
        switch (join.getJoinType()) {
            case FULL:
                maybeBroadcastJoinSide = -1;
                break;
            case RIGHT:
                maybeBroadcastJoinSide = 0;
                break;
            case LEFT:
            case ANTI:
            case SEMI:
                maybeBroadcastJoinSide = 1;
                break;
            case INNER:
                maybeBroadcastJoinSide = 2;
                break;
            default:
        }
        if (join instanceof BatchPhysicalHashJoin) {
            BatchPhysicalHashJoin hashJoin = (BatchPhysicalHashJoin) join;
            join =
                    new BatchPhysicalAdaptiveJoin(
                            hashJoin.getCluster(),
                            hashJoin.getTraitSet(),
                            hashJoin.getLeft(),
                            hashJoin.getRight(),
                            hashJoin.getCondition(),
                            hashJoin.getJoinType(),
                            hashJoin.leftIsBuild(),
                            hashJoin.isBroadcast(),
                            hashJoin.tryDistinctBuildRow(),
                            0,
                            maybeBroadcastJoinSide);
        } else if (join instanceof BatchPhysicalSortMergeJoin) {
            BatchPhysicalSortMergeJoin sortMergeJoin = (BatchPhysicalSortMergeJoin) join;
            join =
                    new BatchPhysicalAdaptiveJoin(
                            sortMergeJoin.getCluster(),
                            sortMergeJoin.getTraitSet(),
                            sortMergeJoin.getLeft(),
                            sortMergeJoin.getRight(),
                            sortMergeJoin.getCondition(),
                            sortMergeJoin.getJoinType(),
                            false,
                            false,
                            false,
                            1,
                            maybeBroadcastJoinSide);
        }
        return join;
    }

    private static boolean isAdaptiveJoinEnabled(RelNode relNode) {
        TableConfig tableConfig = unwrapTableConfig(relNode);
        return tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_JOIN_ENABLED)
                && !TableConfigUtils.isOperatorDisabled(
                        tableConfig, OperatorType.BroadcastHashJoin);
    }
}
