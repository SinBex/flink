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
package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecAdaptiveJoin
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.runtime.operators.join.HashJoinType

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.Util

/** Batch physical RelNode for adaptive [[Join]]. */
class BatchPhysicalAdaptiveJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType,
    // true if LHS is build side, else false
    var leftIsBuild: Boolean,
    // true if build side is broadcast, else false
    val isBroadcast: Boolean,
    val tryDistinctBuildRow: Boolean,
    val originalJobType: Integer,
    val maybeBroadcastJoinSide: Integer)
  extends BatchPhysicalJoinBase(cluster, traitSet, leftRel, rightRel, condition, joinType) {

  val hashJoinType: HashJoinType = HashJoinType.of(
    leftIsBuild,
    getJoinType.generatesNullsOnRight(),
    getJoinType.generatesNullsOnLeft(),
    getJoinType == JoinRelType.SEMI,
    getJoinType == JoinRelType.ANTI)

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new BatchPhysicalAdaptiveJoin(
      cluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType,
      leftIsBuild,
      isBroadcast,
      tryDistinctBuildRow,
      originalJobType,
      maybeBroadcastJoinSide)
  }

  override def translateToExecNode(): ExecNode[_] = {

    val mq = getCluster.getMetadataQuery
    val leftRowSize = Util.first(mq.getAverageRowSize(left), 24).toInt
    val leftRowCount = Util.first(mq.getRowCount(left), 200000).toLong
    val rightRowSize = Util.first(mq.getAverageRowSize(right), 24).toInt
    val rightRowCount = Util.first(mq.getRowCount(right), 200000).toLong
    val (leftEdge, rightEdge) = getInputProperties
    if (originalJobType == 1) {
      leftIsBuild = estimateOutputSize(getLeft) < estimateOutputSize(getRight);
    }
    new BatchExecAdaptiveJoin(
      unwrapTableConfig(this),
      joinSpec,
      leftRowSize,
      rightRowSize,
      leftRowCount,
      rightRowCount,
      isBroadcast,
      leftIsBuild,
      tryDistinctBuildRow,
      leftEdge,
      rightEdge,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription,
      originalJobType,
      maybeBroadcastJoinSide)
  }

  private def estimateOutputSize(relNode: RelNode): Double = {
    val mq = relNode.getCluster.getMetadataQuery
    mq.getAverageRowSize(relNode) * mq.getRowCount(relNode)
  }

  private def getInputProperties: (InputProperty, InputProperty) = {
    val (buildRequiredDistribution, probeRequiredDistribution) = if (isBroadcast) {
      (InputProperty.BROADCAST_DISTRIBUTION, InputProperty.ANY_DISTRIBUTION)
    } else {
      val leftKeys = joinSpec.getLeftKeys
      val rightKeys = joinSpec.getRightKeys
      val (buildKeys, probeKeys) = if (leftIsBuild) (leftKeys, rightKeys) else (rightKeys, leftKeys)
      (InputProperty.hashDistribution(buildKeys), InputProperty.hashDistribution(probeKeys))
    }
    val probeDamBehavior = if (hashJoinType.buildLeftSemiOrAnti()) {
      InputProperty.DamBehavior.END_INPUT
    } else {
      InputProperty.DamBehavior.PIPELINED
    }
    val buildEdge = InputProperty
      .builder()
      .requiredDistribution(buildRequiredDistribution)
      .damBehavior(InputProperty.DamBehavior.BLOCKING)
      .priority(0)
      .build()
    val probeEdge = InputProperty
      .builder()
      .requiredDistribution(probeRequiredDistribution)
      .damBehavior(probeDamBehavior)
      .priority(1)
      .build()

    if (leftIsBuild) {
      (buildEdge, probeEdge)
    } else {
      (probeEdge, buildEdge)
    }
  }
}
