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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;

/**
 * Interface for implementing an adaptive broadcast join operator. This interface allows a join
 * operator to be dynamically optimized to a broadcast join during runtime if a specific input side
 * meets the conditions. If not, the join operator can revert to its original implementation.
 */
@Internal
public interface AdaptiveBroadcastJoin extends Serializable {

    /**
     * Generates a StreamOperatorFactory for this join operator using the provided ClassLoader and
     * config.
     *
     * @param classLoader the ClassLoader to be used for loading classes.
     * @param config the configuration to be applied for creating the operator factory.
     * @return a StreamOperatorFactory instance.
     */
    StreamOperatorFactory<?> genOperatorFactory(ClassLoader classLoader, ReadableConfig config);

    /**
     * Marks the actual build side of the join operation, indicating whether it can be broadcast.
     *
     * @param side the input side to be marked as the build side.
     * @param isBroadcast true if the input side should be treated as a broadcast side.
     */
    void markActualBuildSide(int side, boolean isBroadcast);

    /**
     * Determines if the input side of the join node can be optimized as a broadcast hash join.
     *
     * @param side the input side to evaluate.
     * @return true if the input side can be optimized as a broadcast hash join.
     */
    boolean canBeBuildSide(int side);
}
