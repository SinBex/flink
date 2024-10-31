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

package org.apache.flink.table.runtime.operators.join.adaptive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AdaptiveBroadcastJoin;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.planner.loader.PlannerModule;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * Adaptive broadcast join factory.
 *
 * <p>Note: This class will hold an {@link AdaptiveBroadcastJoin} and serve as a proxy class to
 * provide an interface externally. Due to runtime access visibility constraints with the
 * table-planner module, the {@link AdaptiveBroadcastJoin} object will be serialized during the
 * Table Planner phase and will only be lazily deserialized before the dynamic generation of the
 * JobGraph.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public class AdaptiveBroadcastJoinOperatorFactory<OUT> extends AbstractStreamOperatorFactory<OUT>
        implements AdaptiveBroadcastJoin {
    private static final long serialVersionUID = 1L;

    private final byte[] adaptiveJoinSerialized;

    private transient AdaptiveBroadcastJoin adaptiveBroadcastJoin;

    private StreamOperatorFactory<OUT> finalFactory;

    public AdaptiveBroadcastJoinOperatorFactory(byte[] adaptiveJoinSerialized) {
        this.adaptiveJoinSerialized = adaptiveJoinSerialized;
    }

    @Override
    public StreamOperatorFactory<?> genOperatorFactory(
            ClassLoader classLoader, ReadableConfig config) {
        checkAndLazyInitialize();
        this.finalFactory =
                (StreamOperatorFactory<OUT>)
                        adaptiveBroadcastJoin.genOperatorFactory(classLoader, config);
        return this.finalFactory;
    }

    @Override
    public void markActualBuildSide(int side, boolean isBroadcast) {
        checkAndLazyInitialize();
        this.adaptiveBroadcastJoin.markActualBuildSide(side, isBroadcast);
    }

    @Override
    public boolean canBeBuildSide(int side) {
        checkAndLazyInitialize();
        return this.adaptiveBroadcastJoin.canBeBuildSide(side);
    }

    private void checkAndLazyInitialize() {
        if (this.adaptiveBroadcastJoin == null) {
            lazyInitialize();
        }
    }

    @Override
    public <T extends StreamOperator<OUT>> T createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        if (finalFactory instanceof AbstractStreamOperatorFactory) {
            ((AbstractStreamOperatorFactory<OUT>) finalFactory)
                    .setProcessingTimeService(processingTimeService);
        }
        StreamOperator<OUT> operator = finalFactory.createStreamOperator(parameters);
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return finalFactory.getStreamOperatorClass(classLoader);
    }

    private void lazyInitialize() {
        ClassLoader[] classLoaders =
                new ClassLoader[] {
                    Thread.currentThread().getContextClassLoader(),
                    PlannerModule.getInstance().getSubmoduleClassLoader()
                };

        for (ClassLoader classLoader : classLoaders) {
            try {
                this.adaptiveBroadcastJoin =
                        InstantiationUtil.deserializeObject(adaptiveJoinSerialized, classLoader);
                return;
            } catch (ClassNotFoundException | IOException e) {
                if (classLoader != classLoaders[classLoaders.length - 1]) {
                    continue;
                }
                throw new RuntimeException(
                        "Failed to deserialize object with all available class loaders.", e);
            }
        }
    }
}
