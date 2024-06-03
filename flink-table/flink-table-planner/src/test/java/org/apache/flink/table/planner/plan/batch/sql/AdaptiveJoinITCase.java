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

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

/** Plan test for dynamic filtering. */
@ExtendWith(ParameterizedTestExtension.class)
class AdaptiveJoinITCase extends TableTestBase {

    TableEnvironment env;

    @BeforeEach
    void before() {
        Configuration config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 10);
        config.set(ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_BLOCKING);
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        config.set(OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_JOIN_ENABLED, true);

        env = TableEnvironment.create(config);

        // prepare data
        List<Row> data1 = new ArrayList<>();
        data1.addAll(getRepeatedRow(2, 100));
        data1.addAll(getRepeatedRow(5, 100));
        data1.addAll(getRepeatedRow(10, 100));
        String dataId1 = TestValuesTableFactory.registerData(data1);

        List<Row> data2 = new ArrayList<>();
        data2.addAll(getRepeatedRow(5, 10));
        data2.addAll(getRepeatedRow(10, 10));
        data2.addAll(getRepeatedRow(20, 10));
        String dataId2 = TestValuesTableFactory.registerData(data2);

        env.executeSql(
                String.format(
                        "CREATE TABLE t1 (\n"
                                + "  x INT,\n"
                                + "  y BIGINT,\n"
                                + "  z VARCHAR\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId1));

        env.executeSql(
                String.format(
                        "CREATE TABLE t2 (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c VARCHAR\n"
                                + ")  WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n"
                                + " 'bounded' = 'true'\n"
                                + ")",
                        dataId2));

        env.executeSql(
                "CREATE TABLE sink (\n"
                        + "  x INT,\n"
                        + "  z VARCHAR,\n"
                        + "  a INT,\n"
                        + "  b BIGINT,\n"
                        + "  c VARCHAR\n"
                        + ")  WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")");
    }

    @AfterEach
    void after() {
        TestValuesTableFactory.clearAllData();
    }

    @Test
    void testHashJoin() throws Exception {
        env.executeSql("SELECT /*+ SHUFFLE_HASH(t1) */ x, z, a, b, c FROM t1 JOIN t2 ON t1.x=t2.a")
                .print();
    }

    @Test
    void testBroadcast() throws Exception {
        env.executeSql("SELECT /*+ BROADCAST(t2) */ x, z, a, b, c FROM t1 JOIN t2 ON t1.x=t2.a")
                .print();
    }

    @Test
    void testSortMerge() throws Exception {
        env.executeSql("SELECT /*+ SHUFFLE_MERGE(t1) */ x, z, a, b, c FROM t1 JOIN t2 ON t1.x=t2.a")
                .print();
    }

    private List<Row> getRepeatedRow(int key, int nums) {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < nums; i++) {
            rows.add(Row.of(key, (long) key, String.valueOf(key)));
        }
        return rows;
    }
}
