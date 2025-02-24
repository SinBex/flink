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

package org.apache.flink.runtime.throughput;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test for {@link BufferSizeEMA}. */
class BufferSizeEMATest {

    @Test
    void testCalculationBufferSize() {
        BufferSizeEMA calculator = new BufferSizeEMA(200, 10, 3);

        // The result value seeks to the bottom limit but it will take a while until it reaches it.
        assertThat(calculator.calculateBufferSize(111, 13)).isEqualTo(104);
        assertThat(calculator.calculateBufferSize(107, 7)).isEqualTo(60);
        assertThat(calculator.calculateBufferSize(107, 7)).isEqualTo(38);
        assertThat(calculator.calculateBufferSize(107, 7)).isEqualTo(26);
        assertThat(calculator.calculateBufferSize(107, 7)).isEqualTo(21);
        assertThat(calculator.calculateBufferSize(107, 7)).isEqualTo(18);
        assertThat(calculator.calculateBufferSize(107, 13)).isEqualTo(13);
        assertThat(calculator.calculateBufferSize(107, 13)).isEqualTo(11);
        assertThat(calculator.calculateBufferSize(107, 13)).isEqualTo(10);

        // Upgrade
        assertThat(calculator.calculateBufferSize(333, 1)).isEqualTo(15);
        assertThat(calculator.calculateBufferSize(333, 1)).isEqualTo(23);
        assertThat(calculator.calculateBufferSize(333, 1)).isEqualTo(34);
        assertThat(calculator.calculateBufferSize(333, 1)).isEqualTo(51);
        assertThat(calculator.calculateBufferSize(333, 1)).isEqualTo(76);
        assertThat(calculator.calculateBufferSize(333, 1)).isEqualTo(114);
    }

    /**
     * Check that even with very small alpha, buffer size doesn't get stuck and can not eventually
     * grow.
     */
    @Test
    public void testSmallBufferSizeChanges() {
        int numberOfSamples = 10000;
        int startingBufferSize = 16;
        BufferSizeEMA calculator =
                new BufferSizeEMA(startingBufferSize, 32 * 1024, 16, numberOfSamples);

        for (int i = 0; i < numberOfSamples; i++) {
            calculator.calculateBufferSize(20000, 10);
        }

        assertThat(calculator.calculateBufferSize(10000, 10)).isGreaterThan(startingBufferSize);
    }

    @Test
    void testSizeGreaterThanMaxSize() {
        BufferSizeEMA calculator = new BufferSizeEMA(200, 10, 3);

        // Decrease value to less than max.
        assertThat(calculator.calculateBufferSize(0, 1)).isEqualTo(100);

        // Impossible to exceed maximum.
        assertThat(calculator.calculateBufferSize(1000, 1)).isEqualTo(150);
        assertThat(calculator.calculateBufferSize(1000, 1)).isEqualTo(200);
        assertThat(calculator.calculateBufferSize(1000, 1)).isEqualTo(200);
    }

    @Test
    void testSizeLessThanMinSize() {
        BufferSizeEMA calculator = new BufferSizeEMA(200, 10, 3);

        // Impossible to less than min.
        assertThat(calculator.calculateBufferSize(0, 1)).isEqualTo(100);
        assertThat(calculator.calculateBufferSize(0, 1)).isEqualTo(50);
        assertThat(calculator.calculateBufferSize(0, 1)).isEqualTo(25);
        assertThat(calculator.calculateBufferSize(0, 1)).isEqualTo(13);
        assertThat(calculator.calculateBufferSize(0, 1)).isEqualTo(10);
        assertThat(calculator.calculateBufferSize(0, 1)).isEqualTo(10);
    }

    @Test
    void testNegativeTotalSize() {
        BufferSizeEMA calculator = new BufferSizeEMA(100, 200, 2);
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> calculator.calculateBufferSize(-1, 1));
    }

    @Test
    void testZeroBuffers() {
        BufferSizeEMA calculator = new BufferSizeEMA(100, 200, 2);
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> calculator.calculateBufferSize(1, 0));
    }
}
