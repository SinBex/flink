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

package org.apache.flink.metrics.influxdb;

import java.util.Map;
import java.util.Objects;

final class MeasurementInfo {
    private final String name;
    private final Map<String, String> tags;

    MeasurementInfo(String name, Map<String, String> tags) {
        this.name = name;
        this.tags = tags;
    }

    String getName() {
        return name;
    }

    Map<String, String> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MeasurementInfo that = (MeasurementInfo) o;
        return name.equals(that.name) && tags.equals(that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tags);
    }
}
