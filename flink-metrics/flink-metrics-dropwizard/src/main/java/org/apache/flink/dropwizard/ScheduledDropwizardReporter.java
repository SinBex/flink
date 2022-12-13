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

package org.apache.flink.dropwizard;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.dropwizard.metrics.FlinkCounterWrapper;
import org.apache.flink.dropwizard.metrics.FlinkGaugeWrapper;
import org.apache.flink.dropwizard.metrics.FlinkHistogramWrapper;
import org.apache.flink.dropwizard.metrics.FlinkMeterWrapper;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.ScheduledReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * Base class for {@link org.apache.flink.metrics.reporter.MetricReporter} that wraps a Dropwizard
 * {@link com.codahale.metrics.Reporter}.
 */
@PublicEvolving
public abstract class ScheduledDropwizardReporter
        implements MetricReporter, Scheduled, Reporter, CharacterFilter {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public static final String ARG_HOST = "host";
    public static final String ARG_PORT = "port";
    public static final String ARG_PREFIX = "prefix";
    public static final String ARG_CONVERSION_RATE = "rateConversion";
    public static final String ARG_CONVERSION_DURATION = "durationConversion";

    // ------------------------------------------------------------------------

    protected final MetricRegistry registry;

    protected ScheduledReporter reporter;

    private final Map<String, Gauge<?>> gauges = new HashMap<>();
    private final Map<String, Counter> counters = new HashMap<>();
    private final Map<String, Histogram> histograms = new HashMap<>();
    private final Map<String, Meter> meters = new HashMap<>();

    // ------------------------------------------------------------------------

    protected ScheduledDropwizardReporter() {
        this.registry = new MetricRegistry();
    }

    // ------------------------------------------------------------------------
    //  Getters
    // ------------------------------------------------------------------------

    @VisibleForTesting
    Map<String, Counter> getCounters() {
        return counters;
    }

    @VisibleForTesting
    Map<String, Meter> getMeters() {
        return meters;
    }

    @VisibleForTesting
    Map<String, Gauge<?>> getGauges() {
        return gauges;
    }

    @VisibleForTesting
    Map<String, Histogram> getHistograms() {
        return histograms;
    }

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    @Override
    public void open(MetricConfig config) {
        this.reporter = getReporter(config);
    }

    @Override
    public void close() {
        this.reporter.stop();
    }

    // ------------------------------------------------------------------------
    //  adding / removing metrics
    // ------------------------------------------------------------------------

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        final String fullName = group.getMetricIdentifier(metricName, this);

        synchronized (this) {
            switch (metric.getMetricType()) {
                case COUNTER:
                    counters.put(fullName, (Counter) metric);
                    registry.register(fullName, new FlinkCounterWrapper((Counter) metric));
                    break;
                case GAUGE:
                    gauges.put(fullName, (Gauge<?>) metric);
                    registry.register(fullName, FlinkGaugeWrapper.fromGauge((Gauge<?>) metric));
                    break;
                case HISTOGRAM:
                    Histogram histogram = (Histogram) metric;
                    histograms.put(fullName, histogram);

                    if (histogram instanceof DropwizardHistogramWrapper) {
                        registry.register(
                                fullName,
                                ((DropwizardHistogramWrapper) histogram).getDropwizardHistogram());
                    } else {
                        registry.register(fullName, new FlinkHistogramWrapper(histogram));
                    }
                    break;
                case METER:
                    Meter meter = (Meter) metric;
                    meters.put(fullName, meter);

                    if (meter instanceof DropwizardMeterWrapper) {
                        registry.register(
                                fullName, ((DropwizardMeterWrapper) meter).getDropwizardMeter());
                    } else {
                        registry.register(fullName, new FlinkMeterWrapper(meter));
                    }
                    break;
                default:
                    log.warn(
                            "Cannot add metric of type {}. This indicates that the reporter "
                                    + "does not support this metric type.",
                            metric.getClass().getName());
            }
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        final String fullName = group.getMetricIdentifier(metricName, this);

        synchronized (this) {
            switch (metric.getMetricType()) {
                case COUNTER:
                    counters.remove(fullName);
                    break;
                case GAUGE:
                    gauges.remove(fullName);
                    break;
                case HISTOGRAM:
                    histograms.remove(fullName);
                    break;
                case METER:
                    meters.remove(fullName);
                    break;
                default:
                    log.warn(
                            "Cannot remove metric of type {}. This indicates that the reporter "
                                    + "does not support this metric type.",
                            metric.getClass().getName());
            }

            boolean metricRemoved = registry.remove(fullName);
            if (!metricRemoved) {
                log.warn(
                        "Unregister metric {} of type {} failed.",
                        fullName,
                        metric.getClass().getName());
            }
        }
    }

    @Override
    public String filterCharacters(String metricName) {
        char[] chars = null;
        final int strLen = metricName.length();
        int pos = 0;

        for (int i = 0; i < strLen; i++) {
            final char c = metricName.charAt(i);
            switch (c) {
                case '.':
                    if (chars == null) {
                        chars = metricName.toCharArray();
                    }
                    chars[pos++] = '-';
                    break;
                case '"':
                    if (chars == null) {
                        chars = metricName.toCharArray();
                    }
                    break;

                default:
                    if (chars != null) {
                        chars[pos] = c;
                    }
                    pos++;
            }
        }

        return chars == null ? metricName : new String(chars, 0, pos);
    }

    // ------------------------------------------------------------------------
    //  scheduled reporting
    // ------------------------------------------------------------------------

    @Override
    public void report() {
        // we do not need to lock here, because the dropwizard registry is
        // internally a concurrent map
        @SuppressWarnings("rawtypes")
        final SortedMap<String, com.codahale.metrics.Gauge> gauges = registry.getGauges();
        final SortedMap<String, com.codahale.metrics.Counter> counters = registry.getCounters();
        final SortedMap<String, com.codahale.metrics.Histogram> histograms =
                registry.getHistograms();
        final SortedMap<String, com.codahale.metrics.Meter> meters = registry.getMeters();
        final SortedMap<String, com.codahale.metrics.Timer> timers = registry.getTimers();

        this.reporter.report(gauges, counters, histograms, meters, timers);
    }

    public abstract ScheduledReporter getReporter(MetricConfig config);
}
