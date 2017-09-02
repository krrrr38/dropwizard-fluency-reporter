package com.krrrr38.metrics.fluency;

import static com.codahale.metrics.MetricAttribute.COUNT;
import static com.codahale.metrics.MetricAttribute.M15_RATE;
import static com.codahale.metrics.MetricAttribute.M1_RATE;
import static com.codahale.metrics.MetricAttribute.M5_RATE;
import static com.codahale.metrics.MetricAttribute.MAX;
import static com.codahale.metrics.MetricAttribute.MEAN;
import static com.codahale.metrics.MetricAttribute.MEAN_RATE;
import static com.codahale.metrics.MetricAttribute.MIN;
import static com.codahale.metrics.MetricAttribute.P50;
import static com.codahale.metrics.MetricAttribute.P75;
import static com.codahale.metrics.MetricAttribute.P95;
import static com.codahale.metrics.MetricAttribute.P98;
import static com.codahale.metrics.MetricAttribute.P99;
import static com.codahale.metrics.MetricAttribute.P999;
import static com.codahale.metrics.MetricAttribute.STDDEV;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.komamitsu.fluency.Fluency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricAttribute;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * A reporter which publishes metric values to fluentd.
 *
 * @see <a href="https://www.fluentd.org/">Fluentd is an open source data collector for unified logging layer
 * .</a>
 */
public class FluencyReporter extends ScheduledReporter {
    private static final String DEFAULT_PREFIX = "metrics";

    /**
     * Returns a new {@link Builder} for {@link FluencyReporter}.
     *
     * @param registry the registry to report
     *
     * @return a {@link Builder} instance for a {@link FluencyReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link FluencyReporter} instances. Defaults to not using a tag, using the
     * default clock, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private ScheduledExecutorService executor;
        private boolean shutdownExecutorOnStop;
        private Set<MetricAttribute> disabledMetricAttributes;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = DEFAULT_PREFIX;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.executor = null;
            this.shutdownExecutorOnStop = true;
            this.disabledMetricAttributes = Collections.emptySet();
        }

        /**
         * Specifies whether or not, the executor (used for reporting) will be stopped with same time with
         * reporter.
         * Default value is true.
         * Setting this parameter to false, has the sense in combining with providing external managed
         * executor via {@link #scheduleOn(ScheduledExecutorService)}.
         *
         * @param shutdownExecutorOnStop if true, then executor will be stopped in same time with this reporter
         *
         * @return {@code this}
         */
        public Builder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
            this.shutdownExecutorOnStop = shutdownExecutorOnStop;
            return this;
        }

        /**
         * Specifies the executor to use while scheduling reporting of metrics.
         * Default value is null.
         * Null value leads to executor will be auto created on start.
         *
         * @param executor the executor to use while scheduling reporting of metrics.
         *
         * @return {@code this}
         */
        public Builder scheduleOn(ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         *
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Prefix all metric names with the given string.
         *
         * @param prefix the tag for all metric names
         *
         * @return {@code this}
         */
        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         *
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         *
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         *
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Don't report the passed metric attributes for all metrics (e.g. "p999", "stddev" or "m15").
         * See {@link MetricAttribute}.
         *
         * @param disabledMetricAttributes a {@link MetricFilter}
         *
         * @return {@code this}
         */
        public Builder disabledMetricAttributes(Set<MetricAttribute> disabledMetricAttributes) {
            this.disabledMetricAttributes = disabledMetricAttributes;
            return this;
        }

        /**
         * Builds a {@link FluencyReporter} with the given properties, sending metrics using the
         * given {@link Fluency}.
         *
         * @param fluency a {@link Fluency}
         *
         * @return a {@link FluencyReporter}
         */
        public FluencyReporter build(Fluency fluency) {
            return new FluencyReporter(
                    registry,
                    fluency,
                    clock,
                    prefix,
                    rateUnit,
                    durationUnit,
                    filter,
                    executor,
                    shutdownExecutorOnStop,
                    disabledMetricAttributes
            );
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(FluencyReporter.class);

    private final Fluency fluency;
    private final Clock clock;
    private final String prefix;

    /**
     * Creates a new {@link FluencyReporter} instance.
     *
     * @param registry the {@link MetricRegistry} containing the metrics this
     * reporter will report
     * @param fluency the {@link Fluency} which is responsible for sending metrics to a Carbon server
     * via a transport protocol
     * @param clock the instance of the time. Use {@link Clock#defaultClock()} for the default
     * @param prefix the tag of all metric names (may be null)
     * @param rateUnit the time unit of in which rates will be converted
     * @param durationUnit the time unit of in which durations will be converted
     * @param filter the filter for which metrics to report
     * @param executor the executor to use while scheduling reporting of metrics (may be null).
     * @param shutdownExecutorOnStop if true, then executor will be stopped in same time with this reporter
     */
    FluencyReporter(
            MetricRegistry registry,
            Fluency fluency,
            Clock clock,
            String prefix,
            TimeUnit rateUnit,
            TimeUnit durationUnit,
            MetricFilter filter,
            ScheduledExecutorService executor,
            boolean shutdownExecutorOnStop,
            Set<MetricAttribute> disabledMetricAttributes
    ) {
        super(registry, "fluency-reporter", filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop,
              disabledMetricAttributes);
        this.fluency = fluency;
        this.clock = clock;
        this.prefix = prefix;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void report(
            SortedMap<String, Gauge> gauges,
            SortedMap<String, Counter> counters,
            SortedMap<String, Histogram> histograms,
            SortedMap<String, Meter> meters,
            SortedMap<String, Timer> timers
    ) {
        final long timestamp = clock.getTime() / 1000;

        try {
            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                reportGauge(entry.getKey(), entry.getValue(), timestamp);
            }
            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                reportCounter(entry.getKey(), entry.getValue(), timestamp);
            }
            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                reportHistogram(entry.getKey(), entry.getValue(), timestamp);
            }
            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                reportMetered(entry.getKey(), entry.getValue(), timestamp);
            }
            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                reportTimer(entry.getKey(), entry.getValue(), timestamp);
            }
        } catch (IOException e) {
            LOGGER.warn("Unable to report to fluency: fluency={}, message={}",
                        fluency, e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        try {
            super.stop();
        } finally {
            try {
                fluency.close();
            } catch (IOException e) {
                LOGGER.debug("Error disconnecting from Fluency", fluency, e);
            }
        }
    }

    private void reportTimer(String name, Timer timer, long timestamp) throws IOException {
        final Snapshot snapshot = timer.getSnapshot();
        final Map<String, Object> data = new HashMap<String, Object>();
        registerIfEnabled(data, MAX, convertDuration(snapshot.getMax()));
        registerIfEnabled(data, MEAN, convertDuration(snapshot.getMean()));
        registerIfEnabled(data, MIN, convertDuration(snapshot.getMin()));
        registerIfEnabled(data, STDDEV, convertDuration(snapshot.getStdDev()));
        registerIfEnabled(data, P50, convertDuration(snapshot.getMedian()));
        registerIfEnabled(data, P75, convertDuration(snapshot.get75thPercentile()));
        registerIfEnabled(data, P95, convertDuration(snapshot.get95thPercentile()));
        registerIfEnabled(data, P98, convertDuration(snapshot.get98thPercentile()));
        registerIfEnabled(data, P99, convertDuration(snapshot.get99thPercentile()));
        registerIfEnabled(data, P999, convertDuration(snapshot.get999thPercentile()));
        registerIfEnabled(data, COUNT, timer.getCount());
        registerIfEnabled(data, M1_RATE, convertRate(timer.getOneMinuteRate()));
        registerIfEnabled(data, M5_RATE, convertRate(timer.getFiveMinuteRate()));
        registerIfEnabled(data, M15_RATE, convertRate(timer.getFifteenMinuteRate()));
        registerIfEnabled(data, MEAN_RATE, convertRate(timer.getMeanRate()));
        LOGGER.trace("send metrics to fluentd: name={}, data={}", name, data);
        if (!data.isEmpty()) {
            fluency.emit(tag(name), timestamp, data);
        }
    }

    private void reportMetered(String name, Metered meter, long timestamp) throws IOException {
        final Map<String, Object> data = new HashMap<String, Object>();
        registerIfEnabled(data, COUNT, meter.getCount());
        registerIfEnabled(data, M1_RATE, convertRate(meter.getOneMinuteRate()));
        registerIfEnabled(data, M5_RATE, convertRate(meter.getFiveMinuteRate()));
        registerIfEnabled(data, M15_RATE, convertRate(meter.getFifteenMinuteRate()));
        registerIfEnabled(data, MEAN_RATE, convertRate(meter.getMeanRate()));
        LOGGER.trace("send metrics to fluentd: name={}, data={}", name, data);
        if (!data.isEmpty()) {
            fluency.emit(tag(name), timestamp, data);
        }
    }

    private void reportHistogram(String name, Histogram histogram, long timestamp) throws IOException {
        final Snapshot snapshot = histogram.getSnapshot();
        final Map<String, Object> data = new HashMap<String, Object>();
        registerIfEnabled(data, COUNT, histogram.getCount());
        registerIfEnabled(data, MAX, snapshot.getMax());
        registerIfEnabled(data, MEAN, snapshot.getMean());
        registerIfEnabled(data, MIN, snapshot.getMin());
        registerIfEnabled(data, STDDEV, snapshot.getStdDev());
        registerIfEnabled(data, P50, snapshot.getMedian());
        registerIfEnabled(data, P75, snapshot.get75thPercentile());
        registerIfEnabled(data, P95, snapshot.get95thPercentile());
        registerIfEnabled(data, P98, snapshot.get98thPercentile());
        registerIfEnabled(data, P99, snapshot.get99thPercentile());
        registerIfEnabled(data, P999, snapshot.get999thPercentile());
        LOGGER.trace("send metrics to fluentd: name={}, data={}", name, data);
        if (!data.isEmpty()) {
            fluency.emit(tag(name), timestamp, data);
        }
    }

    private void reportCounter(String name, Counter counter, long timestamp) throws IOException {
        final Map<String, Object> data = new HashMap<String, Object>(1);
        registerIfEnabled(data, COUNT, counter.getCount());
        LOGGER.trace("send metrics to fluentd: name={}, data={}", name, data);
        if (!data.isEmpty()) {
            fluency.emit(tag(name), timestamp, data);
        }
    }

    private void reportGauge(String name, Gauge<?> gauge, long timestamp) throws IOException {
        final Object value = gauge.getValue();
        if (value == null) {
            return;
        }
        final Map<String, Object> data = new HashMap<String, Object>(1);
        data.put(COUNT.getCode(), value);
        LOGGER.trace("send metrics to fluentd: name={}, data={}", name, data);
        fluency.emit(tag(name), timestamp, data);
    }

    private void registerIfEnabled(
            Map<String, Object> data, MetricAttribute type, double value
    ) {
        if (getDisabledMetricAttributes().contains(type)) {
            return;
        }
        data.put(type.getCode(), value);
    }

    private void registerIfEnabled(
            Map<String, Object> data, MetricAttribute type, long value
    ) {
        if (getDisabledMetricAttributes().contains(type)) {
            return;
        }
        data.put(type.getCode(), value);
    }

    private String tag(String... components) {
        return MetricRegistry.name(prefix, components);
    }
}
