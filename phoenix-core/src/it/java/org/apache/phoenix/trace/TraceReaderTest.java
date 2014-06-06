/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.trace;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.ExposedMetricCounterLong;
import org.apache.hadoop.metrics2.impl.ExposedMetricsRecordImpl;
import org.cloudera.htrace.Span;
import org.junit.Test;

import com.google.common.collect.Lists;

import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.metrics.MetricInfo;
import org.apache.phoenix.metrics.MetricsWriter;
import org.apache.phoenix.trace.TraceReader.SpanInfo;
import org.apache.phoenix.trace.TraceReader.TraceHolder;

/**
 * Test that the {@link TraceReader} will correctly read traces written by the
 * {@link PhoenixTableMetricsWriter}
 */
public class TraceReaderTest extends BaseHBaseManagedTimeIT {

    private static final Log LOG = LogFactory.getLog(TraceReaderTest.class);

    @Test
    public void singleSpan() throws Exception {
        PhoenixTableMetricsWriter sink = new PhoenixTableMetricsWriter();
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        sink.initForTesting(conn);

        // create a simple metrics record
        MetricsRecord record =
                createRecord(987654, Span.ROOT_SPAN_ID, 10, "root", 12, 13, "host-name.value",
                    "test annotation for a span");
        putAndFlush(sink, record);

        // start a reader
        TraceReader reader = new TraceReader(conn);
        Collection<TraceHolder> traces = reader.readAll(1);
        assertEquals("Got an unexpected number of traces!", 1, traces.size());
        // make sure the trace matches what we wrote
        TraceHolder trace = traces.iterator().next();
        validateTrace(Arrays.asList(record), trace);
    }

    private void putAndFlush(MetricsWriter sink, MetricsRecord record) {
        PhoenixMetricsWriter writer = new PhoenixMetricsWriter();
        writer.setWriterForTesting(sink);
        writer.putMetrics(record);
        writer.flush();
    }

    /**
     * Test multiple spans, within the same trace. Some spans are independent of the parent span,
     * some are child spans
     * @throws Exception
     */
    @Test
    public void testMultipleSpans() throws Exception {
        PhoenixTableMetricsWriter sink = new PhoenixTableMetricsWriter();
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        sink.initForTesting(conn);

        // create a simple metrics record
        long traceid = 12345;
        List<MetricsRecord> records = new ArrayList<MetricsRecord>();
        MetricsRecord record =
                createRecord(traceid, Span.ROOT_SPAN_ID, 7777, "root", 10, 30, "hostname.value",
                    "root-span tag");
        records.add(record);
        putAndFlush(sink, record);

        // then create a child record
        record = createRecord(traceid, 7777, 6666, "c1", 11, 15, "hostname.value", "first child");
        records.add(record);
        putAndFlush(sink, record);

        // create a different child
        record = createRecord(traceid, 7777, 5555, "c2", 11, 18, "hostname.value", "second child");
        records.add(record);
        putAndFlush(sink, record);

        // create a child of the second child
        record = createRecord(traceid, 5555, 4444, "c3", 12, 16, "hostname.value", "third child");
        records.add(record);
        putAndFlush(sink, record);

        // flush all the values to the table
        sink.flush();

        // start a reader
        TraceReader reader = new TraceReader(conn);
        Collection<TraceHolder> traces = reader.readAll(1);
        assertEquals("Got an unexpected number of traces!", 1, traces.size());
        // make sure the trace matches what we wrote
        TraceHolder trace = traces.iterator().next();
        assertEquals("Got an unexpected traceid", traceid, trace.traceid);
        assertEquals("Got an unexpected number of spans", 4, trace.spans.size());

        validateTrace(records, trace);
    }

    /**
     * @param records
     * @param trace
     */
    private void validateTrace(List<MetricsRecord> records, TraceHolder trace) {
        // drop each span into a sorted list so we get the expected ordering
        Iterator<SpanInfo> spanIter = trace.spans.iterator();
        for (MetricsRecord r : records) {
            SpanInfo spanInfo = spanIter.next();
            LOG.info("Checking span:\n" + spanInfo);
            Iterator<AbstractMetric> metricIter = r.metrics().iterator();
            assertEquals("Got an unexpected span id", metricIter.next().value(), spanInfo.id);
            long parentId = (Long) metricIter.next().value();
            if (parentId == Span.ROOT_SPAN_ID) {
                assertNull("Got a parent, but it was a root span!", spanInfo.parent);
            } else {
                assertEquals("Got an unexpected parent span id", parentId, spanInfo.parent.id);
            }
            assertEquals("Got an unexpected start time", metricIter.next().value(), spanInfo.start);
            assertEquals("Got an unexpected end time", metricIter.next().value(), spanInfo.end);

            // validate the tags as well
            Iterator<MetricsTag> tags = r.tags().iterator();
            assertEquals("Didn't store correct hostname value", tags.next().value(),
                spanInfo.hostname);
            int annotationCount = 0;
            while (tags.hasNext()) {
                assertEquals("Didn't get expected annotation", tags.next().value(),
                    spanInfo.tags.get(annotationCount++));
            }
            assertEquals("Didn't get expected number of annotations", annotationCount,
                spanInfo.tagCount);
        }
    }

    private MetricsRecord createRecord(long traceid, long parentid, long spanid, String desc,
            long startTime, long endTime, String hostname, String... tags) {
        MetricsInfo info = new MetricsInfoImpl(TracingCompat.METRIC_SOURCE_KEY + traceid, desc);
        AbstractMetric span =
                new ExposedMetricCounterLong(new MetricsInfoImpl(MetricInfo.SPAN.traceName, ""),
                        spanid);
        AbstractMetric parent =
                new ExposedMetricCounterLong(new MetricsInfoImpl(MetricInfo.PARENT.traceName, ""),
                        parentid);
        AbstractMetric start =
                new ExposedMetricCounterLong(new MetricsInfoImpl(MetricInfo.START.traceName, ""),
                        startTime);
        AbstractMetric end =
                new ExposedMetricCounterLong(new MetricsInfoImpl(MetricInfo.END.traceName, ""),
                        endTime);
        List<AbstractMetric> metrics = Lists.newArrayList(span, parent, start, end);

        // create an annotation as well
        List<MetricsTag> annotations = new ArrayList<MetricsTag>(tags.length + 1);
        MetricsTag hostnameTag =
                new MetricsTag(new MetricsInfoImpl(MetricInfo.HOSTNAME.traceName, ""), hostname);
        annotations.add(hostnameTag);
        for (int i = 0; i < tags.length; i++) {
            MetricsTag tag =
                    new MetricsTag(new MetricsInfoImpl(MetricInfo.ANNOTATION.traceName, "0"),
                            tags[i]);
            annotations.add(tag);
        }

        return new ExposedMetricsRecordImpl(info, System.currentTimeMillis(), annotations, metrics);
    }
}