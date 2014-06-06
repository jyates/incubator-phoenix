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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.ExposedMetricCounterLong;
import org.apache.hadoop.metrics2.impl.ExposedMetricsRecordImpl;
import org.apache.hadoop.metrics2.lib.ExposedMetricsInfoImpl;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.metrics.MetricInfo;
import org.apache.phoenix.metrics.Metrics;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Test that the logging sink stores the expected metrics/stats
 */
public class PhoenixTableMetricsWriterTest extends BaseHBaseManagedTimeIT {

    private static final Log LOG = LogFactory.getLog(PhoenixTableMetricsWriterTest.class);

    /**
     * IT should create the target table if it hasn't been created yet, but not fail if the table
     * has already been created
     * @throws Exception on failure
     */
    @Test
    public void testCreatesTable() throws Exception {
        PhoenixTableMetricsWriter sink = new PhoenixTableMetricsWriter();
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        sink.initForTesting(conn);
        // check for existence of the logging table
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs =
                dbmd.getTables(null, null, null, new String[] { PTableType.TABLE.toString(),
                        PTableType.VIEW.toString() });
        assertTrue("Didn't create default tracing table", rs.next());
        assertEquals(
            "Default logging table wasn't created!",
            TracingCompat.DEFAULT_STATS_TABLE_NAME,
            SchemaUtil.getTableName(rs.getString(PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA),
                rs.getString(PhoenixDatabaseMetaData.TABLE_NAME)));

        // initialize sink again, which should attempt to create the table, but not fail
        try {
            sink.initForTesting(conn);
        } catch (Exception e) {
            fail("Initialization shouldn't fail if table already exists!");
        }
    }

    /**
     * Simple metrics writing and reading check, that uses the standard wrapping in the
     * {@link PhoenixMetricsWriter}
     * @throws Exception on failure
     */
    @Test
    public void writeMetrics() throws Exception {
        PhoenixTableMetricsWriter sink = new PhoenixTableMetricsWriter();
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        sink.initForTesting(conn);

        // create a simple metrics record
        long traceid = 987654;
        MetricsInfo info =
                new ExposedMetricsInfoImpl(TracingCompat.METRIC_SOURCE_KEY, Long.toString(traceid));
        // setup some metrics for the span
        long spanid = 10;
        AbstractMetric span =
                new ExposedMetricCounterLong(new ExposedMetricsInfoImpl(MetricInfo.SPAN.traceName,
                        ""), spanid);
        long parentid = 11;
        AbstractMetric parent =
                new ExposedMetricCounterLong(new ExposedMetricsInfoImpl(
                        MetricInfo.PARENT.traceName, ""), parentid);
        long startTime = 12;
        AbstractMetric start =
                new ExposedMetricCounterLong(new ExposedMetricsInfoImpl(MetricInfo.START.traceName,
                        ""), startTime);
        long endTime = 13;
        AbstractMetric end =
                new ExposedMetricCounterLong(new ExposedMetricsInfoImpl(MetricInfo.END.traceName,
                        ""), endTime);
        List<AbstractMetric> metrics = Lists.newArrayList(span, parent, start, end);

        // create an annotation as well
        String annotation = "test annotation for a span";
        MetricsTag tag =
                new MetricsTag(new ExposedMetricsInfoImpl(MetricInfo.ANNOTATION.traceName, "0"),
                        annotation);
        String hostnameValue = "host-name.value";
        MetricsTag hostname =
                new MetricsTag(new ExposedMetricsInfoImpl(MetricInfo.HOSTNAME.traceName, ""),
                        hostnameValue);
        List<MetricsTag> tags = Lists.newArrayList(hostname, tag);

        MetricsRecord record =
                new ExposedMetricsRecordImpl(info, System.currentTimeMillis(), tags, metrics);

        // create a generic sink
        PhoenixMetricsWriter writer = new PhoenixMetricsWriter();
        writer.setWriterForTesting(sink);
        writer.putMetrics(record);
        writer.flush();

        // scan the table and make sure we have the expected data and tags
        // hostname is handled separately, so we pull it out
        tags.remove(0);
        String tagsCol = "TAGS" + MetricInfo.ANNOTATION.columnName + "0 VARCHAR";

        // make sure we only get expected stat entry (matcing the trace id), otherwise we could the
        // stats for the update as well
        String query =
                "SELECT * FROM " + TracingCompat.DEFAULT_STATS_TABLE_NAME + "(" + tagsCol
                        + ") WHERE " + MetricInfo.TRACE.columnName + "=" + traceid;
        ResultSet results = conn.createStatement().executeQuery(query);
        assertTrue("No results found", results.next());
        int index = 1;
        assertEquals("Wrong trace stored", traceid, results.getLong(index++));
        assertEquals("Wrong parent stored", parentid, results.getLong(index++));
        assertEquals("Wrong span stored", spanid, results.getLong(index++));
        assertEquals("Wrong start stored", startTime, results.getLong(index++));
        assertEquals("Wrong end stored", endTime, results.getLong(index++));
        assertEquals("Didn't store correct hostname value", hostnameValue,
            results.getString(index++));
        assertEquals("Wrong number of tags stored", tags.size(), results.getInt(index++));
        for (MetricsTag t : tags) {
            assertEquals("Didn't store correct tag value", t.value(), results.getString(index++));
        }
    }

    /**
     * Test that span will actually go into the this sink and be written
     * @throws Exception
     */
    @Test
    public void testIsEnabledForLogging() throws Exception {
        // setup the sink
        PhoenixTableMetricsWriter sink = new PhoenixTableMetricsWriter();
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        // one call for creating the table, one call for writing the update
        final CountDownLatch updated = new CountDownLatch(2);
        Connection countable = new DelegatingConnection(conn) {

            @Override
            public PreparedStatement prepareStatement(String sql) throws SQLException {
                updated.countDown();
                return super.prepareStatement(sql);
            }

        };
        sink.initForTesting(countable);

        // register the sink with the metrics system, so we don't need to specify it in the conf
        Metrics.getManager().register("phoenix", "test sink gets logged", sink);

        // create a dummy table
        String table = "ENABLED_FOR_LOGGING";
        String ddl =
                "create table if not exists " + table + "(" + "k varchar not null, " + "c1 bigint"
                        + " CONSTRAINT pk PRIMARY KEY (k))";
        conn.createStatement().execute(ddl);

        LOG.debug("Doing dummy the writes to the tracked table");
        String insert = "UPSERT INTO " + table + " VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(insert);
        stmt.setString(1, "key1");
        stmt.setLong(2, 1);
        stmt.execute();
        stmt.setString(1, "key2");
        stmt.setLong(2, 2);
        stmt.execute();

        // wait for the latch to countdown, as the metrics system is time-based
        LOG.debug("Waiting for latch to complete!");
        updated.await(60, TimeUnit.SECONDS);// should be way more than GC pauses on the server.
    }
}