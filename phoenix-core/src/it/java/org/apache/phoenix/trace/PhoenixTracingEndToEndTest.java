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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.metrics.Metrics;
import org.apache.phoenix.metrics.MetricsWriter;
import org.apache.phoenix.metrics.TracingTestCompat;
import org.apache.phoenix.trace.TraceReader.SpanInfo;
import org.apache.phoenix.trace.TraceReader.TraceHolder;
import org.apache.phoenix.trace.util.Tracing;
import org.junit.Test;

/**
 * Test that the logging sink stores the expected metrics/stats
 */
public class PhoenixTracingEndToEndTest extends BaseHBaseManagedTimeIT {

    private static final Log LOG = LogFactory.getLog(PhoenixTracingEndToEndTest.class);
    private static final int MAX_RETRIES = 10;
    private final String table = "ENABLED_FOR_LOGGING";
    private final String index = "ENABALED_FOR_LOGGING_INDEX";

    private Connection getTracingConnection() throws Exception {
        Properties props = new Properties(TEST_PROPERTIES);
        props.put(Tracing.TRACING_LEVEL_KEY, Tracing.Frequency.ALWAYS.getKey());
        return DriverManager.getConnection(getUrl(), props);
    }

    /**
     * Register the sink with the metrics system, so we don't need to specify it in the conf
     * @param sink
     */
    private void registerSink(MetricsWriter sink) {
        TestableMetricsWriter writer = TracingTestCompat.newTraceMetricSink();
        writer.setWriterForTesting(sink);
        Metrics.getManager().register("phoenix", "test sink gets logged", writer);
    }

    /**
     * Test that span will actually go into the this sink and be written on both side of the wire,
     * through the indexing code.
     * @throws Exception
     */
    @Test
    public void testClientServerIndexingTracing() throws Exception {
        // setup the sink
        PhoenixTableMetricsWriter sink = new PhoenixTableMetricsWriter();

        // separate connections to minimize amount of traces that are generated
        Connection traceable = getTracingConnection();
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        // one call for client side, one call for server side
        final CountDownLatch updated = new CountDownLatch(2);
        Connection countable = new CountDownConnection(conn, updated);
        sink.initForTesting(countable);

        registerSink(sink);

        createTestTable(conn, true);

        LOG.debug("Doing dummy the writes to the tracked table");
        String insert = "UPSERT INTO " + table + " VALUES (?, ?)";
        PreparedStatement stmt = traceable.prepareStatement(insert);
        stmt.setString(1, "key1");
        stmt.setLong(2, 1);
        // this first trace just does a simple open/close of the span. Its not doing anything
        // terribly
        // interesting because we aren't auto-committing on the connection, so it just updates the
        // mutation state and returns.
        stmt.execute();
        stmt.setString(1, "key2");
        stmt.setLong(2, 2);
        stmt.execute();
        traceable.commit();

        // wait for the latch to countdown, as the metrics system is time-based
        LOG.debug("Waiting for latch to complete!");
        updated.await(200, TimeUnit.SECONDS);// should be way more than GC pauses on the server.

        // read the traces back out

        /*Expected: 
         * 1. Single element trace - for first PreparedStatement#execute span
         * 2. Two element trace for second PreparedStatement#execute span
         *  a. execute call
         *  b. metadata lookup*
         * 3. Commit trace.
         *  a. Committing to tables
         *    i. Committing to single table
         *    ii. hbase batch write*
         *    i.I. span on server
         *    i.II. building index updates
         *    i.III. waiting for latch
         * where '*' is a generically named thread (e.g phoenix-1-thread-X)
         */
        
        TraceReader reader = new TraceReader(conn);
        // look for a trace with indexing info. This is inherently pretty brittle, but we don't have
        // a
        // better way to make sure that the indexing stuff has shown up w/o seriously munging the
        // code.
        int retries = 0;
        boolean indexingCompleted = false;
        while (retries < MAX_RETRIES && !indexingCompleted) {
            Collection<TraceHolder> traces = reader.readAll(100);
            int i = 0;
            for (TraceHolder trace : traces) {
                String traceInfo = trace.toString();
                // skip logging traces that are just traces about tracing
                if (traceInfo.contains(TracingCompat.DEFAULT_STATS_TABLE_NAME)) {
                    continue;
                }
                LOG.info(i + " ******  Got trace: " + traceInfo);
                if (traceInfo.contains("Completing index")) {
                    indexingCompleted = true;
                }
            }
            LOG.info(retries + ") ======  Waiting for indexing updates to be propagated ========");
            Thread.sleep(1000);
            retries++;
        }

        assertTrue("Never found indexing updates", indexingCompleted);
    }

    private void createTestTable(Connection conn, boolean withIndex) throws SQLException {
        // create a dummy table
        String ddl =
                "create table if not exists " + table + "(" + "k varchar not null, " + "c1 bigint"
                        + " CONSTRAINT pk PRIMARY KEY (k))";
        conn.createStatement().execute(ddl);

        // early exit if we don't need to create an index
        if (!withIndex) {
            return;
        }
        // create an index on the table - we know indexing has some basic tracing
        ddl = "CREATE INDEX IF NOT EXISTS " + index + " on " + table + " (c1)";
        conn.createStatement().execute(ddl);
        conn.commit();
    }

    /**
     * Updates to the will create traces, which then get logged to the tracing table. However, that
     * write to the tracing table also creates stats, which then get logged. However, we end up not
     * seeing a parentid for the span that has the info about updating the stats table.
     * @throws Exception
     */
    @Test
    public void testCorrectTracingForTracingStats() throws Exception {
        PhoenixTableMetricsWriter sink = new PhoenixTableMetricsWriter();
        Connection traceable = getTracingConnection();
        final CountDownLatch updated = new CountDownLatch(3);
        Connection countable = new CountDownConnection(traceable, updated);
        sink.initForTesting(countable);

        registerSink(sink);

        // trace the setup of the table
        createTestTable(traceable, false);

        LOG.debug("Doing dummy the writes to the tracked table");
        String insert = "UPSERT INTO " + table + " VALUES (?, ?)";
        PreparedStatement stmt = traceable.prepareStatement(insert);
        stmt.setString(1, "key1");
        stmt.setLong(2, 1);
        stmt.execute();
        stmt.setString(1, "key2");
        stmt.setLong(2, 2);
        stmt.execute();
        traceable.commit();

        updated.await(300, TimeUnit.SECONDS);

        // read the traces back out
        TraceReader reader = new TraceReader(traceable);
        // look for a trace with indexing info. This is inherently pretty brittle, but we don't have
        // a
        // better way to make sure that the indexing stuff has shown up w/o seriously munging the
        // code.
        int retries = 0;
        boolean traceLoggingCompleted = false;
        outer: while (retries < MAX_RETRIES) {
            Collection<TraceHolder> traces = reader.readAll(100);
            for (TraceHolder trace : traces) {
                LOG.info("Got trace: " + trace);
                for (SpanInfo span : trace.spans) {
                    // this is the span with the info the for the logging table
                    if (span.description.contains(TracingCompat.DEFAULT_STATS_TABLE_NAME)) {
                        assertNotNull("No parent span set for the trace for stats table update",
                            span.parent);
                        traceLoggingCompleted = true;
                    }
                }
                if (traceLoggingCompleted) {
                    break outer;
                }
                LOG.info("======  Waiting for tracing updates to be propagated ========");
                Thread.sleep(1000);
                retries++;
            }
        }

        assertTrue("Never found trace-logging updates", traceLoggingCompleted);
    }

    @Test
    public void testScanTracing() throws Exception {
        // setup the sink
        PhoenixTableMetricsWriter sink = new PhoenixTableMetricsWriter();

        // separate connections to minimize amount of traces that are generated
        Connection traceable = getTracingConnection();
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        // one call for client side, one call for server side
        CountDownLatch updated = new CountDownLatch(2);
        Connection countable = new CountDownConnection(conn, updated);
        sink.initForTesting(countable);

        registerSink(sink);

        // create a dummy table
        createTestTable(conn, false);

        // update the table, but don't trace these, to simplify the traces we read
        LOG.debug("Doing dummy the writes to the tracked table");
        String insert = "UPSERT INTO " + table + " VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(insert);
        stmt.setString(1, "key1");
        stmt.setLong(2, 1);
        stmt.execute();
        conn.commit();
        conn.rollback();

        // setup for next set of updates
        stmt.setString(1, "key2");
        stmt.setLong(2, 2);
        stmt.execute();
        conn.commit();
        conn.rollback();

        // do a scan of the table
        String read = "SELECT * FROM " + table;
        ResultSet results = traceable.createStatement().executeQuery(read);
        assertTrue("Didn't get first result", results.next());
        assertTrue("Didn't get second result", results.next());
        results.close();

        assertTrue("Get expected updates to trace table", updated.await(200, TimeUnit.SECONDS));
        // don't trace reads either
        TraceReader reader = new TraceReader(conn);
        int retries = 0;
        boolean tracingComplete = false;
        while (retries < MAX_RETRIES && !tracingComplete) {
            Collection<TraceHolder> traces = reader.readAll(100);
            for (TraceHolder trace : traces) {
                String traceInfo = trace.toString();
                LOG.info("Got trace: " + trace);
                if (traceInfo.contains("Parallel scanner")) {
                    tracingComplete = true;
                }
            }
            if (tracingComplete) {
                continue;
            }
            LOG.info("======  Waiting for tracing updates to be propagated ========");
            Thread.sleep(1000);
            retries++;
        }

        assertTrue("Didn't find the parallel scanner in the tracing", tracingComplete);
    }

    @Test
    public void testScanTracingOnServer() throws Exception {
        // setup the sink
        PhoenixTableMetricsWriter sink = new PhoenixTableMetricsWriter();

        // separate connections to minimize amount of traces that are generated
        Connection traceable = getTracingConnection();
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);

        // one call for client side, one call for server side
        CountDownLatch updated = new CountDownLatch(2);
        Connection countable = new CountDownConnection(conn, updated);
        sink.initForTesting(countable);

        registerSink(sink);

        // create a dummy table
        createTestTable(conn, false);

        // update the table, but don't trace these, to simplify the traces we read
        LOG.debug("Doing dummy the writes to the tracked table");
        String insert = "UPSERT INTO " + table + " VALUES (?, ?)";
        PreparedStatement stmt = conn.prepareStatement(insert);
        stmt.setString(1, "key1");
        stmt.setLong(2, 1);
        stmt.execute();
        conn.commit();
        conn.rollback();

        // setup for next set of updates
        stmt.setString(1, "key2");
        stmt.setLong(2, 2);
        stmt.execute();
        conn.commit();
        conn.rollback();

        // do a scan of the table
        String read = "SELECT COUNT(*) FROM " + table;
        ResultSet results = traceable.createStatement().executeQuery(read);
        assertTrue("Didn't get count result", results.next());
        // make sure we got the expected count
        assertEquals("Didn't get the expected number of row", 2, results.getInt(1));
        results.close();

        assertTrue("Get expected updates to trace table", updated.await(200, TimeUnit.SECONDS));
        // don't trace reads either
        TraceReader reader = new TraceReader(conn);
        int retries = 0;
        boolean tracingComplete = false;
        while (retries < MAX_RETRIES && !tracingComplete) {
            Collection<TraceHolder> traces = reader.readAll(100);
            for (TraceHolder trace : traces) {
                String traceInfo = trace.toString();
                LOG.info("Got trace: " + trace);
                if (traceInfo.contains(BaseScannerRegionObserver.SCANNER_OPENED_TRACE_INFO)) {
                    tracingComplete = true;
                }
            }
            if (tracingComplete) {
                continue;
            }
            LOG.info("======  Waiting for tracing updates to be propagated ========");
            Thread.sleep(1000);
            retries++;
        }

        assertTrue("Didn't find the parallel scanner in the tracing", tracingComplete);
    }

    private static class CountDownConnection extends DelegatingConnection {
        private CountDownLatch commit;

        public CountDownConnection(Connection conn, CountDownLatch commit) {
            super(conn);
            this.commit = commit;
        }

        @Override
        public void commit() throws SQLException {
            commit.countDown();
            super.commit();
        }

    }
}