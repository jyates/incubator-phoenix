/**
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
package org.apache.phoenix.trace;

import static org.apache.phoenix.metrics.MetricInfo.ANNOTATION;
import static org.apache.phoenix.metrics.MetricInfo.DESCRIPTION;
import static org.apache.phoenix.metrics.MetricInfo.END;
import static org.apache.phoenix.metrics.MetricInfo.HOSTNAME;
import static org.apache.phoenix.metrics.MetricInfo.PARENT;
import static org.apache.phoenix.metrics.MetricInfo.SPAN;
import static org.apache.phoenix.metrics.MetricInfo.START;
import static org.apache.phoenix.metrics.MetricInfo.TRACE;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.metrics.MetricInfo;
import org.apache.phoenix.metrics.MetricsWriter;
import org.apache.phoenix.metrics.PhoenixAbstractMetric;
import org.apache.phoenix.metrics.PhoenixMetricTag;
import org.apache.phoenix.metrics.PhoenixMetricsRecord;
import org.apache.phoenix.util.QueryUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;

/**
 * Sink that writes phoenix metrics to a phoenix table
 * <p>
 * Each metric record should only correspond to a single completed span. Each span is only updated
 * in the phoenix table <i>once</i>
 */
public class PhoenixTableMetricsWriter implements MetricsWriter {

    private static final String VARIABLE_VALUE = "?";

    public static final Log LOG = LogFactory.getLog(PhoenixTableMetricsWriter.class);

    static final String TAG_FAMILY = "tags";
    /** Count of the number of tags we are storing for this row */
    static final String TAG_COUNT = TAG_FAMILY + ".count";

    /** Join strings on a comma */
    private static final Joiner COMMAS = Joiner.on(',');

    private Connection conn;

    private String table;

    @Override
    public void initialize() {
        try {
            // create the phoenix connection
            Configuration conf = HBaseConfiguration.create();
            Connection conn = QueryUtil.getConnection(conf);
            // enable bulk loading when we have enough data
            conn.setAutoCommit(true);

            String tableName =
                    conf.get(TracingCompat.TARGET_TABLE_CONF_KEY,
                        TracingCompat.DEFAULT_STATS_TABLE_NAME);

            initializeInternal(conn, tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeInternal(Connection conn, String tableName) throws SQLException {
        this.conn = conn;

        // ensure that the target table already exists
        createTable(conn, tableName);
    }
    
    /**
     * Used for <b>TESTING ONLY</b>
     * <p>
     * Initialize the connection and setup the table to use the
     * {@link TracingCompat#DEFAULT_STATS_TABLE_NAME}
     * @param conn to store for upserts and to create the table (if necessary)
     * @param tableName name of the table to create
     * @throws SQLException if any phoenix operation fails
     */
    @VisibleForTesting
    public void initForTesting(Connection conn) throws SQLException {
        initializeInternal(conn, TracingCompat.DEFAULT_STATS_TABLE_NAME);
    }

    /**
     * Create a stats table with the given name. Stores the name for use later when creating upsert
     * statements
     * @param conn connection to use when creating the table
     * @param table name of the table to create
     * @throws SQLException if any phoenix operations fails
     */
    private void createTable(Connection conn, String table) throws SQLException {
        String ddl =
                "create table if not exists " + table + "(" + " " + TRACE.columnName
                        + " bigint not null," + " " + PARENT.columnName + " bigint not null," + " "
                        + SPAN.columnName + " bigint not null," + " " + DESCRIPTION.columnName
                        + " varchar," + " " + START.columnName + " bigint not null," + " "
                        + END.columnName + " bigint not null," + " " + HOSTNAME.columnName
                        + " varchar," + " " + TAG_COUNT + " smallint"
                        + "  CONSTRAINT pk PRIMARY KEY (" + TRACE.columnName + ", "
                        + PARENT.columnName + ", " + SPAN.columnName + "))\n";
        PreparedStatement stmt = conn.prepareStatement(ddl);
        stmt.execute();
        this.table = table;
    }

    /**
     * Flush the writes to the table.
     */
    @Override
    public void flush() {
        // commit all the changes we have seen
        try {
            LOG.trace("Flushing metrics to stats table");
            conn.commit();
        } catch (SQLException e) {
            LOG.error("Failed to commit trace stats to table!", e);
            // TODO should we do something else here? Try to get a new connection? Kill ourselves?
        }
    }

    /**
     * Add a new metric record to be written.
     * @param record
     */
    @Override
    public void addMetrics(PhoenixMetricsRecord record) {
        // its not a tracing record, we are done. This could also be handled by filters, but safer
        // to do it here, in case it gets misconfigured
        if (!record.name().startsWith(TracingCompat.METRIC_SOURCE_KEY)) {
            return;
        }
        String stmt = "UPSERT INTO " + table + " (";
        // drop it into the queue of things that should be written
        List<String> keys = new ArrayList<String>();
        List<Object> values = new ArrayList<Object>();
        List<String> variableValues = new ArrayList<String>(record.tags().size());
        keys.add(TRACE.columnName);
        values.add(Long.parseLong(record.name().substring(TracingCompat.METRIC_SOURCE_KEY.length())));

        keys.add(DESCRIPTION.columnName);
        values.add(VARIABLE_VALUE);
        variableValues.add(record.description());

        // add each of the metrics
        for (PhoenixAbstractMetric metric : record.metrics()) {
            // name of the metric is also the column name to which we write
            keys.add(MetricInfo.getColumnName(metric.getName()));
            values.add((Long) metric.value());
        }

        // get the tags out so we can set them later (otherwise, need to be a single value)
        int tagCount = 0;
        for (PhoenixMetricTag tag : record.tags()) {
            if (tag.name().equals(ANNOTATION.traceName)) {
                // tags have the count in the their description, so we can set that column as well
                keys.add(TAG_FAMILY + ANNOTATION.columnName + tag.description() + " VARCHAR");
                variableValues.add(tag.value());
                values.add(VARIABLE_VALUE);
                tagCount++;
            } else if (tag.name().equals(HOSTNAME.traceName)) {
                keys.add(HOSTNAME.columnName);
                variableValues.add(tag.value());
                values.add(VARIABLE_VALUE);
            } else {
                LOG.error("Got an unexpected tag: " + tag);
            }
        }

        // add the tag count, now that we know it
        keys.add(TAG_COUNT);
        // ignore the hostname in the tags, if we know it
        values.add(tagCount);

        // compile the statement together
        stmt += COMMAS.join(keys);
        stmt += ") VALUES (" + COMMAS.join(values) + ")";

        if (LOG.isTraceEnabled()) {
            LOG.trace("Logging metrics to phoenix table via: " + stmt);
            LOG.trace("With tags: " + variableValues);
        }
        try {
            PreparedStatement ps = conn.prepareStatement(stmt);
            // need to add each tag as a dynamic value
            int index = 1;
            for (String tag : variableValues) {
                ps.setString(index++, tag);
            }
            ps.execute();
        } catch (SQLException e) {
            LOG.error("Could not write metric: \n" + record + " to prepared statement:\n" + stmt, e);
        }
    }
}