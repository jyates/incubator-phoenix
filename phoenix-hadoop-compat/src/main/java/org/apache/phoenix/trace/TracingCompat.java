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

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.SpanReceiver;

/**
 * Utilities for tracing that are common among the compatibility and core classes.
 */
public class TracingCompat {

    /**
     * @return a new SpanReceiver that will write to the correct metrics system
     */
    public static SpanReceiver newTraceMetricSource() {
        return CompatibilityFactory.getInstance(PhoenixSpanReceiver.class);
    }

    public static final String DEFAULT_STATS_TABLE_NAME = "PHOENIX.TRACING_STATS";

    /**
     * Configuration key to overwrite the tablename that should be used as the target table
     */
    public static final String TARGET_TABLE_CONF_KEY =
            "org.apache.phoenix._internal.trace.tablename";

    public static final String METRIC_SOURCE_KEY = "phoenix.";

    /** Set context to enable filtering */
    public static final String METRICS_CONTEXT = "tracing";

    public static void addAnnotation(Span span, String message, int value) {
        span.addKVAnnotation(message.getBytes(), Bytes.toBytes(value));
    }

    public static Pair<String, String> readAnnotation(byte[] key, byte[] value) {
        return new Pair<String, String>(new String(key), Integer.toString(Bytes.toInt(value)));
    }
}