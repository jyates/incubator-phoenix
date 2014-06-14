/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package org.apache.phoenix.metrics;

/**
 * Metrics and their conversion from the trace name to the name we store in the stats table
 */
public enum MetricInfo {

    TRACE("", "trace_id"),
    SPAN("span_id", "span_id"),
    PARENT("parent_id", "parent_id"),
    START("start_time", "start_time"),
    END("end_time", "end_time"),
    TAG("phoenix.tag", ".t"),
    ANNOTATION("phoenix.annotation", ".a"),
    HOSTNAME("Hostname", "hostname"),
    DESCRIPTION("", "description");

    public final String traceName;
    public final String columnName;

    private MetricInfo(String traceName, String columnName) {
        this.traceName = traceName;
        this.columnName = columnName;
    }

    public static String getColumnName(String traceName) {
        for (MetricInfo info : MetricInfo.values()) {
            if (info.traceName.equals(traceName)) {
                return info.columnName;
            }
        }
        throw new IllegalArgumentException("Unknown tracename: " + traceName);
    }
}
