/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.sep.impl;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;

/**
 * Metrics for the Side-Effect Processor (SEP) system.
 * Note: Moved SepMetrics to metrics2 API because metrics1 API is nomore available in Hadoop-3
 */
public class SepMetrics implements MetricsSource {

    private final String recordName;
    
    private final String sourceName;
    
    private long lastSepTimestamp;
    
    private final MutableHistogram histo = 
//        new MutableHistogram(
        new MutableTimeHistogram(
        "sepProcessed", 
        "SEP operations that have been processed after making it through the filtering process");
    
    private static final String CONTEXT = "repository";
    
    
    public SepMetrics(String recordName) {
        this.recordName = recordName;
        this.sourceName = "SEP." + recordName;
        DefaultMetricsSystem.instance().register(
            sourceName, 
            "HBase Side-Effect Processor Metrics", 
            this);
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
      collector.addRecord(recordName)
          .setContext(CONTEXT)
          .addGauge(
              new MyMetricsInfo(
                  "lastSepTimestamp", 
                  "Write timestamp of the most recent operation in HBase that has been handled by the SEP system"), 
              lastSepTimestamp);      
      
      histo.snapshot(
          collector.addRecord(recordName).setContext(CONTEXT), 
          all);
    }
    
    public void shutdown() {      
        //DefaultMetricsSystem.instance().unregisterSource(sourceName);
        DefaultMetricsSystem.instance().shutdown();
    }

    /**
     * Report that a filtered SEP operation has been processed. This method should only be called to
     * report SEP operations that have been processed after making it through the filtering process.
     * 
     * @param duration The number of millisecods spent handling the SEP operation
     */
    public void reportFilteredSepOperation(long duration) {
        histo.add(duration);
    }

    /**
     * Report the original write timestamp of a SEP operation that was received. Assuming that SEP
     * operations are delivered in the same order as they are originally written in HBase (which
     * will always be the case except for when a region split or move takes place), this metric will always
     * hold the write timestamp of the most recent operation in HBase that has been handled by the SEP system.
     * 
     * @param timestamp The write timestamp of the last SEP operation
     */
    public void reportSepTimestamp(long writeTimestamp) {
        this.lastSepTimestamp = writeTimestamp;      
    }

    
    private static final class MyMetricsInfo implements MetricsInfo {
      
        private final String name;
        private final String description;
  
        public MyMetricsInfo(String name, String description) {
            this.name = name;
            this.description = description;
        }
        
        @Override
        public String name() {
            return name;
        }
        
        @Override
        public String description() {
            return description;
        }
    };
    
}
