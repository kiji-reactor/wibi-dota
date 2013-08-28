/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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

package com.wibidata.wibidota;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;

/**
 * Collects and counts values and from the Kiji dota table and returns the
 * number of times those values appeared. Can be subdived by start_time of
 * the game. Output is of the form:
 *
 * <Columns>=<Value> (<start_range>-<end_range>)  <number of occurances>
 */
// The express job MultHistogram is preferred for this functionality
// This class should just be used for reference
public abstract class DotaValueHistogram extends KijiGatherer {

  private static final Logger LOG =
      LoggerFactory.getLogger(DotaValueHistogram.class);

  private static final LongWritable ONE = new LongWritable(1);

  private static int rows = 0;

  public interface KeyGenerator {
    public String getKey(KijiRowData row) throws IOException;
    public String[] getColumnNames();
  }

  private static class ValueByTime implements KeyGenerator {

    public final String family;
    public final String column;
    public final Long interval;
    public final String nullStr;

    public ValueByTime(String family, String column, Long interval) {
      this.family = family;
      this.column = column;
      this.interval = interval;
      this.nullStr = "NULL";
    }

    public String[] getColumnNames() {
      return new String[]{
          family +":" + column,
          "data:start_time"
      };
    }

    public String getKey(KijiRowData row) throws IOException {
      Integer value = row.getMostRecentValue(family, column);
      StringBuilder sb = new StringBuilder();
      sb.append(family + ":" + column + "=" + (value == null ? nullStr : value));
      if(interval != null) {
        Long startTime = row.getMostRecentValue("data"  , "start_time");
        long slot = startTime / interval;
        sb.append(" [" + slot * interval + "-" + (slot + 1) * interval + ")");
      }
      return sb.toString();
    }
  }

  private static final KeyGenerator[] KEYS = new KeyGenerator[] {
      new ValueByTime("data", "game_mode", 21600L)
  };

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    KijiDataRequestBuilder.ColumnsDef def = builder.newColumnsDef();
    def.withMaxVersions(1);
    HashSet<String> colsAdded = new HashSet<String>();
    for(KeyGenerator kg : KEYS) {
      for(String s : kg.getColumnNames()) {
        if(!colsAdded.contains(s)) {
          def.add(new KijiColumnName(s));
          colsAdded.add(s);
        }
      }
    }
    return builder.addColumns(def).build();
  }

  @Override
  public void gather(KijiRowData input, GathererContext context)
      throws IOException {
    rows++;
    for(KeyGenerator kg: KEYS) {
      context.write(new Text(kg.getKey(input)), ONE);
    }
    if(rows % 1000 == 0) {
      LOG.info("Processes row: " + rows);
    }
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return Text.class;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return LongWritable.class;
  }
}

