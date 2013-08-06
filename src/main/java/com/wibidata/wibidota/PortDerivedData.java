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

import com.wibidata.wibidota.avro.Player;
import com.wibidata.wibidota.avro.Players;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Class to transfer derived data from the dota_matches table to the
 * dota_players table. Issue buffered puts to the dota_players table.
 */
public class PortDerivedData extends KijiGatherer<LongWritable, Text> {

  private static final Logger LOG = LoggerFactory.getLogger(DotaGatherExampleValues.class);

  private static final KijiURI kijiURI = KijiURI.newBuilder()
      .withInstanceName("wibidota").build();
  private static KijiTable mTable;
  private static KijiBufferedWriter mWriter;
  private static Kiji mKiji;
  private static int writes = 0;

  @Override
  public void setup(GathererContext context) {
    try {
      mKiji = Kiji.Factory.open(kijiURI);
      mTable = mKiji.openTable("dota_players");
      mWriter = mTable.getWriterFactory().openBufferedWriter();
    } catch (IOException e) {
      throw new RuntimeException("Unable to open mTable: " + kijiURI.toString());
    }
  }

  @Override
  public void cleanup(GathererContext context) {
    try {
      mWriter.flush();
      mWriter.close();
      mTable.release();
      mKiji.release();
    } catch (IOException e) {
      throw new RuntimeException("Unable to release table: " + kijiURI.toString());
    }
  }

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    KijiDataRequestBuilder.ColumnsDef def = builder.newColumnsDef();
    def.withMaxVersions(1);

    def.add("data", "player_data");
    def.addFamily("derived_data");
    return builder.addColumns(def).build();
  }

  public void setConf(Configuration conf) {
    conf.set("hbase.client.scanner.caching","50");
    super.setConf(conf);
  }

  @Override
  public void gather(KijiRowData kijiRowData,
                     GathererContext<LongWritable, Text> gathererContext) throws IOException {
    Players players = kijiRowData.getMostRecentValue("data", "player_data");
    NavigableMap<String, Double> data = kijiRowData.getMostRecentValues("derived_data");
    if(data.size() == 0) {
      return;
    }
    if(writes > 200) {
      writes = 0;
      mWriter.flush();
    }
    long time = kijiRowData.getMostRecentCell("data", "player_data").getTimestamp();
    for(Player player : players.getPlayers()) {
      Integer accountId = player.getAccountId();
      if(DotaValues.nonAnonPlayer(accountId)) {
        EntityId id = mTable.getEntityId(accountId);
        for(Map.Entry<String, Double> entry : data.entrySet()) {
          writes++;
          mWriter.put(id, "match_derived_data", entry.getKey(), time, entry.getValue());
        }
      }
    }
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return LongWritable.class;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return Text.class;
  }
}
