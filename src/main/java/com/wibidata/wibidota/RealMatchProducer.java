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

import com.wibidata.wibidota.DotaValues.Columns;
import com.wibidata.wibidota.DotaValues.LeaverStatus;
import com.wibidata.wibidota.DotaValues.LobbyType;
import com.wibidata.wibidota.avro.Player;
import com.wibidata.wibidota.avro.Players;
import org.apache.hadoop.conf.Configuration;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Adds a 'serious_match' column to the derived data field that is non null iff the match is a
 * a public matchmaking, tournament, team_match, solo_queue game played with game modes
 * AP, CP, AR, RD, LP, or Compendium and had 10 human players. Timestamp is always time of entry.
 * The values depends on the in-game leavers,
 * 3.0 -> all players stayed
 * 2.0 -> Some players 'safe abandon' (often means they disconnected slightly before the match ended)
 * 1.0 -> One or more players abandoned the game
 */
public class RealMatchProducer extends KijiProducer {

  private static final Logger LOG = LoggerFactory.getLogger(RealMatchProducer.class);

  static enum Counters {
    GOOD_MATCHES,  // Number of matches considered 'real'
    BAD_GAME_MODE, // Number of mathes with discounted due to game_mode
    BAD_LOBBY,     // Number of matches with a non-real lobby type
    LEAVERS,       // Number of mathches with leavers
    SAFE_LEAVERS,  // Number of matches with disconnecters
    REAL_MATCH_WITH_LEAVERS, // Matches valid except for the presence of leavers
    REAL_MATCH_WITH_SAFE_LEAVERS, // Matches valid except for the presence of leavers
    BAD_MATCHES,   // Number  of matches not considered real
    UNDER_TEN_PLAYERS,   // Number  of matches with less than 10 players
  }

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    return builder.addColumns(builder.newColumnsDef()
        .withMaxVersions(1)
        .add(Columns.GAME_MODE.columnName())
        .add(Columns.HUMAN_PLAYERS.columnName())
        .add(Columns.LOBBY_TYPE.columnName())
        .add(Columns.PLAYER_DATA.columnName())
    ).build();
  }

  @Override
  public void setConf(Configuration conf) {
    conf.set("mapred.tasktracker.map.tasks.maximum","1");
    conf.set("hbase.client.scanner.caching","100");
    super.setConf(conf);
  }

  @Override
  public String getOutputColumn() {
    return "derived_data";
  }

  @Override
  public void produce(KijiRowData kijiRowData, ProducerContext producerContext) throws IOException {
    boolean realMatch = true;

    // Check if this match passes
    int gameMode = (Integer) kijiRowData.getMostRecentCell("data", "game_mode").getData();
    if(!DotaValues.GameMode.seriousGame(DotaValues.GameMode.fromInt(gameMode))) {
      producerContext.incrementCounter(Counters.BAD_GAME_MODE);
      realMatch = false;
    }
    LobbyType lobbyType = LobbyType.fromInt((Integer) kijiRowData.getMostRecentValue("data", "lobby_type"));
    if(!LobbyType.seriousLobby(lobbyType)) {
      producerContext.incrementCounter(Counters.BAD_LOBBY);
      realMatch = false;
    }
    if(((Integer) kijiRowData.getMostRecentValue("data", "human_players")) != 10) {
      producerContext.incrementCounter(Counters.UNDER_TEN_PLAYERS);
      realMatch = false;
    }

    // Check for leavers
    boolean leavers = false;
    boolean safeLeavers = false;
    Players player_data = kijiRowData.getMostRecentValue("data", "player_data");
    List<Player> players = player_data.getPlayers();
    for(Player player : players) {
      LeaverStatus ls = LeaverStatus.fromInt(player.getLeaverStatus());
      if(ls == LeaverStatus.SAFE_LEAVE) {
        safeLeavers = true;
       } else if(ls != LeaverStatus.STAYED) {
        leavers = true;
      }
    }

    if(leavers) {
      producerContext.incrementCounter(Counters.LEAVERS);
      if(realMatch) {
        producerContext.incrementCounter(Counters.REAL_MATCH_WITH_LEAVERS);
      }
    } else if(safeLeavers) {
      producerContext.incrementCounter(Counters.SAFE_LEAVERS);
      if(realMatch) {
        producerContext.incrementCounter(Counters.REAL_MATCH_WITH_SAFE_LEAVERS);
      }
    }

    // Write the result
    if(realMatch) {
      double result;
      if(leavers) {
        result = 1.0;
      } else if(safeLeavers) {
        result = 2.0;
      } else {
        result = 3.0;
      }
      producerContext.incrementCounter(Counters.GOOD_MATCHES);
      producerContext.put("real_match", result);
    } else {
      producerContext.incrementCounter(Counters.BAD_MATCHES);
    }
  }
}
