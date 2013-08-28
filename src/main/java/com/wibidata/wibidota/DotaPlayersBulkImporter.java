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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wibidata.wibidota.avro.Player;
import com.wibidata.wibidota.avro.Players;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.schema.EntityId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *  Class to bulk-import data into the dota_players table from the raw Json files.
 */
public class DotaPlayersBulkImporter extends KijiBulkImporter<LongWritable, Text> {

  private static final Logger LOG = LoggerFactory.getLogger(DotaPlayersBulkImporter.class);

  private static final JsonParser PARSER = new JsonParser();

  @Override
  public void produce(LongWritable filePos, Text line, KijiTableContext context)
    throws IOException {

    try {
      // Parse the JSON
      final JsonObject matchData = PARSER.parse(line.toString()).getAsJsonObject();

      // Collect the values we need
      final long matchId = matchData.get("match_id").getAsLong();
      final int gameMode = matchData.get("game_mode").getAsInt();
      final int lobbyType = matchData.get("lobby_type").getAsInt();
      final int direTowers = matchData.get("tower_status_dire").getAsInt();
      final int radiantTowers = matchData.get("tower_status_radiant").getAsInt();
      final int direBarracks = matchData.get("barracks_status_dire").getAsInt();
      final int radiantBarracks = matchData.get("barracks_status_radiant").getAsInt();
      final int cluster = matchData.get("cluster").getAsInt();
      final Integer season = DotaMatchBulkImporter.getNullableInt(matchData.get("season"));
      final long startTime = matchData.get("start_time").getAsLong() * 1000; // Convert to milliseconds
      final long seqNum = matchData.get("match_seq_num").getAsLong();
      final int leagueId = matchData.get("leagueid").getAsInt();
      final int firstBloodTime = matchData.get("first_blood_time").getAsInt();
      final int negativeVotes = matchData.get("negative_votes").getAsInt();
      final int positiveVotes = matchData.get("positive_votes").getAsInt();
      final int duration = matchData.get("duration").getAsInt();
      final boolean radiantWin = matchData.get("radiant_win").getAsBoolean();
      final int humanPlayers = matchData.get("human_players").getAsInt();

      final Players players = DotaMatchBulkImporter.extractPlayers(matchData);

      List<Player> allPlayers = new ArrayList<Player>();
      List<Player> otherPlayers = new ArrayList<Player>();
      allPlayers.addAll(players.getPlayers());
      otherPlayers.addAll(players.getPlayers());
      for(int i = 0; i < allPlayers.size(); i++) {
        Player player = allPlayers.get(i);
        Integer accountId = player.getAccountId();
        if(accountId != null && accountId != -1) {
          otherPlayers.remove(i);
          players.setPlayers(otherPlayers);
          EntityId eid = context.getEntityId(accountId);
          context.put(eid, "data", "match_id", startTime, matchId);
          context.put(eid, "data", "dire_towers_status", startTime, direTowers);
          context.put(eid, "data", "radiant_towers_status", startTime, radiantTowers);
          context.put(eid, "data", "dire_barracks_status", startTime, direBarracks);
          context.put(eid, "data", "radiant_barracks_status", startTime, radiantBarracks);
          context.put(eid, "data", "cluster", startTime, cluster);
          context.put(eid, "data", "season", startTime, season);
          context.put(eid, "data", "match_seq_num", startTime, seqNum);
          context.put(eid, "data", "league_id", startTime, leagueId);
          context.put(eid, "data", "first_blood_time", startTime, firstBloodTime);
          context.put(eid, "data", "negative_votes", startTime, negativeVotes);
          context.put(eid, "data", "positive_votes", startTime, positiveVotes);
          context.put(eid, "data", "duration", startTime, duration);
          context.put(eid, "data", "radiant_win", startTime, radiantWin);
          context.put(eid, "data", "game_mode", startTime, gameMode);
          context.put(eid, "data", "lobby_type", startTime, lobbyType);
          context.put(eid, "data", "human_players", startTime, humanPlayers);
          context.put(eid, "data", "other_players", startTime, players);
          context.put(eid, "data", "player", startTime, player);
          otherPlayers.add(i, player);
        }
      }
    } catch (RuntimeException re) {
      // For RunetimeExceptions we try to log additional information debugging purposes
      try {
        LOG.error("Runtime Exception! MatchId=" +
            "\nLine\n" + line + "\nMessage:\n" + re.toString());
      } catch (RuntimeException ex) {
        LOG.debug("Error loggging the error: " + ex.getMessage());
      }
      throw re;
    }
  }
}
