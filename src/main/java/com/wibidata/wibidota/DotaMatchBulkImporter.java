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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wibidata.wibidota.avro.AbilityUpgrade;
import com.wibidata.wibidota.avro.AdditionalUnit;
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
 * Bulk-importer to load the information about Dota 2 matches
 *
 * <p>Input files should contain JSON data representing a single match. The JSON
 * is expected to follow the API found at http://dev.dota2.com/showthread.php?t=58317.
 * A few exceptions are allowed.
 *
 * <pre>
 * { "user_id" : "0", "play_time" : "1325725200000", "song_id" : "1" }
 * </pre>
 *
 * The result will be a HFile that can then be bulk-loaded into Kiji
 */
public class DotaMatchBulkImporter extends KijiBulkImporter<LongWritable, Text> {

  private static final Logger LOG = LoggerFactory.getLogger(DotaMatchBulkImporter.class);

  static final JsonParser PARSER = new JsonParser();

  /**
   * Gets an Integer from a JsonElement that might be null.
   *
   * @param je, nullable,element to extract the int from.
   * @return the (possibly null) integer.
   */
  public static Integer getNullableInt(JsonElement je) {
      return (je == null ? null : je.getAsInt());
  }

  /**
   * Get a Long from a JsonElement that might be null.
   *
   * @param je, nullable element to extract.
   * @return the possibly null Long.
   */
  public static Long getNullableLong(JsonElement je) {
    return (je == null ? null : je.getAsLong());
  }

  /**
   * Extracts an AbilityUpgrade object from a JsonObject.
   *
   * @param abilityData, the JsonObject.
   * @return an AbilityUgrade object with abilityData's content.
   */
  public static AbilityUpgrade extractAbility(JsonObject abilityData) {
    return AbilityUpgrade.newBuilder()
            .setLevel(abilityData.get("level").getAsInt())
            .setAbilityId(abilityData.get("ability").getAsInt())
            .setTime(abilityData.get("time").getAsInt())
            .build();
  }

  /**
   * Reads an array of ints from a json object by reading
   * item stored in fields item_0, item_2, ... item_5
   *
   * @param items, object containing the item.
   * @return the List of items
   */
  public static List<Integer> readItems(JsonObject items) {
    final List<Integer> itemIds = new ArrayList<Integer>(6);
    for(int i = 0; i < 6; i++) {
      itemIds.add(items.get("item_" + i).getAsInt());
    }
    return itemIds;
  }

  /**
   * Reads a Player from a JsonObject.
   */
  public static Player extractPlayer(JsonObject playerData) {

    Player.Builder builder = Player.newBuilder();

    // Set the abilityUpgrades    q
    final List<AbilityUpgrade> abilityUpgrades = new ArrayList<AbilityUpgrade>();

    final JsonElement uncastAbilities = playerData.get("ability_upgrades");
    // This can be null (players have no abilities selected yet?) use a 0 length list
    if(uncastAbilities != null) {
      for(JsonElement o : uncastAbilities.getAsJsonArray()) {
        abilityUpgrades.add(extractAbility(o.getAsJsonObject()));
      }
    }
    builder.setAbilityUpgrades(abilityUpgrades);

    // Set the additionalUnit
    JsonElement additionalUnitsElem = playerData.get("additional_units");
    if(additionalUnitsElem == null) {
      builder.setAdditionalUnits(null);
    }  else {
      final JsonObject additionalUnit;
      // This is sometimes contained in a list
      if(additionalUnitsElem.isJsonArray()) {
        additionalUnit = additionalUnitsElem.getAsJsonArray().get(0).getAsJsonObject();
      } else {
        additionalUnit = additionalUnitsElem.getAsJsonObject();
      }
      builder.setAdditionalUnits(
          AdditionalUnit.newBuilder()
              .setName(additionalUnit.get("unitname").getAsString())
              .setItemIds(readItems(additionalUnit))
              .build());
    }

    return builder
             .setAccountId(getNullableInt(playerData.get("account_id")))
             .setAssists(playerData.get("assists").getAsInt())
             .setDeaths(playerData.get("deaths").getAsInt())
             .setDenies(playerData.get("denies").getAsInt())
             .setExpPerMinute(playerData.get("xp_per_min").getAsDouble())
             .setHeroId(playerData.get("hero_id").getAsInt())
             .setLastHits(playerData.get("last_hits").getAsInt())
             .setLeaverStatus(getNullableInt(playerData.get("leaver_status")))
             .setLevel(playerData.get("level").getAsInt())
             .setPlayerSlot(playerData.get("player_slot").getAsInt())
             .setTowerDamage(playerData.get("tower_damage").getAsInt())
             .setGoldSpent(playerData.get("gold_spent").getAsInt())
             .setGold(playerData.get("gold").getAsInt())
             .setGoldPerMinute(playerData.get("gold_per_min").getAsDouble())
             .setHeroDamage(playerData.get("hero_damage").getAsInt())
             .setHeroHealing(playerData.get("hero_healing").getAsInt())
             .setKills(playerData.get("kills").getAsInt())
             .setItemIds(readItems(playerData))
            .build();
  }

  /**
   * Reads a list of Players from a JsonObject
   */
  public static Players extractPlayers(JsonObject matchData ) {
    final List<Player> matchStats = new ArrayList<Player>(10);
    for(JsonElement o : matchData.get("players").getAsJsonArray()) {
      matchStats.add(extractPlayer(o.getAsJsonObject()));
    }
    return Players.newBuilder().setPlayers(matchStats).build();
  }


  @Override
  public void produce(LongWritable filePos, Text line, KijiTableContext context)
      throws IOException {

      try {
          // Parse the JSON and wrap a JSONplayerData over it
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
          final Integer season = getNullableInt(matchData.get("season"));
          final long startTime = matchData.get("start_time").getAsLong() * 1000; // convert to milliseconds
          final long seqNum = matchData.get("match_seq_num").getAsLong();
          final int leagueId = matchData.get("leagueid").getAsInt();
          final int firstBloodTime = matchData.get("first_blood_time").getAsInt();
          final int negativeVotes = matchData.get("negative_votes").getAsInt();
          final int positiveVotes = matchData.get("positive_votes").getAsInt();
          final int duration = matchData.get("duration").getAsInt();
          final boolean radiantWin = matchData.get("radiant_win").getAsBoolean();
          final int humanPlayers = matchData.get("human_players").getAsInt();

          // Build and parse the player match stats
          final Players players = extractPlayers(matchData);

          EntityId eid = context.getEntityId(matchId);

          // Produce all our data
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
          context.put(eid, "data", "player_data", startTime, players);
          context.put(eid, "data", "game_mode", startTime, gameMode);
          context.put(eid, "data", "lobby_type", startTime, lobbyType);
          context.put(eid, "data", "human_players", startTime, humanPlayers);
      } catch (RuntimeException re) {
          // For RunetimeExceptions we try to log additional information for debugging purposes
        LOG.error("Runtime Exception! " +
            "\nLine\n" + line + "\nMessage:\n" + re.toString());
        throw re;
      }
  }
}
