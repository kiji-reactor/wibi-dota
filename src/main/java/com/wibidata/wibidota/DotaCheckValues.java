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

import com.wibidata.wibidota.avro.AbilityUpgrade;
import com.wibidata.wibidota.avro.Player;
import com.wibidata.wibidota.avro.Players;
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
import java.util.List;

/**
 * Sanity checks the values in the dota_matches table. Outs puts a list of
 * 'bad_values' of the form:
 *
 * <Field> <type of Error>  <Number of occurances>
 *
 *  Run with a LongSumReducer as the combiner/reducer
 */
public class DotaCheckValues extends KijiGatherer {

  private static final Logger LOG = LoggerFactory.getLogger(KijiGatherer.class);

  // Cache this
  private static LongWritable ONE = new LongWritable(1L);

  // Rows seen so far
  private static int rows = 0;

  // List of team prefixes
  private static final String[] TEAMS = new String[]{"dire", "radiant"};

  // Exception to throw if a field is not formatted correctly
  private static class BadFormat extends RuntimeException {
    public BadFormat(String msg) {
      super(msg);
    }
  }

  private static void checkNull(Object o, String field) {
    if(o == null) {
      throw new BadFormat(field + " was null");
    }
  }

  private static void checkInt(Integer i, String field) {
    checkInt(i, field, 0, Integer.MAX_VALUE / 10);
  }

  private static void checkInt(Integer i, String field, int min, int max) {
    checkNull(i, field);
   if(i < min) {
      throw new BadFormat(field + " was smaller then " + min);
    } else if(i > max) {
      throw new BadFormat(field + " was larger then " + min);
    }
  }

  private static void checkLong(Long i, String field, long min, long max) {
    checkNull(i, field);
    if(i < min) {
      throw new BadFormat(field + " was smaller then " + min);
    } else if(i > max) {
      throw new BadFormat(field + " was larger then " + min);
    }
  }

  private static void checkDouble(Double i, String field, double min, double max) {
    checkNull(i, field);
    if(i < min) {
      throw new BadFormat(field + " was smaller then " + min);
    } else if(i > max) {
      throw new BadFormat(field + " was larger then " + min);
    }
  }

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    KijiDataRequestBuilder.ColumnsDef def = builder.newColumnsDef();
    def.withMaxVersions(1);
    // Everything
    def.add(new KijiColumnName("data","player_data"))
        .add(new KijiColumnName("data", "dire_towers_status"))
        .add(new KijiColumnName("data", "radiant_towers_status"))
        .add(new KijiColumnName("data", "dire_barracks_status"))
        .add(new KijiColumnName("data", "radiant_barracks_status"))
        .add(new KijiColumnName("data", "cluster"))
        .add(new KijiColumnName("data", "season"))
        .add(new KijiColumnName("data", "game_mode"))
        .add(new KijiColumnName("data", "match_seq_num"))
        .add(new KijiColumnName("data", "league_id"))
        .add(new KijiColumnName("data", "first_blood_time"))
        .add(new KijiColumnName("data", "negative_votes"))
        .add(new KijiColumnName("data", "duration"))
        .add(new KijiColumnName("data", "radiant_win"))
        .add(new KijiColumnName("data", "positive_votes"))
        .add(new KijiColumnName("data", "lobby_type"))
        .add(new KijiColumnName("data", "human_players"));
    return builder.addColumns(def).build();
  }

  @Override
  public void gather(KijiRowData kijiRowData, GathererContext gathererContext) throws IOException {
    rows++;
    if(rows % 5000 == 0) {
      LOG.info("Processed row: " + rows);
    }
    try {
      // After 2005, before the future plus 10 minutes to allow for clock misalignments
      checkLong(kijiRowData.getMostRecentCell("data", "match_id").getTimestamp(), "start_time",
          1104537600, System.currentTimeMillis() + 60000);
      for(String team : TEAMS) {
        Integer towerStatus = kijiRowData.getMostRecentValue("data", team + "_towers_status");
        checkInt(towerStatus, team + "_tower_status", 0, ((Double) Math.pow(2, 11)).intValue());
      }
      for(String team : TEAMS) {
        Integer raxStatus = kijiRowData.getMostRecentValue("data", team + "_barracks_status");
        checkInt(raxStatus, team + "_barracks_status", 0, ((Double) Math.pow(2, 6)).intValue());
      }
      checkInt((Integer) kijiRowData.getMostRecentValue("data", "human_players"),
          "human_players", 0, 10);

      DotaValues.LobbyType.fromInt((Integer) kijiRowData.getMostRecentValue("data", "lobby_type"));
      DotaValues.GameMode.fromInt((Integer) kijiRowData.getMostRecentValue("data", "game_mode"));
      for(String s : new String[]{"cluster", "season", "duration",
          "negative_votes", "positive_votes"}) {
        Integer n = kijiRowData.getMostRecentValue("data", s);
        checkInt(n, s, 0, Integer.MAX_VALUE / 2);
      }
      checkInt((Integer) kijiRowData.getMostRecentValue("data", "league_id"), "league_id",
          0, Integer.MAX_VALUE);
      Players players = kijiRowData.getMostRecentValue("data", "player_data");
      for(Player player : players.getPlayers()) {
        Integer n = player.getAccountId();
        if(n != null) {
          if(n != -1) {
            checkInt(n, "account_id", 0, Integer.MAX_VALUE);
          }
        }
        DotaValues.LeaverStatus.fromInt(player.getLeaverStatus());
        checkInt(player.getAssists(), "assists", 0 ,1000);
        checkInt(player.getDeaths(), "deaths", 0, 1000);
        checkInt(player.getDenies(), "denies", 0, 5000);
        checkInt(player.getGold(), "gold", 0, 10000);
        checkInt(player.getGoldSpent(), "gold_spent", 0, 1000000);
        checkInt(player.getHeroDamage(), "hero_damage");
        checkInt(player.getHeroHealing(), "hero_healing");
        checkInt(player.getHeroId(), "hero_id", 0, 200);
        checkInt(player.getKills(), "kills", 0, 1000);
        checkInt(player.getLastHits(), "last_hits", 0, 10000);
        checkInt(player.getPlayerSlot(), "player_slot", 0, 256);
        checkInt(player.getLevel(), "level", 0, 25);
        checkDouble(player.getGoldPerMinute(), "gold_per_minute", 0, 200000);
        checkDouble(player.getExpPerMinute(), "exp_per_minute", 0, 200000);
        List<AbilityUpgrade> aus = player.getAbilityUpgrades();
        checkInt(aus.size(), "ability upgrade size", 0, 25);
        for(AbilityUpgrade au : aus) {
          checkInt(au.getLevel(), "ability upgrade level", 0, 25);
          checkInt(au.getTime(), "ability upgrade time", 0, 1000000);
          checkInt(au.getAbilityId(), "ability id");
        }
      }
    } catch (RuntimeException re) {
      gathererContext.write(new Text(re.getMessage()), ONE);
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
