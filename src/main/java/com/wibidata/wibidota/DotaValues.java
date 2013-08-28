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
import org.kiji.schema.KijiColumnName;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Class that can represent raw values held in the dota_matches tables in a more readable
 * ways. NOTE this should be relatively stable but changes might occur if Valve changes their
 * API. In particular additional enums may be added as more game modes or lobby types are
 * introduced.
 */
/*
 * Note some of these, in particular tower, barracks, and item_slots are untested. Also note
 * item, hero, and abilities mapping are only as accurate as the provided files. Both the
 * item and abilities files were provided by third parties and are unconfirmed.
 */
public final class DotaValues {

  // Location of resources, should be in the CP
  private static final String HEROES_JSON = "com/wibidata/wibidota/heroes.json";

  private static final String ABILITIES_JSON = "com/wibidata/wibidota/abilities.json";

  private static final String ITEMS_JSON = "com/wibidata/wibidota/items.json";


  // This utility class should not be instantiated
  private DotaValues() {}


  /**
   * Checks if an accountId indicates an anonomous player
   *
    * @param accountId, the accountId
   * @return true iff that player is non anonous
   */
  public static boolean nonAnonPlayer(Integer accountId) {
    return accountId != null && accountId != -1;
  }

  /**
   * Maps the play_slot int as returned by the Dota 2 api to
   * a more readable form.
   *
   * @param num, the player slot as encoded by the Dota 2 api
   * @return 0-4 for the first-last slots of the Radiant, 5-9 for
   * the first-last slots on the Dire
   */
  public static int playerSlot(int num) {
    int slot = 0;
    if(num >> 7 == 1) {
      slot += 5;
    }
    return slot + (num & 7);
  }

  /**
   * @param num, the raw player_slot number
   * @return true iff that player was on the radiant
   */
  public static boolean radiantPlayer(int num) {
    return  (num >> 7 == 0);
  }

  // Lazily-loaded mapping for hero, items, and ability names
  private static JsonParser PARSER = new JsonParser();

  private static Map<Integer, String> heroNames = null;

  private static Map<Integer, String> itemNames = null;

  private static Map<Integer, String> abilityNames = null;

  // Translate json records into maps
  private static void readIds(Map<Integer, String> map, String filename,
                              String container, String idField, String nameField) {
    InputStreamReader isr =
        new InputStreamReader(ClassLoader.getSystemResourceAsStream(filename));
    Scanner sc = new Scanner(isr).useDelimiter("\\A");
    JsonObject heroesObj = PARSER.parse(sc.next()).getAsJsonObject();
    try {
      isr.close();
      sc.close();
    } catch (IOException e) {
      throw new RuntimeException("Error reading heroes.json: " + e.getMessage());
    }
    for(JsonElement je : heroesObj.get(container).getAsJsonArray()) {
      JsonObject hero = je.getAsJsonObject();
      map.put(hero.get(idField).getAsInt(),
          hero.get(nameField).getAsString());
    }
  }

  /**
   * Translates hero ids to hero names
   *
   * @param id, the hero's ID
   * @return The name of the hero with that ID
   */
  public static String getHeroName(int id) {
    if(heroNames == null) {
      heroNames = new HashMap<Integer, String>();
      readIds(heroNames, HEROES_JSON, "heroes", "id", "localized_name");
    }
    return heroNames.get(id);
  }

  /**
   * Translates item ids to item names
   *
   * @param id, the item's ID
   * @return The name of the item with that ID
   */
  public static String getItemName(int id) {
    if(itemNames == null) {
      itemNames = new HashMap<Integer, String>();
      readIds(itemNames, ITEMS_JSON, "items", "id", "name");
    }
    return itemNames.get(id);
  }

  /**
   * Translates ability ids to ability names
   *
   * @param id, the abilities's ID
   * @return The name of the abilties with that ID
   */
  public static String getAbilityName(int id) {
    if(abilityNames == null) {
      abilityNames = new HashMap<Integer, String>();
      readIds(abilityNames, ABILITIES_JSON, "abilities", "id", "name");
    }
    return abilityNames.get(id);
  }

  /**
   * Represents Dota 2 towers for one team, can be constructed
   * with the raw int stored in the dota_matches table.
   */
  public static class Towers {

    private final boolean[] towers;
    private short towersStanding;

    public Towers(int num) {
      towersStanding = 0;
      towers = new boolean[11];
      for(int i = 0; i < 11; i++) {
        towers[i] = 1 == (num & 1);
        if(towers[i]) {
          towersStanding++;
        }
        num = num >> 1;
      }
    }

    public short getTowersStanding() {
      return towersStanding;
    }

    public boolean topT1() {
      return towers[0];
    }
    public boolean topT2() {
      return towers[1];
    }
    public boolean topT3() {
      return towers[2];
    }
    public boolean midT1() {
      return towers[3];
    }
    public boolean midT2() {
      return towers[4];
    }
    public boolean midT3() {
      return towers[5];
    }
    public boolean botT1() {
      return towers[6];
    }
    public boolean botT2() {
      return towers[7];
    }
    public boolean botT3() {
      return towers[8];
    }
    public boolean topAncient() {
      return towers[9];
    }
    public boolean botAncient() {
      return towers[10];
    }
  }


  public static enum Barracks {
    TOP_MELEE(0), TOP_RANGED(1), MID_MELEE(2), MID_RANGED(3),
    BOT_MELLE(4), BOT_RANGED(5);

    public final int bitIndex;
    Barracks(int bitIndex) { this.bitIndex = bitIndex; }

    public static boolean raxUp(int status, Barracks rax) {
      return (status >> rax.bitIndex & 1) == 1;
    }

    public static int numRaxesUp(int status) {
      return Integer.bitCount(status);
    }
  }

  /**
   * Utility enum for the column in dota_matches table, the columns names are
   * expected to remain consistent so use of this enum is optional.
   */
  public static enum Columns {
    DIRE_TOWERS_STATUS("dire_tower_staus"),
    RADIANT_TOWERS_STATUS("radiant_tower_status"),
    CLUSTER("cluster"),
    SEASON("season"),
    START_TIME("start_time"),
    GAME_MODE("game_mode"),
    MATCH_SEQ_NUM("match_seq_num"),
    LEAGUE_ID("league_id"),
    FIRST_BLOOD_TIME("first_blood_time"),
    NEGATIVE_VOTES("negative_votes"),
    DURATION("duration"),
    RADIANT_WIN("radiant_win"),
    POSITIVE_VOTES("positive_votes"),
    LOBBY_TYPE("lobby_type"),
    HUMAN_PLAYERS("human_players"),
    PLAYER_DATA("player_data");

    private final String name;
    Columns(String name) { this.name = name; }
    public String toString() { return name; }
    public KijiColumnName columnName() {
      return new KijiColumnName("data", name);
    }
  }

  // ***** enums used in Dota 2. ADDITIONAL ENUMS COULD BE ADDED AS DOTA 2 CHANGES *****

  /**
   * Enum for the different modes a game can be played at
   */
  public static enum GameMode {
    UNKNOWN_ZERO(0), ALL_PICK(1), CAPTAINS_MODE(2), RANDOM_DRAFT(3),
    SINGLE_DRAFT(4), ALL_RANDOM(5), UNKOWN_SIX(6), THE_DIRETIDE(7),
    REVERSE_CAPTAINS_MODE(8), GREEVILING(9), TUTORIAL(10), MID_ONLY(11),
    LEAST_PLAYED(12), NEW_PLAYER_POOL(13), COMPENDIUM(14);

    public int getEncoding() { return rawValue; }
    private final int rawValue;
    GameMode(int rawValue) { this.rawValue = rawValue; }

    public static GameMode fromInt(int i) {
      return GameMode.values()[i];
    }

    public static boolean seriousGame(GameMode gameMode) {
      return gameMode.rawValue < 6 || gameMode == COMPENDIUM || gameMode == LEAST_PLAYED;
    }
  }

  /**
   * Enum for the lobbies (places a player can queue for a match) in Dota 2
   */
  public static enum LobbyType {
    INVALID(-1), PUBLIC_MATCHMAKING(0), PRACTICE(1), TOURNAMENT(2), TUTORIAL(3),
    CO_OP_WITH_BOTS(4), TEAM_MATCH(5), SOLO_QUEUE(6);

    private final int rawValue;
    LobbyType(int rawValue) { this.rawValue = rawValue; }
    public int getEncoding() { return rawValue; }

    public static LobbyType fromInt(Integer i) {

      return LobbyType.values()[i + 1];
    }

    public static boolean seriousLobby(LobbyType lobbyType) {
      return lobbyType == LobbyType.PUBLIC_MATCHMAKING ||
          lobbyType == LobbyType.TOURNAMENT ||
          lobbyType == LobbyType.TEAM_MATCH ||
          lobbyType == LobbyType.SOLO_QUEUE;
    }


  }

  /**
   * Enum for the various leaver statuses a player can get
   */
  public static enum LeaverStatus {
    STAYED(0), // Stayed the whole game
    SAFE_LEAVE(1), // Left when it was safe to leave, or was disconnected but
                   // not timed out when the game ended
    DISCONNECT_ABANDON(2), // Timed out during a disconnect
    ABANDON(3), // Clicked "leave game"
    ABANDON_AND_RECONNECT(4), // Left the game and reconnected
    UNKOWN_FIVE(6),
    UNKNOWN_SIX(7),
    BOT(null);

    private final Integer rawValue;
    LeaverStatus(Integer rawValue) { this.rawValue = rawValue; }
    public Integer getEncoding() { return rawValue; }

    public static LeaverStatus fromInt(Integer i) {
      return (i == null ? LeaverStatus.BOT : LeaverStatus.values()[i]);
    }
  }

  /**
   * Enum for the item slots an item can occupy
   */
  public static enum ItemSlot {
    TOP_LEFT(0), TOP_CENTER(1), TOP_RIGHT(2), BOT_RIGHT(3),
    BOT_CENTER(4), BOT_LEFT(5);


    public final int rawValue;
    ItemSlot(Integer rawValue) { this.rawValue = rawValue; }

    public static ItemSlot getItemSlot(int num) {
      return ItemSlot.values()[num];
    }
  }
}
