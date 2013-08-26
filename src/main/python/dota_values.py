"""
Similar to DotaValues.java this is intended to translate
raw values as stored in our tables to human readable interpretations.
Currently this is being built as needed so it does not contain a complete
mapping. Requires environment variable WIBIDOTA to be set to the home directory
of wibidota.
"""

import os
import json

HERO_NAMES_MAP = None

WIBIDOTA = os.environ.get("WIBIDOTA")

GAME_MODES = ["GAME_MODE_ZERO", "ALL_PICK", "CAPTAINS_MODE", "RANDOM_DRAFT",
    "SINGLE_DRAFT", "ALL_RANDOM", "GAME_MODE_SIX", "THE_DIRETIDE",
    "REVERSE_CAPTAINS_MODE", "GREEVILING", "TUTORIAL", "MID_ONLY",
    "LEAST_PLAYED", "NEW_PLAYER_POOL", "COMPENDIUM"]

LOBBY_TYPES = ["INVALID", "PUBLIC_MATCHMAKING", "PRACTICE", "TOURNAMENT", 
               "TUTORIAL", "CO_OP_WITH_BOTS", "TEAM_MATCH", "SOLO_QUEUE"]

def get_game_mode(gm_id):
  return GAME_MODES[gm_id]

def get_lobby_type(lobby_type_id):
  return LOBBY_TYPES[lobby_type_id + 1]

def build_map_from_json(filename, container, id_field, name_field):
  m = {}
  if(WIBIDOTA == None):
    raise RuntimeError("environment variable WIBIDOTA is not set!")
  json_values = json.load(open(filename))[container]
  for val in json_values:
    m[int(json.dumps(val[id_field]))] = json.dumps(val[name_field]).strip("\"")
  return m
  

def get_hero_name(hero_id, allow_unknown = False):
  global HERO_NAMES_MAP
  if(HERO_NAMES_MAP == None):
    HERO_NAMES_MAP = build_map_from_json(WIBIDOTA + "/src/main/resources/com/wibidata/wibidota/heroes.json", "heroes", "id", "localized_name")
  if(allow_unknown and hero_id not in HERO_NAMES_MAP):
    return "UNKNOWN"
  return HERO_NAMES_MAP[hero_id]
