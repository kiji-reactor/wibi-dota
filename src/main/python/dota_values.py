"""
Similar to DotaValues.java this is intended to translate
raw values as stored in our tables to human readable interpretations.
Currently this is being built as needed so it does not contain a complete
mapping.
"""


GAME_MODES = ["GAME_MODE_ZERO", "ALL_PICK", "CAPTAINS_MODE", "RANDOM_DRAFT",
    "SINGLE_DRAFT", "ALL_RANDOM", "GAME_MODE_SIX", "THE_DIRETIDE",
    "REVERSE_CAPTAINS_MODE", "GREEVILING", "TUTORIAL", "MID_ONLY",
    "LEAST_PLAYED", "NEW_PLAYER_POOL", "COMPENDIUM"]
def get_game_mode(gm_id):
  return GAME_MODES[gm_id]
