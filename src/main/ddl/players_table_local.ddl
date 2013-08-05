USE wibidota;
CREATE TABLE dota_players
ROW KEY FORMAT (account_id LONG, HASH(SIZE=1))
PROPERTIES (NUMREGIONS = 4)
WITH LOCALITY GROUP player_data (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY data (
          match_id "long",
          dire_towers_status "int",
          radiant_towers_status "int",
          dire_barracks_status "int",
          radiant_barracks_status "int",
          cluster "int",
          season ["null", "int"],
          game_mode "int",
          match_seq_num "long",
          league_id "int",
          first_blood_time "int",
          negative_votes "int",
          duration "int",
          radiant_win "boolean",
          positive_votes "int",
          lobby_type ["null", "int"],
          human_players "int",
          player CLASS com.wibidata.wibidota.Player,
          other_players CLASS com.wibidata.wibidota.avro.Players

  ),
  MAP TYPE FAMILY match_derived_data "double",
  MAP TYPE FAMILY derived_data "double"
);

