USE wibidota;
CREATE TABLE heroes WITH DESCRIPTION 'Dota 2 hero statistics'
ROW KEY FORMAT (hero_id INT, HASH(SIZE=1))
WITH LOCALITY GROUP data (
  MAXVERSIONS = INFINITY,
  TTL = FOREVER,
  INMEMORY = false,
  COMPRESSED WITH GZIP,
  FAMILY names (
         name "string"
  ),  
  MAP TYPE FAMILY data "double"
);
