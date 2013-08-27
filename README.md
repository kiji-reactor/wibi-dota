Wibidata's collection and analysis of Dota 2 statistics

Building the Project
-------

Use mvn install to setup the project. Use mvn package to recompile it. Some of the classes require the gson library for parsing json text. The needed jar will be copied to the target/lib folder during the install phase. Add this lib folder to the KIJI_CLASSPATH environment variable before running kiji commands. 

Collecting And Importing the Data
-------

Data is collected through the script src/main/python/dota_slurp.py (see documentation for details). The script produces gziped files containing a json object per a line containing the match data. These files should be imported to hdfs. The data can then be ported into Kiji where it is stored using match_ids as row keys. Build the table by using:

```
kiji install --kiji=wibidota
kiji-schema-shell --file=src/main/ddl/matches_table.ddl 
```

Currently the table is built with 64 regions. If you want to use a different number of region you will need to edit the ddl script. There is also a script matches_table_local.ddl which builds the same table but with 1) only four regions 2) compressed with gzip rather the snappy, which can be used to build and test the table on a local machine.

The data can then be ported to this table using DotaMatchesBulkImporter.java. If you are using snappy compression set KIJI_JAVA_OPT=-Djava.library.path=/path/to/snappy so that kiji can compress the files.

```
kiji bulk-import --importer=com.wibidata.wibidota.DotaMatchBulkImporter \
  --input="format=text file=hdfs://path/to/match/files/directory/" \
  --output="format=hfile file=hdfs://path/to/tmp/file nsplits=64 table=kiji://.env/wibidota/dota_matches" \
  --lib={WIBIDOTA_HOME}/target/lib
kiji bulk-load --table=kiji://.env/wibidota/dota_matches --hfile=hdfs://path/to/tmp/file
```

An additional table exists that pivots the data onto a player centric model using account_ids (of non-anonymous accounts) as row keys. This table is also compressed with snappy and is built with 64 regions and also contain a script to build a 'local' version. This table can be built with

```
kiji-schema-shell --file=src/main/ddl/player_table.ddl
```

Data can be imported to this table using com.wibidata.wibidota.DotaPlayersBulkImporter from the raw json in the same manner as the dota_matches table. 

```
kiji bulk-import --importer=com.wibidata.wibidota.DotaPlayersBulkImporter \
  --input="format=text file=hdfs://path/to/match/files/directory/" \
  --output="format=hfile file=hdfs://path/to/tmp/file nsplits=64 table=kiji://.env/wibidota/dota_players" \
  --lib={WIBIDOTA_HOME}/target/lib
kiji bulk-load --table=kiji://.env/wibidota/dota_matches --hfile=hdfs://path/to/tmp/file
```

Finally there is a 'heroes' table to be used to keep track of per-hero statistics. Build the table with:

```
kiji-schema-shell --file=src/main/ddl/heroes.ddl
```

Import hero names into it using

```
hadoop fs -copyFromLocal src/main/resources/com/wibidata/wibidota/heroes.json;
express job "$WIBIDOTA/lib/wibidota-1.0.0.jar" com.wibidata.wibidota.express.AddHeroNames --hero_names heroes.json --hdfs
```


Interpreting the Data
-------

Examine the ddl files to see the table layouts. We store all non-derived data at the timestamp that 
the game it corresponds to started at. Thus the dota_matches table
will only ever have one value for a given row and column and dota_player may have many. Note Kiji uses
milliseconds timestamp (in contrast to Vavle's API which returns seconds).

Both the matches and players table store the data in a raw form as it was gathered from the Dota API. 
There is one notable exception, we store account_ids as 32bit signed ints where as Valve stores 
them as 32bit unsigned ints. As a consequence some of our account_ids may be negative, however 
thus far this is only the case for account_ids that Valve has set to 0xFFFFFFFF to indicate 
anonymous accounts (in the table these accounts will have an id of -1). The utility class 
DotaValues.java contains methods for translating the raw data into more human readable form, 
including mapping the integer ids for abilities, items, and heroes into there respective names. 
It depends on, and is only as accurate as, the json files in src/main/resources. The python script
get_heroes can be used to retrieve the hero id json file, but Vavle does not provide an API
to aquire ability and item mappings so they must be found through an outside source.

A reasonable overview of how to undersand these values can be found at 
http://dev.dota2.com/showthread.php?t=58317. This threads is, however, slightly out
of date. Look at DotaValues.java for more up to date value mappings. 

Some additional things to be aware of, game_mode of 0 appears exclusively during the first 50 millionish games and stop appearing 
around the same time the other games modes start appearing. As such we suspect it indicates
games that were played before Vavle starting recording game modes, it is uncofirmed but seems
likely that these were simply AP games. No games have a game_mode of 6 and it is currently unkown
what that would indicate. Finally note that games and can start with less than 10 players if a player disconnects early, meaning some
PMM games can have under 10 players. 

Data Integrity
----------

Several tools exists to double check that the data is in a good state. The DotaCheckValues gatherer
examines the rows in the dota_matches table and ensures the values fall within sane defaults.
To check all the matches have been imported the express job CountUniqueMatchIds can be used to the 
number of unique match_ids present in the json files which can be compared to the number of rows
int the dota_matches table. The express job compareMatchIds can be used to do a full cross check 
the match ids stored in the Kiji table with the ids stored in raw json files to identiify
any missing or extra ones. The python script 
check_match_file_continuity will check to make sure the gzip files produced by dota_slurp cover a 
continious range of match sequence numbers based on there file names.

Derived Data
--------
All tables contain map type family columns intended to store the results of analysis that might
be useful in the future. For the dota_players table the intention is that match_derived_data
contains per-match data, inserted at the timestamp the match in question took place. Meanwhile
derived_data is for per player derived data. With the exception of match_derived_data data 
is in general expected to be inserted at the timestamp is was derived.

A useful bit of derived is 'real_match' built to help other jobs know which jobs
were typical, serious Dota 2 games. See RealMathProducer for specifics. DerivedData can
be ported from dota_matches/derived_data to dota_players/match_derived_data using 
PortDerivedToHfiles.java

Data Analysis
---------

The results of some of the analysis that was done is stored in the results folder. The informal format
to subdirectories in this folder is to include a INFO file with a description of the data obtained
along with an outline of what code was used to aquire it. If the unformatted data is not too large a
'raw' folder contains the output as was returned by mapreduce. Formatted versions of that data are
included with the INFO file.
