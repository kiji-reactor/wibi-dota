package com.wibidata.wibidota;

import com.wibidata.wibidota.avro.Player;
import com.wibidata.wibidota.avro.Players;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.kiji.mapreduce.framework.HFileKeyValue;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.*;
import org.kiji.schema.hbase.HBaseColumnName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;


/**
 * Gatherer that can transfer data from the derived_data section of the
 * dota_matches table to the match_derived_data of the dota_players table
 * using hfiles.
 */
public class DerivedDataToHFiles extends KijiGatherer<HFileKeyValue, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(DerivedDataToHFiles.class);

  private ColumnNameTranslator mColumnNameTranslator = null;
  private EntityIdFactory mEntityIdFactory=  null;
  private KijiCellEncoder mCellEncoder = null;
  private Kiji mKiji = null;

  @Override
  public void setup(GathererContext<HFileKeyValue, NullWritable> context) throws IOException {
    Configuration conf = getConf();

    // Grab the output table from the config file
    KijiURI uri = KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)).build();

    // Get the encoder we want. We need to keep our KijiInstance open so its schemaTable
    // remain open for use by the encoder
    mKiji = Kiji.Factory.open(uri);
    KijiTable table = mKiji.openTable(uri.getTable());
    CellSpec cellSpec = table.getLayout().getCellSpec(new KijiColumnName("derived_data"))
        .setSchemaTable(mKiji.getSchemaTable());
    table.release();
    mCellEncoder = DefaultKijiCellEncoderFactory.get().create(cellSpec);

    // Grab the some other translating tools we need, we assume the
    // table's layout is static
    mColumnNameTranslator = new ColumnNameTranslator(table.getLayout());
    mEntityIdFactory = EntityIdFactory.getFactory(table.getLayout());
    super.setup(context);
  }

  public void setConf(Configuration conf) {
    conf.set("hbase.client.scanner.caching","50");
    conf.set("mapred.tasktracker.map.tasks.maximum", "1");
    super.setConf(conf);
  }


  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    KijiDataRequestBuilder.ColumnsDef def = builder.newColumnsDef();
    def.withMaxVersions(1);

    def.add("data", "player_data");
    def.addFamily("derived_data");
    return builder.addColumns(def).build();
  }

  @Override
  public void gather(KijiRowData input, GathererContext<HFileKeyValue, NullWritable> context) throws IOException {
    NavigableMap<String, Double> data = input.getMostRecentValues("derived_data");
    if(data.size() == 0) {
      return;
    }

    KijiCell<Players> playerCell = input.getMostRecentCell("data", "player_data");
    Players players = playerCell.getData();
    long time = playerCell.getTimestamp();
    for(Player player : players.getPlayers()) {
      Integer accountId = player.getAccountId();
      if(DotaValues.nonAnonPlayer(accountId)) {
        EntityId id = mEntityIdFactory.getEntityId(accountId);
        for(Map.Entry<String, Double> entry : data.entrySet()) {
          final KijiColumnName kijiColumn = new KijiColumnName("match_derived_data", entry.getKey());
          final HBaseColumnName hbaseColumn = mColumnNameTranslator.toHBaseColumnName(kijiColumn);
          final HFileKeyValue mrKey = new HFileKeyValue(
              id.getHBaseRowKey(),
              hbaseColumn.getFamily(),
              hbaseColumn.getQualifier(),
              time,
              mCellEncoder.encode(entry.getValue()));
          context.write(mrKey, NullWritable.get());
        }
      }
    }
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return HFileKeyValue.class;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return NullWritable.class;
  }
}
