package examples;

import edb.common.Schema;
import examples.utils.RDDUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;


public class JReadPartitionAware extends Helper {
  @Override
  protected List<edb.common.Row> populateData() {
    List<edb.common.Row> toInsert = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      edb.common.Row r = new edb.common.Row();
      // String column with four distinct values for clustering
      r.addField(new edb.common.Row.StringField("g", "G_" + (i % 4)));
      r.addField(new edb.common.Row.Int64Field("u", i * 100));

      toInsert.add(r);
    }
    return toInsert;
  }

  @Override
  protected Schema createSchema() {
    Schema schema = new Schema();
    schema.addColumn("g", Schema.ColumnType.STRING);
    schema.addColumn("u", Schema.ColumnType.INT64);

    return schema;
  }

  @Override
  protected Dataset<Row> createDataset(SparkSession spark, String datasourceName) {
    //
    // This is where we read from our DataSource. Notice how we use the
    // fully qualified class name and provide the information needed to connect to
    // ExampleDB using options. We specify two partitions so that each can be expected
    // to contain two clusters.
    //
    Dataset<Row> data = spark.read()
        .format(datasourceName)
        .option("host", serverHost)
        .option("port", serverPort)
        .option("table", "myTable")
        .option("partitions", 2) // number of partitions specified here
        .load();

    return data;
  }

  @Override
  protected void otherTests(Dataset<Row> data) {
    RDDUtils.analyze(data);

    Dataset<Row> aggregated = data.groupBy(col("g")).agg(sum(col("u")));

    System.out.println("*** Query result: ");
    aggregated.show();

    RDDUtils.analyze(aggregated);
  }

  public static void main(String[] args) throws Exception {
    JReadPartitionAware partitionAware = new JReadPartitionAware();
    partitionAware.test("JReadPartitionAware", "datasources.PartitioningRowDataSource");
  }
}