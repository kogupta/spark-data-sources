package examples;

import edb.common.Schema;
import edb.server.DBServer;
import examples.utils.RDDUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class JReadParallel extends Helper {
  @Override
  protected List<edb.common.Row> populateData() {
    List<edb.common.Row> toInsert = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      edb.common.Row r = new edb.common.Row();
      r.addField(new edb.common.Row.Int64Field("u", i * 100));
      r.addField(new edb.common.Row.DoubleField("v", i + 0.2));
      toInsert.add(r);
    }
    return toInsert;
  }

  @Override
  protected Schema createSchema() {
    Schema schema = new Schema();
    schema.addColumn("u", Schema.ColumnType.INT64);
    schema.addColumn("v", Schema.ColumnType.DOUBLE);
    return schema;
  }

  @Override
  protected Dataset<Row> createDataset(SparkSession spark, String datasourceName) {
    //
    // This is where we read from our DataSource. Notice how we use the
    // fully qualified class name and provide the information needed to connect to
    // ExampleDB using options. This time we'll ExampleDB's default number of table
    // partitions, 4, so we don't need to specify it.
    //
    Dataset<Row> data = spark.read()
        .format(datasourceName)
        .option("host", serverHost)
        .option("port", serverPort)
        .option("table", "myTable")
        .load();

    return data;
  }

  @Override
  protected void otherTests(Dataset<Row> data) {
    //
    // Since this DataSource supports reading from one executor,
    // there will be a multiple partitions.
    //
    RDDUtils.analyze(data);
  }

  @Override
  public void test(String appName, String datasourceName) throws Exception {
    DBServer server = createAndStartServer();

    createClient();

    SparkSession spark = SparkSession.builder()
        .appName(appName)
        .master("local[4]")
        .getOrCreate();

    Dataset<Row> data = createDataset(spark, datasourceName);
    basicDataChecks(data);
    //
    // Since this DataSource supports reading from one executor, there will be a multiple partitions.
    //
    RDDUtils.analyze(data);

    //
    // We can specify a different number of partitions too, overriding ExampleDB's default.
    //
    data = spark.read()
        .format(datasourceName)
        .option("host", serverHost)
        .option("port", serverPort)
        .option("table", "myTable")
        .option("partitions", 6) // number of partitions specified here
        .load();
    basicDataChecks(data);
    //
    // This time we see six partitions.
    //
    RDDUtils.analyze(data);

    shutdown(server, spark);
  }

  public static void main(String[] args) throws Exception {
    JReadParallel jReadParallel = new JReadParallel();
    jReadParallel.test("JReadParallel", "datasources.ParallelRowDataSource");
  }
}