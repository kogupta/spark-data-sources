package examples;

import edb.common.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class JReadNamedTable extends Helper {
  @Override
  protected List<edb.common.Row> populateData() {
    List<edb.common.Row> toInsert = new ArrayList<>();
    edb.common.Row r1 = new edb.common.Row();
    r1.addField(new edb.common.Row.Int64Field("u", 100));
    r1.addField(new edb.common.Row.DoubleField("v", 200.2));
    toInsert.add(r1);
    edb.common.Row r2 = new edb.common.Row();
    r2.addField(new edb.common.Row.Int64Field("u", 300));
    r2.addField(new edb.common.Row.DoubleField("v", 400.4));
    toInsert.add(r2);

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
    // This is where we read from our DataSource. Notice how we use the
    // fully qualified class name and provide the information needed to connect to
    // ExampleDB using options. Also, notice we specify the name of the table
    // as an option.
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
    // Since this DataSource only supports reading from one executor,
    // there will only be a single partition.
    System.out.println("*** Number of partitions: " + data.rdd().partitions().length);
  }

  public static void main(String[] args) throws Exception {
    JReadNamedTable table = new JReadNamedTable();
    table.test("JReadNamedTable", "datasources.FlexibleRowDataSource");
  }
}