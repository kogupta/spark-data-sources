package examples;

import edb.client.DBClient;
import edb.common.ExistingTableException;
import edb.common.Schema;
import edb.common.UnknownTableException;
import edb.server.DBServer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.List;

public abstract class Helper {
  public static final String serverHost = "localhost";
  public static final int serverPort = 50199;

  public void test(String appName, String datasourceName) throws Exception {
    DBServer server = createAndStartServer();

    createClient();

    SparkSession spark = SparkSession.builder()
        .appName(appName)
        .master("local[4]")
        .getOrCreate();

    Dataset<Row> data = createDataset(spark, datasourceName);

    basicDataChecks(data);

    otherTests(data);

    shutdown(server, spark);
  }

  static void shutdown(DBServer server, SparkSession spark) {
    spark.stop();
    server.stop();
  }

  static void basicDataChecks(Dataset<Row> data) {
    System.out.println("*** Schema: ");
    data.printSchema();

    System.out.println("*** Data: ");
    data.show();

    System.out.println("*** Explain: ");
    data.explain(true);

    System.out.println("*** Number of partitions: " + data.rdd().partitions().length);
  }

  protected void createClient() throws ExistingTableException, UnknownTableException {
    DBClient client = new DBClient(serverHost, serverPort);
    client.createTable("myTable", createSchema());
    client.bulkInsert("myTable", populateData());
  }

  static DBServer createAndStartServer() throws IOException {
    System.out.println("*** Example database server starting ...");
    DBServer server = new DBServer(serverPort);
    server.start();
    System.out.println("*** Example database server started");
    return server;
  }

  protected abstract List<edb.common.Row> populateData();

  protected abstract Schema createSchema();

  protected abstract Dataset<Row> createDataset(SparkSession spark, String datasourceName);

  protected abstract void otherTests(Dataset<Row> data);
}