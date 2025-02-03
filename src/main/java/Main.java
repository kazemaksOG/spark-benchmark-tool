
import config.Config;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.PoissonWait;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
        System.out.printf("Hello and welcome!");
//        PoissonWait wait = new PoissonWait("Job1", 20);
//
//        double acc = 0;
//        for(int i = 0; i < 10; i++ ) {
//            acc+= wait.getNextWaitMillis();
//            System.out.println(acc / 1000 / 60);
//        }


        Config config = null;
        try {
            config = Config.parseBase("configs/base_config.json");
        } catch (IOException e) {
            System.out.println("Couldn't read the file " +e.getMessage());
        }

        System.out.println(config.toString());
        SparkSession spark = SparkSession.builder().appName("Bench Runner")
                .master(config.getMaster())
                .config(config.getSparkConfig())
                .getOrCreate();
        Dataset<Row> parquet = spark.read().parquet("resources/fhvhv_tripdata_2024-08.parquet");
        parquet.printSchema();

        Dataset<Row> mappedParquet = parquet.alias("taxiA").join(parquet.alias("taxiB"), col("taxiA.pickup_datetime").equalTo(col("taxiB.dropoff_datetime")));
//        mappedParquet.orderBy(col("taxiA.trip_time"));

        Row[] collected = (Row[]) mappedParquet.select("taxiA.trip_time").take(20);

        int i = 0;
        for(Row row : collected) {
            System.out.println(row);
            if(i++ > 30) {
                break;
            }
        }

//        Dataset<Row> subsetParquet = spark.sql("select overlay_scalar, rawmeasured_overlay_x, measureprocessjob_recipename, exposureprocessjob_equipment_equipmentid  from " + "parquet_file");
//        Dataset<Row> mappedParquet = subsetParquet.withColumn("sum", subsetParquet.col("rawmeasured_overlay_x").plus(subsetParquet.col("measureprocessjob_recipename")));
//        Dataset<Row> reducedData = mappedParquet.agg(sum(mappedParquet.col("sum").alias("total")));

//        reducedData.show();
        System.out.println("END\n\n\n");
    }
}