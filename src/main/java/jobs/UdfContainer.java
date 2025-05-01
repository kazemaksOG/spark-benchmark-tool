package jobs;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class UdfContainer {

    public static void registerUdfs(SparkSession session) {
        session.udf().register("loop_1000", loop_1000, DataTypes.IntegerType);
        session.udf().register("loop_500", loop_500, DataTypes.IntegerType);
        session.udf().register("loop_100", loop_100, DataTypes.IntegerType);
    }

    static UDF1<Integer, Integer> loop_1000 = x -> {
        // Simulate CPU work or artificial delay
        double dummy = 0;
        for (int i = 0; i < 1_000; i++) {
            dummy += Math.sqrt(i);
        }
        return x + (int)(dummy % 100);
    };

    static UDF1<Integer, Integer> loop_500 = x -> {
        // Simulate CPU work or artificial delay
        double dummy = 0;
        for (int i = 0; i < 10_000; i++) {
            dummy += Math.sqrt(i);
        }
        return x + (int)(dummy % 100);
    };

    static UDF1<Integer, Integer> loop_100 = x -> {
        // Simulate CPU work or artificial delay
        double dummy = 0;
        for (int i = 0; i < 100; i++) {
            dummy += Math.sqrt(i);
        }
        return x + (int)(dummy % 100);
    };
}
