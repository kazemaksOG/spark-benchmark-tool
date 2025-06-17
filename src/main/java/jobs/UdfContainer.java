package jobs;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class UdfContainer {

    private static final double accountForUnderestimation = 1.3;
    private static int convertRuntime(double runtime) {

        double converted = 3.216105 * Math.pow(runtime, 1.221352) * accountForUnderestimation;
        int iterations = (int)(converted);

        return Math.max(1, iterations);
    }
    // Factory method to generate a UDF dynamically
    public static UDF1<Integer, Integer> generateLoopCustom(double runtime_s) {

        System.out.println("runtime: " + runtime_s);
        System.out.println("converted: " + convertRuntime(runtime_s));
        return x -> {
            int iterations = convertRuntime(runtime_s);
            double dummy = 0;
            for (int i = 0; i < iterations; i++) {
                dummy += Math.sqrt(i);
            }
            return x + (int) (dummy % 100);
        };
    }

    public static void registerUdfs(SparkSession session) {
        session.udf().register("loop_1000", loop_1000, DataTypes.IntegerType);
        session.udf().register("loop_500", loop_500, DataTypes.IntegerType);
        session.udf().register("loop_100", loop_100, DataTypes.IntegerType);
        session.udf().register("loop_20", loop_20, DataTypes.IntegerType);
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
        for (int i = 0; i < 500; i++) {
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

    static UDF1<Integer, Integer> loop_20 = x -> {
        // Simulate CPU work or artificial delay
        double dummy = 0;
        for (int i = 0; i < 20; i++) {
            dummy += Math.sqrt(i);
        }
        return x + (int)(dummy % 100);
    };
}
