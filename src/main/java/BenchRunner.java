import com.google.gson.Gson;
import config.Config;
import config.User;
import config.Workload;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.StageListener;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public class BenchRunner {

    static String BASE_CONFIG = "configs/base_config.json";
    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 2) {
            LOGGER.log(ERROR, "Usage: BenchRunner <base_config> <scheduler_name>");
            return;
        }
        String workloadLocation = args[0];
        String schedulerName = args[1];
        try {
            Config config = Config.parseBase(Paths.get(BASE_CONFIG).toAbsolutePath().toString());
            DateTimeFormatter formatTime = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH_mm");
            String benchName = "bench_"
                    + schedulerName
                    + "_"
                    + getName(workloadLocation);
            SparkSession spark = SparkSession.builder().appName(benchName)
                    .config(config.getSparkConfig())
                    .getOrCreate();
            LOGGER.log(INFO, "Created spark session with app name {}", benchName);

            StageListener s = new StageListener();
            spark.sparkContext().addSparkListener(s);
            LOGGER.log(INFO, "Listener added");

            String benchNameFile = benchName + LocalDateTime.now().format(formatTime) + ".json";
            // get user workloads and run them
            ArrayList<User> users = Config.parseUsers(workloadLocation);
            config.setUsers(users);
            LOGGER.log(INFO, "Starting benchmarks {}", benchName);

            // print executor info
            log_executor_data(spark);

            runUserBenchmark(spark, config, benchNameFile);

            if (config.isHoldThread()) {
                Thread.sleep(10000000);
            }

        } catch (IOException | InterruptedException e) {
            LOGGER.log(ERROR, "Error occurred while parsing or running the config:" + e.getMessage());
        }

    }

    private static String getName(String path) {
        String fileName = new File(path).getName();
        return fileName.substring(0, fileName.lastIndexOf('.'));
    }

    private static void log_executor_data(SparkSession spark) {
        RuntimeConfig conf = spark.conf();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        // Print number of executors
        String numExecutors = conf.get("spark.executor.instances", "Dynamic");
        String coresPerExecutor = conf.get("spark.executor.cores", "Unknown");
        LOGGER.log(INFO, "\n### Spark Executor Information ###" + "\nNumber of Executors: " + numExecutors + "\nCores per Executor: " + coresPerExecutor);

        // Get executor details (node locations)
        Map<String, Tuple2<Object, Object>> executorInfo =
                JavaConverters.mapAsJavaMap(sc.sc().getExecutorMemoryStatus());

        LOGGER.log(INFO, "\n### Executor Memory Status ###");
        for (Map.Entry<String, Tuple2<Object, Object>> entry : executorInfo.entrySet()) {
            String executorId = entry.getKey();
            long totalMemory = (long) entry.getValue()._1();
            long usedMemory = (long) entry.getValue()._2();

            LOGGER.log(INFO, "Executor: " + executorId + "\n  - Total Memory: " + (totalMemory / (1024 * 1024)) + " MB" + "\n  - Used Memory: " + (usedMemory / (1024 * 1024)) + " MB");
        }


        // Get total available cores
        int totalCores = sc.defaultParallelism();
        LOGGER.log(INFO, "\nTotal Available Cores: " + totalCores);
    }

    private static void runUserBenchmark(SparkSession spark, Config config, String benchName) throws InterruptedException {


        // Warm up the Spark session
        if( config.getWarmup() != null) {
            for(Workload workload : config.getWarmup()) {
                workload.setSpark(spark);
                workload.run();
            }
        }

        ArrayList<Thread> threads = new ArrayList<>();
        // Start all users in parallel and then wait for them to finish
        for (User user : config.getUsers()) {
            // Reset bench start times to exclude warmup time
            user.resetBenchStartTime();
            // Start up all user workloads
            user.setSpark(spark);
            Thread thread = new Thread(user);
            thread.start();
            threads.add(thread);
        }
        for (Thread thread : threads) {
            thread.join();
        }

        Gson gson = new Gson();
        String json = gson.toJson(config);
        Path filePath = Paths.get("target/bench_outputs/" + benchName).toAbsolutePath();
        try {
            Files.createDirectories(filePath.getParent());
            try(FileWriter writer = new FileWriter(filePath.toString())) {
                writer.write(json);
                LOGGER.log(INFO, "Benchmark results written to {}", filePath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


}
