import com.google.gson.Gson;
import config.Config;
import config.User;
import org.apache.commons.compress.utils.FileNameUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.SparkSession;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

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
        String scheduler = args[1];
        try {
            Config config = Config.parseBase(Paths.get(BASE_CONFIG).toAbsolutePath().toString());
            DateTimeFormatter formatTime = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH_mm");
            String benchName = "bench_"
                    + scheduler
                    + "_"
                    + FilenameUtils.removeExtension(FilenameUtils.getName(workloadLocation))
                    + "_";
            SparkSession spark = SparkSession.builder().appName(benchName)
                    .config(config.getSparkConfig())
                    .getOrCreate();
            LOGGER.log(INFO, "Created spark session with app name {}", benchName);

            String benchNameFile = benchName + LocalDateTime.now().format(formatTime) + ".json";
            // get user workloads and run them
            ArrayList<User> users = Config.parseUsers(workloadLocation);
            config.setUsers(users);
            LOGGER.log(INFO, "Starting benchmarks {}", benchName);
            runUserBenchmark(spark, config, benchNameFile);

            if (config.isHoldThread()) {
                Thread.sleep(10000000);
            }

        } catch (IOException | InterruptedException e) {
            LOGGER.log(ERROR, "Error occurred while parsing or running the config:" + e.getMessage());
        }

    }

    private static void runUserBenchmark(SparkSession spark, Config config, String benchName) throws InterruptedException {
        ArrayList<Thread> threads = new ArrayList<>();
        // Start all users in parallel and then wait for them to finish
        for (User user : config.getUsers()) {
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
