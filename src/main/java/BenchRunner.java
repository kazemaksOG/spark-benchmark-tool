import com.google.gson.Gson;
import config.Config;
import config.User;
import org.apache.commons.compress.utils.FileNameUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.SparkSession;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

public class BenchRunner {

    static String BASE_CONFIG = "/var/scratch/dkazemak/performance_test/configs/base_config.json" ;;

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) {
            System.out.println("Expected argument: config");
        }
        String workloadLocation= args[0];
        try {
            Config config = Config.parseBase(BASE_CONFIG.toString());
            String benchName = "bench_" + FilenameUtils.removeExtension(FilenameUtils.getName(workloadLocation));
            SparkSession spark = SparkSession.builder().appName(benchName)
                    .config(config.getSparkConfig())
                    .getOrCreate();

            // We set the configs through command line for easier scripting
            String scheduleMode = spark.conf().get("spark.scheduler.mode");
            DateTimeFormatter formatTime = DateTimeFormatter.ofPattern("dd-MM-yyyy_HH_mm");
            benchName += "_" + scheduleMode + "_"+ LocalDateTime.now().format(formatTime) + ".json";

            // get user workloads and run them
            ArrayList<User> users = Config.parseUsers(workloadLocation);
            config.setUsers(users);
            runUserBenchmark(spark, config, benchName);

            if(config.isHoldThread()) {
                Thread.sleep(10000000);
            }

        } catch (IOException | InterruptedException e ) {
            System.err.println("Error occurred while parsing or running the config:" + e.getMessage());
        }

    }

    private static void runUserBenchmark(SparkSession spark, Config config, String benchName) throws InterruptedException {
        ArrayList<Thread> threads = new ArrayList<>();
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

        try (FileWriter writer = new FileWriter(Paths.get("target/"+ benchName).toAbsolutePath().toString())) {
            writer.write(json);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }



}
