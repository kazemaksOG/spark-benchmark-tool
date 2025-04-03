import org.apache.spark.scheduler.*;
import org.apache.spark.storage.RDDInfo;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class OraclePerformanceEstimator implements PerformanceEstimatorInterface {
    private static final String JOB_CLASS_PROPERTY = "job.class";
    private static final long DEFAULT_RUNTIME = 1000L;
    private final Map<Integer, StageProfile> stageIdToProfile = new ConcurrentHashMap<>();
    private final Map<String, StageProfile> stageSecIdToProfile = new ConcurrentHashMap<>();
    private final Map<Integer, CompletableFuture<Void>> stageIdToFuture = new ConcurrentHashMap<>();
    private final StageListener listener;
    private BlockingQueue<SparkListenerEvent> queue;
    private Thread estimationThread;

    public OraclePerformanceEstimator() {
        this.queue = new LinkedBlockingQueue<>();
        listener = new StageListener(queue);

        // Add job runtimes
        stageSecIdToProfile.put("jobs.implementations.ShortOperation-compute",
                new StageProfile("jobs.implementations.ShortOperation-compute", -1, 20000L));
        stageSecIdToProfile.put("jobs.implementations.LongOperation-compute",
                new StageProfile("jobs.implementations.LongOperation-compute", -1, 235740L));
        stageSecIdToProfile.put("jobs.implementations.SuperShortOperation-compute",
                new StageProfile("jobs.implementations.SuperShortOperation-compute", -1, 4000L));
    }



    @Override
    public long getRuntimeEstimate(int stageId) {
        StageProfile profile = stageIdToProfile.get(stageId);
        if (profile != null) {
            return profile.getEstimate();
        } else {
            CompletableFuture<Void> future = stageIdToFuture.get(stageId);
            if(future != null) {
                System.out.println("####### ERROR: profile still executing: " + stageId + " is done:" + future.isDone());
            } else {
                System.out.println("####### ERROR: no profile found for: " + stageId);
            }
            return DEFAULT_RUNTIME;
        }
    }

    @Override
    public void registerListener(LiveListenerBus listenerBus) {
        listenerBus.addToSharedQueue(listener);
    }

    @Override
    public void startEstimationThread() {
        estimationThread = new Thread(() -> {
            try {
                for(;;) {
                    SparkListenerEvent event = queue.take();

                    if (event instanceof SparkListenerStageSubmitted stageEvent) {
                        int stageId = stageEvent.stageInfo().stageId();
                        System.out.println("####### Received event: stage submitted: " + stageId);
                        CompletableFuture<StageProfile> futureEstimate = CompletableFuture.supplyAsync(() -> estimateStage(stageEvent));
                        CompletableFuture<Void> result = futureEstimate.thenAccept(profile -> {
                            System.out.println("####### finished profile for stage: " + stageId);
                            stageIdToProfile.put(stageId, profile);
                        });
                        stageIdToFuture.put(stageId, result);
                    }
                }


            } catch (InterruptedException e) {
                System.out.println("####### WARNING: Shutting down performance thread");
            }
        });
        estimationThread.setDaemon(true);
        estimationThread.start();
    }

    @Override
    public void shutdown() {
        estimationThread.interrupt();
    }

    public StageProfile estimateStage(SparkListenerStageSubmitted stageSubmitted) {
        String jobClass = stageSubmitted.properties().getProperty(JOB_CLASS_PROPERTY, "DEFAULT");
        String stageSection = getStageSection(stageSubmitted);
        String stageSecId = jobClass + "-" + stageSection;

        int stageId = stageSubmitted.stageInfo().stageId();
        System.out.println("####### Stage section id: " + stageSecId + " for stageid: " + stageId);

        return stageSecIdToProfile.computeIfAbsent(stageSecId, key -> {
            System.out.println("####### no profile found for: " + stageSecId);
            return new StageProfile(stageSecId, stageId, 1000L);
        });
    }


    private static final String READ_SECTION = "ParallelCollectionRDD";
    private static final String WRITE_SECTION = "ShuffledRowRDD";
    private static final String COMPUTE_SECTION = "FileScanRDD";

    private String getStageSection(SparkListenerStageSubmitted submitted) {
        String section = "UNKNOWN";
        List<RDDInfo> rddInfos =  JavaConverters.seqAsJavaList(submitted.stageInfo().rddInfos());
        for (var rdd : rddInfos) {
            String name = rdd.name();
            if (name.contains(READ_SECTION)) {
                section = "read";
            } else if (name.contains(WRITE_SECTION)) {
                section = "write";
            } else if (name.contains(COMPUTE_SECTION)) {
                section = "compute";
            }
        }
        return section;
    }


}
