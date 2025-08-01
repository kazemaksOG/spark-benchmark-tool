SPARK_DAEMON_MEMORY=32g SPARK_DAEMON_JAVA_OPTS="-Dspark.ui.retainedJobs=100000 -Dspark.ui.retainedStages=100000 -Dspark.ui.retainedTasks=10000000" $SPARK_HOME/sbin/start-history-server.sh
