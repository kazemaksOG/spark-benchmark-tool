set -e

MVN=../frameworks/spark-custom/build/mvn

# java needs to be loaded for das5
source master-env.sh

# Add maven dependancy, only needed once
# $MVN install:install-file -Dfile=$SPARK_HOME/core/target/scala-2.13/spark-core_2.13-4.1.0-SNAPSHOT.jar -DgroupId=org.apache.spark -DartifactId=spark-core_2.12 -Dversion=3.5.4-custom -Dpackaging=jar


$MVN package
$MVN -f ./schedulers/RandomScheduler package
$MVN -f ./schedulers/UserFairScheduler package 
$MVN -f ./schedulers/ShortestFirstScheduler package
$MVN -f ./schedulers/ClusterFairScheduler package
$MVN -f ./schedulers/UserClusterFairScheduler package
$MVN -f ./estimators/OraclePerformanceEstimator package
