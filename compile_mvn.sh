set -e

MVN=../spark/build/mvn

# java needs to be loaded for das5
module load java/jdk-17

$MVN package
$MVN -f ./schedulers/RandomScheduler package
$MVN -f ./schedulers/UserFairScheduler package 
$MVN -f ./schedulers/ShortestFirstScheduler package
