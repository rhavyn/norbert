java -Xms1g -Xmx1g -server -XX:PermSize=128m -XX:MaxPermSize=128m -XX:+UseParallelOldGC -jar `dirname $0`/build/sbt-launch.jar "$@"
