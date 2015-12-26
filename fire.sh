BASE_DIR=$( dirname $( readlink -f $0 ) )
JAR_FILE=$( ls -1 $BASE_DIR/cloudera-impala-jdbc-example-*.jar 2> /dev/null )

if [ ! -f "$JAR_FILE" ]; then
  $BASE_DIR/build-for-current-cdh.sh
fi

mvn exec:java -Dexec.mainClass=com.project.MRCube.MRCube
