#!/bin/sh
for file in ${ANALYSIS_HOME}/lib/*.jar;
do
    CLASSPATH=$CLASSPATH:$file
done
export CLASSPATH=$CLASSPATH
ANALYSIS=/app/dempe/share-analysis

exec java -Djava.ext.dirs=${ANALYSIS_HOME}/lib -cp ${ANALYSIS}/share-mds-1.0-SNAPSHOT.jar cn.sharesdk.analysis.Startup
