#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JAR=${DIR}/../target/scala-2.10/extract_0.0.1.jar
CLASS=com.chrisomeara.pillar.cli.App
JAVA_OPTIONS="-Dlog4j.configuration=pillar-log4j.properties"
java -cp $JAR $JAVA_OPTIONS $CLASS $*