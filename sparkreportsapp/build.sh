#!/bin/bash
echo build started...
echo cd ~/repos/spark_reports/sparkreportsapp
cd ~/repos/spark_reports/sparkreportsapp
echo sbt package
sbt package
echo build ended...
