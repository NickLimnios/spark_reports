#!/bin/bash
echo run started...
echo ~/repos/spark_reports/sparkreportsapp
cd ~/repos/spark_reports/sparkreportsapp
echo sparkreportsapp starting...
/opt/spark/bin/spark-submit --class "sparkreportsapp" --master local[1] target/scala-2.12/sparkreportsapp_2.12-1.0.jar all