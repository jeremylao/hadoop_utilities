#!/bin/bash  -e

program=$1
suffix=".java"

javac -classpath "$(yarn classpath)" -d . $program$suffix

jar -cvf FRED.jar *.class

hadoop jar FRED.jar $program /user/jjl359/FRED/GDP/GDP.csv /user/jjl359/FRED/GDP/count


#directory=$1
#hdfs dfs -rm -r /user/jjl359/page_rank_bonus_two/$directory