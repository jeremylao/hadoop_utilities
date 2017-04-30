#!/bin/bash  -e

program=$1
suffix=".java"

javac -classpath "$(yarn classpath)" -d . $program$suffix

jar -cvf FRED.jar *.class

hadoop jar FRED.jar $program /user/jjl359/FRED/GDP/GDP.csv /user/jjl359/FRED/GDP/average