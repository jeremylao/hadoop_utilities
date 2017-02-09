This is a repositry of snippets of Hadoop utilities that I found useful in my own work. 

The file PassingInputParameters.java is a snippet that helps with taking command line inputs (of locations of files in HDFS) and passes the path names to other classes in the framework.  For example, main will read the command line arguments and create a string that will be stored in 'Configuration'.  When the reducer runs, it will look into configuration and get the string stored in configuration and use that string to create a new Path to use in the reducing class. 

