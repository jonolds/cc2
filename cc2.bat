copy src\BigramCount.java .
javac BigramCount.java -Xlint:deprecation
jar -cvf BigramCount.jar ./BigramCount*.class
hadoop jar BigramCount.jar BigramCount input output
#cat ./output/part-r-00000
