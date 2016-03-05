rm mp1.jar
cd mp1_classes
rm P2*.*
rm P1*.*
cd ..

echo 'Compile new jar'

javac -classpath hadoop-2.6.4/share/hadoop/common/hadoop-common-2.6.4.jar:hadoop-2.6.4/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.4.jar -d mp1_classes src/main/java/P1.java src/main/java/P2.java
jar -cvf mp1.jar -C mp1_classes/ .
hadoop-2.6.4/bin/hadoop jar mp1.jar P1

