all:
	export TEMP_HADOOP_CLASSPATH=$(hadoop classpath)
	javac -classpath ${TEMP_HADOOP_CLASSPATH} -d compiled PageRank.java 
	jar -cvf PageRank.jar -C compiled/ .
	hadoop jar PageRank.jar org.limonadev.PageRank group/ input/ output/



nani:
	hadoop jar PageRank.jar org.limonadev.PageRank gs://test_bucket_limonadev/ input/ output/