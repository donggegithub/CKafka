#!bin/bash
if [ $# != 2 ]
then
echo "USAGE: $0 ./kafka-topic.sh node0:2181"
exit 1;
fi

topics=`$1  --zookeeper  $2  --list`
for topic in $topics:
do 
	$1   --zookeeper $2  --describe --topic $topic | head -n 1
done

