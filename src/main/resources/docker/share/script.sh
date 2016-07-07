/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties &

sleep 3s
echo "ZOOKEEPER UP !!!!!!!!!!!"

/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &
echo "KAFKA UP !!!!!!!!!!!"

sudo service ssh restart

echo "SSH RESTART !!!!!!!!!!!"
tail -f $(echo)

