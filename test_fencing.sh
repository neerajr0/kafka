#!/bin/bash

TOPIC="fence-test"
BROKER=localhost:9092
DURATION_SEC=30
TARGET_MBPS=50
BYTES_PER_MSG=1024      # 1KB
MESSAGES_PER_SEC=$(( (TARGET_MBPS * 1024 * 1024) / BYTES_PER_MSG ))

echo "Step 1: Create topic"
./bin/kafka-topics.sh --bootstrap-server $BROKER --create --topic $TOPIC --partitions 1 --replication-factor 1 || true

echo "Step 2: Produce at ${TARGET_MBPS} MBps for ${DURATION_SEC} sec (approx)"
START_TIME=$(date +%s)
while [ $(( $(date +%s) - START_TIME )) -lt $DURATION_SEC ]; do
  yes "$(head -c $BYTES_PER_MSG < /dev/zero | tr '\0' 'X')" | \
    head -n $MESSAGES_PER_SEC | \
    ./bin/kafka-console-producer.sh --bootstrap-server $BROKER --topic $TOPIC > /dev/null
done
echo "✅ Done producing. Check logs for fencing..."

read -p "Press Enter to continue once fencing has been confirmed in logs..."

echo "Step 3: Confirm metadata still includes a broker"
./bin/kafka-topics.sh --bootstrap-server $BROKER --list

echo "Choose mitigation strategy:"
echo "1) Delete topic"
echo "2) Set retention to 0ms"
read -p "Enter choice (1 or 2): " choice

case $choice in
  1)
    echo "Step 4: Deleting topic"
    ./bin/kafka-topics.sh --bootstrap-server $BROKER --delete --topic $TOPIC
    echo "✅ Topic deletion requested. Wait for deletion to complete and unfencing to be triggered..."
    ;;
  2)
    echo "Step 4: Setting retention to 0ms"
    ./bin/kafka-configs.sh --bootstrap-server $BROKER \
      --alter --entity-type topics --entity-name $TOPIC \
      --add-config retention.ms=0
    echo "✅ Retention set to 0. Wait for logs to be deleted and unfencing to be triggered..."
    ;;
  *)
    echo "Invalid choice"
    exit 1
    ;;
esac

read -p "Press Enter to finish once you see unfencing triggered in logs..."

echo "🟢 Test complete. You should now see broker back to UNFENCED state."
