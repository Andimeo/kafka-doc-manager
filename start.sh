rm -rf mongo-connector.log my.log oplog.timestamp

MONGO_ADDR=us0207adbd172004000154.t2dev.tango-dib.gbl
MONGO_PORT=27017

KAFKA_ADDR=54.201.106.80
KAFKA_PORT=9092

cd mongo_connector
export PYTHONPATH=.
cd ..

nohup mongo-connector -m $MONGO_ADDR:$MONGO_PORT -t $KAFKA_ADDR:$KAFKA_PORT -d kafka_zhaoliu_manager -n preferenator.user_preference -v > my.log 2>&1 &
