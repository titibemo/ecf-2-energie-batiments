

echo " Soumission du job: $1"
echo " Spark UI disponible sur: http://localhost:4040"
echo ""

docker exec -it spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    --executor-cores 1 \
    --total-executor-cores 6 \
    /apps/$1
