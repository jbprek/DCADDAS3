# Spark Docker Use

## docker-compose example

Example with one master and two workers

```yaml
# Bitnami based latest
version: '3'
services:
  spark-master:
    image: bitnami/spark:latest
    container_name: spark-master
    environment:
      - SPARK_MODE=master
    ports:
      - 8080:8080
      - 7077:7077
    volumes:
      - ./shared-data:/data
      - ./shared-app:/app
    networks:
      - spark-net

  spark-worker-1:
    image: bitnami/spark:latest
    container_name: spark-worker-1
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
    volumes:
      - ./shared-data:/data
      - ./shared-app:/app
    networks:
      - spark-net

  spark-worker-2:
    image: bitnami/spark:latest
    container_name: spark-worker-2
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
    volumes:
      - ./shared-data:/data
      - ./shared-app:/app
    networks:
      - spark-net

networks:
  spark-net:


```

## Access master shell
```bash
docker exec -it spark-master bash
```