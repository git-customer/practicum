# Общее описание

В составе решения присутствует кластер Kafka и приложение на Python, выполняющее функции producer и consumer. Оба компонета работают на базе контейнеров docker.
Параметры кластера Kafka заданы в файле cluster/docker-compose.yml. Кластер состоит из 3 узлов broker, 1 узла zookeeper, а также узла kafka-ui.
Для сохранения состояния кластера при выключении настроены volumes на диске. Для кластера Kafka и приложения настроена общая сеть "kafka_shared_network", что позволяет отдельным контейнерам общаться между собой.
Параметры приложения Python заданы в файлах app/docker-compose.yml и app/Dockerfile.

Приложение состоит из следующих компонентов (микросервисов):
- Producer.py - продюсер Kafka, отправляющий сообщения каждую секунду.
- SingleMessageConsumer.py - консьюмер Kafka, вычитывающий по 1 сообщению.
- BatchMessageConsumer.py - консьюмер Kafka, вычитывающий по 10 сообщений.
Для обоих консьюмеров настроен запуск 2 экземпляров сервиса.


# Разворачивание Kafka-кластера.

1. Установить docker по инструкции https://docs.docker.com/engine/install
2. Запустить контейнеры кластера Kafka. Если не указывать ключ -d, при запуске сразу будут видны логи, по которым можно убедиться в отсутствии ошибок.  
`cd cluster && sudo docker compose up -d`


# Проверка работы кластера Kafka в консоли сервера.

1. Вывести список запущенных контейнеров.  
`sudo docker ps`
3. Выполнить команду внутри контейнера. В общем случае команда выглядит так:  
`docker exec -it <kafka_container_name> kafka-topics.sh --list --bootstrap-server kafka:9092`  
В образах Confluent расширение .sh часто отсутствует, поэтому итоговая команда приведена ниже. Команда должна вернуть пустой список, так как топики ещё не созданы.  
`sudo docker exec -it cluster-kafka2-1 kafka-topics --list --bootstrap-server kafka2:9092`
4. Можно проверить логи конкретного контейнера и найти там значения настроенных параметров, например папок логов.  
```
sudo docker logs cluster-kafka2-1 | grep 'log.dirs ='
sudo docker logs -f cluster-kafka1-1
```


# Проверка работы кластера Kafka через Kafka UI.

WEB-интерфейс для проверки доступен на порту 8080. В случае обращения с ПК/сервера, на котором запущен docker, адрес будет http://localhost:8080
Статус кластера должен быть online, должна отображаться информация о версии, брокерах, партициях и т.д.


# Создание топика через консоль.

Для работы приложения необходимо создать топик "my-topic" с 3 партициями и фактором репликации 2. Команда:  
`sudo docker exec -it cluster-kafka1-1 kafka-topics --create --topic my-topic --partitions 3 --replication-factor 2 --bootstrap-server kafka1:9091`
В результате успешного выполнения должно появиться сообщение:  
Created topic my-topic.


# Проверка созданного топика через консоль.

Команда:  
`sudo docker exec -it cluster-kafka1-1 kafka-topics --describe --topic my-topic --bootstrap-server kafka1:9091`

Пример успешного сообщения:  
```
Topic: my-topic TopicId: lj7uBh-GTT2o8dAdjeg17Q PartitionCount: 3       ReplicationFactor: 2    Configs:
        Topic: my-topic Partition: 0    Leader: 1       Replicas: 1,3   Isr: 1,3
        Topic: my-topic Partition: 1    Leader: 2       Replicas: 2,1   Isr: 2,1
        Topic: my-topic Partition: 2    Leader: 3       Replicas: 3,2   Isr: 3,2
```


# Запуск приложения.

Команда:  
`cd app && sudo docker compose up`

Примером успешного запуска будут логи в консоли об отправке и получении сообщений.  
```
[+] up 10/10
 ✔ Image app-single-consumer       Built                                                                                                                                                 8.3s
 ✔ Image app-batch-consumer        Built                                                                                                                                                 8.3s
 ✔ Image app-producer              Built                                                                                                                                                 8.3s
 ✔ Volume app_batch_consumer_logs  Created                                                                                                                                               0.0s
 ✔ Volume app_single_consumer_logs Created                                                                                                                                               0.0s
 ✔ Container app-batch-consumer-2  Created                                                                                                                                               1.5s
 ✔ Container app-producer-1        Created                                                                                                                                               1.8s
 ✔ Container app-single-consumer-2 Created                                                                                                                                               1.8s
 ✔ Container app-batch-consumer-1  Created                                                                                                                                               1.9s
 ✔ Container app-single-consumer-1 Created                                                                                                                                               1.8s
Attaching to batch-consumer-1, batch-consumer-2, producer-1, single-consumer-1, single-consumer-2
producer-1  | Продюсер начинает отправку
producer-1  | Сообщение доставлено в топик my-topic и партицию 2
producer-1  | Сообщение доставлено в топик my-topic и партицию 0
producer-1  | Сообщение доставлено в топик my-topic и партицию 2
producer-1  | Сообщение доставлено в топик my-topic и партицию 0
producer-1  | Сообщение доставлено в топик my-topic и партицию 2
producer-1  | Сообщение доставлено в топик my-topic и партицию 0
single-consumer-2  | 2026-05-24 13:47:24,358 INFO: Получено сообщение: key=2, value='Сообщение 2', offset=0
single-consumer-1  | 2026-05-24 13:47:24,361 INFO: Получено сообщение: key=1, value='Сообщение 1', offset=0
single-consumer-2  | 2026-05-24 13:47:24,369 INFO: Получено сообщение: key=4, value='Сообщение 4', offset=1
single-consumer-2  | 2026-05-24 13:47:24,378 INFO: Получено сообщение: key=6, value='Сообщение 6', offset=2
single-consumer-1  | 2026-05-24 13:47:24,372 INFO: Получено сообщение: key=3, value='Сообщение 3', offset=1
single-consumer-1  | 2026-05-24 13:47:24,379 INFO: Получено сообщение: key=5, value='Сообщение 5', offset=2
single-consumer-1  | 2026-05-24 13:47:24,398 INFO: Получено сообщение: key=7, value='Сообщение 7', offset=3
producer-1         | Сообщение доставлено в топик my-topic и партицию 2
single-consumer-2  | 2026-05-24 13:47:25,040 INFO: Получено сообщение: key=8, value='Сообщение 8', offset=3
producer-1         | Сообщение доставлено в топик my-topic и партицию 0
single-consumer-1  | 2026-05-24 13:47:26,032 INFO: Получено сообщение: key=9, value='Сообщение 9', offset=4
producer-1         | Сообщение доставлено в топик my-topic и партицию 2
single-consumer-2  | 2026-05-24 13:47:27,042 INFO: Получено сообщение: key=10, value='Сообщение 10', offset=0
producer-1         | Сообщение доставлено в топик my-topic и партицию 1
single-consumer-2  | 2026-05-24 13:47:28,033 INFO: Получено сообщение: key=11, value='Сообщение 11', offset=4
producer-1         | Сообщение доставлено в топик my-topic и партицию 0
single-consumer-2  | 2026-05-24 13:47:29,034 INFO: Получено сообщение: key=12, value='Сообщение 12', offset=1
producer-1         | Сообщение доставлено в топик my-topic и партицию 1
single-consumer-2  | 2026-05-24 13:47:30,049 INFO: Получено сообщение: key=13, value='Сообщение 13', offset=2
producer-1         | Сообщение доставлено в топик my-topic и партицию 1
single-consumer-1  | 2026-05-24 13:47:31,034 INFO: Получено сообщение: key=14, value='Сообщение 14', offset=5
producer-1         | Сообщение доставлено в топик my-topic и партицию 2
single-consumer-2  | 2026-05-24 13:47:32,034 INFO: Получено сообщение: key=15, value='Сообщение 15', offset=3
producer-1         | Сообщение доставлено в топик my-topic и партицию 1
batch-consumer-1   | 2026-05-24 13:47:33,037 INFO: Получено сообщение: key=2, value='Сообщение 2', offset=0
batch-consumer-1   | 2026-05-24 13:47:33,038 INFO: Получено сообщение: key=4, value='Сообщение 4', offset=1
single-consumer-2  | 2026-05-24 13:47:33,044 INFO: Получено сообщение: key=16, value='Сообщение 16', offset=5
batch-consumer-1   | 2026-05-24 13:47:33,038 INFO: Получено сообщение: key=6, value='Сообщение 6', offset=2
batch-consumer-1   | 2026-05-24 13:47:33,048 INFO: Получено сообщение: key=8, value='Сообщение 8', offset=3
batch-consumer-1   | 2026-05-24 13:47:33,055 INFO: Получено сообщение: key=10, value='Сообщение 10', offset=0
batch-consumer-1   | 2026-05-24 13:47:33,056 INFO: Получено сообщение: key=11, value='Сообщение 11', offset=4
batch-consumer-1   | 2026-05-24 13:47:33,061 INFO: Получено сообщение: key=12, value='Сообщение 12', offset=1
batch-consumer-1   | 2026-05-24 13:47:33,062 INFO: Получено сообщение: key=13, value='Сообщение 13', offset=2
batch-consumer-1   | 2026-05-24 13:47:33,070 INFO: Получено сообщение: key=15, value='Сообщение 15', offset=3
batch-consumer-1   | 2026-05-24 13:47:33,072 INFO: Получено сообщение: key=16, value='Сообщение 16', offset=5
batch-consumer-1   | 2026-05-24 13:47:33,096 INFO: Успешный коммит 10 сообщений
```
