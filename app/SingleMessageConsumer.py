### Консьюмер, вычитывающий по 1 сообщению
import logging
from confluent_kafka import Consumer
from confluent_kafka.serialization import IntegerDeserializer, StringDeserializer, SerializationContext, MessageField

# Настройка логирования для возможности сохранения логов в файл
logger = logging.getLogger("KafkaConsumer")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
# Для консоли
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
# Для сохранения в файл
log_filename = f"/app/logs/consumer.log"
file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Настройка консьюмера
conf = {
    "bootstrap.servers": "kafka1:9091,kafka2:9092,kafka3:9093",
    # Отдельная группа для этого типа консьюмера
    "group.id": "single-consumer-group",
    "auto.offset.reset": "earliest",
    # Настройка для ручного коммита сообщений
    "enable.auto.commit": False,
    # Настройки размера вычитки и максимального времени ожидания для вычитки по 1 сообщению
    "fetch.min.bytes": 1,
    "fetch.wait.max.ms": 100
}
# Создание консьюмера
consumer = Consumer(conf)

# Подписка на топик
consumer.subscribe(["my-topic"])

# Определение десериализации ключа и значения
key_deserializer = IntegerDeserializer()
value_deserializer = StringDeserializer('utf-8')

# Чтение сообщений в бесконечном цикле
try:
    while True:
        # Получение сообщений раз в 0,1с
        msg = consumer.poll(0.1)

        if msg is None:
            continue
        if msg.error():
            loger.error(f"Ошибка: {msg.error()}")
            continue

        #key = msg.key().decode("utf-8")
        #value = msg.value().decode("utf-8")
        key = key_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
        value = value_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        logger.info(f"Получено сообщение: {key=}, {value=}, offset={msg.offset()}")
        # Ручной коммит после обработки сообщения
        consumer.commit(msg, asynchronous=False)
finally:
    # Закрытие консьюмера
    consumer.close()
