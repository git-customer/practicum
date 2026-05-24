### Консьюмер, вычитывающий по 10 сообщений
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
    "group.id": "batch-consumer-group",
    "auto.offset.reset": "earliest",
    # Настройка для ручного коммита сообщений
    "enable.auto.commit": False
}
# Создание консьюмера
consumer = Consumer(conf)

# Подписка на топик
consumer.subscribe(["my-topic"])

# Определение десериализации ключа и значения
key_deserializer = IntegerDeserializer()
value_deserializer = StringDeserializer('utf-8')

# Задаём количество сообщений в пачке
batch_size = 10
# Буфер для накопления кол-ва сообщений, равного batch_size
msg_buffer = []
# Чтение сообщений в бесконечном цикле
try:
    while True:
        # Получение до 10 сообщений за раз с таймаутом 0.1с
        messages = consumer.consume(num_messages=batch_size-len(msg_buffer), timeout=0.1)
        # Если за timeout вообще ничего не пришло, идем на следующий цикл (наш продюсер отправляет раз в 1 секунду)
        if not messages:
            continue

        for msg in messages:
            if msg.error():
                logger.error(f"Ошибка: {msg.error()}")
                continue        
            # Добавляем сообщение в буфер
            msg_buffer.append(msg)
        # Если буфер еще не полон — идем на следующий цикл consume()
        if len(msg_buffer) < batch_size:
            continue

        # Обработка полученной пачки сообщений
        messages_in_batch = []
        for msg in msg_buffer:
            try:
                # Десериализация
                key = key_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
                value = value_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                logger.info(f"Получено сообщение: {key=}, {value=}, offset={msg.offset()}")
                # Добавляем сообщение в список для последующего коммита
                messages_in_batch.append(msg)
            except Exception as deserializer_error:
                logger.error(f"Ошибка десериализации сообщения на оффсете {msg.offset()}: {deserializer_error}")
                messages_in_batch.append(msg)
        # Один коммит последнего сообщения в пачке после обработки всей пачки сообщений 
        if messages_in_batch:
            consumer.commit(message=messages_in_batch[-1], asynchronous=False)
            logger.info(f"Успешный коммит {len(messages_in_batch)} сообщений")
        # Очистка буфера
        msg_buffer.clear()
except Exception as e:
    logger.error(f"Критическая ошибка консьюмера: {e}")
finally:
    # Закрытие консьюмера
    consumer.close()
