from confluent_kafka import Producer
from confluent_kafka.serialization import IntegerSerializer, StringSerializer, SerializationContext, MessageField
import time

# Конфигурация продюсера
conf = {
    "bootstrap.servers": "kafka1:9091,kafka2:9092,kafka3:9093",
    # Гарантия доставки
    "acks": "all",
    # Повторы отправки при сетевых сбоях
    "retries": 5,
    # Пауза между повторными попытками в мc
    "retry.backoff.ms": 200
}

# Сериализация ключа и значения
key_serializer = IntegerSerializer()
value_serializer = StringSerializer('utf-8')

# Функция обратного вызова для подтверждения доставки
def delivery_report(err, msg):
   if err is not None:
       print(f"Сбой доставки сообщения: {err}", flush=True)
   else:
       print(f"Сообщение доставлено в топик {msg.topic()} и партицию {msg.partition()}", flush=True)

# Создание продюсера
producer = Producer(conf)

# Имя топика
topic_name = "my-topic"

# Определение сериализации ключа и значения
key_serializer = IntegerSerializer()
value_serializer = StringSerializer('utf-8')

# Указываем KEY/VALUE для полей сообщения
key_context = SerializationContext(topic_name, MessageField.KEY)
value_context = SerializationContext(topic_name, MessageField.VALUE)

# Задание счётчика сообщений
i = 1

# Отправка сообщений в бесконечном цикле
print("Продюсер начинает отправку", flush=True)
try:
    while True:
        # Сообщение для отправки
        message_key = i
        message_value = "Сообщение " + str(i)
        # Сериализация
        serialized_key = key_serializer(message_key, key_context)
        serialized_value = value_serializer(message_value, value_context)
        # Отправка сообщения
        producer.produce(
            topic = topic_name,
            key = serialized_key,
            value = serialized_value,
            on_delivery = delivery_report
        )
        producer.flush()
        i = i + 1
        time.sleep(1)
except Exception as e:
    print(f"Произошла ошибка при отправке: {e}", flush=True)
finally:
    # Ожидание завершения отправки всех сообщений
    producer.flush()

