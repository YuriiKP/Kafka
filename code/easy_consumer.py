from confluent_kafka import Consumer, KafkaError, TopicPartition


def consume_message(topic: str | None = None, offset: int | None = None) -> None:
    '''
    Метод читает сообщения из кафки и выводит их в консоль
    '''

    conf = {
        "bootstrap.servers": "localhost:19092",
        "group.id": "mygroup",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(conf)

    if offset:
        partitions = consumer.list_topics(topic).topics[topic].partitions # Получаем все партиции топика 
        for partition in partitions:
            consumer.assign([TopicPartition(topic, partition, offset)])
    else:
        consumer.subscribe([topic])

    
    try:
        while True:
            message = consumer.poll(1.0)
            
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError:
                    print('Конец партиции')
                else:
                    print(f'Ошбика: {message.error()}')

            else:
                print(f'Полученное сообщение: {message.value().decode('utf-8')}')

    except KeyboardInterrupt: 
        pass
    
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_message(topic='my_topic')

        