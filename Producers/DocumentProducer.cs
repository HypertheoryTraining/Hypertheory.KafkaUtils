using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Google.Protobuf;
using Hypertheory.KafkaUtils.Handlers;

namespace Hypertheory.KafkaUtils.Producers;


/// <summary>
/// Warning: This only works with the Protobuf or Avro Schema Types.
/// JSON is not supported, as it's serializer needs additional configuration
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TValue"></typeparam>
/// <typeparam name="TKeySerializer"></typeparam>
/// <typeparam name="TValueSerializer"></typeparam>
public class DocumentProducer<TKey, TValue, TKeySerializer, TValueSerializer>
    where TValue : IMessage<TValue>, new()
    where TKey : IMessage<TKey>, new()
    where TKeySerializer : IAsyncSerializer<TKey>
    where TValueSerializer : IAsyncSerializer<TValue>

{
    private readonly IProducer<TKey, TValue> _kafkaHandle;

    private delegate ISerializer<TValue> SerializerValueFactory(CachedSchemaRegistryClient schemaRegistry);

    private delegate ISerializer<TKey> SerializeKeyFactory
        (CachedSchemaRegistryClient schemaRegistry);

    private readonly Dictionary<Type, SerializerValueFactory> _valueConstructors = new()
    {
        { typeof(ProtobufSerializer<TValue>), (s) => new ProtobufSerializer<TValue>(s).AsSyncOverAsync() },
        { typeof(AvroSerializer<TValue>), (s) => new AvroSerializer<TValue>(s).AsSyncOverAsync() },
    };

    private readonly Dictionary<Type, SerializeKeyFactory> _keyConstructors = new()
    {
        { typeof(ProtobufSerializer<TKey>), (s) => new ProtobufSerializer<TKey>(s).AsSyncOverAsync() },
        { typeof(AvroSerializer<TKey>), (s) => new AvroSerializer<TKey>(s).AsSyncOverAsync() }
    };

    public DocumentProducer(ClientHandle handler, string registryUrl)
    {
        var schemaRegistryConfig = new SchemaRegistryConfig { Url = registryUrl };
        var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        var handleBuilder = new DependentProducerBuilder<TKey, TValue>(handler.Handle);

        // Since this all happens at startup, I'm not doing error checkig here. Jus throw baby throw.
        var keySerializer = _keyConstructors[typeof(TKeySerializer)](schemaRegistry);
        var valueSerializer = _valueConstructors[typeof(TValueSerializer)](schemaRegistry);

        handleBuilder.SetValueSerializer(valueSerializer);
        handleBuilder.SetKeySerializer(keySerializer);

        _kafkaHandle = handleBuilder.Build();
    }

    public Task ProduceAsync(string topic, Message<TKey, TValue> message) => _kafkaHandle.ProduceAsync(topic, message);

    public void Produce(string topic, Message<TKey, TValue> message, Action<DeliveryReport<TKey, TValue>>? deliveryHandler = null) => _kafkaHandle.Produce(topic, message, deliveryHandler);
    public void Flush(TimeSpan timeout)
        => _kafkaHandle.Flush(timeout);
}
