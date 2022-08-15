using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using Google.Protobuf;
using Hypertheory.KafkaUtils.Handlers;
using Confluent.Kafka.SyncOverAsync;

namespace Hypertheory.KafkaUtils.Producers;

/// <summary>
/// Warning: This only works with the Protobuf or Avro Schema Types.
/// JSON is not supported, as it's serializer needs additional configuration
/// </summary>
/// <typeparam name="TValue"></typeparam>
/// <typeparam name="TValueSerializer"></typeparam>
public class EventProducer<TValue, TValueSerializer>
    where TValue : IMessage<TValue>, new()
    where TValueSerializer : IAsyncSerializer<TValue>

{
    private readonly IProducer<Null, TValue> _kafkaHandle;

    private delegate ISerializer<TValue> SerializerValueFactory(CachedSchemaRegistryClient schemaRegistry);



    private readonly Dictionary<Type, SerializerValueFactory> _valueConstructors = new()
    {
        { typeof(ProtobufSerializer<TValue>), (s) => new ProtobufSerializer<TValue>(s).AsSyncOverAsync() },
        { typeof(AvroSerializer<TValue>), (s) => new AvroSerializer<TValue>(s).AsSyncOverAsync() },
    };

    
    public EventProducer(ClientHandle handler, string registryUrl)
    {
        var schemaRegistryConfig = new SchemaRegistryConfig { Url = registryUrl };
        var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        var handleBuilder = new DependentProducerBuilder<Null, TValue>(handler.Handle);

        // Since this all happens at startup, I'm not doing error checkig here. Jus throw baby throw.

        var valueSerializer = _valueConstructors[typeof(TValueSerializer)](schemaRegistry);

        handleBuilder.SetValueSerializer(valueSerializer);
      

        _kafkaHandle = handleBuilder.Build();
    }

    public Task ProduceAsync(string topic, Message<Null, TValue> message) => _kafkaHandle.ProduceAsync(topic, message);

    public void Produce(string topic, Message<Null, TValue> message, Action<DeliveryReport<Null, TValue>>? deliveryHandler = null) => _kafkaHandle.Produce(topic, message, deliveryHandler);
    public void Flush(TimeSpan timeout)
        => _kafkaHandle.Flush(timeout);
}