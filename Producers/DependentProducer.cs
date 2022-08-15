using Confluent.Kafka;
using Hypertheory.KafkaUtils.Handlers;

namespace Hypertheory.KafkaUtils.Producers;

/* Adapted From: 
 * https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/examples/Web/KafkaDependentProducer.cs
 */


public class DependentProducer<K, V>
{
    private readonly IProducer<K, V> _kafkaHandle;

    public DependentProducer(ClientHandle handle)
    {
        _kafkaHandle = new DependentProducerBuilder<K, V>(handle.Handle).Build();
    }

    /// <summary>
    ///     Asychronously produce a message and expose delivery information
    ///     via the returned Task. Use this method of producing if you would
    ///     like to await the result before flow of execution continues.
    /// <summary>
    public Task ProduceAsync(string topic, Message<K, V> message)
        => _kafkaHandle.ProduceAsync(topic, message);

    /// <summary>
    ///     Asynchronously produce a message and expose delivery information
    ///     via the provided callback function. Use this method of producing
    ///     if you would like flow of execution to continue immediately, and
    ///     handle delivery information out-of-band.
    /// </summary>
    public void Produce(string topic, Message<K, V> message, Action<DeliveryReport<K, V>>? deliveryHandler = null)
        => _kafkaHandle.Produce(topic, message, deliveryHandler);

    public void Flush(TimeSpan timeout)
        => _kafkaHandle.Flush(timeout);
}