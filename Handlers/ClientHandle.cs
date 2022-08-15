using Confluent.Kafka;

namespace Hypertheory.KafkaUtils.Handlers;

public class ClientHandle : IDisposable
{
    private readonly IProducer<byte[], byte[]> _kafkaProducer;

    public ClientHandle(ProducerConfig config)
    {
        _kafkaProducer = new ProducerBuilder<byte[], byte[]>(config).Build();
    }

    public Handle Handle => _kafkaProducer.Handle;


    void IDisposable.Dispose()
    {
        // Block until all outstanding produce requests have completed (with or
        // without error).
        _kafkaProducer.Flush();
        _kafkaProducer.Dispose();
    }
}
