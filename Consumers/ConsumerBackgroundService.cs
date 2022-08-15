using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hypertheory.KafkaUtils.Consumers;



/// <summary>
/// Based on https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/examples/Web/RequestTimeConsumer.cs
/// </summary>
/// <typeparam name="TKey">The type of the incoming key</typeparam>
/// <typeparam name="TValue">The type of the incoming message</typeparam>
public abstract class ConsumerBackgroundService<TKey, TValue> : BackgroundService

{
    private readonly string _topic;

    private readonly IConsumer<TKey, TValue> _kafkaConsumer;


    private delegate IDeserializer<TValue> DeserializeValueFactory(CachedSchemaRegistryClient schemaRegistry);

    private delegate IDeserializer<TKey> DeserializeKeyFactory
        (CachedSchemaRegistryClient schemaRegistry);


    public ConsumerBackgroundService(ConsumerConfig consumerConfig, string topic)
    {

        _topic = topic;
        _kafkaConsumer = new ConsumerBuilder<TKey, TValue>(consumerConfig).Build();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        new Thread(() => StartConsumerLoop(stoppingToken)).Start();

        return Task.CompletedTask;
    }

    private void StartConsumerLoop(CancellationToken cancellationToken)
    {
        _kafkaConsumer.Subscribe(this._topic);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var cr = this._kafkaConsumer.Consume(cancellationToken);

                // Handle message...
                HandleConsumeLoop(cr);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException e)
            {
                // Consumer errors should generally be ignored (or logged) unless fatal.
                HandleConsumeExcpetion(e);

                if (e.Error.IsFatal)
                {
                    // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                    break;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Unexpected error: {e}");
                break;
            }
        }
    }

    protected virtual void HandleConsumeExcpetion(ConsumeException e)
    {
        Console.WriteLine($"Consume error: {e.Error.Reason}");
    }

    protected abstract void HandleConsumeLoop(ConsumeResult<TKey, TValue> result);


    public override void Dispose()
    {
        this._kafkaConsumer.Close(); // Commit offsets and leave the group cleanly.
        this._kafkaConsumer.Dispose();

        base.Dispose();
    }
}