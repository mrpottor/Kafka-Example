using Confluent.Kafka;
using Newtonsoft.Json;
using ConsumerAPI.Models;

namespace ConsumerAPI;

public class ConsumerService : IHostedService
{
    private readonly IConfiguration configuration;
    public ConsumerService(IConfiguration config)
    {
        this.configuration = config;
    }


    public Task StartAsync(CancellationToken cancellationtoken)
    {

        ConsumerConfig config = new ConsumerConfig()
        {
            BootstrapServers = configuration["Kafka:Server"],
            GroupId = "Test group"
        };

        using (var consumer = new ConsumerBuilder<Null, string>(config).Build())
        {
            consumer.Subscribe(configuration["Kafka:Topic"]);
            var cancelToken = new CancellationTokenSource();

            try
            {
                while (true)
                {
                    var topicConsumer = consumer.Consume(cancelToken.Token);
                    var orderData = JsonConvert.DeserializeObject<Order>(topicConsumer.Message.Value);
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        };
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationtoken)
    {
        return Task.CompletedTask;
    }
}