using Confluent.Kafka;
using System;
using System.IO;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

await Main();

static async Task Main()
{

    var clientConfig = new ClientConfig();
    DotNetEnv.Env.Load();
    clientConfig.BootstrapServers=Environment.GetEnvironmentVariable("bootstrap_servers");
    clientConfig.SecurityProtocol=Confluent.Kafka.SecurityProtocol.SaslSsl;
    clientConfig.SaslMechanism=Confluent.Kafka.SaslMechanism.Plain;
    clientConfig.SaslUsername=Environment.GetEnvironmentVariable("sasl_username");
    clientConfig.SaslPassword=Environment.GetEnvironmentVariable("sasl_password");
    clientConfig.SslCaLocation = "probe"; // /etc/ssl/certs
    // await Produce("recent_changes", clientConfig);
    Consume("pksqlc-qpm3pEDITS_PER_PAGE", clientConfig);
 
    Console.WriteLine("Exiting");
}

static async Task Produce(string topicName, ClientConfig config)
{
    Console.WriteLine($"{nameof(Produce)} starting");

    string eventStreamsUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

    IProducer<string, string> producer = null;

    try
    {
        producer = new ProducerBuilder<string, string>(config).Build();
        using(var httpClient = new HttpClient())

        using(var stream = await httpClient.GetStreamAsync(eventStreamsUrl))

        using(var reader = new StreamReader(stream))
        {
            while(!reader.EndOfStream)
            {
                var line = reader.ReadLine();

                if(!line.StartsWith("data:"))
                {
                    continue;
                }

                int openBraceIndex = line.IndexOf('{');
                string jsonData = line.Substring(openBraceIndex);
                Console.WriteLine($"Data string: {jsonData}");

                var jsonDoc = JsonDocument.Parse(jsonData);
                var metaElement = jsonDoc.RootElement.GetProperty("meta");
                var uriElement = metaElement.GetProperty("uri");
                var key = uriElement.GetString();

                producer.Produce(topicName, new Message<string, string> { Key = key, Value = jsonData },
                    (deliveryReport) =>
                    {
                        if (deliveryReport.Error.Code != ErrorCode.NoError)
                        {
                            Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                        }
                        else
                        {
                            Console.WriteLine($"Produced message to {deliveryReport.TopicPartitionOffset}");
                        }
                    });
            }
        }
    }
    finally
    {
        var queueSize = producer.Flush(TimeSpan.FromSeconds(5));
        if(queueSize > 0)
        {
            Console.WriteLine("WARNING: Producer event queue has " + queueSize + "pending events on exit.");
        }
        producer.Dispose();
    }
}

static void Consume(string topicName, ClientConfig config)
{
    Console.WriteLine($"{nameof(Consume)} starting");

    var consumerConfig = new ConsumerConfig(config);
    consumerConfig.GroupId = "wiki-edit-stram-group-1";
    consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
    consumerConfig.EnableAutoCommit = false;
    CancellationTokenSource cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
    };

    using(var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
    {
        consumer.Subscribe(topicName);
        try
        {
            while(true)
            {
                var cr = consumer.Consume(cts.Token);

                var jsonDoc = JsonDocument.Parse(cr.Message.Value);

                // var metaElement = jsonDoc.RootElement.GetProperty("meta");
                // var uriElement = metaElement.GetProperty("uri");
                // var uri = uriElement.GetString();
                var editsElement = jsonDoc.RootElement.GetProperty("NUM_EDITS");
                var edits = editsElement.GetInt32();
                var uri = $"{cr.Message.Key}, edits = {edits}";
                Console.WriteLine($"Consumed record with URI {uri}");

            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"Ctrl+C pressed, consumer exiting");
        }
        finally
        {
            consumer.Close();
        }
    }
}
