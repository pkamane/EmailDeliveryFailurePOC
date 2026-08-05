using Confluent.Kafka;
using MailKit.Net.Smtp;
using MailKit.Security;
using Microsoft.Extensions.Configuration;
using MimeKit;

var environmentName = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production";
Console.WriteLine($"Environment: {environmentName}");

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
    .AddJsonFile($"appsettings.{environmentName}.json", optional: true, reloadOnChange: false)
    .AddEnvironmentVariables()
    .Build();

var gmailUsername = configuration["Gmail:Username"];
var gmailAppPassword = configuration["Gmail:AppPassword"];
var gmailRecipient = configuration["Gmail:TestRecipient"];

Console.WriteLine($"Gmail username configured: {!string.IsNullOrWhiteSpace(gmailUsername)}");
Console.WriteLine($"Gmail app password configured: {!string.IsNullOrWhiteSpace(gmailAppPassword)}");
Console.WriteLine($"Test recipient configured: {!string.IsNullOrWhiteSpace(gmailRecipient)}");

var kafkaBootstrapServers = configuration["Kafka:BootstrapServers"];
var kafkaFailureTopic = configuration["Kafka:FailureTopic"];

Console.WriteLine($"Kafka bootstrap servers configured: {!string.IsNullOrWhiteSpace(kafkaBootstrapServers)}");
Console.WriteLine($"Kafka failure topic configured: {!string.IsNullOrWhiteSpace(kafkaFailureTopic)}");

RunMenu();

void RunMenu()
{
    while (true)
    {
        Console.WriteLine();
        Console.WriteLine("1 - Send test Kafka message");
        Console.WriteLine("2 - Receive Kafka messages");
        Console.WriteLine("3 - Send test email");
        Console.WriteLine("0 - Exit");
        Console.Write("Select an option: ");

        var choice = Console.ReadLine();

        switch (choice)
        {
            case "1":
                ProduceTestMessage();
                break;
            case "2":
                ConsumeMessages();
                break;
            case "3":
                SendTestEmail();
                break;
            case "0":
                return;
            default:
                Console.WriteLine("Invalid option.");
                break;
        }
    }
}

void ProduceTestMessage()
{
    if (string.IsNullOrWhiteSpace(kafkaBootstrapServers) || string.IsNullOrWhiteSpace(kafkaFailureTopic))
    {
        Console.WriteLine("Kafka:BootstrapServers or Kafka:FailureTopic is not configured.");
        return;
    }

    Console.Write("Enter message: ");
    var text = Console.ReadLine() ?? string.Empty;

    var producerConfig = new ProducerConfig
    {
        BootstrapServers = kafkaBootstrapServers
    };

    try
    {
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var message = new Message<string, string>
        {
            Key = Guid.NewGuid().ToString(),
            Value = text
        };

        producer.Produce(kafkaFailureTopic, message);
        producer.Flush(TimeSpan.FromSeconds(10));

        Console.WriteLine("Message published successfully.");
    }
    catch (ProduceException<string, string> ex)
    {
        Console.WriteLine("Failed to publish message to Kafka.");
        Console.WriteLine(ex.Error.Reason);
    }
    catch (Exception ex)
    {
        Console.WriteLine("An unexpected error occurred while publishing to Kafka.");
        Console.WriteLine(ex);
    }
}

void ConsumeMessages()
{
    if (string.IsNullOrWhiteSpace(kafkaBootstrapServers) || string.IsNullOrWhiteSpace(kafkaFailureTopic))
    {
        Console.WriteLine("Kafka:BootstrapServers or Kafka:FailureTopic is not configured.");
        return;
    }

    var consumerConfig = new ConsumerConfig
    {
        BootstrapServers = kafkaBootstrapServers,
        GroupId = "email-poc-consumer",
        AutoOffsetReset = AutoOffsetReset.Earliest
    };

    using var cts = new CancellationTokenSource();
    ConsoleCancelEventHandler cancelHandler = (_, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
    };
    Console.CancelKeyPress += cancelHandler;

    try
    {
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(kafkaFailureTopic);

        Console.WriteLine("Listening for Kafka messages. Press Ctrl+C to stop.");

        while (true)
        {
            var result = consumer.Consume(cts.Token);
            Console.WriteLine("Message received from Kafka:");
            Console.WriteLine(result.Message.Value);
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("Kafka consumer stopped.");
    }
    catch (ConsumeException ex)
    {
        Console.WriteLine("Failed to consume message from Kafka.");
        Console.WriteLine(ex.Error.Reason);
    }
    catch (Exception ex)
    {
        Console.WriteLine("An unexpected error occurred while consuming Kafka messages.");
        Console.WriteLine(ex);
    }
    finally
    {
        Console.CancelKeyPress -= cancelHandler;
    }
}

void SendTestEmail()
{
    Console.WriteLine("Starting email test");

    try
    {
        if (string.IsNullOrWhiteSpace(gmailUsername) ||
            string.IsNullOrWhiteSpace(gmailAppPassword) ||
            string.IsNullOrWhiteSpace(gmailRecipient))
        {
            throw new InvalidOperationException(
                "Missing required configuration. Ensure Gmail:Username, Gmail:AppPassword, and Gmail:TestRecipient are all set.");
        }

        var message = new MimeMessage();
        message.From.Add(MailboxAddress.Parse(gmailUsername));
        message.To.Add(MailboxAddress.Parse(gmailRecipient));
        message.Subject = "Kafka POC - Test Email";
        message.Body = new TextPart("plain")
        {
            Text = "This is a test email from the .NET Kafka POC."
        };

        using var client = new SmtpClient();

        Console.WriteLine("Connecting to Gmail SMTP");
        client.Connect("smtp.gmail.com", 587, SecureSocketOptions.StartTls);
        client.Authenticate(gmailUsername, gmailAppPassword);

        Console.WriteLine("Sending email");
        client.Send(message);

        client.Disconnect(true);

        Console.WriteLine("Email sent successfully");
    }
    catch (Exception ex)
    {
        Console.WriteLine("An error occurred while sending the test email.");
        Console.WriteLine(ex);
    }
}
