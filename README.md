EmailDeliveryFailurePOC

A .NET 8 console proof-of-concept exploring the building blocks for handling email delivery failures asynchronously via Kafka: sending email through Gmail SMTP, and producing/consuming messages on a local Kafka broker.

What this is

Enterprise.Console is a menu-driven console app with three standalone actions:

Send test Kafka message — publishes a message you type to the email-delivery-failures topic.
Receive Kafka messages — subscribes to that topic and prints incoming messages until you press Ctrl+C.
Send test email — sends a plain-text test email via Gmail SMTP using an app password.

The topic name and the pairing of "email" and "Kafka" in one app suggest the intended end state: when an email fails to send, publish a failure event to Kafka so a downstream consumer can retry, alert, or route it to a dead-letter flow. Today the three actions are independent — sending a test email does not yet publish anything to Kafka on failure — so this repo currently validates the two integrations (SMTP and Kafka) in isolation rather than wiring them together end to end.

Stack
.NET 8 (C#), console app
Confluent.Kafka for producing/consuming
MailKit / MimeKit for SMTP email
Microsoft.Extensions.Configuration (JSON + environment variables) for settings
Apache Kafka (KRaft mode, single broker) via Docker Compose for local development
Project layout
src/Enterprise.Console/   Console app (Program.cs, config, launch settings)
infra/kafka/               docker-compose.yml for a local single-node Kafka broker
Getting started
1. Start Kafka locally
bash
cd infra/kafka
docker compose up -d

This runs a single-node Kafka broker (KRaft mode, no ZooKeeper) on localhost:9092. Topics must be created explicitly — auto-creation is disabled — so create the failure topic before producing/consuming:

bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create --topic email-delivery-failures \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
2. Configure the app

Copy the example config files and fill in real values (these are git-ignored so real credentials never get committed):

bash
cd src/Enterprise.Console
cp appsettings.Development.json.example appsettings.Development.json

You'll need:

Gmail:Username — the sending Gmail address
Gmail:AppPassword — a Gmail app password (not your regular password)
Gmail:TestRecipient — where test emails get sent
Kafka:BootstrapServers — defaults to localhost:9092
Kafka:FailureTopic — defaults to email-delivery-failures

The app reads appsettings.json, then layers appsettings.{DOTNET_ENVIRONMENT}.json on top (defaults to Production if unset), then environment variables.

3. Run it
bash
dotnet run --project src/Enterprise.Console

Pick an option from the menu to test Kafka producing, Kafka consuming, or Gmail SMTP sending.

Status

Proof of concept / exploratory. Kafka and email are each verified working independently; connecting them (publish a failure event to Kafka automatically when SendTestEmail throws) is the natural next step.
