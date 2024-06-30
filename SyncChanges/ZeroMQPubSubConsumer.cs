using System;
using NetMQ;
using NetMQ.Sockets;

namespace SyncChanges;

/// <summary>
/// Represents a ZeroMQ PUB-SUB pattern subscriber.
/// </summary>
public class ZeroMQPubSubConsumer
{
    private static readonly string _consumerUrl = "tcp://127.0.0.1:5556";

    /// <summary>
    /// Registers the ZeroMQ subscriber and starts receiving messages in a PUB-SUB pattern.
    /// </summary>
    public static void RegisterZeroMQConsumer()
    {
        using (var subscriber = new SubscriberSocket())
        {
            subscriber.Connect(_consumerUrl);
            subscriber.Subscribe("");

            Console.WriteLine("Subscriber: Ready to receive messages.");

            while (true)
            {
                string message = subscriber.ReceiveFrameString();
                Console.WriteLine($"Received: {message}");
            }
        }
    }


}