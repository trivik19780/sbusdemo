using System.IO;
using System.Security.AccessControl;
using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace sbusdemo
{
    class Program
    {
        static string sbuscon="Endpoint=sb://tv-az204svcbus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=I6OVTWWpysrFbDL2UfZdgQkrXG0z6tuclne9zBD/KT4=";
        static string qname="tvsbusq01";
        static ServiceBusClient serviceBusClient;
        static ServiceBusSender serviceBusSender;

        static ServiceBusProcessor processor;
        private const int mcount=40;
        static async Task Main()
        {
            serviceBusClient =new ServiceBusClient(sbuscon);
            serviceBusSender = serviceBusClient.CreateSender(qname);
            Console.WriteLine("Hello World! Testing the ServiceBus");
            ServiceBusMessageBatch messageBatch= await serviceBusSender.CreateMessageBatchAsync();
            for(int ctr=0 ;ctr <=mcount;ctr++){
                if(!messageBatch.TryAddMessage(new ServiceBusMessage($"QueueMessage{ctr}"))){
                    throw new Exception($"The message {ctr} is too large to fit in the batch.");
                }
            }
            //await serviceBusSender.SendMessageAsync(messageBatch);
            try
            {
                // Use the producer client to send the batch of messages to the Service Bus queue
                await serviceBusSender.SendMessagesAsync(messageBatch);
                Console.WriteLine($"A batch of  messages has been published to the queue.");
            }
            finally
            {
                    // Calling DisposeAsync on client types is required to ensure that network
                    // resources and other unmanaged objects are properly cleaned up.
                    //await serviceBusSender.DisposeAsync();
                    //await serviceBusClient.DisposeAsync();
            }
            Console.Write("Total {ctr} messges deliverd");
            Console.WriteLine("Press any key to start reading the messages ....");
            Console.ReadKey();
                await RecvMessage();
        }

        // handle received messages
        static async Task MessageHandler(ProcessMessageEventArgs args)
        {
            string body = args.Message.Body.ToString();
            Console.WriteLine($"Received: {body}");
            // complete the message. messages is deleted from the queue.
            await args.CompleteMessageAsync(args.Message);
        }
        // handle any errors when receiving messages
        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }

        static async Task RecvMessage()
        {
        // Create the client object that will be used to create sender and receiver objects
        processor = serviceBusClient.CreateProcessor(qname, new ServiceBusProcessorOptions());
        try
        {
        // add handler to process messages
        processor.ProcessMessageAsync += MessageHandler;
        // add handler to process any errors
        processor.ProcessErrorAsync += ErrorHandler;
        // start processing
        await processor.StartProcessingAsync();
        Console.WriteLine("Wait for a minute and then press any key to end the processing");
        Console.ReadKey();
        // stop processing
        Console.WriteLine("\nStopping the receiver...");
        await processor.StopProcessingAsync();
        Console.WriteLine("Stopped receiving messages");
        }
        finally
        {
        // Calling DisposeAsync on client types is required to ensure that network
        // resources and other unmanaged objects are properly cleaned up.
        await processor.DisposeAsync();
        await serviceBusClient.DisposeAsync();
        }
        }
    }
}
