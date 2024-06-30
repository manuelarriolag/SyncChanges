using NetMQ;
using NetMQ.Sockets;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NetMQ.Devices;

namespace SyncChanges
{
    /// <summary>
    /// 
    /// </summary>
    public static class QueueManager
    {
        private static readonly QueueDevice queue;
        private static readonly CancellationTokenSource source;

        private const string FRONTEND_BIND_ADDRESS = "tcp://localhost:5555";
        private const string BACKEND_BIND_ADDRESS = "tcp://localhost:5556";

        static QueueManager() {
            //Console.Title = "NetMQ Multi-threaded Service";
            queue = new QueueDevice(FRONTEND_BIND_ADDRESS, BACKEND_BIND_ADDRESS, DeviceMode.Threaded);
            source = new CancellationTokenSource();
        }

        /// <summary>
        /// 
        /// </summary>
        public static void Stop() {
            source.Cancel();
            queue.Stop();
        }

        /// <summary>
        /// 
        /// </summary>
        public static async Task Start() {
            for (int threadId = 0; threadId< 10; threadId++)
            {
                _ = Task.Factory.StartNew(() => WorkerRoutine(source.Token));
            }

            queue.Start();

            var tasks = new List<Task>();
            for (int i = 0; i< 1000; i++)
            {
                int clientId = i;
                Task task = Task.Factory.StartNew(() => ClientRoutine(clientId));
                tasks.Add(task);
            }

            await Task.WhenAll(tasks.ToArray());
        }

        private static async void ClientRoutine(int clientId) {
            try
            {
                using var req = new RequestSocket();
                req.Connect(FRONTEND_BIND_ADDRESS);

                byte[] message = Encoding.Unicode.GetBytes($"{clientId} Hello");

                await Console.Out.WriteLineAsync($"Client {clientId} sent \"{clientId} Hello\"");
                req.SendFrame(message, message.Length);

                var response = req.ReceiveFrameString(Encoding.Unicode);
                await Console.Out.WriteLineAsync($"Client {clientId} received \"{response}\"");
            } catch (Exception ex)
            {
                await Console.Out.WriteLineAsync($"Exception on ClientRoutine: {ex.Message}");
            }
        }

        private static async Task WorkerRoutine(CancellationToken cancelToken) {
            try
            {
                using ResponseSocket rep = new();
                rep.Options.Identity = Encoding.Unicode.GetBytes(Guid.NewGuid().ToString());
                rep.Connect(BACKEND_BIND_ADDRESS);
                //rep.Connect("inproc://workers");
                rep.ReceiveReady += RepOnReceiveReady;
                while (!cancelToken.IsCancellationRequested)
                {
                    rep.Poll(TimeSpan.FromMilliseconds(100));
                }
            } catch (Exception ex)
            {
                await Console.Out.WriteLineAsync($"Exception on WorkerRoutine: {ex.Message}");
                throw;
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        //public async Task Start() {

        //    //GlobalConfiguration.Configuration
        //    //    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
        //    //    .UseSimpleAssemblyNameTypeSerializer()
        //    //    .UseRecommendedSerializerSettings()
        //    //    .UseSqlServerStorage("Server=(localdb)\\MSSQLLocalDB;Database=(localdb);Integrated Security=True;");

        //    // Inicializar la queue
        //    var queue = new QueueDevice(FRONTEND_BIND_ADDRESS, BACKEND_BIND_ADDRESS, DeviceMode.Threaded);

        //    // Inicializar el cancelToken para cancelar los threads 
        //    var source = new CancellationTokenSource();
        //    var cancelToken = source.Token;

        //    // Abrir los threads Worker
        //    for (int threadId = 0; threadId < 10; threadId++) {

        //        _ = Task.Factory.StartNew(() => WorkerRoutine(cancelToken));
        //    }

        //    queue.Start();

        //    // Abrir los threads Client
        //    var tasks = new List<Task>();
        //    for (int i = 0; i < 1000; i++)
        //    {
        //        int clientId = i;
        //        Task task = Task.Factory.StartNew(() => ClientRoutine(clientId));
        //        tasks.Add(task);
        //    }

        //    await Console.Out.WriteLineAsync("Started!");

        //    await Task.WhenAll(tasks.ToArray());

        //    source.Cancel();
        //    queue.Stop();

        //    await Console.Out.WriteLineAsync("Completed!");
        //}


        //private static async void ClientRoutine(int clientId)
        //{
        //    try
        //    {
        //        using var req = new RequestSocket();
        //        req.Connect(FRONTEND_BIND_ADDRESS);

        //        byte[] message = Encoding.Unicode.GetBytes($"{clientId} Hello");

        //        await Console.Out.WriteLineAsync($"Client {clientId} sent \"{clientId} Hello\"");
        //        req.SendFrame(message, message.Length);

        //        var response = req.ReceiveFrameString(Encoding.Unicode);
        //        await Console.Out.WriteLineAsync($"Client {clientId} received \"{response}\"");
        //    } catch (Exception ex)
        //    {
        //        await Console.Out.WriteLineAsync($"Exception on ClientRoutine: {ex.Message}");
        //    }
        //}

        //private static async void WorkerRoutine(CancellationToken cancelToken)
        //{
        //    try
        //    {
        //        using ResponseSocket rep = new();
        //        rep.Options.Identity = Encoding.Unicode.GetBytes(Guid.NewGuid().ToString());
        //        rep.Connect(BACKEND_BIND_ADDRESS);
        //        //rep.Connect("inproc://workers");
        //        rep.ReceiveReady += RepOnReceiveReady;
        //        while (!cancelToken.IsCancellationRequested)
        //        {
        //            rep.Poll(TimeSpan.FromMilliseconds(100));
        //        }
        //    } catch (Exception ex)
        //    {
        //        await Console.Out.WriteLineAsync($"Exception on WorkerRoutine: {ex.Message}");
        //        throw;
        //    }
        //}

        private static async void RepOnReceiveReady(object sender, NetMQSocketEventArgs e)
        {
            try
            {
                NetMQSocket rep = e.Socket;

                byte[] message = rep.ReceiveFrameBytes();

                //Thread.Sleep(1000); //  Simulate 'work'

                byte[] response =
                    Encoding.Unicode.GetBytes(Encoding.Unicode.GetString(message) + " World from worker " + Encoding.Unicode.GetString(rep.Options.Identity));

                rep.TrySendFrame(response, response.Length);
            } catch (Exception ex)
            {
                await Console.Out.WriteLineAsync($"Exception on RepOnReceiveReady: {ex.Message}");
                throw;
            }

        }



    }
}
