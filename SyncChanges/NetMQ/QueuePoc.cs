using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Input;
using NetMQ;

namespace SyncChanges.NetMQ;

public class QueuePoc {
    
    public static async void Main(string[] args) {
        await new QueuePoc().Start();
    }

    public async Task Start() {

        using (var queue = new NetMQQueue<ICommand>()) {
            using (var poller = new NetMQPoller { queue }) {
                Console.WriteLine($"{GetCurrentThreadId()} Iniciando ...");
                queue.ReceiveReady += (sender, args) => ProcessCommand(queue.Dequeue());

                poller.RunAsync();

                // Then, from various threads...
                queue.Enqueue(new DoOneCommand());
                queue.Enqueue(new DoSecondCommand());

                Console.WriteLine($"{GetCurrentThreadId()} queue.Count: {queue.Count}");

                while (queue.Any())
                {
                    //Console.WriteLine($"{GetCurrentThreadId()} Espere...");
                    Thread.Sleep(1000);
                }

                poller.StopAsync();

                Console.WriteLine($"{GetCurrentThreadId()} Terminado!");
                Console.ReadLine();

            }
        }

    }

    private static string GetCurrentThreadId() {
        return $"[{Thread.CurrentThread.ManagedThreadId}]";
    }

    private void ProcessCommand(ICommand command) {
        if (command.CanExecute(null)) {
            Console.WriteLine($"{GetCurrentThreadId()} Ejecutando...");
            command.Execute(EventArgs.Empty);
        }
    }

    private class DoOneCommand : ICommand
    {
        public bool CanExecute(object parameter)
        {
            return true;
        }

        public void Execute(object parameter)
        {
            Console.WriteLine($"{GetCurrentThreadId()} + DoOneCommand Inicia");
            //Thread.Sleep(TimeSpan.FromSeconds(2));
            Thread.SpinWait(5000000);
            Console.WriteLine($"{GetCurrentThreadId()} + DoOneCommand Termina");
        }

        public event EventHandler CanExecuteChanged;
    }

    private class DoSecondCommand : ICommand
    {
        public bool CanExecute(object parameter)
        {
            return true;
        }

        public void Execute(object parameter)
        {
            Console.WriteLine($"{GetCurrentThreadId()} + DoSecondCommand Inicia");
            //Thread.Sleep(TimeSpan.FromSeconds(2));
            Thread.SpinWait(5000000);
            Console.WriteLine($"{GetCurrentThreadId()} + DoSecondCommand Termina");
        }

        public event EventHandler CanExecuteChanged;
    }

}

