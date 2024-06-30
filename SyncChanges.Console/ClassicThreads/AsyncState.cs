using System;
using System.Threading;
using System.Threading.Tasks;

namespace SyncChanges.Console.ClassicThreads;

class CustomData
{
    public long CreationTime;
    public int Name;
    public int ThreadNum;
}

public static class AsyncState
{
    public static void Main()
    {
        System.Console.WriteLine("Iniciando...");

        Task[] taskArray = new Task[1000];
        for (int i = 0; i < taskArray.Length; i++)
        {
            taskArray[i] = Task.Factory.StartNew((obj) =>
                {
                    CustomData data = obj as CustomData;
                    if (data == null) return;

                    data.ThreadNum = Thread.CurrentThread.ManagedThreadId;
                },
                new CustomData() { Name = i, CreationTime = DateTime.Now.Ticks });
        }
        System.Console.WriteLine("Esperando...");
        Task.WaitAll(taskArray);

        System.Console.WriteLine("Leyendo resultados...");
        foreach (var task in taskArray)
        {
            var data = task.AsyncState as CustomData;
            if (data != null)
                System.Console.WriteLine("Task #{0} created at {1}, ran on thread #{2}.",
                    data.Name, data.CreationTime, data.ThreadNum);
        }
        System.Console.WriteLine("Terminado");


    }

}