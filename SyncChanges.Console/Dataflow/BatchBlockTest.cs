using System.Threading.Tasks.Dataflow;

namespace SyncChanges.Console.Dataflow;

public static class BatchBlockTest
{
    public static void Main()
    {
        // Create a BatchBlock<int> object that holds ten
        // elements per batch.
        var batchBlock = new BatchBlock<int>(10);

        // Post several values to the block.
        for (int i = 0; i < 52; i++)
        {
            batchBlock.Post(i);
        }
        // Set the block to the completed state. This causes
        // the block to propagate out any remaining
        // values as a final batch.
        batchBlock.Complete();

        int totalBatches = batchBlock.OutputCount;

        System.Console.WriteLine($"totalBatches: {totalBatches}");

        // Print the list of each batch.
        for (int i = 0; i < totalBatches; i++)
        {
            int num = i + 1;
            int[] batchN = batchBlock.Receive();
            foreach (int v in batchN)
            {
                System.Console.WriteLine($"Batch {num}: {v}");
            }
        }

    }

}