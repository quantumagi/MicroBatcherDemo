using MicroBatcherDemo;
using Moq;
using Xunit;

namespace MicroBatcherDemoTests;

[Collection("MicroBatcherTests")]
public class MicroBatcherTests
{
    /// <summary>
    /// Running tests with different number of jobs and batch sizes.
    /// </summary>
    /// <param name="numJobs">Number of jobs.</param>
    /// <param name="batchSize">Batch size.</param>
    /// <param name="maxAsyncBatches">Number of maximum asynchronous batches.</param>
    [Theory]
    [InlineData(1, 10, 3)]
    [InlineData(10, 10, 1)]
    [InlineData(23, 10, 3)]
    [InlineData(3, 1, 3)]
    [InlineData(1, 1, 1)]
    public async Task CanBatchJobs(int numJobs, int batchSize, int maxAsyncBatches)
    {
        // Create a mock batch processor.
        // This batch processor just does x >= x * 2.
        var batchProcessorMock = new Mock<IBatchProcessor<int, int>>();
        batchProcessorMock.Setup(x => x.ProcessBatchAsync(It.IsAny<List<int>>()))
            .Returns((List<int> jobs) =>
            {
            // Check that the batch size is being respected.
            Assert.True(jobs.Count <= numJobs);

            // Multiply the input by 2.
            var res = jobs.Select(x => x * 2).ToList();
                return Task.FromResult(res);
            });

        // Create and start the batcher.
        using (var batcher = new MicroBatcher<int, int>(batchProcessorMock.Object, batchSize, TimeSpan.FromSeconds(1), maxAsyncBatches))
        {
            batcher.Startup();

            // Submit the jobs.
            var jobs = Enumerable.Range(1, numJobs).ToList();
            var results = jobs.Select(job => batcher.SubmitJobAsync(job)).ToList();

            // Check that the processor multiplied the input by 2.
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(jobs[i] * 2, results[i].Result);
            }

            await batcher.ShutdownAsync();
        }
    }

    [Fact]
    public async Task BatchProcessorErrorThrowsError()
    {
        // Create a mock batch processor.
        // This batch processor will just throw an error.
        var batchProcessorMock = new Mock<IBatchProcessor<int, int>>();
        batchProcessorMock.Setup(x => x.ProcessBatchAsync(It.IsAny<List<int>>()))
            .Returns((List<int> jobs) =>
            {
                throw new Exception("Batch processor error");
            });

        // Create and start the batcher.
        using (var batcher = new MicroBatcher<int, int>(batchProcessorMock.Object, 10, TimeSpan.FromSeconds(1)))
        {
            batcher.Startup();

            // Submit a job and check that it throws an error.
            Assert.Throws<AggregateException>(() => batcher.SubmitJobAsync(1).Result);

            await batcher.ShutdownAsync();
        }
    }

    // TODO: Add more test cases.
}