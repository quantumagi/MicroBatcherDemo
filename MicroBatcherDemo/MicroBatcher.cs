using System.Collections.Concurrent;

namespace MicroBatcherDemo;

/// <summary>
/// Micro-batching is a technique used in processing pipelines where individual tasks are grouped
/// together into small batches. This can improve throughput by reducing the number of requests made
/// to a downstream system. The micro-batching library, fulfills the following requirements:<list type="bullet">
/// <item>It allows the caller to submit a single <see cref="TJob"/>, and it should return a <see cref="TJobResult"/>.</item>
/// <item>It processes accepted <see cref="TJob"/>s in batches using a <see cref="IBatchProcessor"/>.</item>
/// <item>It provides a way to configure the batching behaviour i.e. size and frequency.</item>
/// <item>It exposes a <see cref="ShutdownAsync"/> method which returns after all previously accepted <see cref="TJob"/>s are processed.</item>
/// <item>Optionally, it also allows multiple batches to be processed concurrently.</item>
/// </list>
/// </summary>
/// <remarks>
/// The class uses generic type arguments <see cref="TJob"/> and <see cref="TJobResult"/> to allow for flexibility in the types of
/// jobs and results that can be processed. Importantly, it does not require the classes to be derived from a specific base class
/// or implement a specific interface.
/// </remarks>
public class MicroBatcher<TJob, TJobResult> : IDisposable, IAsyncDisposable
{
    private readonly object lockObj;
    private readonly IBatchProcessor<TJob, TJobResult> batchProcessor;
    private readonly ConcurrentQueue<(TJob, TaskCompletionSource<TJobResult>)> jobQueue;

    private CancellationTokenSource cancellationTokenSource;
    private Task? processingTask;
    private bool shutdownRequested;
    private TaskCompletionSource<bool> queueEmptyCompletionSource = new TaskCompletionSource<bool>();
    private int batchSize;
    private TimeSpan batchFrequency;
    private int maxAsyncBatches;
    private bool disposed = false;

    /// <summary>
    /// Returns the number of jobs in the queue.
    /// </summary>
    public int Count => this.jobQueue.Count;

    /// <summary>
    /// The number of jobs to process in a single batch.
    /// </summary>
    public int BatchSize
    {
        get => batchSize;
        set
        {
            if (value <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(BatchSize), "Batch size must be greater than 0.");
            }
            batchSize = value;
        }
    }

    /// <summary>
    /// How often to process batches.
    /// </summary>
    public TimeSpan BatchFrequency
    {
        get => batchFrequency;
        set
        {
            if (value <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(BatchFrequency), "Batch frequency must be greater than TimeSpan.Zero.");
            }
            batchFrequency = value;
        }
    }

    /// <summary>
    /// The maximum number of asynchronous batches to process.
    /// </summary>
    public int MaxAsyncBatches
    {
        get => maxAsyncBatches;
        set
        {
            if (value < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(MaxAsyncBatches), "Maximum asynchronous batches must be at least 1.");
            }
            maxAsyncBatches = value;
        }
    }

    /// <summary>
    /// Creates a new instance of the MicroBatcher class.
    /// </summary>
    /// <param name="batchProcessor">Batch processor of type <see cref="IBatchProcessor"/>.</param>
    /// <param name="batchSize">Number of jobs to process in a single batch.</param>
    /// <param name="batchFrequency">How often to process batches.</param>
    /// <param name="maxAsyncBatches">Maximum number of asynchronous batches.</param>
    public MicroBatcher(IBatchProcessor<TJob, TJobResult> batchProcessor, int batchSize, TimeSpan batchFrequency, int maxAsyncBatches = 1)
    {
        // Validate arguments.
        ArgumentNullException.ThrowIfNull(batchProcessor, nameof(batchProcessor));

        // Use the setters of these members to leverage argument validation.
        this.BatchSize = batchSize;
        this.BatchFrequency = batchFrequency;
        this.MaxAsyncBatches = maxAsyncBatches;

        this.lockObj = new object();
        this.batchProcessor = batchProcessor;
        this.processingTask = null;
        this.shutdownRequested = false;
        this.cancellationTokenSource = new CancellationTokenSource();
        this.jobQueue = new ConcurrentQueue<(TJob, TaskCompletionSource<TJobResult>)>();
    }

    /// <summary>
    /// Initializes and starts the micro-batching process. This method sets up the internal processing
    /// task that continuously checks for new jobs in the queue, batching and processing them according
    /// to the configured size and frequency. If the processing task is already running, calling this method
    /// will throw an <see cref="InvalidOperationException"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if <c>Startup</c> is called when the processing task is already active.</exception>
    /// <remarks>
    /// This method should be called once during the lifecycle of a <see cref="MicroBatcher{TJob, TJobResult}"/> instance
    /// before submitting any jobs via <see cref="SubmitJobAsync"/>. It is responsible for initiating the
    /// asynchronous loop that handles job batching and processing.
    /// </remarks>
    public void Startup()
    {
        // This short-duration lock ensures a consistent state across multiple member variables.
        // It does not affect the concurrency of the processing task.
        lock (this.lockObj)
        {
            if (this.processingTask != null)
            {
                // Ignore the call if already started or starting up.
                return;
            }

            this.shutdownRequested = false;
            this.cancellationTokenSource = new CancellationTokenSource();
            this.processingTask = Task.Run(async () =>
            {
                while (!this.cancellationTokenSource.Token.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay((int)this.batchFrequency.TotalMilliseconds, cancellationTokenSource.Token).ConfigureAwait(false);
                    }
                    catch (TaskCanceledException)
                    {
                    // This is expected to happen occasionally.
                    // It does not require logging or handling.
                }

                    if (this.maxAsyncBatches > 1)
                    {
                        await ProcessBatchesAsync().ConfigureAwait(false);
                    }
                    else
                    {
                        await ProcessBatchAsync().ConfigureAwait(false);
                    }

                // This short-duration lock ensures atomicity of the job queue and queueEmptyCompletionSource access.
                lock (this.lockObj)
                    {
                        if (this.jobQueue.IsEmpty)
                        {
                            queueEmptyCompletionSource.TrySetResult(true);
                        }
                    }
                }
            }, cancellationTokenSource.Token);
        }
    }

    /// <summary>
    /// Asynchronously shuts down the micro-batching process, ensuring that all previously submitted jobs
    /// are processed before the shutdown completes. This method cancels the internal processing task and waits
    /// for it to finish. If the method is called multiple times, subsequent calls will return immediately without
    /// effect, making the method idempotent.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous shutdown operation. The task completes once
    /// the micro-batching process has been successfully shut down and all jobs have been processed.</returns>
    /// <remarks>
    /// It is recommended to call this method before disposing of the <see cref="MicroBatcher{TJob, TJobResult}"/> instance
    /// to ensure graceful termination of the processing task and completion of all pending jobs. This method
    /// sets an internal flag to prevent new jobs from being submitted during the shutdown process. It waits
    /// for the job queue to become empty and then signals cancellation to the processing task. The method
    /// is designed to be safe to call from any thread.
    /// </remarks>
    /// <exception cref="Exception">Exceptions thrown by job processing will be propagated to the caller, and should
    /// be handled appropriately.</exception>
    public async Task ShutdownAsync()
    {
        // This short-duration lock ensures atomicity of read/write of the shutdownRequested flag.
        lock (this.lockObj)
        {
            if (this.shutdownRequested || this.processingTask == null)
            {
                // Allow method to be idempotent.
                return;
            }

            this.shutdownRequested = true;
        }

        try
        {
            // Wait for queue to empty.
            await queueEmptyCompletionSource.Task.ConfigureAwait(false);

            // Signal the processing task to complete.
            this.cancellationTokenSource.Cancel();

            // Wait for the processing task to complete.
            await this.processingTask.ConfigureAwait(false);
        }
        finally
        {
            // This short-duration lock ensures atomicity of read/write of the processingTask variable and state.
            lock (this.lockObj)
            {
                cancellationTokenSource.Dispose();

                if (this.processingTask!.IsCompleted)
                {
                    this.processingTask.Dispose();
                }

                this.processingTask = null;
            }
        }
    }

    /// <summary>
    /// Submits a job to be processed asynchronously by the micro-batcher. Jobs are queued and
    /// processed according to the configured batch size and frequency. Each submitted job is
    /// eventually processed by the <see cref="IBatchProcessor{TJob, TJobResult}"/>.
    /// </summary>
    /// <param name="job">The job to be submitted for processing. The job must conform to the
    /// generic type parameter <typeparamref name="TJob"/> specified for this micro-batcher instance.</param>
    /// <returns>A <see cref="Task{TJobResult}"/> that represents the asynchronous operation and
    /// eventually yields the result of processing the submitted job. The result conforms to the
    /// generic type parameter <typeparamref name="TJobResult"/>.</returns>
    /// <exception cref="SystemException">Thrown if the method is called after a shutdown request
    /// has been initiated, indicating that no more jobs can be accepted for processing.</exception>
    /// <remarks>
    /// This method is thread-safe and can be called from multiple threads concurrently. Jobs are
    /// enqueued in a first-come, first-served basis and are processed in batches. If the method is
    /// called after <see cref="ShutdownAsync"/> has been requested, it will throw a <see cref="SystemException"/>
    /// to prevent new jobs from being accepted. Ensure that <see cref="Startup"/> has been called
    /// to start the processing task before submitting any jobs.
    /// </remarks>
    public Task<TJobResult> SubmitJobAsync(TJob job)
    {
        // This short-duration lock ensures atomicity of read/write of the processingTask variable and state.
        // Also ensures atomicity of the job queue and queueEmptyCompletionSource access.
        lock (this.lockObj)
        {
            if (this.shutdownRequested || this.processingTask == null)
            {
                throw new SystemException("Not started or shutting down.");
            }

            var completionSource = new TaskCompletionSource<TJobResult>();
            this.jobQueue.Enqueue((job, completionSource));

            // Reset queue empty status.
            if (queueEmptyCompletionSource.Task.IsCompleted)
            {
                queueEmptyCompletionSource = new TaskCompletionSource<bool>();
            }

            return completionSource.Task;
        }
    }

    /// <summary>
    /// Launches multiple <see cref="ProcessBatchAsync"/> threads up to <see cref="maxAsyncBatches"/>.
    /// </summary>
    private async Task ProcessBatchesAsync()
    {
        var tasks = new List<Task>();
        for (int i = 0; i < this.maxAsyncBatches; i++)
        {
            tasks.Add(ProcessBatchAsync());
            if (this.jobQueue.Count == 0)
                break;
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    /// <summary>
    /// This is the asynchronous thread used to process a batch of jobs from the job queue.
    /// </summary>
    /// <remarks>
    /// Ideally <see cref="IBatchProcessor"/> will not raise any errors of its own.
    /// If it does then the tasks that failed to complete, as accessed via <see cref="SubmitJobAsync"/>, will reflect such errors.
    /// </remarks>
    private async Task ProcessBatchAsync()
    {
        var batchJobs = new List<TJob>();
        var batchCompletionSources = new List<TaskCompletionSource<TJobResult>>();

        // This short-duration lock ensures atomicity of the job queue access.
        lock (this.lockObj)
        {
            // Dequeue up to config.BatchSize jobs.
            while (batchJobs.Count < this.batchSize)
            {
                if (!this.jobQueue.TryDequeue(out (TJob job, TaskCompletionSource<TJobResult> completionSource) queueItem))
                {
                    break;
                }

                batchJobs.Add(queueItem.job);
                batchCompletionSources.Add(queueItem.completionSource);
            }
        }

        // If jobs found in the queue.
        if (batchJobs.Count > 0)
        {
            // No locking required here as we're working with local variables.
            try
            {
                // Process the batch by invoking the batch processor.
                List<TJobResult> results = await batchProcessor.ProcessBatchAsync(batchJobs).ConfigureAwait(false);

                // Transfer the job results to the completion sources.
                for (int i = 0; i < batchCompletionSources.Count; i++)
                {
                    if (i < results.Count)
                    {
                        batchCompletionSources[i].TrySetResult(results[i]);
                    }
                    else
                    {
                        batchCompletionSources[i].TrySetException(new InvalidOperationException("Batch processor did not return a result for the job."));
                    }
                }
            }
            catch (Exception ex)
            {
                // The batch processor should handle its errors and not raise any errors.
                // This is the most elegant way to deal with errors if they do happen.
                foreach (var source in batchCompletionSources)
                {
                    source.SetException(ex);
                }
            }
        }
    }

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources synchronously.
    /// </summary>
    public void Dispose()
    {
        // Dispose of managed resources and suppress finalization.
        Dispose(true);
        // Suppress finalization to prevent the finalizer from calling ~Dispose() for this object,
        // as resources will already have been released by this point.
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Protected implementation of Dispose pattern.
    /// </summary>
    /// <param name="disposing">A boolean value indicating whether the method has been called directly or indirectly by a user's code.</param>
    protected virtual void Dispose(bool disposing)
    {
        // Check to see if Dispose has already been called.
        if (!disposed)
        {
            // If disposing equals true, dispose all managed and unmanaged resources.
            if (disposing)
            {
                // Dispose managed resources here.
                // Blockingly waits for the asynchronous shutdown process to complete.
                // Note: This may potentially cause deadlocks in certain scenarios,
                // such as when called from a UI thread. Use with caution.
                this.ShutdownAsync().Wait();
            }

            // Free unmanaged resources here and perform other cleanup operations.
            // For example: set large fields to null.

            // Mark the disposal as done.
            disposed = true;
        }
    }

    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources asynchronously.
    /// </summary>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        // Perform asynchronous cleanup operations.
        // Await the ShutdownAsync method without capturing the synchronization context.
        await ShutdownAsync().ConfigureAwait(false);

        // Dispose of synchronous (managed) resources.
        // Note: Since we're performing asynchronous disposal, we pass 'false' to not repeat freeing managed resources.
        Dispose(false);

        // Suppress finalization.
        GC.SuppressFinalize(this);
    }
}