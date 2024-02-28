namespace MicroBatcherDemo;

/// <summary>
/// Processes a batch of jobs.
/// </summary>
public interface IBatchProcessor<TJob, TJobResult>
{
    /// <summary>
    /// Processes a batch of jobs asynchronously.
    /// </summary>
    /// <param name="jobs">A list of <see cref="TJob"/>s.</param>
    /// <returns>A list of <see cref="TJobResult"/>s corresponding 1-to-1 with the <see cref="TJob"/>s.</returns>
    Task<List<TJobResult>> ProcessBatchAsync(List<TJob> jobs);
}
