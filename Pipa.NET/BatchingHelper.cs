using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Pipa.NET
{
    public interface IBatchingHelper<I, O>
    {
        public Task<O> ProcessAsync(I input);
    }

    public class BatchingHelper<I, O> : IBatchingHelper<I, O>, IDisposable, IAsyncDisposable
    {
        private int _batchSize = 1, _parallelism = 1;
        private TimeSpan _maxWaitTime = TimeSpan.FromSeconds(1);
        private readonly Func<int, I[], Task<O[]>> _batchProcessor;
        private Channel<(I input, TaskCompletionSource<O> tcs)> _queue;
        private CancellationTokenSource _cts;
        private Task[] _loopTasks;
        private bool _isStarted = false;
        private bool _disposed = false;

        public BatchingHelper(int batchSize, Func<int, I[], Task<O[]>> batchProcessor)
        {
            _batchSize = batchSize;
            _batchProcessor = batchProcessor;
            _queue = Channel.CreateUnbounded<(I, TaskCompletionSource<O>)>();
        }

        public BatchingHelper<I, O> WithBatchSize(int batchSize)
        {
            _batchSize = Math.Max(1, batchSize);
            return this;
        }

        public BatchingHelper<I, O> WithParallelism(int parallelism)
        {
            _parallelism = Math.Max(1, parallelism);
            return this;
        }

        public BatchingHelper<I, O> WithMaxWaitTime(TimeSpan maxWaitTime)
        {
            _maxWaitTime = maxWaitTime;
            return this;
        }

        public BatchingHelper<I, O> Start(CancellationToken ct = default)
        {
            if (_isStarted) return this;
            _isStarted = true;

            _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _loopTasks = new Task[_parallelism];
            for (var i = 0; i < _parallelism; i++)
                _loopTasks[i] = Loop(i, _cts.Token);

            return this;
        }

        protected async ValueTask DisposeAsyncCore()
        {
            if (_cts != null)
            {
                _cts.Cancel();
                _cts.Dispose();
                _cts = null;
            }

            if (_loopTasks != null)
            {
                try { await Task.WhenAll(_loopTasks); }
                catch { }
                _loopTasks = null;
            }

            if (_queue != null)
            {
                if (_queue.Writer.TryComplete())
                {
                    while (_queue.Reader.TryRead(out var item))
                    {
                        item.tcs.TrySetCanceled();
                    }
                }
                _queue = null;
            }
        }

        public void Dispose()
        {
            DisposeAsyncCore().AsTask().Wait();
            GC.SuppressFinalize(this);
        }

        public async ValueTask DisposeAsync()
        {
            await DisposeAsyncCore().ConfigureAwait(false);
            GC.SuppressFinalize(this);
        }

        public async Task<O> ProcessAsync(I input)
        {
            if (!_isStarted)
                throw new InvalidOperationException("BatchingHelper is not started");

            var tcs = new TaskCompletionSource<O>();
            await _queue.Writer.WriteAsync((input, tcs));
            return await tcs.Task;
        }

        private async Task Loop(int id, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                // Wait until something is available in the queue
                var batch = new List<(I input, TaskCompletionSource<O> tcs)>(_batchSize);
                var firstItem = await _queue.Reader.ReadAsync(ct);
                batch.Add(firstItem);

                // Collect items until we reach _batchSize, or until _maxWaitTime has passed
                var startTime = DateTime.UtcNow;
                while (batch.Count < _batchSize)
                {
                    var elapsed = DateTime.UtcNow - startTime;
                    if (elapsed > _maxWaitTime) break;

                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    cts.CancelAfter(_maxWaitTime - elapsed);

                    try { batch.Add(await _queue.Reader.ReadAsync(cts.Token)); }
                    catch (OperationCanceledException) { break; }
                }

                try
                {
                    // Process items in batch
                    var results = await _batchProcessor(id, batch.Select(i => i.input).ToArray());

                    var i = 0;
                    foreach (var result in results)
                        batch[i++].tcs.SetResult(result);
                }
                catch (Exception ex)
                {
                    foreach (var (_, tcs) in batch)
                        tcs.SetException(ex);
                }
            }
        }
    }
}