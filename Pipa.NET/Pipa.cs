using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Pipa.NET
{

    public class Pipeline<TIn, TOut> : IDisposable, IAsyncDisposable
    {
        private CancellationTokenSource _cts;
        private Task[] _tasks;
        private Func<TIn, Task<TOut>> _step;

        public Pipeline(Func<TIn, Task<TOut>> step, Func<CancellationToken, Task>[] workers)
        {
            _step = step;

            _tasks = new Task[workers.Length];
            _cts = new CancellationTokenSource();
            for (var i = 0; i < workers.Length; i++)
            {
                _tasks[i] = workers[i](_cts.Token);
            }
        }

        protected virtual async ValueTask DisposeAsyncCore()
        {
            if (_cts != null)
            {
                _cts.Cancel();
                _cts.Dispose();
                _cts = null;
            }

            if (_tasks != null)
            {
                try { await Task.WhenAll(_tasks); }
                catch { }
                _tasks = null;
            }

            if (_step != null)
                _step = null;
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

        /// <summary>
        /// Execute the pipeline with the specified input.
        /// </summary>
        /// <param name="input">Input for the pipeline</param>
        /// <returns>Output of the pipeline</returns>
        public async Task<TOut> ExecuteAsync(TIn input)
        {
            return await _step(input);
        }
    }

    public static class PipelineBuilder
    {
        /// <summary>
        /// Create a new pipeline builder with the specified input type.
        /// </summary>
        /// <typeparam name="T">The type of the input</typeparam>
        /// <returns>A new instance of PiplineBuilder</returns>
        public static PipelineBuilder<T, T> Create<T>()
        {
            return new PipelineBuilder<T, T>();
        }
    }

    public class PipelineBuilder<TIn, TOut>
    {
        private readonly List<Func<CancellationToken, Task>> _workers;
        private readonly List<Func<object, Task<object>>> _steps;

        public PipelineBuilder()
        {
            _workers = new List<Func<CancellationToken, Task>>();
            _steps = new List<Func<object, Task<object>>>();
        }

        private PipelineBuilder(List<Func<CancellationToken, Task>> workers, List<Func<object, Task<object>>> steps)
        {
            _workers = workers;
            _steps = steps;
        }

        private static PipelineBuilder<NewIn, NewOut> From<OldIn, OldOut, NewIn, NewOut>(PipelineBuilder<OldIn, OldOut> pipeline)
        {
            return new PipelineBuilder<NewIn, NewOut>(pipeline._workers, pipeline._steps);
        }

        private void PushStep<TResult>(Func<TOut, Task<TResult>> step)
        {
            if (_steps.Count > 0)
            {
                var lastStep = _steps[^1];
                _steps.Add(async (object input) => await step((TOut)await lastStep(input)));
            }
            else
            {
                _steps.Add(async (object input) => await step((TOut)input));
            }
        }

        /// <summary>
        /// Add a new step to the pipeline.
        /// </summary>
        /// <typeparam name="TResult">Output type of the step</typeparam>
        /// <param name="step">Async function that takes in input from the previous Step and produces output of TResult</param>
        /// <returns>PipelineBuilder</returns>
        public PipelineBuilder<TIn, TResult> Step<TResult>(Func<TOut, Task<TResult>> step)
        {
            PushStep(step);
            return PipelineBuilder<TIn, TResult>.From<TIn, TOut, TIn, TResult>(this);
        }

        /// <summary>
        /// Add a new synchronous step to the pipeline.
        /// </summary>
        /// <typeparam name="TResult">Output type of the step</typeparam>
        /// <param name="step">Function that takes in input from the previous Step and produces output of TResult</param>
        /// <returns>PipelineBuilder</returns>
        public PipelineBuilder<TIn, TResult> StepSync<TResult>(Func<TOut, TResult> step)
        {
            return Step((TOut input) => Task.FromResult(step(input)));
        }

        /// <summary>
        /// Unrolls a collection of items from the input and processes them in
        /// parallel within a sub-pipeline. The results are then rolled back up
        /// into a single result.
        /// </summary>
        /// <typeparam name="TResult">Output type of the step</typeparam>
        /// <typeparam name="TItem">Type of the items in the collection</typeparam>
        /// <typeparam name="TItemResult">Output type of the sub-pipeline</typeparam>
        /// <param name="unroll">Function that returns a collection from the input</param>
        /// <param name="pipeline">Function that returns a new sub-pipeline</param>
        /// <param name="roll">Function that compiles the results from the sub-pipeline</param>
        /// <returns>PipelineBuilder</returns>
        public PipelineBuilder<TIn, TResult> Unroll<TResult, TItem, TItemResult>(
            Func<TOut, IEnumerable<TItem>> unroll,
            Func<PipelineBuilder<(TOut, TItem, int), (TOut Parent, TItem Item, int Index)>, PipelineBuilder<(TOut, TItem, int), TItemResult>> pipeline,
            Func<(TOut Input, IEnumerable<TItemResult> Results), TResult> roll)
        {
            var pipe = pipeline(PipelineBuilder.Create<(TOut, TItem, int)>()).Build();

            async Task<TResult> step(TOut input)
            {
                var tasks = unroll(input)
                    .Select((item, i) => pipe.ExecuteAsync((input, item, i)));

                var results = await Task.WhenAll(tasks);

                return roll((input, results.AsEnumerable()));
            };

            PushStep(step);

            return PipelineBuilder<TIn, TResult>.From<TIn, TOut, TIn, TResult>(this);
        }

        /// <summary>
        /// Collects items from the input and processes them in batches.
        /// </summary>
        /// <typeparam name="TResult">Output type of the step</typeparam>
        /// <param name="batchSize">Maximum number of items in a single batch</param>
        /// <param name="maxWaitTime">Maximum amount of time spent collecting items before the batch gets processed anyway</param>
        /// <param name="pipeline">Function that returns a new sub-pipeline for processing the batch</param>
        /// <returns>PipelineBuilder</returns>
        public PipelineBuilder<TIn, TResult> Batch<TResult>(int batchSize, TimeSpan maxWaitTime, Func<PipelineBuilder<TOut[], TOut[]>, PipelineBuilder<TOut[], TResult[]>> pipeline)
        {
            var pipe = pipeline(PipelineBuilder.Create<TOut[]>()).Build();

            var batcher = new Util.BatchingHelper<TOut, TResult>(batchSize, async (int id, TOut[] inputs) => await pipe.ExecuteAsync(inputs))
                .WithMaxWaitTime(maxWaitTime);

            _workers.Add(async (CancellationToken ct) =>
            {
                batcher.Start(ct);

                try { await Task.Delay(Timeout.Infinite, ct); }
                catch (OperationCanceledException) { }

                await batcher.DisposeAsync();
                await pipe.DisposeAsync();
            });

            PushStep(batcher.ProcessAsync);

            return PipelineBuilder<TIn, TResult>.From<TIn, TOut, TIn, TResult>(this);
        }

        /// <summary>
        /// Processes items concurrently using a sub-pipeline.
        /// </summary>
        /// <typeparam name="TResult">Output type of the step</typeparam>
        /// <param name="workers">Number of concurrent executions of the sub-pipeline</param>
        /// <param name="pipeline">Function that returns a new sub-pipeline for processing the batch</param>
        /// <returns></returns>
        public PipelineBuilder<TIn, TResult> Workers<TResult>(int workers, Func<PipelineBuilder<(int, TOut), (int ThreadId, TOut Item)>, PipelineBuilder<(int, TOut), TResult>> pipeline)
        {
            var pipe = pipeline(PipelineBuilder.Create<(int, TOut)>()).Build();

            var batcher = new Util.BatchingHelper<TOut, TResult>(1, async (int id, TOut[] inputs) => new TResult[] { await pipe.ExecuteAsync((id, inputs[0])) })
                .WithParallelism(workers);

            _workers.Add(async (CancellationToken ct) =>
            {
                batcher.Start(ct);

                try { await Task.Delay(Timeout.Infinite, ct); }
                catch (OperationCanceledException) { }

                await batcher.DisposeAsync();
                await pipe.DisposeAsync();
            });

            PushStep(batcher.ProcessAsync);

            return PipelineBuilder<TIn, TResult>.From<TIn, TOut, TIn, TResult>(this);
        }

        /// <summary>
        /// Builds the pipeline.
        /// </summary>
        /// <returns>Pipeline</returns>
        public Pipeline<TIn, TOut> Build()
        {
            return new Pipeline<TIn, TOut>(async (TIn input) => (TOut)await _steps[^1](input), _workers.ToArray());
        }
    }
}
