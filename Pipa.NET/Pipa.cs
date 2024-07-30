using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Pipa.NET
{

    public class Pipeline<TIn, TOut> : IDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly Task[] _tasks;
        private readonly Func<TIn, TOut> _step;

        public Pipeline(Func<TIn, TOut> step, Func<CancellationToken, Task>[] workers)
        {
            _step = step;

            _tasks = new Task[workers.Length];
            _cts = new CancellationTokenSource();
            for (var i = 0; i < workers.Length; i++)
            {
                _tasks[i] = workers[i](_cts.Token);
            }
        }

        public void Dispose()
        {
            try
            {
                _cts.Cancel();
                Task.WaitAll(_tasks);
                _cts.Dispose();
            }
            catch
            {
            }
        }

        public TOut Execute(TIn input)
        {
            return _step(input);
        }
    }

    public static class PipelineBuilder
    {
        public static PipelineBuilder<T, T> Create<T>()
        {
            return new PipelineBuilder<T, T>();
        }
    }

    public class PipelineBuilder<TIn, TOut>
    {
        private List<Func<CancellationToken, Task>> _workers = new List<Func<CancellationToken, Task>>();
        private List<Func<object, object>> _steps = new List<Func<object, object>> { (object input) => input };

        public PipelineBuilder()
        {
        }

        private PipelineBuilder(List<Func<CancellationToken, Task>> workers, List<Func<object, object>> steps)
        {
            _workers = workers;
            _steps = steps;
        }

        private static PipelineBuilder<NewIn, NewOut> From<OldIn, OldOut, NewIn, NewOut>(PipelineBuilder<OldIn, OldOut> pipeline)
        {
            return new PipelineBuilder<NewIn, NewOut>(pipeline._workers, pipeline._steps);
        }

        private void PushStep<TResult>(Func<TOut, TResult> step)
        {
            var lastStep = _steps[^1];
            Func<object, object> newStep = (object input) => step((TOut)lastStep(input));
            _steps.Add(newStep);
        }

        public PipelineBuilder<TIn, TResult> Step<TResult>(Func<TOut, TResult> step)
        {
            PushStep(step);
            return PipelineBuilder<TIn, TResult>.From<TIn, TOut, TIn, TResult>(this);
        }

        public PipelineBuilder<TIn, TResult> Batch<TResult>(int batchSize, TimeSpan maxWaitTime, Func<PipelineBuilder<TOut[], TOut[]>, PipelineBuilder<TOut[], TResult[]>> pipeline)
        {
            var pipe = pipeline(PipelineBuilder.Create<TOut[]>()).Build();

            var batcher = new BatchingHelper<TOut, TResult>(batchSize, (TOut[] inputs) => Task.FromResult(pipe.Execute(inputs)))
                .WithMaxWaitTime(maxWaitTime);

            _workers.Add(async (CancellationToken ct) =>
            {
                batcher.Start(ct);

                try { await Task.Delay(Timeout.Infinite, ct); }
                catch (OperationCanceledException) { }

                batcher.Dispose();
                pipe.Dispose();
            });

            PushStep((TOut input) => batcher.ProcessAsync(input).Result);

            return PipelineBuilder<TIn, TResult>.From<TIn, TOut, TIn, TResult>(this);
        }

        public PipelineBuilder<TIn, TResult> Workers<TResult>(int workers, Func<PipelineBuilder<TOut, TOut>, PipelineBuilder<TOut, TResult>> pipeline)
        {
            var pipe = pipeline(PipelineBuilder.Create<TOut>()).Build();

            var batcher = new BatchingHelper<TOut, TResult>(1, (TOut[] inputs) => Task.FromResult(new TResult[] { pipe.Execute(inputs[0]) }))
                .WithParallelism(workers);

            _workers.Add(async (CancellationToken ct) =>
            {
                batcher.Start(ct);

                try { await Task.Delay(Timeout.Infinite, ct); }
                catch (OperationCanceledException) { }

                batcher.Dispose();
                pipe.Dispose();
            });

            PushStep((TOut input) => batcher.ProcessAsync(input).Result);

            return PipelineBuilder<TIn, TResult>.From<TIn, TOut, TIn, TResult>(this);
        }

        public Pipeline<TIn, TOut> Build()
        {
            return new Pipeline<TIn, TOut>((TIn input) => (TOut)_steps[^1](input), _workers.ToArray());
        }
    }
}
