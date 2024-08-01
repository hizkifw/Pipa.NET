using System.Diagnostics;
using Pipa.NET;

namespace Pipa.NET.Test;

public class UnitTest
{
    [Fact]
    public async Task TestSteps()
    {
        await using var pipeline = PipelineBuilder.Create<int>()
            .StepSync(i => i.ToString())
            .StepSync(int.Parse)
            .Build();

        Assert.Equal(1, await pipeline.ExecuteAsync(1));
        Assert.Equal(2, await pipeline.ExecuteAsync(2));
    }

    [Fact]
    public async Task TestBatch()
    {
        var nCalls = 0;
        await using var pipeline = PipelineBuilder.Create<int>()
            .Batch(2, TimeSpan.FromMilliseconds(100), p =>
                p.StepSync(i =>
                {
                    nCalls++;
                    Assert.Equal(i[0] == 10 ? 1 : 2, i.Length);
                    return i.Select(x => x * 2).ToArray();
                })
            )
            .StepSync(i => i - 1)
            .Build();

        List<Task> tasks = [];
        for (var i = 0; i < 11; i++)
        {
            tasks.Add(Task.Run(async () =>
            {
                Assert.Equal((i * 2) - 1, await pipeline.ExecuteAsync(i));
            }));
            Thread.Sleep(10);
        }

        await Task.WhenAll(tasks);

        Assert.Equal(6, nCalls);
    }

    [Fact]
    public async Task TestWorkers()
    {
        int[] nCalls = [0, 0];
        await using var pipeline = PipelineBuilder.Create<int>()
            .StepSync(i => i * 100)
            .Workers(2, pipe => pipe.StepSync(arg =>
            {
                nCalls[arg.ThreadId]++;
                Thread.Sleep(arg.Item);
                return $"wow! {arg.Item}";
            }))
            .Build();

        var sw = Stopwatch.StartNew();
        var one = await pipeline.ExecuteAsync(1);
        sw.Stop();
        Assert.Equal("wow! 100", one);
        Assert.Equal(1, nCalls[0] + nCalls[1]);

        nCalls[0] = 0;
        nCalls[1] = 0;
        Task.WaitAll(
            Task.Run(async () => Assert.Equal("wow! 100", await pipeline.ExecuteAsync(1))),
            Task.Run(async () => Assert.Equal("wow! 200", await pipeline.ExecuteAsync(2))),
            Task.Run(async () => Assert.Equal("wow! 100", await pipeline.ExecuteAsync(1)))
        );
        Assert.Equal(3, nCalls[0] + nCalls[1]);
        Assert.NotEqual(nCalls[0], nCalls[1]);
        Assert.NotEqual(0, nCalls[0]);
        Assert.NotEqual(0, nCalls[1]);
    }

    [Fact]
    public async Task TestUnroll()
    {
        IEnumerable<((string name, int[] scores) input, int[] expected)> data =
        [
            (("Alice", [10, 20, 30]), [20, 40, 60]),
            (("Bob", [40, 50, 60]), [80, 100, 120]),
            (("Charlie", [70, 80, 90]), [140, 160, 180])
        ];

        await using var pipeline = PipelineBuilder.Create<(string name, int[] scores)>()
            .Unroll(
                unroll: it => it.scores,
                pipeline: p => p.StepSync(it => it.Item * 2),
                roll: (it) =>
                {
                    it.Input.scores = it.Results.ToArray();
                    return it.Input;
                }
            )
            .Build();

        foreach (var (input, expected) in data)
        {
            var (name, scores) = await pipeline.ExecuteAsync(input);
            Assert.Equal(expected, scores);
            Assert.Equal(input.name, name);
        }
    }
}