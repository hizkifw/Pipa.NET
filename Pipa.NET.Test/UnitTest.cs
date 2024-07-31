using System.Diagnostics;
using Pipa.NET;

namespace Pipa.NET.Test;

public class UnitTest
{
    [Fact]
    public void TestSteps()
    {
        var pipeline = PipelineBuilder.Create<int>()
            .Step(i => i.ToString())
            .Step(int.Parse)
            .Build();

        Assert.Equal(1, pipeline.Execute(1));
        Assert.Equal(2, pipeline.Execute(2));
    }

    [Fact]
    public void TestBatch()
    {
        var nCalls = 0;
        var pipeline = PipelineBuilder.Create<int>()
            .Batch(2, TimeSpan.FromMilliseconds(100), p =>
                p.Step(i =>
                {
                    nCalls++;
                    Assert.Equal(i[0] == 10 ? 1 : 2, i.Length);
                    return i.Select(x => x * 2).ToArray();
                })
            )
            .Step(i => i - 1)
            .Build();

        List<Task> tasks = [];
        for (var i = 0; i < 11; i++)
        {
            tasks.Add(Task.Run(() =>
            {
                Assert.Equal((i * 2) - 1, pipeline.Execute(i));
            }));
            Thread.Sleep(10);
        }

        Task.WaitAll([.. tasks]);

        Assert.Equal(6, nCalls);
    }

    [Fact]
    public void TestWorkers()
    {
        int[] nCalls = [0, 0];
        var pipeline = PipelineBuilder.Create<int>()
            .Step(i => i * 100)
            .Workers(2, pipe => pipe.Step(arg =>
            {
                nCalls[arg.ThreadId]++;
                Thread.Sleep(arg.Item);
                return $"wow! {arg.Item}";
            }))
            .Build();

        var sw = Stopwatch.StartNew();
        var one = pipeline.Execute(1);
        sw.Stop();
        Assert.Equal("wow! 100", one);
        Assert.Equal(1, nCalls[0] + nCalls[1]);

        nCalls[0] = 0;
        nCalls[1] = 0;
        Task.WaitAll(
            Task.Run(() => Assert.Equal("wow! 100", pipeline.Execute(1))),
            Task.Run(() => Assert.Equal("wow! 200", pipeline.Execute(2))),
            Task.Run(() => Assert.Equal("wow! 100", pipeline.Execute(1)))
        );
        Assert.Equal(3, nCalls[0] + nCalls[1]);
        Assert.NotEqual(nCalls[0], nCalls[1]);
        Assert.NotEqual(0, nCalls[0]);
        Assert.NotEqual(0, nCalls[1]);
    }
}