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
        var pipeline = PipelineBuilder.Create<int>()
            .Step(i => i * 100)
            .Workers(4, pipe => pipe.Step(i =>
            {
                Thread.Sleep(i);
                return $"wow! {i}";
            }))
            .Build();

        var sw = Stopwatch.StartNew();
        var one = pipeline.Execute(1);
        sw.Stop();
        Assert.InRange(sw.ElapsedMilliseconds, 99, 105);

        sw.Restart();
        Task.WaitAll(
            Task.Run(() => pipeline.Execute(1)),
            Task.Run(() => pipeline.Execute(1)),
            Task.Run(() => pipeline.Execute(1)),
            Task.Run(() => pipeline.Execute(2)),
            Task.Run(() => pipeline.Execute(1)),
            Task.Run(() => pipeline.Execute(1))
        );
        sw.Stop();
        Assert.InRange(sw.ElapsedMilliseconds, 199, 205);
    }
}