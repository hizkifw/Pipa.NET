# Pipa.NET

Batching and parallelization helper for .NET.

1. Construct a pipeline

   ```csharp
   using Pipa.NET;

   var pipeline = PipelineBuilder.Create<string>()
       .Workers(4, pipe => pipe
           .Step(arg => LoadImageFromPath(arg.Item))
           .Step(image => CropImage(input, 128, 128)))
       .Batch(batchSize: 16, maxWaitTime: TimeSpan.FromMilliseconds(100), pipe => pipe
           .Step(images => GetImageEmbeddingsBatch(images))
           .Step(embeddings => NormalizeEmbeddingsBatch(embeddings))
       )
       .Build();
   ```

2. Call the pipeline. The input from multiple concurrent executions will be
   batched and parallelized according to the pipeline configuration.

   ```csharp
   // Get a single embedding from a single image path
   var normalizedEmbeddings = pipeline.Execute(imagePath);
   ```
