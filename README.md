# Pipa.NET

Batching and parallelization helper for .NET.

1. Construct a pipeline

   ```csharp
   using Pipa.NET;

   await using var pipeline = PipelineBuilder.Create<string>()
       .Workers(4, pipe => pipe
           .Step(async arg => await LoadImageFromPath(arg.Item))
           .Step(async image => await CropImage(image, 128, 128)))
       .Batch(batchSize: 16, maxWaitTime: TimeSpan.FromMilliseconds(100), pipe => pipe
           .Step(async images => await GetImageEmbeddingsBatch(images))
           .Step(async embeddings => await NormalizeEmbeddingsBatch(embeddings))
       )
       .Build();
   ```

2. Call the pipeline. The input from multiple concurrent executions will be
   batched and parallelized according to the pipeline configuration.

   ```csharp
   // Get a single embedding from a single image path
   var normalizedEmbeddings = await pipeline.ExecuteAsync(imagePath);
   ```

3. Dispose of the pipeline when you're done using it, to free up resources and
   stop background tasks.
