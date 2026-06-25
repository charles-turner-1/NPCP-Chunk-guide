# Principles for chunking climate model output

This draft offers general guidance on choosing chunking schemes for climate model output. It is not intended to be prescriptive. The goal is to set out a small number of principles that climate modellers can apply when writing data.

A few overarching ideas matter throughout:

- Climate datasets from model output are rarely contained in a single netCDF file. They are usually written as a series of files, often covering the full spatial domain but only part of the time domain. Files are not usually described as chunks, but they are still a fundamental unit of storage. We focus mostly on within-file chunking here, while keeping file structure in view.
- There is **no single perfect chunking scheme**. What is best depends on the intended analysis. A scheme that is ideal for map-making will usually be poor for time-series extraction, and vice versa.
- When we refer to 'optimal chunking', we really mean the least suboptimal scheme for an unknown use case. We are not trying to optimise for one exact workflow, but to choose a scheme that is likely to perform reasonably well across many workflows.
- Data producers and publishers should keep rechunking in mind. Specific, common use cases may later justify analysis-ready archives with different chunking. Where possible, avoid primary chunking schemes that make future rechunking needlessly painful on current HPC systems.
- Different file formats (e.g. netCDF, HDF, Zarr) impose different constraints on chunking. We aim to give guidance that applies across formats, while recognising that Zarr and virtualisation are less flexible than a concatenated set of netCDF files. As a hard rule, we avoid recommending chunking schemes that are incompatible with Zarr and virtualisation, to help future-proof datasets.

Throughout, we use the language of modern analysis workflows, which in practice usually means Python. The `dask` library is ubiquitous when working with chunked datasets, so we use it as our reference implementation for chunking.

Because this document is intended for the NPCP, we also assume readers are familiar with the `xarray` data model.

For brevity, we do not cover codecs or compression here. They matter, and chunking affects them, but they are outside the scope of this document.

___

# Key Concepts
- **Chunking**: The process of dividing a dataset into smaller, more manageable pieces (chunks) for efficient storage and retrieval.
- **Header Data**: Metadata that describes the structure and content of the dataset, including information about variables, dimensions, and attributes.
- **Rectilinear Chunk Grid**: A chunking scheme where the dataset is divided into regular, rectangular chunks along its dimensions. The last chunk along each dimension may be smaller if the total size of the dimension is not perfectly divisible by the chunk size.
- **Optimal Chunking**: A chunking scheme that is designed to be efficient for a wide range of use cases, even if it is not perfectly optimized for any specific use case. It aims to balance the trade-offs between different access patterns and analysis needs.
- **Isotropic Chunking**: A chunking scheme where each dimension is composed of an approximately equal number of chunks. For example, a grid of size 360x180x50, divided into chunks of size 36x18x5, would be considered isotropic, as each dimension is divided into 10 chunks.
- **Disk/ File Chunks**: The size of a chunk on disk. Typically these are relatively small - on the order of kilobytes to a few megabytes. 
- **Dask Chunks**: The size of a dask chunk in memory, when loading a chunked dataset using the `dask` library. These are typically larger than disk chunks, and can be on the order of tens to hundreds of megabytes, depending on the use case and available memory.
- **Rechunking**: The process of changing the chunking scheme of a dataset after it has been created. This can be done to optimize for different access patterns or analysis needs, but can be computationally expensive and may require additional storage space during the rechunking process. 
- **Serialisation**: The process of converting a dataset into a format that can be stored on disk or transmitted over a network. This typically involves writing the dataset to a file format such as netCDF, HDF, or Zarr, which may have specific requirements for chunking and metadata.
- **Virtualisation**: A family of technologies that index the byte ranges within a file in order to directly access those byte ranges.

## Chunking - the 30,000 foot view

When a chunked dataset is opened with `xarray` and `dask`, the workflow usually looks something like this:
1. `xarray` reads the header data from the file(s) so it can understand the variables, dimensions, and attributes. This is usually fast because the header is small.
2. `xarray` creates a `dask` array for each variable, using the chunking scheme defined in the file(s). Each dask chunk corresponds to an integer number of on-disk chunks along each dimension.
3. If multiple files are opened, `xarray` concatenates those arrays along the appropriate dimension(s) to produce one logical array per variable for the whole dataset.
4. The user specifies an analysis. In the background, `xarray` and `dask` build a task graph describing the computations needed to produce that result.
5. When the user calls `.compute()`, `dask` executes that graph: reading the needed chunks from disk, performing the computations in memory, and producing the final result.

### Executing the task graph

Start with computation on a single dask chunk. Each dask chunk ultimately corresponds to a NumPy array. That whole array must be realised in memory, so the chunk must fit within the available system memory. This gives a conservative upper bound on dask chunk size. Put simply: a laptop with 8 GB of RAM cannot process a dask chunk larger than 8 GB. In practice, the usable limit is much lower, because memory is also needed for other processes, overheads, and often multiple dask chunks at once.

> [!TIP]
> The example choice of a laptop with 8GB of RAM here is not purely coincidental. The optimal dask chunk choice may be much larger for, e.g., a megamem ARE session with 192GB of RAM than on a personal laptop. As dask chunks should be an integer multiple of disk chunks, it is important that disk chunks are small enough that they can comfortably fit into memory on all reasonably anticipated hardware.

### Combining Chunks

To perform an analysis, dask builds a **task graph** that combines the results of operations on individual chunks into a final result. Each node in that graph represents work on a chunk, and each edge represents a dependency between operations. Smaller dask chunks mean more nodes and edges, and therefore more scheduling overhead.

### Principle: Larger chunks result in a smaller task graph, and therefore less dask overhead in executing the graph. This comes at the expense of increased memory pressure.

___
___

## A disk chunk is the quanta of storage on disk and a dask chunk is the quanta of computation in memory. 

Assume a chunked dataset with two simplifying assumptions:
- it contains a single variable
- we use the same dask and disk chunks

When `xarray` opens the file, it creates one dask chunk for each disk chunk. Each on-disk chunk therefore maps to one in-memory NumPy array.

Now imagine opening the 'first' chunk of the dataset and selecting only part of it. That operation has three steps:
1. Open the dataset and read the metadata to determine the chunking scheme.
2. Read the entire first chunk from disk into memory.
3. Discard the part of that chunk that is irrelevant to the selection.

From this, we can infer a couple of important principles:
1. Ideally, we do not want our disk chunks to be much larger than the typical size of a selection that a user might make. If they are, then users will be forced to read large chunks of data into memory, only to discard most of it. 
2. If we want to support efficient selection of small subsets of the data, we need to ensure that our disk chunks are small enough to allow for this. 
3. If our dask chunks are not well aligned with our disk chunks - for example, a dask chunk spans only half a disk chunk - then we will be forced to read and decode the disk chunk twice in order to write it into two separate dask chunks. IO is typically *the largest bottleneck* in analysis workflows, and so this is a situation we want to avoid.

### Principle: Disk chunks should be small enough to allow for efficient selection of small subsets of the data, and dask chunks should be an integer multiple of disk chunks in order to avoid unnecessary IO overhead.

___
___

## Rechunking: It is cheaper to combine than split chunks

Consider two 'orthogonal' analysis workflows: one which generates a map, and one which generates a time series.
- The map workflow is optimised by chunking the data along the time dimension, so that each chunk contains the full spatial domain of the data, but only a single slice of time.
- The time series workflow is optimised by chunking the data along the spatial dimensions, so that each chunk contains a single point in space, but the full time domain of the data.

This can be visualised as a 'pancakes to churros' or 'burgers to hotdogs' scenario:

```
 +-------------------+      +-------------------+           
 |                   |      |    |    |    |    |                            
 +-------------------+      +    |    |    |    +                            
 |                   |      |    |    |    |    |                            
 +-------------------+      +    |    |    |    +                            
 |                   |  ->  |    |    |    |    |    
 +-------------------+      +    |    |    |    +                            
 |                   |      |    |    |    |    |                            
 +-------------------+      +    |    |    |    +                            
 |                   |      |    |    |    |    |                            
 +-------------------+      +-------------------+                            
```

As a concrete example, consider producing time-series-optimised chunks from map-optimised chunks, as illustrated above.

In that scenario, we would need to either:
a. Read the entire dataset into one dask chunk (NumPy array) in memory, then split it into smaller chunks.
b. Repeatedly read chunks from disk and write slices of them into the appropriate output chunks. For example, to produce the first churro, we might read the first pancake, write its first 10% into the first churro, then read the second pancake and do the same, and so on until every pancake has contributed to that first churro. We would then repeat the process for each subsequent churro.

Now consider an isotropically chunked dataset. We can produce either pancakes or churros purely by combining chunks. We do not need to split chunks or over-read them. This is illustrated in the following diagrams:

```
+---+---+---+---+---+        +-------------------+            
|   |   |   |   |   |        |    |    |    |    |                             
+---+---+---+---+---+        +    |    |    |    +                             
|   |   |   |   |   |        |    |    |    |    |                             
+---+---+---+---+---+        +    |    |    |    +                             
|   |   |   |   |   |    ->  |    |    |    |    |     
+---+---+---+---+---+        +    |    |    |    +                             
|   |   |   |   |   |        |    |    |    |    |                             
+---+---+---+---+---+        +    |    |    |    +                             
|   |   |   |   |   |        |    |    |    |    |                             
+---+---+---+---+---+        +-------------------+                             
```
```
+---+---+---+---+---+         +-------------------+
|   |   |   |   |   |         |                   |
+---+---+---+---+---+         +-------------------+
|   |   |   |   |   |         |                   |
+---+---+---+---+---+         +-------------------+
|   |   |   |   |   |    ->   |                   |
+---+---+---+---+---+         +-------------------+
|   |   |   |   |   |         |                   |
+---+---+---+---+---+         +-------------------+
|   |   |   |   |   |         |                   |
+---+---+---+---+---+         +-------------------+
```

In this sense, although the isotropically chunked dataset is suboptimal for both workflows, it is the least suboptimal for both workflows, and therefore represents the optimal chunking scheme for an unknown use case.


---
---

## Why not tiny disk chunks?

As with dask chunks, smaller disk chunks require additional coordination overhead. This also happens unavoidably at an IO/filesystem level. Each chunk is compressed individually, and so one decompression operation is required per chunk. Smaller chunks therefore require, amongst other things, more decompression operations, increasing overhead.

### Principle: The best chunking for a given operation is the largest chunking scheme that does not cause us to over-read from disk or exceed our memory constraints. To accommodate multiple possible use cases, we should aim to produce the largest chunks that are still small enough to allow for efficient selection of small subsets of the data, and that can comfortably fit into memory on reasonably anticipated hardware.

___
___

## Considering the whole dataset: Chunks and Files

So far we have discussed chunking within a file. In practice, though, climate datasets are usually written as a series of files rather than a single file. Each file often contains the full spatial domain, but only part of the time domain.

Crucially, files can themselves be treated as chunks.

This matters when choosing isotropic chunking schemes. Climate models are numerical integrations, so output is often written as time slices, such as one month per file.

If we aim for isotropy only within each file, we may end up with a highly anisotropic scheme at the whole-dataset level. For example, suppose a dataset is 360x180x50 (lon x lat x time), written as 5 files of 360x180x10. We might chunk each file into 36x18x1. That is isotropic within each file, with ten chunks per dimension, but across the full dataset it yields ten chunks in each spatial dimension and 50 in time.

That looks highly anisotropic. But because there are already 5 files, there is no way to have fewer than 5 chunks in time. So the best we can do may be 5 chunks in time and 10 in each spatial dimension. That is still reasonably isotropic, and is likely to be a sensible choice for an unknown use case.

### Principle: Files *are* chunks, and cannot be ignored. Chunking schemes must take these 'file chunks' into account, as they are less mutable than disk chunks, and so are a stronger constraint on the optimal chunking scheme.

___
___

## Future Proofing: Avoiding Zarr-incompatible chunking schemes for virtualisation

Historically, climate model output has usually been written as netCDF. But netCDF performs poorly on cloud storage because it assumes access patterns that make much more sense on a local filesystem.

Zarr is a cloud-optimised format that extends the idea of a dataset being split across many files all the way down to the chunk level. A Zarr store is a hierarchical directory tree with separate metadata and a file for each chunk (or for a group of chunks, when sharding is used). Historically, though, Zarr has performed poorly on some HPC systems because it can create huge inode counts unless sharding is available and used.

In the Zarr data model, a large multi-file netCDF dataset can be represented as a single Zarr store. Because copying archival multi-petabyte datasets into Zarr is often prohibitively expensive, the Zarr ecosystem developed a family of approaches known as virtualisation. Virtualisation indexes byte ranges within the original netCDF files so individual chunks can be accessed directly through a Zarr view.

This brings substantial performance benefits and opens up the wider Zarr ecosystem for netCDF datasets. But it only works if the source dataset can be represented in the Zarr data model. In particular, a multi-file dataset must be representable as a *rectilinear chunk grid*. That means chunking decisions for multi-file datasets need extra care: it is easy to choose a scheme that blocks future virtualisation.

As a simple example, consider the following: daily data, written at monthly frequency. This *cannot* be virtualised, as the chunking scheme in time will be (31, 28, 31, 30 ...) for the different files, and so cannot be represented as a rectilinear chunk grid.

For a calendar without leap years, this can easily be solved by writing out data at either a daily, 73 day, or yearly frequency: these all produce rectilinear chunk grids. For a calendar with leap years, the implementation is more complex, but the principles are the same.

### Principle: Combining file chunks in a way which produces variable chunk lengths prohibits future virtualisation. Avoid wherever possible.