# Principles for chunking climate model output

This is a draft document aiming to provide some general guidance on deciding chunking for climate model output. There is little intent here to be prescriptive: instead, we aim to provide a set of general principles climate modellers can apply when writing outputs.

In this document, there are a few overarching principles that we think are important to considers:

- Climate datasets produced from the output of a model run are generally not contained within a single netCDF file. Instead, they are typically written out as a series of files, often containing the full spatial domain but for a subset of the time domain. Files are not often considered to be a chunk in the typical sense, but represent a fundamental unit of storage and chunking. We aim to describe our principles here at a within file level, but it is important to consider the structure of the files themselves.
- There is **no such thing as a perfect or optimal chunk scheme**. The optimal chunking scheme for a dataset is fundamentally dependent on the intended analysis to be performed on that dataset. For example, chunking a dataset optimally for producing maps will result in the least optimal chunk scheme for producing time series, and vice versa.
- In this document, we may occasionally refer to 'optimal chunking'. More precisely, what we mean by optimal chunking is the least suboptimal chunking for an unknown use case. That is, we are not trying to optimise chunking for a specific use case, but instead trying to find a chunking scheme that is likely to be reasonably performant for a wide range of use cases.
- Different file formats (eg. netCDF, HDF, Zarr) have different limitations on how they may chunk data. We will endeavour to provide guidance that is applicable across file formats. However, it is important to note that Zarr and virtualisation technologies require more prescriptive/less flexible chunking schemes than can be created via the concatenation of netCDF files. We will, as a hard rule, avoid recommending chunking schemes that are incompatible with Zarr and virtualisation. This is necessary to ensure the future proofing of datasets, and to ensure that they can be easily converted to Zarr if desired.

In this document, we will, in general, refer to chunking using terminology that is common to analyis workflows. These are typically in Python in modern workflows. The `dask` library is ubiquitous when working with local chunked datasets, and so we use it as our reference implementation of chunking.

As this document is intended for the NPCP, we will also assume users are familiar with the `xarray` data model, which is similarly ubiquitous in modern analysis workflows.

In the interests of brevity, we will not consider codecs, compression, or anything like that. They are all important considerations too and chunking plays a role in their effectiveness, but we would like this document to end eventually.

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
- **Virtualisation**: A family of technologies that index the byte ranges within a file in order to directly access those bytes ranges. 

## Chunking - the 30,000 foot view

When opening a chunked dataset with `xarray` and `dask`, the opening of a dataset typically looks something like the following:
1. `xarray` reads the header data from the file(s) to understand the structure of the dataset, including the variables, dimensions, and attributes. This is typically a very fast operation, as the header data is small and can be read quickly.
2. `xarray` creates a `dask` array for each variable in the dataset, using the chunking scheme defined in the file(s). This involves creating a `dask` array that is composed of many smaller chunks, each of which corresponds to an integer number of chunks on disk along each dimension.
3. If multiple files are opened, xarray concatenates the `dask` arrays from each file along the appropriate dimension(s) to create a single `dask` array for each variable that represents the entire dataset. 
4. The user specifies an analysis to be performed on the dataset. In the background, xarray and dask build a 'task graph', a graph which describes the series of computations that must be performed in order to compute the analysis the user has described.
5. When the user calls `.compute()` on the result of their analysis, `dask` executes the task graph, which involves reading the necessary chunks from disk, performing the necessary computations in memory, and creates the final result.

### Executing the task graph

Let's first consider computation on a single dask chunk. Fundamentally, each dask chunk corresponds to a numpy array. The whole dask chunk is realised into memory as a single array, and so the entire numpy array corresponding to that chunk must, at the very least, fit into the total available system memory. This provides *the most conservative upper bound* on the size of a dask chunk. Put simply, a laptop with 8GB of RAM cannot process a dask chunk that is larger than 8GB. In practice, the maximum size of a dask chunk is likely to be much smaller than the total available memory, as the system needs to allocate memory for other processes, and the analysis being performed may require multiple dask chunks to be loaded into memory at the same time.

> [!TIP]
> The example choice of a laptop with 8GB of RAM here is not purely coincidental. The optimal dask chunk choice may be much larger for eg. a megamem ARE session with 192GB of RAM, than on a personal laptop. As dask chunks should be an integer multiple of disk chunks, it is important that disk chunks are small enough that they can comfortablty fit into memory on all reasonably anticipated hardware.

### Combining Chunks

In order to perform an analysis, dask must build a **task graph**, which it then uses to combine the results of each dask chunk computation into the final result. Each node in the task graph represents an operation on a single dask chunk. Each edge in the graph represents a dependency between operations on two dask chunks. The smaller the dask chunks, the more nodes and edges there will be in the task graph, and the more overhead there will be in executing the task graph. 

### Principle: Larger chunks result in a smaller task graph, and therefore less dask overhead in executing the graph. This comes at the expense of increased memory pressure.

___
___

## A disk chunk is the quanta of storage on disk and a dask chunk is the quanta of computation in memory. 

Let's assume we have a chunked dataset, with a couple of simplifying assumptions:
- Comprising a single variable
- We intend to use the same dask and disk chunks. 

When `xarray` opens this file, it creates a dask array for the variable, with one dask chunk for each disk chunk. This means that for each disk chunk, there will be a corresponding in-memory numpy array.

Imagine now that we want to open the 'first' chunk of the dataset, and select a subset of it. This whole operation occurs in three parts:
1. Open the chunked dataset, and read the metadata in order to determine the chunking scheme.
2. Read the entirety of the first chunk from disk into memory.
3. Discard the parts of the chunk which are irrelevant to our selection.

From this, we can infer a couple of important principles:
1. Ideally, we do not want our disk chunks to be much larger than the typical size of a selection that a user might make. If they are, then users will be forced to read large chunks of data into memory, only to discard most of it. 
2. If we want to support efficient selection of small subsets of the data, we need to ensure that our disk chunks are small enough to allow for this. 
3. If our dask chunks are not well aligned with our disk chunks - for example, a dask chunk spans only half a disk chunk - then we will be forced to read the decode the disk chunk twice in order to write it into two separate dask chunks. IO is typically *the largest bottleneck* in analysis workflows, and so this is a situation we want to avoid.

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

As a concrete example of this, consider a workflow where we wish to produce timeseries optimised chunks from map optimised chunks - as illustrated above.

In such a scenario, we would need to either:
a. Read the entire dataset into a single dask chunk (numpy array) in memory, and then split it into smaller chunks.
b. Read each chunk repeatedly from disk, and write it into the appropriate dask chunks on disk. For example, to prodfuce the first churro, we might need to read the first pancake, write the first 10% of it into the first churro, then read the second pancake, write the first 10% of it into the first churro, and so on until we have read all pancakes and written the first churro. We would then repeat this process for each subsequent churro.


Now consider an isotropically chunked dataset. We can produce either pancakes, or churros, purely by combining chunks: it it not necessary to split any chunks, nor 'overread' any chunks. This is illustrated in the following diagrams:

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

*Question: Typically, it is common to produce either maps **or** timeseries. Combination plots such as Hoevmuller plots are less common.  Therefore, it stands to reason that our notion of isotropic chunks ought to consider latitude and longitude to be somewhat entangled, and weighted together somehow. A mathematically pure notion of this currently escapes me, but I think it should involve square roots somehow.*

---
---

## Why not tiny disk chunks?

As with dask chunks, smaller disk chunks require additional coordination overhead. This also happens unavoidably at an IO/filesystem level. Each chunk is compressed individually, and so one decompression operation is required per chunk. Smaller chunks therefore require, amongst other things, more decompression operations, increasing overhead.

### Principle: The best chunking for a given operation are the largest chunks that do not cause us to over-read from disk, or exceed our memory constraints. To accomodate for multiple possible use cases, we should aim to produce the largest chunks that are still small enough to allow for efficient selection of small subsets of the data, and that can comfortably fit into memory on reasonably anticipated hardware.

___
___

## Considering the whole dataset: Chunks and Files

So far, we have only discussed chunking at a within-file level. However, as we have already noted, climate datasets are typically not contained within a single file, but are instead written out as a series of files. Each file typically contains the full spatial domain of the data, but only a subset of the time domain.

Crucially, we can think of files themselves as a chunk. 

When choosing isotropic chunking schemes, this becomes important. Climate models are numerical integrations, and so typically written out in terms of time slices - one month of data per file, for example. 

If we then try to create a chunking scheme that is isotropic at a within-file level, we may end up with a chunking scheme that is highly anisotropic at the whole dataset level. For example, if we have a dataset that is 360x180x50 (lon x lat x time), and we write it out as 5 files of 360x180x10, then we might choose to chunk each file into chunks of size 36x18x1. This would be isotropic at the within-file level, with ten chunks per dimension, but at the whole dataset level, we would have ten chunks per spatial dimension, but 50 in time. 

This seems highly anisotropic - we have many more chunks in time than in space. However, as we have 5 files, there is no way to have fewer than 5 chunks in time. Therefore, the 'best' we can do is to have 5 chunks in time, and 10 chunks in each spatial dimension. This is still reasonably isotropic, and is likely to be the optimal chunking scheme for an unknown use case.

### Principle: Files *are* chunks, and cannot be ignored. Chunking schemes must take these 'file chunks' into account, as they are less mutable than disk chunk, and so are a stronger constraint on the optimal chunking scheme.

___
___

## Future Proofing: Avoiding Zarr-incompatible chunking schemes for virtualisation

Historically, climate model output has typically been written as netCDF. However, netCDF fares extremely poorly on cloud storage, due to assumptions which only hold on local filesystem storage.

Zarr is a modern, cloud optimised data format, which takes the notion of a dataset being comprised of multiple files, and extends that to the chunk level. A zarr store is a hierarchial directory tree, with separated metadata and a file for each chunk (or a group of chunks, known as sharding). However, this file format has historically fared poorly on HPC systems, as it creates large numbers of inodes unless sharding (unavailable prior to zarr v3) in used.

In the zarr data model, a large, multi-file netCDF dataset is represented by a single zarr store. The developers of zarr, noting that copying archival, multi PB datasets to zarr is prohibitively expensive, devloped a set of technologies known as virtualisation. Virtualisation takes a group of netCDF files, and creates a zarr store which indexes byte ranges within those files in order to directly access individual chunks.

This has a wide range of performance benefits and enables the used of Zarr's cloud optimised features and burgeoning ecosystem with netCDF datasets. However, it requires that the dataset being virtualised respects the zarr data model. In particular, for a multi file dataset, it requires that the chunking scheme for the combined dataset can be represented as a *rectilinear chunk grid*. This can require particular care when choosing chunking schemes for multi-file datasets, as it is easy to end up with a chunking scheme that is incompatible with virtualisation, and therefore cannot be easily converted to virtual zarr in the future.

As a simple example, consider the following: daily data, written at monthly frequency. This *cannot* be virtualised, as the chunking scheme in time will be (31, 28, 31, 30 ...) for the different files, and so cannot be represented as a rectilinear chunk grid.

For a calendar without leap years, this can easily be solved by writing out data at either a daily, 73 day, or yearly frequency: these all produce rectilinear chunk grids. For a calendar with leap years, the implementation is more complex, but the principles are the same.

### Principle: Combining file chunks in a way which produces variable chunk lengths prohibits future virtualisation. Avoid wherever possible.