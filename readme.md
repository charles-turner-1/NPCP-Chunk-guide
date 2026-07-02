# Principles for chunking climate model output

This document is a short guide to choosing chunking schemes for climate model output. It is not meant to be prescriptive. The aim is to give data producers a small set of principles that usually lead to sensible outcomes.

The core problem is simple: there is **no single optimal chunking scheme**. Chunking that is ideal for one workflow is often poor for another. A map-friendly layout is usually bad for extracting long time series, and vice versa. So in practice we are usually looking for the **least bad default** for an unknown future use case.

A few assumptions shape the discussion:

- climate datasets are usually spread across many files, not one
- chunking needs to work for modern `xarray`/`dask` workflows
- rechunking later may be useful, but should not be made unnecessarily expensive
- chunking choices should not block future Zarr conversion or virtualisation
- codecs and compression matter too, but are out of scope here

## The basic mental model

When a chunked dataset is opened with `xarray` and `dask`, the workflow is roughly:

1. `xarray` reads metadata from the file(s)
2. `xarray` builds `dask` arrays using the on-disk chunk layout
3. if there are multiple files, those arrays are concatenated into one logical dataset
4. the user describes an analysis
5. `dask` builds and executes a task graph to perform it

That gives us two distinct chunking scales:

- **Disk chunks** are the quanta of storage and IO
- **Dask chunks** are the quanta of computation in memory

Those are related, but they are not the same thing.

## Principle 1: dask chunks must fit comfortably in memory

For data providers, this matters because dask chunks are usually formed from one or more on-disk chunks. In other words, disk chunking is not separate from dask chunking: it sets the building blocks from which sensible dask chunks can be assembled.

Each dask chunk ultimately becomes an in-memory NumPy array. So the most basic constraint is that a dask chunk has to fit into available memory.

That upper bound is obvious, but it is not generous. A machine with 8 GB of RAM cannot safely process an 8 GB dask chunk: memory is also needed for overheads, other arrays, and the rest of the system. In practice, usable dask chunks are much smaller than total RAM.

Smaller dask chunks, however, create larger task graphs. More chunks means more nodes, more dependencies, and more scheduling overhead.

So there is a trade-off:

- **larger dask chunks** reduce graph overhead
- **smaller dask chunks** reduce memory pressure

**Principle:** choose dask chunks that are as large as practical without creating memory problems.

> [!TIP]
> The right dask chunk size depends on expected hardware. A chunking choice that is fine on a very large ARE session may be painful on a laptop. If you expect users to work across a range of environments, disk chunks should be small enough that dask chunks can still be formed sensibly on modest hardware.

## Principle 2: disk chunks should be small enough to avoid over-reading

Now consider a simple selection from a chunked variable. To read a small subset of data, the system generally has to:

1. inspect metadata
2. read the relevant chunk from disk
3. discard the part of that chunk that was not actually needed

That means a disk chunk should not be wildly larger than the typical selection a user might make. If it is, users are forced to read and decode large amounts of irrelevant data just to get a small result.

At the same time, disk chunks should align cleanly with dask chunks. If a dask chunk cuts across disk chunks awkwardly, the same disk chunk may need to be read and decoded multiple times.

So disk chunks should satisfy two goals:

- be small enough to support efficient selection of common subsets
- compose cleanly into dask chunks, ideally as integer multiples

In practice, this usually means erring toward fairly small disk chunks. They do not need to be tiny, but they should usually be small enough that downstream users can combine them flexibly into dask chunks on modest hardware.

**Principle:** disk chunks should be small enough for efficient subset access, and dask chunks should be integer multiples of disk chunks wherever possible.

## Principle 3: it is cheaper to combine chunks than split them

A useful default strategy is to prefer chunk layouts that are easy to recombine later.

Consider two common workflows:

- a **map-oriented** workflow prefers chunks that are thin in time and broad in space
- a **time-series** workflow prefers chunks that are thin in space and long in time

Moving from one extreme to the other is expensive if existing chunks have to be split. Splitting means either:

- loading very large regions into memory and rewriting them, or
- repeatedly re-reading the same source chunks and slicing pieces into new outputs

Combining chunks is much cheaper than splitting them.

This can be visualised as a 'pancakes to churros' problem:

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

This is why an **isotropic** chunking scheme is often a good compromise. It is not usually optimal for any one workflow, but it is often easier to recombine into more specialised layouts later:

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

**Principle:** for an unknown use case, prefer chunking schemes that are easy to recombine, because rechunking by combining is much cheaper than rechunking by splitting.

## Principle 4: tiny chunks are not free

It is tempting to make chunks very small in order to avoid over-reading. But tiny chunks create their own costs.

Each chunk carries coordination overhead. Each chunk may need separate IO, separate bookkeeping, and separate decompression. So very small chunks can become inefficient even when they make access patterns look tidy on paper.

This gives a second balancing act:

- chunks should be small enough to avoid excessive over-reading
- chunks should be large enough to avoid excessive per-chunk overhead

In practice, that usually means disk chunks should be **small, but not tiny**. A few-megabyte disk chunk is often a more forgiving starting point than a very large one, because users can combine small chunks later more easily than they can undo over-large chunks.

**Principle:** the best chunking for a given workflow is usually the largest chunking scheme that does not create unacceptable over-reading or memory pressure. However, err on the side of smaller chunks.

## Principle 5: files are chunks too

It is not enough to think only about within-file chunking. Climate model output is usually written as multiple files, often covering the full spatial domain for only part of the time domain.

Those files are themselves a structural constraint.

For example, imagine a dataset with shape 360x180x50 (lon x lat x time), written as 5 files of 360x180x10. If each file is internally chunked into 36x18x1, that looks isotropic within a file: ten chunks along each dimension. But across the full dataset, time now has 50 chunks while space has 10.

That may look anisotropic, but the file layout has already imposed a minimum amount of chunking in time. File structure therefore has to be considered part of the chunking scheme, not an unrelated detail.

**Principle:** files are effectively chunks, and chunking decisions must account for them.

## Principle 6: do not block future Zarr conversion or virtualisation

Historically, climate model output has usually been stored as netCDF. That works well on traditional filesystems, but less well on cloud storage.

Zarr is much better suited to cloud-native access patterns, and virtualisation technologies now make it possible to expose existing netCDF archives through Zarr-like views without rewriting the entire dataset.

But that only works if the original chunking can be represented as a **rectilinear chunk grid**.

This matters most for multi-file datasets. A chunking scheme that looks reasonable in a netCDF context can still be incompatible with future virtualisation.

A simple example is daily data written one month per file. The implied time chunk lengths are 31, 28, 31, 30, and so on. That is not rectilinear, so it cannot be represented cleanly in the Zarr model.

Where possible, prefer write frequencies and chunk layouts that preserve a rectilinear chunk grid across the full dataset.

**Principle:** avoid chunking and file layouts that produce variable chunk lengths across files if future virtualisation matters.

## Practical summary

If you need a short version, it is this:

1. choose dask chunks that fit comfortably in memory
2. choose disk chunks small enough to support common selections
3. make dask chunks clean multiples of disk chunks
4. prefer layouts that can be recombined later rather than split
5. remember that file boundaries are part of the chunking scheme
6. avoid layouts that prevent future Zarr virtualisation

That still will not produce a universally optimal layout, because none exists. But it will usually produce a layout that is serviceable now, adaptable later, and less likely to paint the dataset into a corner.
