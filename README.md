# lisa_meta: The Metadata Store (MDS) built with lisa framework

## Introduction

This is POC (prove of concept) application. The main purpose of this application is to show how we can utilize HllSets introduced in:

https://github.com/alexmy21/lisa.

**lisa_meta** is designed as a library and it includes the following files:

- lisa_graph.jl
- lisa_hdf5.jl
- lisa_neo4j.jl
- lisa_sets.jl (this is modified and enhanced version of the **lisa.jl** file in the lisa project. See git link above.)
- lisa_store.jl

The following is short descriptions of each of these files. 

We will start with two main library files:

- lisa_store.jl
- lisa_neo4j.jl.

First provides support for data ingestion and and building SQLiteDB presentation for the metadata graph. The second one is a tool that allows us to convert metadata graph in SQLiteDB into Neo4J Graph.

## lisa_store.jl

Metadata is a data about data. It provides description of original data that reflects following aspects:

- origin of the data like location;
- data type (tabular data, docs, images, streams, video, sound and etc.);
- encoding;
- access properties (uid, password and so on);
- modifications of the original version of data with information about changes;
- some other aspects that provide additional metadata in special cases.

In lisa_meta metadata represented as a collection of nodes (the actual metadata) and edges (relations between nodes). There are two designated tables in SQLiteDB:

- nodes with a following structure:
  
  - sha1 (SHA1 based node ID);
  - labels (graph labels that we are going to assign to the noe);
  - d_sha1 (SHA1 ID that represent ID the HllSet presentation of the original dataset);
  - dataset (compact dump of the HllSet);
  - props (a dictionary structure to hold additional properties);
- edges:
  
  - source (sha1 of the source node in relation);
  - target (sha1 of the target node in relation);
  - r_type (label that we are assigning to the edge);
  - props (dict structure for additional properties). 

The modifications represent the history of the original data and should be managed the same way how we are managing version in, for example, github.

Git as you know keeps history of changes by recording snapshots on each commit. The newest version of document is always living in the working directory. This directory is separated from the repository and should be committed in order to add to the repository.

We are using similar approach to manage metadata. Initially we are collecting fresh metadata in the temporary t_nodes and t_edges tables. Then, after we will complete processing, we are committing to the nodes and edges tables.

During commit we can find that the node or the edge already exist. In this case we are comparing new and old version of the node or edge and, if they are the same, we do nothing. Otherwise, we are replacing old version with a new one, by moving old node or edge to the archive file. 

We are using HDF5 as an archive storage. Here is an example of nodes and edges in the HDF5 file.

In the provided HDF5 layout the top folder 📂1b5af3f4-e7bb-4cf8-b3b9-6114155d9cf3 is the time based commit ID. All nested folders hold metadata for all nodes and edges submitted in this commit.

Detailed information about commit is in **commit** SQLiteDB table.

```🗂️ HDF5.File: (read-only) lisa_arch.hdf5
 ├─ 📂 1b5af3f4-e7bb-4cf8-b3b9-6114155d9cf3
 │  ├─ 📂 edges
 │  │  ├─ 📂 1c4c950dda437c3d55ee6ef6a43548484facde02
 │  │  │  └─ 📂 has_column
 │  │  │     ├─ 🔢 668d871e8b13df7e378375c676168703761fedd3
 │  │  │     │  └─ (5 children)
 │  │  │     └─ 🔢 88568fd93f5b196bc5495ff637fcb4ef1dd52a81
 │  │  │        └─ (5 children)
 │  │  ├─ 📂 3da13fcace26bf7993425973090823e88c394be2
 │  │  │  └─ 📂 has_column
 │  │  │     ├─ 🔢 02b4e972ea698768322c30eff0fab5a64afda302
 │  │  │     │  └─ (5 children)
 │  │  │     ├─ 🔢 7bb6a94745236688ae32e5f7013745148f9ffb98
 │  │  │     │  └─ (5 children)
 │  │  │     └─ 🔢 c3157aee164669eba31fc4d4816fe43a34f34372
 │  │  │        └─ (5 children)

 │  └─ 📂 nodes
 │     ├─ 📂 02b4e972ea698768322c30eff0fab5a64afda302
 │     │  └─ 🔢 _csv_column_
 │     │     ├─ 🏷️ column_name
 │     │     ├─ 🏷️ column_type
 │     │     ├─ 🏷️ commit_id
 │     │     └─ 🏷️ file_sha1
 │     ├─ 📂 0c3f59ddd3d4e9a33773de121a62f456ac3b5388
 │     │  └─ 🔢 _csv_file_
 │     │     ├─ 🏷️ commit_id
 │     │     ├─ 🏷️ file_name
 │     │     └─ 🏷️ file_type
 ```

Check **lisa_store.ipynb** for more details about using **lisa_store.jl**.

## lisa_neo4j.jl

The **julia_neo4j.ipynb** file provides an example of utilizing **lisa_neo4j.jl** file.

Here we are implementing a very simple scenario.

1. We are running search against **tokens** SQLiteDB table that actually represent an inverted index of all collected terms from all processed datasets with the references to the original nodes. So, the query should return all nodes that include tokens in the query.
2. We are collecting all nodes and edges and we are creating Neo4J Graph.
3. As an additional step, we are creating relations between similar csv files. We are using the Jaccard index to measure similarity between two HllSets as a substitute for the measuring similarity between original files that could be very big and in many cases are not accessible.

Here are snapshots of the graphs:

Neo4J Graph created after step 2.

![alt text](<Screenshot from 2024-02-22 14-03-20.png>)

And this is a snapshot of similar csv files in the graph.

![alt text](<Screenshot from 2024-02-22 14-11-18.png>)
