# GO BTree
Embedded btree database package fully implemented in GO.
This package provides an implementation of a concurrent paged disk BTree. A BTree is a self-balancing search tree, in which each node contains multiple keys and links to child nodes. It's optimized for systems that read and write large blocks of data, making it ideal for disk-based data structures.

## Features
- **Concurrent**: The BTree is safe for concurrent use.
- **Disk-based**: The BTree is designed to be used with a disk-based storage engine.
- **Customizable**: The BTree allows you to specify the page size and T.

## From Author
I created this package for other projects I am working on. I wanted a simple, easy-to-use disk BTree that I could use in my projects and now hopefully yours.

## Usage
coming soon