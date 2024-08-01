# GO BTree
Embedded btree database package fully implemented in GO.
This package provides an implementation of a concurrent paged disk BTree. A BTree is a self-balancing search tree, in which each node contains multiple keys and links to child nodes. It's optimized for systems that read and write large blocks of data, making it ideal for disk-based data structures.

I created this package for other projects I am working on. I wanted a simple, easy-to-use disk BTree that I could use in my projects and now hopefully yours.
Feel free to drop a PR or an issue if you have any suggestions or improvements.

## Features
- **Concurrent**: The BTree is safe for concurrent use.
- **Disk-based**: The BTree is designed to be used with a disk-based storage engine.
- **Customizable**: The BTree allows you to specify the page size and T.

## Supported key types
```
int
int8
int16
int32
int64
uint
uint8
uint16
uint32
uint64
float32
float64
string
[]byte
```

## Supported value types
```
Any, using interface{}
```

## Usage
### Importing
```
import "github.com/guycipher/btree"
```

### Creating a new BTree

You can create a new BTree by calling the NewBTree function and passing in the path to the database file, the permissions for the file, and the order of the BTree.

```
btree := btree.NewBTree("path/to/db", 777, 3)
```

### Inserting a key-value pair

You can insert a key-value pair into the BTree by calling the Insert function and passing in the key and value.

```
btree.Insert("key", "value")
```

Inserting the same key appends the value to the key.

### Getting a value

You can get a value from the BTree by calling the Get function and passing in the key.

```
value := btree.Get("key")
```
This will get all the values associated with the key.

### Deleting a key

You can delete a key from the BTree by calling the Delete function and passing in the key.

```
btree.Delete("key")
```
This will delete the key and all its values.

### Removing a value within key

You can remove a value from a key by calling the Remove function and passing in the key and the value.

```
btree.Remove("key", "value")
```
Removes the value from the key.

### Iterator

The iterator is used to iterate over values of a key

```
it := b.Iterator()

for it.Next() {
    value, err := it.Value()
    if err != nil {
        // handle error
    }

    fmt.Println(value)
}
```

### Closing the BTree

You can close the BTree by calling the Close function.

```
btree.Close()
```