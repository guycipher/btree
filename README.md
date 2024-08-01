# GO BTree
Embedded btree database package fully implemented in GO.

This package provides an implementation of a concurrent paged disk BTree.  I created this package for other projects I am working on. I wanted a simple, easy-to-use disk BTree that I could use in my projects and now hopefully yours.
Feel free to drop a PR or an issue if you have any suggestions or improvements.

## Features
- **Concurrent**: The BTree is safe for concurrent use.
- **Disk-based**: The BTree is designed to be used with a disk-based storage engine.

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

You can use the ``Open`` method to open an existing btree or create a new one.
You can specify the file, permission and T(degree)
```
btree := btree.Open("path/to/db", 777, 3)
```

### Inserting a key-value pair

You can insert a value into a key using the ``Put`` method.  Keys can store many values.
```
btree.Put("key", "value")
```

### Getting a value

To get a value you can you the ``Get`` method.  The get method will return all the keys values.
```
value := btree.Get("key")
```

### Deleting a key

To delete a key and all of it's values you can use the ``Delete`` method.
```
btree.Delete("key")
```

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

### Range query
```
keys, err := bt.Range(12, 16)
if err != nil {
    t.Fatal(err)
}
```

### Closing the BTree

You can close the BTree by calling the Close function.

```
btree.Close()
```