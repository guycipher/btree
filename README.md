# GO BTree
This is an open source licensed under GPL paged disk btree implementation in Go.

## Features
- Easy to use API with Put, Get, Delete, Remove, Iterator, Range methods
- Fine grained concurrency control with node level locks
- Disk based storage
- Range queries
- Iterator
- Supports keys with multiple values

### **Not production ready just yet**

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
bt, err := btree.Open("path/to/btree.db", 0644, 3)
```

### Inserting a key-value pair

You can insert a value into a key using the ``Put`` method.  Keys can store many values.
```
err := bt.Put("key", "value")
```

### Getting a value

To get a value you can you the ``Get`` method.  The get method will return all the keys values.
```
values, err := bt.Get("key")
```

### Deleting a key

To delete a key and all of it's values you can use the ``Delete`` method.
```
err := btree.Delete("key")
```

### Removing a value within key

To remove a value from a key you can use the ``Remove`` method.
```
err := btree.Remove("key", "value")
```

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
```

### Closing the BTree

You can close the BTree by calling the Close function.

```
err := btree.Close()
```