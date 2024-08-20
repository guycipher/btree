# GO BTree
A fast, simple persistent BTree implementation in Go.

## Features
- Easy to use API with Put, Get, Delete, Remove, Iterator, Range methods
- Disk based storage
- Supports keys with multiple values
- Supports large keys and values

### **not thread safe**
> [!WARNING]
> Not thread safe.  You must handle concurrency control yourself.

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
err := bt.Put([]byte("key"), []byte("value"))
```

### Getting a value

To get a value you can you the ``Get`` method.  The get method will return all the keys values.
```
values, err := bt.Get([]byte("key"))
```

### Deleting a key

To delete a key and all of it's values you can use the ``Delete`` method.
```
err := btree.Delete([]byte("key"))
```

### Removing a value within key

To remove a value from a key you can use the ``Remove`` method.
```
err := btree.Remove([]byte("key"), []byte("value"))
```

### Iterator

The iterator is used to iterate over values of a key

```
iterator := key.Iterator()

for {
    value, ok := iterator()
    if !ok {
        break
    }

    fmt.Println(string(value))
}
```

Result
```
value1
value2
value3
```

### Range query
```
keys, err := bt.Range([]byte("key1"), []byte("key3"))
```

### Closing the BTree

You can close the BTree by calling the Close function.

```
err := btree.Close()
```