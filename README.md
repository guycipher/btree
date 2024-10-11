# GO BTree
A fast, simple disk based BTree implementation in Go.

https://pkg.go.dev/github.com/guycipher/btree

## Features
- Easy to use API with `Put`, `Get`, `Delete`, `Remove`, `Iterator`, `Range` methods
- Disk based storage with underlying pager
- Supports keys with multiple values
- Supports large keys and values

## Extra features
- `NGet` get's keys not equal to the key
- `NRange` get's keys not equal to provided range
- `GreaterThan` get's keys greater than the provided key
- `GreaterThanEq` get's keys greater than or equal to the provided key
- `LessThan` get's keys less than the provided key
- `LessThanEq` get's keys less than or equal to the provided key


> [!NOTE]
> 11th Gen Intel(R) Core(TM) i7-11700K @ 3.60GHz UBuntu with WDC WDS500G2B0A-00SM50(HDD) we insert 1 MILLION keys in `1m43` 1 minute and 43 seconds
> Write speed is roughly `10,000` keys per second with this setup
> This is at a page side of 1024 and a degree of 3
> File size is 577.0 megabytes

> [!WARNING]
> Not thread safe.  You must handle concurrency control yourself.

## Usage
### Importing
```
import "github.com/guycipher/btree"
```

### Creating a new BTree

You can use the ``Open`` method to open an existing btree or create a new one.
You can specify the file name, flags, file mode, and the degree of the btree.
```go
bt, err := btree.Open("btree.db", os.O_CREATE|os.O_RDWR, 0644, 3)
if err != nil {
..
}
```

### Inserting a key-value pair

You can insert a value into a key using the ``Put`` method.  Keys can store many values.
```go
err := bt.Put([]byte("key"), []byte("value"))
if err != nil {
..
}
```

### Getting a value

To get a value you can you the ``Get`` method.  The get method will return all the keys values.
```go
values, err := bt.Get([]byte("key"))
if err != nil {
..
}
```

#### NGet
To get all keys not equal to the key you can use the ``NGet`` method.
```go
keys, err := bt.NGet([]byte("key"))
if err != nil {
..
}
```

### GreaterThan
To get all keys greater than the key you can use the ``GreaterThan`` method.
```go
keys, err := bt.GreaterThan([]byte("key"))
if err != nil {
..
}
```

### GreaterThanEq
To get all keys greater than or equal to the key you can use the ``GreaterThanEq`` method.
```go
keys, err := bt.GreaterThanEq([]byte("key"))
if err != nil {
..
}
```

### LessThan
To get all keys less than the key you can use the ``LessThan`` method.
```go
keys, err := bt.LessThan([]byte("key"))
if err != nil {
..
}
```

### LessThanEq
To get all keys less than or equal to the key you can use the ``LessThanEq`` method.
```go
keys, err := bt.LessThanEq([]byte("key"))
if err != nil {
..
}
```

### Deleting a key

To delete a key and all of it's values you can use the ``Delete`` method.
```go
err := bt.Delete([]byte("key"))
if err != nil {
..
}
```

### Removing a value within key

To remove a value from a key you can use the ``Remove`` method.
```go
err := bt.Remove([]byte("key"), []byte("value"))
if err != nil {
..
}
```

### Key Iterator

The iterator is used to iterate over values of a key

```go
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
Get all keys between key1 and key3
```go
keys, err := bt.Range([]byte("key1"), []byte("key3"))
if err != nil {
..
}
```

### Not Range query
Get all keys not between key1 and key3
```go
keys, err := bt.NRange([]byte("key1"), []byte("key3"))
if err != nil {
..
}
```

### Closing the BTree

You can close the BTree by calling the Close function.
This will close the underlying file and free up resources.
```go
err := bt.Close()
if err != nil {
..
}
```

## Technical Details
This is an on disk btree implementation.  This btree has an underlying pager that handles reading and writing nodes to disk as well as overflows.
When an overflow is required for a page the overflow is created and the data is split between however many pages.
When a page gets deleted its page number gets placed into an in-memory slice as well as gets written to disk. These deleted pages are reused when new pages are needed.

A key on this btree can store many values.  Mind you a keys values are read into memory; So if you have a key like A with values Alex, Alice, Adam, and you call Get(A) all of those values will be read into memory.
You can use a key iterator to iterate over the values of a key.

The btree is not thread safe.  You must handle concurrency control yourself.

You can play with page size and degree(T) to see how it affects performance.  My recommendation is a smaller page size and smaller degree for faster reads and writes.

## License
View the [LICENSE](LICENSE) file
