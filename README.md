# skarndb

A RocksDB style embeddable persistent key-value store written in Rust

```rust
let db = Database::open_with_config(
    DatabaseConfig::default().directory(PathBuf::from("/tmp/db")),
)?;
db.insert("key".to_string().as_bytes(), "value".to_string().as_bytes())?;

assert_eq!(
    db.get("key")
        .map(|v| String::from_utf8(v.to_vec()).unwrap()),
    Some("value".to_string())
);
db.compact();
```

# Features

- API similar to a threadsafe BTreeMap<[u8], [u8]>
- background flushing
- zstd compression
- compaction

# Notes

- Needs more testing, for production uses you should probably just use RocksDB, sled or SQLite
- The on-disk format will change
- No write ahead log yet, meaning you will lose data if it's suddenly killed
- Lots of missing features
