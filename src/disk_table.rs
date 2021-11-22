use crate::mem_table::MemTable;
use crate::{DiskTableConfig, MemTableConfig};
use core::cmp;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use zstd::block::{compress_to_buffer, decompress};

const U64_BYTES: usize = (u64::BITS / 8) as usize;

pub struct DiskTable {
    file: File,
    blocks: Vec<Block>,
}

impl DiskTable {
    pub fn create(
        path: &Path,
        mem_table: &MemTable,
        disk_table_config: &DiskTableConfig,
    ) -> DiskTable {
        let mut file = File::create(path).expect("TODO handle file failed to create");
        let mut blocks = Vec::new();

        let len = disk_table_config.block_size;
        let mut buf = Vec::with_capacity(len);

        let max_compressed_len = (0.8 * len as f32).ceil() as usize; // if it doesn't compress a minimum amount we'll store it uncompressed
        let mut compressed_buf = Vec::with_capacity(max_compressed_len);

        let mut mem_table_iter = mem_table.iter();

        let mut next = mem_table_iter.next();
        let mut prev: Option<(&Box<[u8]>, &Box<[u8]>)> = None;
        let mut block_offset = 0 as u64;

        if next.is_none() {
            panic!("Tried to write empty MemTable");
        }

        let first_key = next.unwrap().0;
        let mut last_key = None;

        loop {
            // Create a block
            println!("Creating block");
            loop {
                if let Some((key, value)) = next {
                    let entry = Entry {
                        key: &key,
                        value: &value,
                    };

                    let space_left = buf.capacity() - buf.len();
                    if entry.len() > space_left {
                        break; // this block is full
                    }
                    println!("entry {} left:{}", entry.len(), space_left);
                    entry.write_to(&mut buf);
                } else {
                    break; // finished
                }
                prev = next;
                next = mem_table_iter.next();
            }

            last_key = prev.map(|kv| kv.0);

            println!("buf len = {}", buf.len());
            let (compression_type, block_size) =
                match compress_to_buffer(&buf, &mut compressed_buf, 0) {
                    Ok(compressed_size) => {
                        println!("compressed len = {}", compressed_size);
                        file.write_all(&compressed_buf[..compressed_size]);
                        (CompressionType::Zstd, compressed_size)
                    }
                    Err(e) => {
                        file.write_all(&buf);
                        (CompressionType::None, buf.len())
                    }
                };
            blocks.push(Block {
                compression_type,
                offset: block_offset,
                size: block_size,
                first_key: first_key.clone(),
                last_key: last_key.unwrap().clone(),
            });
            block_offset += block_size as u64;
            buf.clear();
            compressed_buf.clear();

            if next.is_none() {
                break; // done
            }
        }

        // Write blocks
        for block in &blocks {
            block.write_to(&mut file);
        }
        // Write number of blocks
        file.write_all(&((blocks.len() as u64).to_le_bytes()))
            .expect("Failed to write to file");

        // Flush then reopen readonly
        file.flush().expect("Failed flush");
        drop(file);
        let file = File::open(path).expect("TODO handle file failed to open");

        println!("{} blocks", blocks.len());

        return DiskTable { file, blocks };
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Box<[u8]>> {
        let key_ref = key.as_ref();
        // Find which block it could be in
        // TODO binary search
        let block_search = self.blocks.iter().find(|&block| {
            key_ref.cmp(block.first_key.as_ref()).is_ge()
                && key_ref.cmp(block.last_key.as_ref()).is_le()
        });
        if block_search.is_none() {
            return None;
        }

        let block = block_search.unwrap();
        let mut buf = vec![0; block.size];
        self.file
            .read_exact_at(&mut buf, block.offset)
            .expect("Failed to read from file");

        let block_data = match (block.compression_type) {
            CompressionType::None => buf,
            CompressionType::Zstd => decompress(&buf, usize::MAX).expect("Failed to decompress"),
        };

        println!("block size: {}", block_data.len());

        let mut index = 0;
        loop {
            let key_size =
                u64::from_le_bytes(block_data[index..index + U64_BYTES].try_into().unwrap());
            index += U64_BYTES;
            let mut key_matches = false;
            // Only need to compare key if it's the same length
            if key_size == key_ref.len() as u64 {
                let entry_key = &block_data[index..index + key_size as usize];
                if key_ref == entry_key {
                    key_matches = true;
                }
            }
            index += key_size as usize;
            let value_size =
                u64::from_le_bytes(block_data[index..index + U64_BYTES].try_into().unwrap());
            index += U64_BYTES;

            if key_matches {
                return Some(
                    block_data[index..index + value_size as usize]
                        .to_vec()
                        .into_boxed_slice(),
                );
            }

            index += value_size as usize;

            if index >= block_data.len() {
                break;
            }
        }
        return None;
    }
}

#[derive(Clone)]
enum CompressionType {
    None = 0,
    Zstd,
}

struct Block {
    compression_type: CompressionType,
    offset: u64,
    size: usize,
    first_key: Box<[u8]>,
    last_key: Box<[u8]>,
}

impl Block {
    fn write_to(&self, file: &mut File) {
        file.write_all(&((self.compression_type.clone() as u8).to_le_bytes()))
            .expect("Failed to write to file");
        file.write_all(&(self.offset.clone().to_le_bytes()))
            .expect("Failed to write to file");
        file.write_all(&((self.size as u64).to_le_bytes()))
            .expect("Failed to write to file");
        file.write_all(&*self.first_key)
            .expect("Failed to write to file");
        file.write_all(&*self.last_key)
            .expect("Failed to write to file");
    }
}

struct Entry<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl Entry<'_> {
    fn len(&self) -> usize {
        self.key.len() + self.value.len() + 8
    }

    fn write_to(&self, buffer: &mut Vec<u8>) {
        buffer.write_all(&(self.key.len() as u64).to_le_bytes());
        buffer
            .write_all(self.key)
            .expect("Failed to write to buffer");
        buffer.write_all(&(self.value.len() as u64).to_le_bytes());
        buffer
            .write_all(self.value)
            .expect("Failed to write to buffer");
    }
}

#[cfg(test)]
mod tests {
    use crate::disk_table::DiskTable;
    use crate::mem_table::MemTable;
    use crate::{DiskTableConfig, MemTableConfig};
    use std::path::Path;

    #[test]
    fn write() {
        let mut mem_table = MemTable::new(MemTableConfig { max_size: 10000 });

        for i in 0..500 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            mem_table.insert(key.as_bytes(), value.as_bytes());
        }

        let path = Path::new("test.sst");
        let table = DiskTable::create(
            path,
            &mem_table,
            &DiskTableConfig::default().block_size(2_000),
        );

        for i in 0..500 {
            let key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            let value = table.get(key.as_bytes());

            assert_eq!(
                value.as_ref().map(|v| v.as_ref()),
                Some(expected_value.as_bytes())
            )
        }

        for i in 501..600 {
            let key = format!("key{}", i);
            let value = table.get(key.as_bytes());

            assert_eq!(value.as_ref().map(|v| v.as_ref()), None);
        }
    }
}
