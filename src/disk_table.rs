use std::cmp::Ordering;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use zstd::block::{compress_to_buffer, decompress};

use crate::DiskTableConfig;

const U64_BYTES: usize = (u64::BITS / 8) as usize;

pub struct DiskTable {
    file: File,
    path: PathBuf,
    blocks: Vec<Block>,
    pub age: u64,
}

impl Ord for DiskTable {
    fn cmp(&self, other: &Self) -> Ordering {
        self.age.cmp(&other.age).reverse()
    }
}

impl PartialOrd for DiskTable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DiskTable {
    fn eq(&self, other: &Self) -> bool {
        self.age == other.age
    }
}

impl Eq for DiskTable {}

impl DiskTable {
    pub fn create<I, R>(
        path: &Path,
        mut iter: I,
        disk_table_config: &DiskTableConfig,
        age: u64,
        write_deletes: bool,
    ) -> DiskTable
    where
        I: Iterator<Item = (R, R)>,
        R: AsRef<[u8]>,
    {
        let mut file = File::create(path).expect("TODO handle file failed to create");

        // Create blocks
        let mut blocks = Vec::new();

        let len = disk_table_config.block_size;
        let mut buf = Vec::with_capacity(len);

        let max_compressed_len = (0.8 * len as f32).ceil() as usize; // if it doesn't compress a minimum amount we'll store it uncompressed
        let mut compressed_buf = Vec::with_capacity(max_compressed_len);

        let mut next = iter.next();
        let mut prev: Option<(R, R)> = None;
        let mut block_offset = 0_u64;

        if next.is_none() {
            panic!("Tried to write empty MemTable");
        }

        let first_key = next
            .as_ref()
            .unwrap()
            .0
            .as_ref()
            .to_vec()
            .into_boxed_slice();
        let mut last_key;

        loop {
            // Create a block
            println!("Creating block");
            while let Some((key, value)) = next.as_ref() {
                let value_ref = value.as_ref();
                if write_deletes || !value_ref.is_empty() {
                    let entry = Entry {
                        key: key.as_ref(),
                        value: value_ref,
                    };

                    let space_left = buf.capacity() - buf.len();
                    if entry.len() > space_left {
                        break; // this block is full
                    }
                    entry.write_to(&mut buf);

                    prev = next;
                }
                next = iter.next();
            }

            if prev.is_none() {
                break; // block is empty
            }

            last_key = prev
                .as_ref()
                .map(|kv| kv.0.as_ref().to_vec().into_boxed_slice());

            println!("buf len = {}", buf.len());
            let (compression_type, block_size) =
                match compress_to_buffer(&buf, &mut compressed_buf, 0) {
                    Ok(compressed_size) => {
                        println!("compressed len = {}", compressed_size);
                        file.write_all(&compressed_buf[..compressed_size])
                            .expect("Write failed");
                        (CompressionType::Zstd, compressed_size)
                    }
                    Err(_) => {
                        file.write_all(&buf).expect("Write failed");
                        (CompressionType::None, buf.len())
                    }
                };
            blocks.push(Block {
                compression_type,
                offset: block_offset,
                size: block_size,
                first_key: first_key.clone(),
                last_key: last_key.unwrap(),
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

        // Write age
        file.write_all(&(age.to_le_bytes()))
            .expect("Failed to write to file");

        // Flush then reopen readonly
        file.flush().expect("Failed flush");
        drop(file);
        let file = File::open(path).expect("TODO handle file failed to open");

        println!("{} blocks", blocks.len());

        DiskTable {
            file,
            path: path.to_path_buf(),
            blocks,
            age,
        }
    }

    fn get_block_data(&self, block: &Block) -> Vec<u8> {
        let mut buf = vec![0; block.size];
        self.file
            .read_exact_at(&mut buf, block.offset)
            .expect("Failed to read from file");

        match block.compression_type {
            CompressionType::None => buf,
            CompressionType::Zstd => decompress(&buf, usize::MAX).expect("Failed to decompress"),
        }
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Box<[u8]>> {
        let key_ref = key.as_ref();
        // Find which block it could be in
        // TODO binary search
        let block = self.blocks.iter().find(|&block| {
            key_ref.cmp(block.first_key.as_ref()).is_ge()
                && key_ref.cmp(block.last_key.as_ref()).is_le()
        })?;

        let block_data = self.get_block_data(block);

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
        None
    }

    pub fn iter(&self) -> Iter {
        Iter {
            table: self,
            block_index: 0,
            position_in_block: 0,
            block_data: None,
        }
    }

    pub fn delete(&self) {
        fs::remove_file(self.path.as_path()).expect("Remove file failed");
    }
}

pub struct Iter<'a> {
    table: &'a DiskTable,
    block_index: usize,
    position_in_block: usize,
    block_data: Option<Vec<u8>>,
}

impl Iterator for Iter<'_> {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_data.is_none() {
            if self.block_index >= self.table.blocks.len() {
                return None;
            }
            let block = &self.table.blocks[self.block_index];
            let block_data = self.table.get_block_data(block);
            self.block_data = Some(block_data);
        }
        let block_data = self.block_data.as_ref().unwrap();

        let key_size = u64::from_le_bytes(
            block_data[self.position_in_block..self.position_in_block + U64_BYTES]
                .try_into()
                .unwrap(),
        );
        self.position_in_block += U64_BYTES;
        let key = block_data[self.position_in_block..self.position_in_block + key_size as usize]
            .to_vec()
            .into_boxed_slice();
        self.position_in_block += key_size as usize;
        let value_size = u64::from_le_bytes(
            block_data[self.position_in_block..self.position_in_block + U64_BYTES]
                .try_into()
                .unwrap(),
        );
        self.position_in_block += U64_BYTES;
        let value = block_data
            [self.position_in_block..self.position_in_block + value_size as usize]
            .to_vec()
            .into_boxed_slice();
        self.position_in_block += value_size as usize;
        if self.position_in_block >= block_data.len() {
            self.block_data = None;
            self.position_in_block = 0;
            self.block_index += 1;
        }
        Some((key, value))
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
        file.write_all(&(self.offset.to_le_bytes()))
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
        buffer
            .write_all(&(self.key.len() as u64).to_le_bytes())
            .expect("Failed to write to buffer");
        buffer
            .write_all(self.key)
            .expect("Failed to write to buffer");
        buffer
            .write_all(&(self.value.len() as u64).to_le_bytes())
            .expect("Failed to write to buffer");
        if !self.value.is_empty() {
            buffer
                .write_all(self.value)
                .expect("Failed to write to buffer");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::disk_table::DiskTable;
    use crate::mem_table::MemTable;
    use crate::{DiskTableConfig, MemTableConfig};

    #[test]
    fn write() {
        let mut mem_table = MemTable::new(MemTableConfig { max_size: 10000 }, 0);

        for i in 0..500 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            mem_table.insert(key.as_bytes(), value.as_bytes());
        }

        let tmpdir = tempfile::tempdir().unwrap();
        let table = DiskTable::create(
            tmpdir.as_ref().join("test.sst").as_path(),
            mem_table.iter(),
            &DiskTableConfig::default().block_size(2_000),
            mem_table.age,
            true,
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
