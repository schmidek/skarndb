use std::cmp::Ordering;
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use zstd::block::{compress_to_buffer, decompress};

use crate::error::Result;
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
    pub fn open(path: &Path) -> Result<DiskTable> {
        let mut file = File::open(path).expect("TODO handle file failed to create");

        // Read blocks
        let mut blocks = Vec::new();

        let mut buf = [0; U64_BYTES];
        let mut index = file.seek(SeekFrom::End(0))?;
        println!("end position {}", index);

        index -= U64_BYTES as u64;
        file.read_exact_at(&mut buf, index)?;
        let age = u64::from_le_bytes(buf);
        println!("age = {}", age);

        index -= U64_BYTES as u64;
        file.read_exact_at(&mut buf, index)?;
        let num_blocks = u64::from_le_bytes(buf);
        println!("num_blocks = {}", num_blocks);

        index -= U64_BYTES as u64;
        file.read_exact_at(&mut buf, index)?;
        let starting_block_position = u64::from_le_bytes(buf);
        println!("starting_block_position = {}", starting_block_position);

        file.seek(SeekFrom::Start(starting_block_position))?;
        for _ in 0..num_blocks {
            blocks.push(Block::read_from(&mut file)?);
        }

        Ok(DiskTable {
            file,
            path: path.to_path_buf(),
            blocks,
            age,
        })
    }

    pub fn create<I, R>(
        path: &Path,
        mut iter: I,
        disk_table_config: &DiskTableConfig,
        age: u64,
        write_deletes: bool,
    ) -> Result<DiskTable>
    where
        I: Iterator<Item = (R, R)>,
        R: AsRef<[u8]>,
    {
        let mut file = File::create(path)?;

        // Create blocks
        let mut blocks = Vec::new();

        let len = disk_table_config.block_size;
        let mut buf = Vec::with_capacity(len);

        let max_compressed_len = (0.8 * len as f32).ceil() as usize; // if it doesn't compress a minimum amount we'll store it uncompressed
        let mut compressed_buf = Vec::with_capacity(max_compressed_len);

        let mut next = iter.next();
        let mut prev: Option<(R, R)>;
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
            prev = None;
            while let Some((key, value)) = next.as_ref() {
                let value_ref = value.as_ref();
                if write_deletes || !value_ref.is_empty() {
                    let entry = Entry {
                        key: key.as_ref(),
                        value: value_ref,
                    };

                    let space_left = buf.capacity() - buf.len();
                    if prev.is_some() /* must have written at least 1 entry */ && entry.len() > space_left
                    {
                        break; // this block is full
                    }
                    entry.write_to(&mut buf)?;

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

        let starting_block_position = file.seek(SeekFrom::Current(0))?;

        // Write blocks
        for block in &blocks {
            block.write_to(&mut file)?;
        }

        // Write starting block position
        file.write_all(&(starting_block_position.to_le_bytes()))?;

        // Write number of blocks
        file.write_all(&((blocks.len() as u64).to_le_bytes()))?;

        // Write age
        file.write_all(&(age.to_le_bytes()))?;

        // Flush then reopen readonly
        file.flush()?;
        drop(file);
        let file = File::open(path)?;

        println!("{} blocks", blocks.len());

        Ok(DiskTable {
            file,
            path: path.to_path_buf(),
            blocks,
            age,
        })
    }

    fn get_block_data(&self, block: &Block) -> Result<Vec<u8>> {
        let mut buf = vec![0; block.size];
        self.file.read_exact_at(&mut buf, block.offset)?;

        Ok(match block.compression_type {
            CompressionType::None => buf,
            CompressionType::Zstd => decompress(&buf, usize::MAX).expect("Failed to decompress"),
        })
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Box<[u8]>>> {
        let key_ref = key.as_ref();
        // Find which block it could be in
        // TODO binary search
        let block_search = self.blocks.iter().find(|&block| {
            key_ref.cmp(block.first_key.as_ref()).is_ge()
                && key_ref.cmp(block.last_key.as_ref()).is_le()
        });

        if block_search.is_none() {
            return Ok(None);
        }

        let block = block_search.unwrap();
        let block_data = self.get_block_data(block)?;

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
                return Ok(Some(
                    block_data[index..index + value_size as usize]
                        .to_vec()
                        .into_boxed_slice(),
                ));
            }

            index += value_size as usize;

            if index >= block_data.len() {
                break;
            }
        }
        Ok(None)
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
            self.block_data = Some(block_data.unwrap());
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

impl CompressionType {
    fn from(n: u8) -> Option<CompressionType> {
        match n {
            0 => Some(CompressionType::None),
            1 => Some(CompressionType::Zstd),
            _ => None,
        }
    }
}

struct Block {
    compression_type: CompressionType,
    offset: u64,
    size: usize,
    first_key: Box<[u8]>,
    last_key: Box<[u8]>,
}

impl Block {
    fn read_from(file: &mut File) -> Result<Block> {
        let compression_type = {
            let mut buf = [0; 1];
            file.read_exact(&mut buf)?;
            CompressionType::from(u8::from_le_bytes(buf)).expect("Unknown compression type")
        };

        let mut buf = [0; U64_BYTES];

        file.read_exact(&mut buf)?;
        let offset = u64::from_le_bytes(buf);

        file.read_exact(&mut buf)?;
        let size = u64::from_le_bytes(buf) as usize;

        file.read_exact(&mut buf)?;
        let first_key_size = u64::from_le_bytes(buf) as usize;
        println!("first_key_size = {}", first_key_size);
        let mut first_key = vec![0; first_key_size];
        file.read_exact(&mut first_key)?;
        println!(
            "first_key = {}",
            String::from_utf8(first_key.clone()).unwrap()
        );

        file.read_exact(&mut buf)?;
        let last_key_size = u64::from_le_bytes(buf) as usize;
        println!("last_key_size = {}", last_key_size);
        let mut last_key = vec![0; last_key_size];
        file.read_exact(&mut last_key)?;

        Ok(Block {
            compression_type,
            offset,
            size,
            first_key: first_key.into_boxed_slice(),
            last_key: last_key.into_boxed_slice(),
        })
    }

    fn write_to(&self, file: &mut File) -> Result<()> {
        file.write_all(&((self.compression_type.clone() as u8).to_le_bytes()))?;
        file.write_all(&(self.offset.to_le_bytes()))?;
        file.write_all(&((self.size as u64).to_le_bytes()))?;
        file.write_all(&((self.first_key.len() as u64).to_le_bytes()))?;
        file.write_all(&*self.first_key)?;
        file.write_all(&((self.last_key.len() as u64).to_le_bytes()))?;
        file.write_all(&*self.last_key)?;
        Ok(())
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

    fn write_to(&self, buffer: &mut Vec<u8>) -> Result<()> {
        buffer.write_all(&(self.key.len() as u64).to_le_bytes())?;
        buffer.write_all(self.key)?;
        buffer.write_all(&(self.value.len() as u64).to_le_bytes())?;
        if !self.value.is_empty() {
            buffer.write_all(self.value)?;
        }
        Ok(())
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
            mem_table.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let tmpdir = tempfile::tempdir().unwrap();
        let table = DiskTable::create(
            tmpdir.as_ref().join("test.sst").as_path(),
            mem_table.iter(),
            &DiskTableConfig::default().block_size(2_000),
            mem_table.age,
            true,
        )
        .unwrap();

        for i in 0..500 {
            let key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            let value = table.get(key.as_bytes()).unwrap();

            assert_eq!(
                value.as_ref().map(|v| v.as_ref()),
                Some(expected_value.as_bytes())
            )
        }

        for i in 501..600 {
            let key = format!("key{}", i);
            let value = table.get(key.as_bytes()).unwrap();

            assert_eq!(value.as_ref().map(|v| v.as_ref()), None);
        }
    }
}
