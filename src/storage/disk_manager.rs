use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::sync::{Mutex, RwLock};
use crate::storage::PAGE_SIZE;

// Used https://github.com/kw7oe/mini-db/blob/main/src/storage/disk_manager.rs as reference
pub struct DiskManager {
    file_name: String,
    db_file: Mutex<File>,
    num_writes: RwLock<u32>,
}

impl DiskManager {
    pub fn new(file_name: &str) -> DiskManager {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_name) {
            Ok(file)  =>
                DiskManager{
                            file_name: file_name.to_string(),
                            db_file: Mutex::new(file),
                            num_writes: RwLock::new(0)},
            Err(error) =>
                panic!("Error {:?} caught while opening the file {}", error, file_name),
        }
    }
    ///
    /// Reads the page for the given page_id
    ///
    /// * `page_id` The page id
    /// Returns the array of size storage::PAGE_SIZE with the data read from disk
    ///
    pub fn read_page(&self, page_id: u64) -> Result<[u8; PAGE_SIZE], Error> {
        let offset = page_id  * PAGE_SIZE as u64;
        let mut file= self.db_file.lock().unwrap();
        if offset > self.get_file_size(&file) {
            Err(Error::new(ErrorKind::UnexpectedEof,
                           format!("page_id {} out of bounds of the file {}",
                                   page_id, self.file_name)))
        } else {
            file.seek(SeekFrom::Start(offset))?;
            let mut res = [0; PAGE_SIZE];
            file.read_exact(&mut res)?;
            Ok(res)
        }
    }

    ///
    /// Writes the page for the given _page_id
    ///
    /// * `page_id` the page id to be written
    /// * `page` the page to be flushed to the disk
    pub fn write_page(&self, page_id: u64, page: &[u8; PAGE_SIZE]) -> Result<(), Error> {
        let offset = page_id  * PAGE_SIZE as u64;
        let mut file = self.db_file.lock().unwrap();
        *self.num_writes.write().unwrap() += 1;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(page)?;
        // Adding sync_all makes the tests run for ~20 secs on this machine compared to
        // ~30ms ms not fsyncing on every write
        file.sync_all()?;
        Ok(())

    }

    // TODO: Implement read and write log when needed

    ///
    /// Returns the number of disk flushes
    pub fn get_num_flushes(&self) -> u32 {
        unimplemented!()
    }

    ///
    /// Returns true iff the in-memory content has not been flushed yet
    pub fn get_flush_state(&self) -> bool {
        unimplemented!()
    }

    ///
    /// Same as get_flush_state, just better named
    pub fn is_not_flushed(&self) -> bool {
        self.get_flush_state()
    }

    ///
    /// Gets the number of disk writes
    pub fn get_num_writes(&self) -> u32 {
        *self.num_writes.read().unwrap()
    }

    ///
    /// Private function to get the size of the file, it assumes the lock is already held by caller
    #[inline(always)]
    fn get_file_size(&self, file: &File) -> u64 {
        file.metadata().unwrap().len()
    }

    pub fn file_size(&self) -> u64 {
        self.get_file_size(&self.db_file.lock().unwrap())
    }
}

#[cfg(test)]
mod test{
    use std::sync::Arc;
    use std::thread;
    use crate::storage::{DiskManager, PAGE_SIZE};

    #[test]
    fn test_read_concurrently() {
        let file_name = format!("test_db_file_{:?}", thread::current().id());
        let disk_manager = Arc::new(DiskManager::new(&file_name));
        // Write pages sequentially
        for page_id in 1..10 {
            disk_manager.write_page(page_id as u64, &[page_id as u8; PAGE_SIZE]).unwrap();
        }
        // Test reads in parallel

        for _ in 1..1000 {
            (1..10).map( |page_id| {
                let dm = disk_manager.clone();
                (page_id, thread::spawn(move || dm.read_page(page_id).unwrap()))
            }).for_each( |(page_id, join_res)| {
                assert_eq!([page_id as u8; PAGE_SIZE], join_res.join().unwrap());
            });
        }
        std::fs::remove_file(&file_name).unwrap();
    }

    #[test]
    fn test_write_concurrently() {
        let file_name = format!("test_db_file_{:?}", thread::current().id());
        let disk_manager = Arc::new(DiskManager::new(&file_name));
        (1..=1000).map( |page_id| {
            let dm = disk_manager.clone();
            thread::spawn(move || dm.write_page(page_id as u64, &[page_id as u8; PAGE_SIZE]).unwrap())
        }).for_each(|join_handle| {
            join_handle.join().unwrap();
        });
        assert_eq!(disk_manager.get_num_writes(), 1000);
        for page_id in 1..1000 {
            let buffer_read = disk_manager.read_page(page_id as u64).unwrap();
            assert_eq!(buffer_read, [page_id as u8; PAGE_SIZE]);
        }
        std::fs::remove_file(file_name).unwrap();

    }
}