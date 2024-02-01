use std::sync::{RwLock, RwLockReadGuard};
use std::sync::atomic::{AtomicBool, Ordering};

pub const PAGE_SIZE: usize = 4096;
pub struct Page {
    page_id: Option<u64>,
    is_dirty: AtomicBool,
    data: RwLock<[u8; PAGE_SIZE]>,

}

impl Page {

    pub fn page_id(&self) -> Option<u64> {
        self.page_id
    }

    pub fn reset_with_data(&mut self, page_id: u64, data_to_copy: &[u8; PAGE_SIZE]) {
        let mut data = self.data.write().unwrap();
        self.page_id = Some(page_id);
        self.is_dirty.store(false, Ordering::Relaxed);
        // TODO: See if this is needed
        data.copy_from_slice(data_to_copy);
    }

    pub fn reset(&mut self, page_id: u64) {
        self.reset_with_data(page_id, &[0; PAGE_SIZE]);
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::Acquire)
    }

    pub fn read_bytes(&self) -> RwLockReadGuard<[u8; PAGE_SIZE]> {
        self.data.read().unwrap()
    }

    pub fn set_dirty(&self) {
        self.is_dirty.store(true, Ordering::Release);
    }

    pub fn unset_dirty(&self) {
        self.is_dirty.store(false, Ordering::Release);
    }

    pub fn new() -> Self {
        Self {
            page_id: None,
            is_dirty: AtomicBool::new(false),
            data: RwLock::new([0; PAGE_SIZE])
        }
    }


    pub fn write(&self, src: &[u8; PAGE_SIZE]) {
        // TODO: Assert the page_id is initialized
        let mut writable_data = self.data.write().unwrap();
        writable_data.copy_from_slice(src);
    }

    pub fn read(&self) -> RwLockReadGuard<[u8; PAGE_SIZE]> {
        // TODO: Assert the page_id is initialized
        self.data.read().unwrap()
    }
}