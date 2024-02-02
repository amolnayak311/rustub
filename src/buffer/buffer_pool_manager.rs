use std::collections::HashMap;
use std::slice::Iter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use crate::buffer::lru_k_replacer::AccessType;
use crate::buffer::LRUKReplacer;
use crate::storage::{DiskManager, Page, PAGE_SIZE, WritePageGuard};

// Contents taken from https://github.com/cmu-db/bustub/blob/master/src/include/buffer/buffer_pool_manager.h
pub struct BufferPoolManager<'a> {
    // /** Pointer to the disk scheduler. */
    // std::unique_ptr<DiskScheduler> disk_scheduler_ __attribute__((__unused__));
    disk_manager: &'a DiskManager,
    // /** Number of pages in the buffer pool. */
    // const size_t pool_size_;
    pool_size: usize,
    // /** The next page id to be allocated  */
    // std::atomic<page_id_t> next_page_id_ = 0;
    next_page_id: AtomicU64,
    // /** Array of buffer pool pages. */
    // Page *pages_;
    pages: Arc<Vec<RwLock<Page>>>,
    // /** List of free frames that don't have any pages on them. */
    // std::list<frame_id_t> free_list_;
    free_list: RwLock<Vec<usize>>,
    // /** Page table for keeping track of buffer pool pages. */
    // std::unordered_map<page_id_t, frame_id_t> page_table_;
    page_table : RwLock<HashMap<u64, usize>>,
    // /** Replacer to find unpinned pages for replacement. */
    // std::unique_ptr<LRUKReplacer> replacer_;
    replacer: LRUKReplacer,
    // /** Pointer to the log manager. Please ignore this for P1. */
    // LogManager *log_manager_ __attribute__((__unused__));
    //replacer_k: usize,
}

impl <'a> BufferPoolManager<'a> {

    pub fn new(disk_manager: &'a DiskManager, pool_size: usize, replacer_k: usize) -> Self {
        let mut pages = Vec::<RwLock<Page>>::with_capacity(pool_size);
        for _ in 0..pool_size {
            pages.push(RwLock::new(Page::new()))
        }

        let mut free_list = Vec::with_capacity(pool_size);
        for idx in (0..pool_size).rev() {
            // Add in reverse order so that pop will start popping frame_ids from low to high
            free_list.push(idx);
        }
        Self {
            disk_manager,
            pool_size,
            next_page_id : AtomicU64::new(disk_manager.file_size() / PAGE_SIZE as u64),
            pages: Arc::new(pages),
            free_list: RwLock::new(free_list),
            page_table: RwLock::new(HashMap::with_capacity(pool_size)),
            replacer: LRUKReplacer::new(replacer_k),
            //replacer_k
        }
    }


    ///  Return the size (number of frames) of the buffer pool
    pub fn get_pool_size(&self) -> usize {
        self.pool_size
    }

    // Return the iterator to all the pages in the buffer pool
    pub fn get_pages(&self) -> Iter<'a, Page> {
        unimplemented!();
    }


    ///
    ///
    /// @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
    /// are currently in use and not evictable (in another word, pinned).
    ///
    /// You should pick the replacement frame from either the free list or the replacer (always find from the free list
    /// first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
    /// you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
    ///
    /// Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
    /// so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
    /// Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
    ///
    ///
    /// @return None if no new pages could be created, otherwise Some(page)
    ////
    pub fn new_page(&self) -> Option<RwLockWriteGuard<Page>> {
        let page_id = self.next_page_id.fetch_add(1, Ordering::Acquire);
        let page = self.bring_page_in_buffer(page_id);
        page.map(|mut x| {
            // Reset the page's content before before returning
            x.reset(page_id);
            x
        })

    }

    fn bring_page_in_buffer(&self, page_id: u64) -> Option<RwLockWriteGuard<Page>> {
        // Find an empty page first
        let mut free_list_write = self.free_list.write().unwrap();
        let frame_id = free_list_write.pop().or_else(|| {
            self.replacer.evict()
        });
        // drop the guard explicitly so the lock is released and not held for longer than needed
        drop(free_list_write);
        if let Some(frame_id) = frame_id {
            // Now that we have the frame id, we assign it a new page_id and return the reference
            // to the page in that frame
            let pages_read_locked = self.pages.get(frame_id).unwrap();
            let page = pages_read_locked.write().unwrap();
            if page.is_dirty() {
                assert!(page.page_id().is_some(), "Expecting the page to be not dirty if page_id undefined");
                self.disk_manager.write_page(page.page_id().unwrap(), &page.read()).unwrap();
            }

            let mut page_table_write = self.page_table.write().unwrap();
            if let Some(page_id) = page.page_id() {
                // Remote the mapping from page table
                page_table_write.remove(&page_id);
            }
            page_table_write.insert(page_id, frame_id);

            drop(page_table_write);
            self.replacer.record_access(frame_id);
            self.replacer.set_evictable(frame_id, false);
            Some(page)
        } else {
            // Unable to find a frame free frame in the free_list nor can be evicted
            None
        }
    }

    ////
    ///
    /// TODO(P2): Add implementation, implement this later
    ///
    /// @brief PageGuard wrapper for NewPage
    ///
    /// Functionality should be the same as NewPage, except that
    /// instead of returning a pointer to a page, you return a
    /// BasicPageGuard structure.
    ///
    /// @param[out] page_id, the id of the new page
    /// @return BasicPageGuard holding a new page
    ///
    //fn new_page_guard() -> BasicPageGuard;
    ////


    ///
    ///
    /// @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
    /// but all frames are currently in use and not evictable (in another word, pinned).
    ///
    /// First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
    /// the replacer (always find from the free list first), read the page from disk by scheduling a read DiskRequest with
    /// disk_scheduler_->Schedule(), and replace the old page in the frame. Similar to NewPage(), if the old page is dirty,
    /// you need to write it back to disk and update the metadata of the new page
    ///
    /// In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
    ///
    /// @param page_id id of page to be fetched
    /// @param access_type
    /// @return None if page_id cannot be fetched, otherwise Some(page) to the requested page
    ////
    pub fn fetch_page(&self, page_id: u64, _access_type: AccessType) -> Option<RwLockWriteGuard<Page>> {

        let page_table = self.page_table.read().unwrap();
        if let Some(frame_id) = page_table.get(&page_id) {
            // Page already in memory
            let frame_id = *frame_id;
            drop(page_table);
            self.replacer.record_access(frame_id);
            self.replacer.set_evictable(frame_id, false);
            Some(self.pages[frame_id].write().unwrap())
        } else {
            drop(page_table);
            // Page not in memory, bring in from disk
            let page = self.bring_page_in_buffer(page_id);
            // this page doesnt have the right page_id and data set
            page.map(|mut x| {
                let data = self.disk_manager.read_page(page_id).unwrap();
                x.reset_with_data(page_id, &data);
                x
            })
        }
    }

    ////
    ///
    /// TODO(P2): Add implementation
    ///
    /// @brief PageGuard wrappers for FetchPage
    ///
    /// Functionality should be the same as FetchPage, except
    /// that, depending on the function called, a guard is returned.
    /// If FetchPageRead or FetchPageWrite is called, it is expected that
    /// the returned page already has a read or write latch held, respectively.
    ///
    // fn fetch_page_basic(page_id: u64) -> BasicPageGuard;
    // fn fetch_page_read(page_id: u64) -> ReadPageGuard;
    pub  fn fetch_page_write(&self, page_id: u64, access_type: AccessType) -> Option<WritePageGuard<'_, '_>> {
        match self.fetch_page(page_id, access_type) {
            x @ Some(_) => Some(WritePageGuard::new(&self, x.unwrap())),
            None => None
        }
    }
    ////

    ///
    ///
    /// @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
    /// 0, return false.
    ///
    /// Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
    /// Also, set the dirty flag on the page to indicate if the page was modified.
    ///
    /// @param page_id id of page to be unpinned
    /// @param is_dirty true if the page should be marked as dirty, false otherwise
    /// @param access_type type of access to the page, only needed for leaderboard tests.
    /// @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
    ///
    pub fn unpin_page(&self, page_id: u64, is_dirty: bool, _access_type: AccessType) -> bool {
        let page_table = self.page_table.read().unwrap();
        if let Some(frame_id) = page_table.get(&page_id) {
            let frame_id = *frame_id;
            drop(page_table);
            if is_dirty {
                let page = self.pages[frame_id].write().unwrap();
                page.set_dirty();
                drop(page);
            }
            self.replacer.set_evictable(frame_id, true)
        } else {
            false
        }
    }

    ///
    ///
    /// @brief Flush the target page to disk.
    ///
    /// Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
    /// Unset the dirty flag of the page after flushing.
    ///
    /// @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
    /// @return false if the page could not be found in the page table, true otherwise
    ///
    pub fn flush_page(&self, page_id: u64) -> bool {
        match self.page_table.read().unwrap().get(&page_id) {
            Some(frame)  => {
                let p = self.pages[*frame].write().unwrap();
                self.disk_manager.write_page(page_id, &p.read()).unwrap();
                p.unset_dirty();
                true
            },
            None                   => false
        }
    }

    ///
    /// TODO(P1): Add implementation
    ///
    /// @brief Flush all the pages in the buffer pool to disk.
    ///
    pub fn flush_all_pages(&self) {
        unimplemented!()
    }


    ///
    /// TODO(P1): Add implementation
    ///
    /// @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
    /// page is pinned and cannot be deleted, return false immediately.
    ///
    /// After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
    /// back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
    /// imitate freeing the page on the disk.
    ///
    /// @param page_id id of page to be deleted
    /// @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
    ///
    pub fn delete_page(&self, _page_id: u64) -> bool {
        unimplemented!()
    }

    pub fn is_evictable(&self, page_id: u64) -> bool {
        let unlocked_page_table = self.page_table.read().unwrap();
        match unlocked_page_table.get(&page_id) {
            Some(frame_id) => self.replacer.is_evictable(*frame_id),
            None                   => false
        }
    }


}

#[cfg(test)]
mod test {
    use std::thread;
    use rand::thread_rng;
    use crate::buffer::BufferPoolManager;
    use crate::buffer::lru_k_replacer::AccessType;
    use crate::storage::{DiskManager, PAGE_SIZE};

    #[test]
    fn basic_test_1() {

        use rand::Rng;

        let file_name = format!("test_db_file_{:?}", thread::current().id());
        let buffer_pool_size = 10;
        let replacer_k = 5;
        let dm = DiskManager::new(&file_name);
        let bpm = BufferPoolManager::new(&dm, buffer_pool_size, replacer_k);



        let page_option = bpm.new_page();
        assert!(page_option.is_some(), "Expected to see a page created");
        let page = page_option.unwrap();
        assert_eq!(0, page.page_id().unwrap());
        let mut rng = thread_rng();
        let mut page_data = [0u8; PAGE_SIZE];
        for i in 0..PAGE_SIZE {
            page_data[i] = rng.gen();
        }
        page.write(&page_data);
        assert!(page.read().iter().zip(page_data).fold(true, |acc, (lhs, rhs)|
            acc && *lhs == rhs));

        // Needed else, the RwLockWriteGuard acquired here causes deadlock in unpin calls
        drop(page);

         // Scenario: We should be able to create new pages until we fill up the buffer pool.
        // Notice we start from 1 as we alreadt created 1 page
         for _ in 1..buffer_pool_size {
             assert!(bpm.new_page().is_some());
         }

         // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
         for _ in 0..buffer_pool_size {
             assert!(bpm.new_page().is_none());
         }

         // Scenario: After unpinning pages {0, 1, 2, 3, 4}, we should be able to create 5 new pages
        for page_id in 0..5 {
            assert!(bpm.unpin_page(page_id, true, AccessType::Unknown))
        }

        for _ in 0..5 {
            let new_page = bpm.new_page();
            assert!(new_page.is_some());
             // Unpin the page here to allow future fetching
            assert!(bpm.unpin_page(new_page.unwrap().page_id().unwrap(), false, AccessType::Unknown));
         }

        // Scenario: We should be able to fetch the data we wrote a while ago.
        // Page 0 was evicted and was dirty and now with fetch_page it will fetch from disk and original
        // content should be visible
        let page = bpm.fetch_page(0, AccessType::Unknown);
        assert!(page.is_some());
        assert!(page.unwrap().read().iter().zip(page_data).fold(true, |acc, (lhs, rhs)|
            acc && *lhs == rhs));

        std::fs::remove_file(&file_name).unwrap();
     }


    #[test]
    fn basic_test_2() {

        let file_name = format!("test_db_file_{:?}", thread::current().id());
        let buffer_pool_size = 10;
        let replacer_k = 5;
        let dm = DiskManager::new(&file_name);
        let bpm = BufferPoolManager::new(&dm, buffer_pool_size, replacer_k);

        let page_option = bpm.new_page();
        assert!(page_option.is_some(), "Expected to see a page created");
        let page = page_option.unwrap();
        assert_eq!(0, page.page_id().unwrap());
        let src = "Hello World".as_bytes();
        let mut data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        data[0..src.len()].copy_from_slice(src);
        page.write(&data);

        // Needed else, the RwLockWriteGuard acquired here causes deadlock in unpin calls
        drop(page);

        // Scenario: We should be able to create new pages until we fill up the buffer pool.
        for _ in 1..buffer_pool_size {
            assert!(bpm.new_page().is_some());
        }

        // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
        for _ in 0..buffer_pool_size {
            assert!(bpm.new_page().is_none());
        }

        // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
        // there would still be one buffer page left for reading page 0.
        // for (int i = 0; i < 5; ++i) {
        //     EXPECT_EQ(true, bpm->UnpinPage(i, true));
        // }
        for page_id in 0..5 {
            assert!(bpm.unpin_page(page_id, true, AccessType::Unknown))
        }

        // for (int i = 0; i < 4; ++i) {
        //     EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
        // }
        for _ in 0..4 {
            let new_page = bpm.new_page();
            assert!(new_page.is_some());
        }

        // Scenario: We should be able to fetch the data we wrote a while ago.
        // Page 0 was evicted and was dirty and now with fetch_page it will fetch from disk and original
        // content should be visible
        let page = bpm.fetch_page(0, AccessType::Unknown);
        assert!(page.is_some());
        assert!(page.unwrap().read().iter().zip(data).fold(true, |acc, (lhs, rhs)|
            acc && *lhs == rhs));


        // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
        // now be pinned. Fetching page 0 again should fail.
        // Unpinning will let this page be evicted and new page creation succeed
        assert!(bpm.unpin_page(0, true, AccessType::Unknown));
        // This should succeed, since its new, the page is pinned by default, which means all pages are now pinned
        // and none be evictable
        assert!(bpm.new_page().is_some());
        // Now fetching any page not in buffer should fail as no page can be evicted
        assert!(bpm.fetch_page(0, AccessType::Unknown).is_none());
//

        std::fs::remove_file(&file_name).unwrap();

        // Shutdown the disk manager and remove the temporary file we created.
        // disk_manager->ShutDown();
        // remove("test.db");
        //
        // delete bpm;
        // delete disk_manager;
    }

    #[test]
    fn test_page_write_guard() {
        use rand::Rng;

        let file_name = format!("test_db_file_{:?}", thread::current().id());
        let buffer_pool_size = 10;
        let replacer_k = 5;
        let dm = DiskManager::new(&file_name);
        let bpm = BufferPoolManager::new(&dm, buffer_pool_size, replacer_k);


        let page_option = bpm.new_page();
        assert!(page_option.is_some(), "Expected to see a page created");
        let page = page_option.unwrap();
        assert_eq!(0, page.page_id().unwrap());
        let mut rng = thread_rng();
        let mut page_data = [0u8; PAGE_SIZE];
        for i in 0..PAGE_SIZE {
            page_data[i] = rng.gen();
        }
        page.write(&page_data);
        assert!(page.read().iter().zip(page_data).fold(true, |acc, (lhs, rhs)|
            acc && *lhs == rhs));
        drop(page);

        // Assuming page 0 is in frame 0
        // Initially the page is not evictable
        assert!(!bpm.is_evictable(0));
        bpm.unpin_page(0, false, AccessType::Unknown);
        // After unpin, it should become evictable
        assert!(bpm.is_evictable(0));
        {
            let guarded_page = bpm.fetch_page_write(0, AccessType::Unknown);
            assert!(guarded_page.is_some());
            let write_guard = guarded_page.unwrap();
            // assert the data is correct and as expected
            assert!(write_guard.read().iter().zip(page_data).fold(true, |acc, (lhs, rhs)|
                acc && *lhs == rhs));
            // After getting the guarded page it should again be non evictable
            assert!(!bpm.is_evictable(0));
        }
        // the page guard is no longer in scope and dropped, after guarded_page goes out of scope, the page again is evictable
        assert!(bpm.is_evictable(0));
        std::fs::remove_file(&file_name).unwrap();
    }

}