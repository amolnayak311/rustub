use std::ops::Deref;
use crate::storage::index::b_plus_tree::PageType::{INTERNAL, LEAF};
use crate::storage::PAGE_SIZE;

#[derive(PartialEq, Debug)]
pub enum PageType {
    INTERNAL, LEAF
}

pub const INTERNAL_PAGE_HEADER_SIZE: usize = 12;
pub const INTERNAL_PAGE_SIZE: usize = PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE;

pub const LEAF_PAGE_HEADER_SIZE: usize = 16;
pub const LEAF_PAGE_SIZE: usize = PAGE_SIZE - LEAF_PAGE_HEADER_SIZE;

// Taken from https://github.com/cmu-db/bustub/blob/master/src/include/storage/page/b_plus_tree_page.h
/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 12 bytes in total):
 * ---------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |  ...   |
 * ---------------------------------------------------------
 */
//================================ Base Page ================================

trait BaseBPlusTree {
    fn get_page_type(&self) -> &PageType;

    fn get_size(&self)  -> usize;

    fn get_max_size(&self) -> usize;

    fn increase_size(&mut self, amount: usize);

    fn is_leaf(&self) -> bool {
        *self.get_page_type() == LEAF
    }
}

pub struct BPlusTreePage {
    page_type: PageType,
    current_size : usize,
    max_size: usize,
}

impl  BPlusTreePage {

    fn new(page_type: PageType, current_size: usize, max_size: usize) -> Self {
        Self{page_type, current_size, max_size}
    }
}
impl  BaseBPlusTree for BPlusTreePage {
    fn get_page_type(&self) -> &PageType {
        &self.page_type
    }

    fn get_size(&self) -> usize {
        self.current_size
    }

    fn get_max_size(&self) -> usize {
        self.max_size
    }

    fn increase_size(&mut self, amount: usize) {
        self.current_size += amount;
    }
}

//================================ Internal Page ================================

pub struct BPlusTreeInternalPage {

    base: BPlusTreePage,
}

impl  Deref for BPlusTreeInternalPage {
    type Target = BPlusTreePage;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

pub struct GenericKey {
    // TODO Impl
}

pub struct RID {
    // TODO Impl

}
impl  BPlusTreeInternalPage {

    ///
    /// @param index The index of the key to get. Index must be non-zero.
    /// @return Key at index
    ///
    pub fn key_at(&self, _index: usize) -> GenericKey {
        todo!()
    }

    ///
    /// @param index The index of the key to set. Index must be non-zero.
    /// @param key The new value for key
    ///
    pub fn set_key_at(&self, _index: usize, _key: &GenericKey) {
        todo!()
    }

    ///
    /// @param value The value to search for
    /// @return The index that corresponds to the specified value
    ///
    pub fn value_index(&self, _value: u64) -> usize {
        todo!()
    }

    ///
    /// @param index The index to search for
    /// @return The value at the index
    ///
    pub fn value_at(&self, _index: usize) -> u64 {
        todo!()
    }

    pub fn new() -> Self {
        Self{
            base: BPlusTreePage::new(INTERNAL, 0, INTERNAL_PAGE_SIZE)
        }
    }

}

//================================ Leaf Page ================================


pub struct BPlusTreeLeafPage {

    base: BPlusTreePage,
}

impl  Deref for BPlusTreeLeafPage {
    type Target = BPlusTreePage;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}


#[cfg(test)]
mod test {
    use crate::storage::{BPlusTreeInternalPage, PAGE_SIZE};
    use crate::storage::index::b_plus_tree::{BaseBPlusTree, INTERNAL_PAGE_SIZE, PageType};

    #[test]
    fn  build_empty_internal_page() {
        let empty_internal_page = BPlusTreeInternalPage::new();
        assert!(!empty_internal_page.is_leaf());
        assert_eq!(empty_internal_page.get_max_size(), INTERNAL_PAGE_SIZE);
        assert_eq!(empty_internal_page.get_max_size(), PAGE_SIZE - 12);
        assert_eq!(*empty_internal_page.get_page_type(), PageType::INTERNAL);
        assert_eq!(empty_internal_page.get_size(), 0);
    }
}
