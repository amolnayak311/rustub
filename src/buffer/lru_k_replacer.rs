use std::cell::RefCell;
use std::cmp::Ordering;
use std::cmp::Ordering::{Greater, Less};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use priority_queue::PriorityQueue;

//Implemented based on https://15445.courses.cs.cmu.edu/spring2023/project1/#lru-k-replacer
pub struct LRUKReplacer {
    k: usize,
    page_to_node_mapping: RwLock<HashMap<usize, Rc<LRUKNode>>>,
    kpq: RwLock<PriorityQueue<usize, Rc<LRUKNode>>>
}

pub enum AccessType {
    Unknown,
    Lookup,
    Scan,
    Index
}
#[derive(Debug)]
struct LRUKNode {
    k: usize,
    frame_id: usize,
    access_times: RefCell<Box<[u128]>>,
    recent_access_time_idx: RefCell<usize>,
    is_evictable: RefCell<bool>
}

impl LRUKNode {}

impl LRUKNode {

    fn new(k: usize, frame_id: usize) -> LRUKNode {
        LRUKNode{k,
            frame_id,
            access_times : RefCell::new(vec![u128::MAX; k].into_boxed_slice()),
            recent_access_time_idx : RefCell::new(0),
            is_evictable : RefCell::new(true),
        }
    }

    fn record_access(&self) -> () {
        let mut idx = self.recent_access_time_idx.borrow_mut();
        self.access_times.borrow_mut()[*idx] =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        *idx = (*idx + 1) % self.k;
    }
}
impl PartialEq<Self> for LRUKNode {
    fn eq(&self, other: &Self) -> bool {
        self.access_times == other.access_times
            && self.frame_id == other.frame_id
            && self.is_evictable == other.is_evictable
            && self.k == other.k
    }
}

impl Eq for LRUKNode {}

impl Ord for LRUKNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd<Self> for LRUKNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self == other {
            Some(Ordering::Equal)
        } else {
            let this_earliest_time_access_idx = *self.recent_access_time_idx.borrow();
            let this_earliest_access = self.access_times.borrow()[this_earliest_time_access_idx];
            let this_recent_access_idx = if *self.recent_access_time_idx.borrow() == 0 {
              self.k - 1
            } else {
                (*self.recent_access_time_idx.borrow() - 1usize) % self.k
            };
            let this_recent_access = self.access_times.borrow()[this_recent_access_idx];

            let other_earliest_time_access_idx = *other.recent_access_time_idx.borrow();
            let other_earliest_access = other.access_times.borrow()[other_earliest_time_access_idx];
            let other_recent_access_idx = if *other.recent_access_time_idx.borrow() == 0 {
                other.k - 1
            } else {
                (*other.recent_access_time_idx.borrow() - 1usize) % other.k
            };
            let other_recent_access = other.access_times.borrow()[other_recent_access_idx];

            // If both nodes have less than k accesses, the one that was accessed earliest is considered greater
            if this_earliest_access == u128::MAX && other_earliest_access == u128::MAX {
                if this_recent_access < other_recent_access {
                    Some(Greater)
                } else {
                    Some(Less)
                }
            } else {
                // We are here because at least one of the nodes has at least k access times
                if this_earliest_access == u128::MAX {
                    Some(Greater)
                } else if other_earliest_access == u128::MAX {
                    Some(Less)
                } else {
                    // This means both this and other has at least 3 accesses and thus
                    // the one with greater difference in earliest and last access time is greater
                    if this_recent_access - this_earliest_access >
                        other_recent_access - other_earliest_access {
                        Some(Greater)
                    } else {
                        Some(Less)
                    }
                }
            }
        }
    }
}

impl LRUKReplacer {

    ///
    /// Constructs new LRUKAccessor with size k
    ///
    pub fn new(k: usize) -> LRUKReplacer {
        LRUKReplacer {
            k,
            page_to_node_mapping: RwLock::new(HashMap::new()),
            kpq: RwLock::new(PriorityQueue::new())
        }
    }

    ///
    /// Record access for the frame_id, if frame_id is greater than the max_num_frames the
    /// process will panic
    pub fn record_access(&self, frame_id: usize) -> () {
        let mut wl = self.page_to_node_mapping.write().unwrap();
        let knode = match wl.get(&frame_id) {
            Some(knode)  => {
                knode.record_access();
                knode.clone()
            }
            None                         => {
                let new_knode = Rc::new(LRUKNode::new(self.k, frame_id));
                new_knode.record_access();
                wl.insert(frame_id, new_knode.clone());
                new_knode
            }
        };
        let mut kpq_ = self.kpq.write().unwrap();
        match kpq_.get_priority(&frame_id) {
            Some(_)  => {
                kpq_.change_priority(&frame_id, knode).unwrap();
            },
            None     => {
                kpq_.push(frame_id, knode);
            }
        };
    }

    pub fn set_evictable(&self, frame_id: usize, is_evictable: bool) -> bool {

        match self.page_to_node_mapping.read().unwrap().get(&frame_id) {
            Some(lru_k_node)  => {
                *lru_k_node.is_evictable.borrow_mut() = is_evictable;
                if is_evictable {
                    self.kpq.write().unwrap().push(frame_id, lru_k_node.clone());
                } else {
                    self.kpq.write().unwrap().remove(&frame_id);
                }
                true
            },
            None                          => false,
        }
    }

    pub fn size(&self) -> usize {
        self.kpq.read().unwrap().len()
    }

    pub fn evict(&self) -> Option<usize> {
        // TODO: Is this needed to avoid possible deadlock scenario by not getting the locks in right order?
        let mut page_to_node_mapping_ = self.page_to_node_mapping.write().unwrap();
        self.kpq.write().unwrap().pop().map(|(frame, _)| {
            page_to_node_mapping_.remove(&frame);
            frame
        })
    }

}

#[cfg(test)]
mod test {
    use std::cell::RefCell;
    use crate::buffer::lru_k_replacer::LRUKNode;
    use crate::buffer::LRUKReplacer;

    #[test]
    fn record_access_test() {
        let new_node = LRUKNode::new(3, 10);
        // record access 3 times to start with
        assert!(new_node.access_times.borrow().iter().fold(true, |acc , val|
            acc && *val == u128::MAX), "Expected all of the value to be u128::MAX");
        assert_eq!(0, *new_node.recent_access_time_idx.borrow());
        new_node.record_access();
        assert_eq!(1, *new_node.recent_access_time_idx.borrow());
        new_node.record_access();
        assert_eq!(2, *new_node.recent_access_time_idx.borrow());
        new_node.record_access();
        assert!(new_node.access_times.borrow().iter().fold(true, |acc , val|
            acc && *val != u128::MAX), "Expected none of the value to be u128::MAX");
        assert_eq!(0, *new_node.recent_access_time_idx.borrow());
    }
    #[test]
    fn test_lru_node_ordering() {

        // Case 1, test two nodes are equal
        let node1 = LRUKNode{k: 3,
            access_times:
            RefCell::new(Box::new([0, 0, 1])),
            frame_id : 1,
            recent_access_time_idx : RefCell::new(0),
            is_evictable: RefCell::new(false)
        };

        let node2 = LRUKNode{k: 3,
            access_times:
            RefCell::new(Box::new([0, 0, 1])),
            frame_id : 1,
            recent_access_time_idx : RefCell::new(0),
            is_evictable: RefCell::new(false)
        };

        assert_eq!(node1, node2, "Expected node1 == node2");

        // Case 2, two nodes, both less than 3 accesses, One with earliest access time should
        // be considered greater

        let node1 = LRUKNode{k: 3,
            access_times:
            RefCell::new(Box::new([1, u128::MAX, u128::MAX])),
            frame_id : 1,
            recent_access_time_idx : RefCell::new(0),
            is_evictable: RefCell::new(false)
        };

        let node2 = LRUKNode{k: 3,
            access_times:
            RefCell::new(Box::new([2, u128::MAX, u128::MAX])),
            frame_id : 1,
            recent_access_time_idx : RefCell::new(0),
            is_evictable: RefCell::new(false)
        };

        assert!(node1 > node2, "Expected node1 > node2");


        // Case 3, two nodes, one with less than 3 access, other has 3 accesses. The one with
        // Less than three should be considered greater

        let node1 = LRUKNode{k: 3,
            access_times:
            RefCell::new(Box::new([1, 2, 3])),
            frame_id : 1,
            recent_access_time_idx : RefCell::new(0),
            is_evictable: RefCell::new(false)
        };

        let node2 = LRUKNode{k: 3,
            access_times:
            RefCell::new(Box::new([4, u128::MAX, u128::MAX])),
            frame_id : 1,
            recent_access_time_idx : RefCell::new(1),
            is_evictable: RefCell::new(false)
        };

        assert!(node1 < node2, "Expected node1 < node2");


        let node1 = LRUKNode{k: 3,
            access_times:
            RefCell::new(Box::new([3, u128::MAX, u128::MAX])),
            frame_id : 1,
            recent_access_time_idx : RefCell::new(1),
            is_evictable: RefCell::new(false)
        };

        let node2 = LRUKNode{k: 3,
            access_times:
            RefCell::new(Box::new([10, 11, 12])),
            frame_id : 1,
            recent_access_time_idx : RefCell::new(2),
            is_evictable: RefCell::new(false)
        };

        assert!(node1 > node2, "Expected node1 > node2");

        // Case 4: Both nodes have at least 3 accesses, the one with largest difference in
        // earliest and recent time is considered the greater one

        let node1 = LRUKNode{k: 3,
            access_times:
            RefCell::new(Box::new([1, 5, 10])),
            frame_id : 1,
            recent_access_time_idx : RefCell::new(0),
            is_evictable: RefCell::new(false)
        };

        let node2 = LRUKNode{k: 3,
            access_times:
            RefCell::new(Box::new([8, 9, 10])),
            frame_id : 1,
            recent_access_time_idx : RefCell::new(0),
            is_evictable: RefCell::new(false)
        };

        assert!(node1 > node2, "Expected node1 > node2");
    }

    #[test]
    fn mega_test_single_threaded() {

        // Used https://github.com/cmu-db/bustub/blob/master/test/buffer/lru_k_replacer_test.cpp
        // for basic test cases
        // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
        let lru_replacer = LRUKReplacer::new(2);
        lru_replacer.record_access(1);
        lru_replacer.record_access(2);
        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(6);
        lru_replacer.set_evictable(1, true);
        lru_replacer.set_evictable(2, true);
        lru_replacer.set_evictable(3, true);
        lru_replacer.set_evictable(4, true);
        lru_replacer.set_evictable(5, true);
        lru_replacer.set_evictable(6, false);

        assert_eq!(5, lru_replacer.size());

        // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
        // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].

        lru_replacer.record_access(1);

        // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
        // first based on LRU.
        assert_eq!(2, lru_replacer.evict().unwrap());
        assert_eq!(3, lru_replacer.evict().unwrap());
        assert_eq!(4, lru_replacer.evict().unwrap());

        assert_eq!(2, lru_replacer.size());

        // Scenario: Now replacer has frames [5,1].
        // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(4);
        lru_replacer.set_evictable(3, true);
        lru_replacer.set_evictable(4, true);
        assert_eq!(4, lru_replacer.size());

        // Scenario: continue looking for victims. We expect 3 to be evicted next.
        assert_eq!(3, lru_replacer.evict().unwrap());
        assert_eq!(3, lru_replacer.size());

        // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
        lru_replacer.set_evictable(6, true);
        assert_eq!(4, lru_replacer.size());
        assert_eq!(6, lru_replacer.evict().unwrap());
        assert_eq!(3, lru_replacer.size());

        // Now we have [1,5,4]. Continue looking for victims.
        lru_replacer.set_evictable(1, false);
        assert_eq!(2, lru_replacer.size());
        assert_eq!(5, lru_replacer.evict().unwrap());
        assert_eq!(1, lru_replacer.size());

        // Update access history for 1. Now we have [4,1]. Next victim is 4.
        lru_replacer.record_access(1);
        lru_replacer.record_access(1);
        lru_replacer.set_evictable(1, true);
        assert_eq!(2, lru_replacer.size());
        assert_eq!(4, lru_replacer.evict().unwrap());

        assert_eq!(1, lru_replacer.size());
        assert_eq!(1, lru_replacer.evict().unwrap());
        assert_eq!(0, lru_replacer.size());

        // This operation should not modify size
        match lru_replacer.evict() {
            Some(_)   => assert!(false, "Unexpected to see this operation succeed"),
            None      => {},
        }
        assert_eq!(0, lru_replacer.size());
    }

    #[test]
    fn mega_test_multi_threaded() {
        // TODO: To be implemented
    }

}