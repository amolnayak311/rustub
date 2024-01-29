use crate::storage::{DiskManager, PAGE_SIZE};
pub mod storage;
pub mod buffer;

fn print(data: &[u8; PAGE_SIZE]) {
    println!("{:?}", data);
}
fn main() {
    let dm = DiskManager::new("Test");
    let data = dm.read_page(0).unwrap();
    print(&data);

}
