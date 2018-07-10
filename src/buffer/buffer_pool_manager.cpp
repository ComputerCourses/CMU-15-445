#include <algorithm>
#include "buffer/buffer_pool_manager.h"

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     const std::string &db_file)
    : pool_size_(pool_size), disk_manager_{db_file} {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(100);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  FlushAllPages();
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an entry
 * for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  if (page_id==INVALID_PAGE_ID) {
    return nullptr;
  }

  std::lock_guard<std::mutex> lock(latch_);

  Page *page = nullptr;
  if (page_table_->Find(page_id, page)) {
    page->IncreasePinCount();
    return page;
  }

  if (!free_list_->empty()) {
    page = free_list_->front();
    free_list_->pop_front();
  } else if (replacer_->Victim(page)) {
    releaseDirty(page);
  } else {
    return nullptr;
  }

  page->Reset();
  page->SetPageId(page_id);
  page->IncreasePinCount();
  disk_manager_.ReadPage(page_id, page->GetData());
  page_table_->Insert(page_id, page);

  return page;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to replacer
 * if pin_count<=0 before this call, return false.
 * is_dirty: set the dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  Page *page = nullptr;

  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_->Find(page_id, page)) {
    assert(page!=nullptr);
    int pin_count = page->GetPinCount();
    if (pin_count > 0) {
      page->DecreasePinCount();
      if (page->GetPinCount()==0) {
        if (is_dirty) {
          page->SetDirty(true);
          dirty_pages_[page_id] = page;
        }
        replacer_->Insert(page);
      }
      return true;
    } else if (pin_count <= 0) {
      return false;
    }
  }

  return false;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {

  if (page_id==INVALID_PAGE_ID) {
    return false;
  }

  Page *page = nullptr;
  bool found;
  {
    std::lock_guard<std::mutex> lock(latch_);
    found = page_table_->Find(page_id, page);
  }

  assert(page->GetPageId()==page_id);
  assert(page->GetPageId()!=INVALID_PAGE_ID);

  if (found && page->IsDirty()) {
    disk_manager_.WritePage(page_id, page->GetData());
    page->SetDirty(false);

    auto it = dirty_pages_.find(page_id);
    assert(it!=dirty_pages_.end());
    dirty_pages_.erase(it);
    return true;
  }

  return false;
}

/*
 * Used to flush all dirty pages in the buffer pool manager
 */
void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);

  std::for_each(dirty_pages_.begin(), dirty_pages_.end(),
                [&](const auto p) {
                  Page *page = p.second;
                  if (page->IsDirty()) {
                    disk_manager_.WritePage(p.first, page->GetData());
                    page->SetDirty(false);
                  }
                });
  dirty_pages_.clear();

}

/**
 * User should call this method for deleting a page. This routine will call disk
 * manager to deallocate the page.
 * First, if page is found within page table, buffer pool manager should be
 * reponsible for removing this entry out of page table, reseting page metadata
 * and adding back to free list. Second, call disk manager's DeallocatePage()
 * method to delete from disk file.
 * If the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  Page *page = nullptr;
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_->Find(page_id, page) && page->GetPinCount()==0) {

    page_table_->Remove(page_id);
    replacer_->Erase(page);
    dirty_pages_.erase(page_id);
    disk_manager_.DeallocatePage(page_id);
    page->Reset();
    free_list_->push_back(page);
  }

  return false;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either from
 * free list or lru replacer(NOTE: always choose from free list first), update
 * new page's metadata, zero out memory and add corresponding entry into page
 * table.
 * return nullptr is all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  return allocatePage(page_id);
}

void BufferPoolManager::releaseDirty(cmudb::Page *page) {
  assert(page!=nullptr);
  if (page->IsDirty()) {
    disk_manager_.WritePage(page->GetPageId(), page->GetData());
    page->SetDirty(false);
  }
  page_table_->Remove(page->GetPageId());
}

Page *BufferPoolManager::allocatePage(page_id_t &page_id) {
  Page *page = nullptr;

  if (!free_list_->empty()) {
    page = free_list_->front();
    free_list_->pop_front();
  } else if (replacer_->Victim(page)) {
    releaseDirty(page);
  } else {
    return nullptr;
  }

  const auto id = disk_manager_.AllocatePage();
  page_id = id;
  page->Reset();
  page->SetPageId(id);
  page->IncreasePinCount();
  disk_manager_.ReadPage(id, page->GetData());
  page_table_->Insert(id, page);

  return page;
}

} // namespace cmudb
