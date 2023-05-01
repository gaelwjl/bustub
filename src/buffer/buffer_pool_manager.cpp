//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock_guard(latch_);
  frame_id_t frame_id;
  if (!GetAvailableFrameId(&frame_id)) {
    return nullptr;
  }
  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
  }
  page_table_.erase(pages_[frame_id].page_id_);
  return NewPageHelper(page_id, frame_id);
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock_guard(latch_);
  return DoFetchPage(page_id);
}

auto BufferPoolManager::DoFetchPage(page_id_t page_id) -> Page * {
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    pages_[it->second].is_dirty_ = true;
    PinPageToFrame(&pages_[it->second], page_table_[page_id]);
    return &pages_[it->second];
  }
  frame_id_t frame_id;
  if (!GetAvailableFrameId(&frame_id)) {
    return nullptr;
  }
  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
  }
  page_table_.erase(pages_[frame_id].page_id_);
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  PinPageToFrame(&pages_[frame_id], frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock_guard(latch_);
  Page *page = GetPage(page_id);
  if (page == nullptr) {
    return false;
  }
  page->is_dirty_ = is_dirty;
  if (page->pin_count_ <= 0) {
    return false;
  }
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID || page_id >= static_cast<int>(next_page_id_)) {
    return false;
  }
  Page *const page = GetPage(page_id);
  if (page == nullptr) {
    return false;
  }
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t frame_id = 0; frame_id < pool_size_; ++frame_id) {
    page_id_t page_id = pages_[frame_id].page_id_;
    if (page_id != INVALID_PAGE_ID && pages_[frame_id].is_dirty_) {
      FlushPage(page_id);
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock_guard(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
    return false;
  }
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

auto BufferPoolManager::NewPageHelper(page_id_t *page_id, frame_id_t frame_id) -> Page * {
  *page_id = AllocatePage();
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  PinPageToFrame(&pages_[frame_id], frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManager::GetPage(page_id_t page_id) -> Page * {
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return nullptr;
  }
  return &pages_[it->second];
}

auto BufferPoolManager::GetAvailableFrameId(frame_id_t *frame_id) -> bool {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  return replacer_->Evict(frame_id);
}

auto BufferPoolManager::PinPageToFrame(Page *page, frame_id_t frame_id) -> void {
  page_table_[page->page_id_] = frame_id;
  page->pin_count_++;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
}

}  // namespace bustub
