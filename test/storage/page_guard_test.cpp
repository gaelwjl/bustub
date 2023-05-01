//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

TEST(PageGuardTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);
  bpm->UnpinPage(0, false);

  auto guarded_page = bpm->FetchPageBasic(0);
  auto guarded_page_moved = std::move(guarded_page);

  EXPECT_EQ(page0->GetData(), guarded_page_moved.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page_moved.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  guarded_page.Drop();
  guarded_page_moved.Drop();

  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, MoveTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);
  {
    auto guard1 = BasicPageGuard(bpm.get(), page0);
    guard1 = bpm->FetchPageBasic(0);
  }

  EXPECT_EQ(0, page0->GetPinCount());

  {
    std::vector<ReadPageGuard> page_guards(10);
    for (int i = 0; i < 10; ++i) {
      page_guards[i] = bpm->FetchPageRead(0);
    }
    page_guards[0].Drop();
    EXPECT_EQ(9, page0->GetPinCount());
  }
  EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, WriteReadTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);
  page_id_t page_id_tmp;
  Page *page0 = bpm->NewPage(&page_id_tmp);
  {
    BasicPageGuard guarded_page = BasicPageGuard(bpm.get(), page0);
    snprintf(guarded_page.GetDataMut(), BUSTUB_PAGE_SIZE, "World");
    auto guarded_page_moved = std::move(guarded_page);
  }
  EXPECT_EQ(0, page0->GetPinCount());
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_tmp));
  }

  bpm->UnpinPage(1, false);

  {
    WritePageGuard write_page = bpm->FetchPageWrite(0);
    EXPECT_EQ(0, strcmp(write_page.GetData(), "World"));
    snprintf(write_page.GetDataMut(), BUSTUB_PAGE_SIZE, "ChangedData");
    WritePageGuard write_page2;
    write_page2 = std::move(write_page);
    write_page2.Drop();
    ReadPageGuard read_page = bpm->FetchPageRead(0);
    EXPECT_EQ(0, strcmp(read_page.GetData(), "ChangedData"));
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

}  // namespace bustub
