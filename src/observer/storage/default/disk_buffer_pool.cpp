/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Longda on 2021/4/13.
//
#include "disk_buffer_pool.h"
#include <errno.h>
#include <string.h>

#include "common/log/log.h"

using namespace common;

unsigned long current_time()
{
  struct timespec tp;
  clock_gettime(CLOCK_MONOTONIC, &tp);
  return tp.tv_sec * 1000 * 1000 * 1000UL + tp.tv_nsec;
}

DiskBufferPool *theGlobalDiskBufferPool()
{
  static DiskBufferPool *instance = new DiskBufferPool();

  return instance;
}

//add bzb [LRU+LFU] 20211031:b
RC DiskBufferPool::get_FileHandle (int file_id, BPFileHandle* &FileHandle) {
  if (open_list_[file_id]) {
    FileHandle = open_list_[file_id];
    LOG_INFO("success in get_FileHandle");
    return RC::SUCCESS;
  }
  LOG_INFO("failed in get_FileHandle");
  return RC::GENERIC_ERROR;
}
//20211031:e

RC DiskBufferPool::create_file(const char *file_name)
{
  int fd = open(file_name, O_RDWR | O_CREAT | O_EXCL, S_IREAD | S_IWRITE);
  if (fd < 0) {
    LOG_ERROR("Failed to create %s, due to %s.", file_name, strerror(errno));
    return RC::SCHEMA_DB_EXIST;
  }

  close(fd);

  /**
   * Here don't care about the failure
   */
  fd = open(file_name, O_RDWR);
  if (fd < 0) {
    LOG_ERROR("Failed to open for readwrite %s, due to %s.", file_name, strerror(errno));
    return RC::IOERR_ACCESS;
  }

  Page page;
  memset(&page, 0, sizeof(Page));

  BPFileSubHeader *fileSubHeader;
  fileSubHeader = (BPFileSubHeader *)page.data;
  fileSubHeader->allocated_pages = 1;
  fileSubHeader->page_count = 1;

  char *bitmap = page.data + (int)BP_FILE_SUB_HDR_SIZE;
  bitmap[0] |= 0x01;
  if (lseek(fd, 0, SEEK_SET) == -1) {
    LOG_ERROR("Failed to seek file %s to position 0, due to %s .", file_name, strerror(errno));
    close(fd);
    return RC::IOERR_SEEK;
  }

  if (write(fd, (char *)&page, sizeof(Page)) != sizeof(Page)) {
    LOG_ERROR("Failed to write header to file %s, due to %s.", file_name, strerror(errno));
    close(fd);
    return RC::IOERR_WRITE;
  }

  close(fd);
  LOG_INFO("Successfully create %s.", file_name);
  return RC::SUCCESS;
}

RC DiskBufferPool::open_file(const char *file_name, int *file_id)
{
  int fd, i;
  // This part isn't gentle, the better method is using LRU queue.
  for (i = 0; i < MAX_OPEN_FILE; i++) {
    if (open_list_[i]) {
      if (!strcmp(open_list_[i]->file_name, file_name)) {
        *file_id = i;
        LOG_INFO("%s has already been opened.", file_name);
        return RC::SUCCESS;
      }
    }
  }
  i = 0;
  while (i < MAX_OPEN_FILE && open_list_[i++])
    ;
  if (i >= MAX_OPEN_FILE && open_list_[i - 1]) {
    LOG_ERROR("Failed to open file %s, because too much files has been opened.", file_name);
    return RC::BUFFERPOOL_OPEN_TOO_MANY_FILES;
  }

  if ((fd = open(file_name, O_RDWR)) < 0) {
    LOG_ERROR("Failed to open file %s, because %s.", file_name, strerror(errno));
    return RC::IOERR_ACCESS;
  }
  LOG_INFO("Successfully open file %s.", file_name);

  BPFileHandle *file_handle = new (std::nothrow) BPFileHandle();
  if (file_handle == nullptr) {
    LOG_ERROR("Failed to alloc memory of BPFileHandle for %s.", file_name);
    close(fd);
    return RC::NOMEM;
  }

  RC tmp;
  file_handle->bopen = true;
  int file_name_len = strlen(file_name) + 1;
  char *cloned_file_name = new char[file_name_len];
  snprintf(cloned_file_name, file_name_len, "%s", file_name);
  cloned_file_name[file_name_len - 1] = '\0';
  file_handle->file_name = cloned_file_name;
  file_handle->file_desc = fd;
  if ((tmp = allocate_block(&file_handle->hdr_frame)) != RC::SUCCESS) {
    LOG_ERROR("Failed to allocate block for %s's BPFileHandle.", file_name);
    delete file_handle;
    close(fd);
    return tmp;
  }
  file_handle->hdr_frame->dirty = false;
  file_handle->hdr_frame->acc_time = current_time();
  //add bzb [LRU+LFU] 20211031:b
  file_handle->hdr_frame->LFU_count = 0;
  //20211031:e
  file_handle->hdr_frame->file_desc = fd;
  file_handle->hdr_frame->pin_count = 1;
  if ((tmp = load_page(0, file_handle, file_handle->hdr_frame)) != RC::SUCCESS) {
    file_handle->hdr_frame->pin_count = 0;
    dispose_block(file_handle->hdr_frame);
    close(fd);
    delete file_handle;
    return tmp;
  }

  file_handle->hdr_page = &(file_handle->hdr_frame->page);
  file_handle->bitmap = file_handle->hdr_page->data + BP_FILE_SUB_HDR_SIZE;
  file_handle->file_sub_header = (BPFileSubHeader *)file_handle->hdr_page->data;
  open_list_[i - 1] = file_handle;
  *file_id = i - 1;
  LOG_INFO("Successfully open %s. file_id=%d, hdr_frame=%p", file_name, *file_id, file_handle->hdr_frame);
  return RC::SUCCESS;
}

RC DiskBufferPool::close_file(int file_id)
{
  RC tmp;
  if ((tmp = check_file_id(file_id)) != RC::SUCCESS) {
    LOG_ERROR("Failed to close file, due to invalid fileId %d", file_id);
    return tmp;
  }

  BPFileHandle *file_handle = open_list_[file_id];
  file_handle->hdr_frame->pin_count--;
  if ((tmp = force_all_pages(file_handle)) != RC::SUCCESS) {
    file_handle->hdr_frame->pin_count++;
    LOG_ERROR("Failed to closeFile %d:%s, due to failed to force all pages.", file_id, file_handle->file_name);
    return tmp;
  }

  if (close(file_handle->file_desc) < 0) {
    LOG_ERROR("Failed to close fileId:%d, fileName:%s, error:%s", file_id, file_handle->file_name, strerror(errno));
    return RC::IOERR_CLOSE;
  }
  open_list_[file_id] = nullptr;
  delete (file_handle);
  LOG_INFO("Successfully close file %d:%s.", file_id, file_handle->file_name);
  return RC::SUCCESS;
}

RC DiskBufferPool::get_this_page(int file_id, PageNum page_num, BPPageHandle *page_handle)
{
  RC tmp;
  if ((tmp = check_file_id(file_id)) != RC::SUCCESS) {
    LOG_ERROR("Failed to load page %d, due to invalid fileId %d", page_num, file_id);
    return tmp;
  }

  BPFileHandle *file_handle = open_list_[file_id];
  LOG_INFO("IN GET PAGE");
  if ((tmp = check_page_num(page_num, file_handle)) != RC::SUCCESS) {
    LOG_ERROR("Failed to load page %s:%d, due to invalid pageNum.", file_handle->file_name, page_num);
    return tmp;
  }
  LOG_INFO("OUT GET PAGE");
  for (int i = 0; i < BP_BUFFER_SIZE; i++) {
    if (!bp_manager_.allocated[i])
      continue;
    if (bp_manager_.frame[i].file_desc != file_handle->file_desc)
      continue;

    // This page has been loaded.
    if (bp_manager_.frame[i].page.page_num == page_num) {
      page_handle->frame = bp_manager_.frame + i;
      page_handle->frame->pin_count++;
      page_handle->frame->acc_time = current_time();
      //add bzb [LRU+LFU] 20211031:b
      if(page_handle->frame->LFU_count < 10000){
        page_handle->frame->LFU_count++;
      }
      //20211031:e
      page_handle->open = true;
      return RC::SUCCESS;
    }
  }

  // Allocate one page and load the data into this page
  if ((tmp = allocate_block(&(page_handle->frame))) != RC::SUCCESS) {
    LOG_ERROR("Failed to load page %s:%d, due to failed to alloc page.", file_handle->file_name, page_num);
    return tmp;
  }
  page_handle->frame->dirty = false;
  page_handle->frame->file_desc = file_handle->file_desc;
  page_handle->frame->pin_count = 1;
  page_handle->frame->acc_time = current_time();
  //add bzb [LRU+LFU] 20211031:b
  page_handle->frame->LFU_count = 0;
  //20211031:e
  if ((tmp = load_page(page_num, file_handle, page_handle->frame)) != RC::SUCCESS) {
    LOG_ERROR("Failed to load page %s:%d", file_handle->file_name, page_num);
    page_handle->frame->pin_count = 0;
    dispose_block(page_handle->frame);
    return tmp;
  }

  page_handle->open = true;
  return RC::SUCCESS;
}

RC DiskBufferPool::allocate_page(int file_id, BPPageHandle *page_handle)
{
  RC tmp;
  if ((tmp = check_file_id(file_id)) != RC::SUCCESS) {
    LOG_ERROR("Failed to alloc page, due to invalid fileId %d", file_id);
    return tmp;
  }

  BPFileHandle *file_handle = open_list_[file_id];

  int byte = 0, bit = 0;
  if ((file_handle->file_sub_header->allocated_pages) < (file_handle->file_sub_header->page_count)) {
    // There is one free page
    for (int i = 0; i < file_handle->file_sub_header->page_count; i++) {
      byte = i / 8;
      bit = i % 8;
      if (((file_handle->bitmap[byte]) & (1 << bit)) == 0) {
        (file_handle->file_sub_header->allocated_pages)++;
        file_handle->bitmap[byte] |= (1 << bit);
        return get_this_page(file_id, i, page_handle);
      }
    }
  }

  if ((tmp = allocate_block(&(page_handle->frame))) != RC::SUCCESS) {
    LOG_ERROR("Failed to allocate page %s, due to no free page.", file_handle->file_name);
    return tmp;
  }

  PageNum page_num = file_handle->file_sub_header->page_count;
  LOG_INFO("page_num = %d", page_num);
  file_handle->file_sub_header->allocated_pages++;
  file_handle->file_sub_header->page_count++;

  byte = page_num / 8;
  bit = page_num % 8;
  file_handle->bitmap[byte] |= (1 << bit);
  file_handle->hdr_frame->dirty = true;

  page_handle->frame->dirty = false;
  page_handle->frame->file_desc = file_handle->file_desc;
  page_handle->frame->pin_count = 1;
  page_handle->frame->acc_time = current_time();
  memset(&(page_handle->frame->page), 0, sizeof(Page));
  page_handle->frame->page.page_num = file_handle->file_sub_header->page_count - 1;
  //add bzb [LRU+LFU] 20211031:b
  page_handle->frame->LFU_count = 0;
  //20211031:e

  // Use flush operation to extion file
  if ((tmp = flush_block(page_handle->frame)) != RC::SUCCESS) {
    LOG_ERROR("Failed to alloc page %s , due to failed to extend one page.", file_handle->file_name);
    return tmp;
  }

  page_handle->open = true;
  return RC::SUCCESS;
}

RC DiskBufferPool::get_page_num(BPPageHandle *page_handle, PageNum *page_num)
{
  if (!page_handle->open)
    return RC::BUFFERPOOL_CLOSED;
  *page_num = page_handle->frame->page.page_num;
  return RC::SUCCESS;
}

RC DiskBufferPool::get_data(BPPageHandle *page_handle, char **data)
{
  if (!page_handle->open)
    return RC::BUFFERPOOL_CLOSED;
  *data = page_handle->frame->page.data;
  return RC::SUCCESS;
}

RC DiskBufferPool::mark_dirty(BPPageHandle *page_handle)
{
  page_handle->frame->dirty = true;
  return RC::SUCCESS;
}

/*
//add bzb [LRU+LFU] 20211031:b
RC DiskBufferPool::increase_LFU_count(BPPageHandle *page_handle)
{
  if(page_handle->frame->LFU_count < 10000){
    page_handle->frame->LFU_count++;
    LOG_INFO("increase_LFU_count LFU_count = %d", page_handle->frame->LFU_count);
  }
  return RC::SUCCESS;
}
//20211031:e
*/

RC DiskBufferPool::unpin_page(BPPageHandle *page_handle)
{
  page_handle->open = false;
  page_handle->frame->pin_count--;
  return RC::SUCCESS;
}

//add bzb [LRU+LFU] 20211031:b
RC DiskBufferPool::unpin_page_to_0(BPPageHandle *page_handle) {
  page_handle->open = false;
  page_handle->frame->pin_count = 0;
  return RC::SUCCESS;
}
//20211031:e

/**
 * dispose_page will delete the data of the page of pageNum
 * force_page will flush the page of pageNum
 * @param fileID
 * @param pageNum
 * @return
 */
RC DiskBufferPool::dispose_page(int file_id, PageNum page_num)
{
  RC rc;
  if ((rc = check_file_id(file_id)) != RC::SUCCESS) {
    LOG_ERROR("Failed to alloc page, due to invalid fileId %d", file_id);
    return rc;
  }

  BPFileHandle *file_handle = open_list_[file_id];
  if ((rc = check_page_num(page_num, file_handle)) != RC::SUCCESS) {
    LOG_ERROR("Failed to dispose page %s:%d, due to invalid pageNum", file_handle->file_name, page_num);
    return rc;
  }

  for (int i = 0; i < BP_BUFFER_SIZE; i++) {
    if (!bp_manager_.allocated[i])
      continue;
    if (bp_manager_.frame[i].file_desc != file_handle->file_desc) {
      continue;
    }

    if (bp_manager_.frame[i].page.page_num == page_num) {
      if (bp_manager_.frame[i].pin_count != 0)
        return RC::BUFFERPOOL_PAGE_PINNED;
      bp_manager_.allocated[i] = false;
    }
  }

  file_handle->hdr_frame->dirty = true;
  file_handle->file_sub_header->allocated_pages--;
  // file_handle->pFileSubHeader->pageCount--;
  char tmp = 1 << (page_num % 8);
  file_handle->bitmap[page_num / 8] &= ~tmp;
  return RC::SUCCESS;
}

RC DiskBufferPool::force_page(int file_id, PageNum page_num)
{
  RC rc;
  if ((rc = check_file_id(file_id)) != RC::SUCCESS) {
    LOG_ERROR("Failed to alloc page, due to invalid fileId %d", file_id);
    return rc;
  }
  BPFileHandle *file_handle = open_list_[file_id];
  return force_page(file_handle, page_num);
}
//add bzb [drop table] 20211022:b
RC DiskBufferPool::force_all_pages(int file_id)
{
  RC rc;
  if ((rc = check_file_id(file_id)) != RC::SUCCESS) {
    LOG_ERROR("Failed to alloc page, due to invalid fileId %d", file_id);
    return rc;
  }
  LOG_INFO("DROP FILE file_id = %d", file_id);
  BPFileHandle *file_handle = open_list_[file_id];
  return force_all_pages(file_handle);
}
//20211022:e
/**
 * dispose_page will delete the data of the page of pageNum
 * force_page will flush the page of pageNum
 * @param fileHandle
 * @param pageNum
 * @return
 */
RC DiskBufferPool::force_page(BPFileHandle *file_handle, PageNum page_num)
{
  int i;
  for (i = 0; i < BP_BUFFER_SIZE; i++) {
    if (!bp_manager_.allocated[i])
      continue;

    Frame *frame = &bp_manager_.frame[i];
    if (frame->file_desc != file_handle->file_desc)
      continue;
    if (frame->page.page_num != page_num && page_num != -1) {
      continue;
    }

    if (frame->pin_count != 0) {
      LOG_ERROR("Page :%s:%d has been pinned.", file_handle->file_name, page_num);
      return RC::BUFFERPOOL_PAGE_PINNED;
    }

    if (frame->dirty) {
      RC rc = RC::SUCCESS;
      if ((rc = flush_block(frame)) != RC::SUCCESS) {
        LOG_ERROR("Failed to flush page:%s:%d.", file_handle->file_name, page_num);
        return rc;
      }
    }
    bp_manager_.allocated[i] = false;
    return RC::SUCCESS;
  }
  return RC::SUCCESS;
}
RC DiskBufferPool::flush_all_pages(int file_id)
{
  RC rc = check_file_id(file_id);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to flush pages due to invalid file_id %d", file_id);
    return rc;
  }

  BPFileHandle *file_handle = open_list_[file_id];
  return force_all_pages(file_handle);
}

RC DiskBufferPool::force_all_pages(BPFileHandle *file_handle)
{

  for (int i = 0; i < BP_BUFFER_SIZE; i++) {
    if (!bp_manager_.allocated[i])
      continue;

    if (bp_manager_.frame[i].file_desc != file_handle->file_desc)
      continue;

    if (bp_manager_.frame[i].dirty) {
      RC rc = flush_block(&bp_manager_.frame[i]);
      if (rc != RC::SUCCESS) {
        LOG_ERROR("Failed to flush all pages' of %s.", file_handle->file_name);
        return rc;
      }
    }
    bp_manager_.allocated[i] = false;
  }
  return RC::SUCCESS;
}

RC DiskBufferPool::flush_block(Frame *frame)
{
  // The better way is use mmap the block into memory,
  // so it is easier to flush data to file.

  s64_t offset = ((s64_t)frame->page.page_num) * sizeof(Page);
  if (lseek(frame->file_desc, offset, SEEK_SET) == offset - 1) {
    LOG_ERROR("Failed to flush page %lld of %d due to failed to seek %s.", offset, frame->file_desc, strerror(errno));
    return RC::IOERR_SEEK;
  }

  if (write(frame->file_desc, &(frame->page), sizeof(Page)) != sizeof(Page)) {
    LOG_ERROR("Failed to flush page %lld of %d due to %s.", offset, frame->file_desc, strerror(errno));
    return RC::IOERR_WRITE;
  }
  frame->dirty = false;
  LOG_DEBUG("Flush block. file desc=%d, page num=%d", frame->file_desc, frame->page.page_num);

  return RC::SUCCESS;
}

RC DiskBufferPool::allocate_block(Frame **buffer)
{
  LOG_INFO("IM IN allocate_block");
  // There is one Frame which is free.
  for (int i = 0; i < BP_BUFFER_SIZE; i++) {
    if (!bp_manager_.allocated[i]) {
      bp_manager_.allocated[i] = true;
      *buffer = bp_manager_.frame + i;
      LOG_DEBUG("Allocate block frame=%p", bp_manager_.frame + i);
      LOG_INFO("Allocate block frame-free=%p", bp_manager_.frame + i);
      return RC::SUCCESS;
    }
  }
  //add bzb [LRU+LFU] 20211031:b
  int min = 0;
  unsigned long mintime = 0;
  unsigned int min_LFU_count = 0;
  bool flag = false;
  for (int i = 0; i < BP_BUFFER_SIZE; i++) {
    LOG_INFO("i = %d, pin_count = %d", i, bp_manager_.frame[i].pin_count);
    if (bp_manager_.frame[i].pin_count != 0)
      continue;
    if (!flag) {
      flag = true;
      min = i;
      mintime = bp_manager_.frame[i].acc_time;
      min_LFU_count = bp_manager_.frame[i].LFU_count;
      LOG_INFO("IN FLAG!!!!");
      LOG_INFO("min = %d", min);
      LOG_INFO("bp_manager_.frame[i].pin_count = %d", bp_manager_.frame[i].pin_count);
      LOG_INFO("min_LFU_count = %d", min_LFU_count);
      LOG_INFO("bp_manager_.frame[i].acc_time = %d", bp_manager_.frame[i].acc_time);
    }
    LOG_INFO("OUT FLAG!!!!");
    if (bp_manager_.frame[i].acc_time < mintime) {
      min = i;
      mintime = bp_manager_.frame[i].acc_time;
      min_LFU_count = bp_manager_.frame[i].LFU_count;
      LOG_INFO("EXECUTE IN  < minimum, min = %d", min);
    }
    else if (bp_manager_.frame[i].acc_time == mintime) {
      if (bp_manager_.frame[i].LFU_count < min_LFU_count) {
        min = i;
        mintime = bp_manager_.frame[i].acc_time;
        min_LFU_count = bp_manager_.frame[i].LFU_count;
        LOG_INFO("EXECUTE IN  == minimum, min = %d", min);
      }
    }
    else {
      LOG_INFO("HUI_QI???????????????");
    }
  }
  //20211031:e
  if (!flag) {
    LOG_ERROR("All pages have been used and pinned.");
    return RC::NOMEM;
  }
  LOG_INFO("success min = %d", min);
  if (bp_manager_.frame[min].dirty) {
    RC rc = flush_block(&(bp_manager_.frame[min]));
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to flush block of %d for %d.", min, bp_manager_.frame[min].file_desc);
      return rc;
    }
  }
  *buffer = bp_manager_.frame + min;
  return RC::SUCCESS;
}

RC DiskBufferPool::dispose_block(Frame *buf)
{
  if (buf->pin_count != 0) {
    LOG_WARN("Begin to free page %d of %d, but it's pinned.", buf->page.page_num, buf->file_desc);
    return RC::LOCKED_UNLOCK;
  }
  if (buf->dirty) {
    RC rc = flush_block(buf);
    if (rc != RC::SUCCESS) {
      LOG_WARN("Failed to flush block %d of %d during dispose block.", buf->page.page_num, buf->file_desc);
      return rc;
    }
  }
  buf->dirty = false;
  int pos = buf - bp_manager_.frame;
  bp_manager_.allocated[pos] = false;
  LOG_DEBUG("dispost block frame =%p", buf);
  return RC::SUCCESS;
}

RC DiskBufferPool::check_file_id(int file_id)
{
  if (file_id < 0 || file_id >= MAX_OPEN_FILE) {
    LOG_ERROR("Invalid fileId:%d.", file_id);
    return RC::BUFFERPOOL_ILLEGAL_FILE_ID;
  }
  if (!open_list_[file_id]) {
    LOG_ERROR("Invalid fileId:%d, it is empty.", file_id);
    return RC::BUFFERPOOL_ILLEGAL_FILE_ID;
  }
  return RC::SUCCESS;
}

RC DiskBufferPool::get_page_count(int file_id, int *page_count)
{
  RC rc = RC::SUCCESS;
  if ((rc = check_file_id(file_id)) != RC::SUCCESS) {
    return rc;
  }
  *page_count = open_list_[file_id]->file_sub_header->page_count;
  return RC::SUCCESS;
}

RC DiskBufferPool::check_page_num(PageNum page_num, BPFileHandle *file_handle)
{
  if (page_num >= file_handle->file_sub_header->page_count) {
    LOG_ERROR("Invalid pageNum:%d, file's name:%s", page_num, file_handle->file_name);
    return RC::BUFFERPOOL_INVALID_PAGE_NUM;
  }
  if ((file_handle->bitmap[page_num / 8] & (1 << (page_num % 8))) == 0) {
    LOG_ERROR("Invalid pageNum:%d, file's name:%s", page_num, file_handle->file_name);
    return RC::BUFFERPOOL_INVALID_PAGE_NUM;
  }
  return RC::SUCCESS;
}

RC DiskBufferPool::load_page(PageNum page_num, BPFileHandle *file_handle, Frame *frame)
{
  s64_t offset = ((s64_t)page_num) * sizeof(Page);
  if (lseek(file_handle->file_desc, offset, SEEK_SET) == -1) {
    LOG_ERROR(
        "Failed to load page %s:%d, due to failed to lseek:%s.", file_handle->file_name, page_num, strerror(errno));

    return RC::IOERR_SEEK;
  }
  if (read(file_handle->file_desc, &(frame->page), sizeof(Page)) != sizeof(Page)) {
    LOG_ERROR(
        "Failed to load page %s:%d, due to failed to read data:%s.", file_handle->file_name, page_num, strerror(errno));
    return RC::IOERR_READ;
  }
  return RC::SUCCESS;
}
