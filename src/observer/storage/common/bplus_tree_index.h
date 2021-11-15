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
// Created by wangyunlai.wyl on 2021/5/19.
//

#ifndef __OBSERVER_STORAGE_COMMON_BPLUS_TREE_INDEX_H_
#define __OBSERVER_STORAGE_COMMON_BPLUS_TREE_INDEX_H_

#include "storage/common/index.h"
#include "storage/common/bplus_tree.h"

class BplusTreeIndex : public Index {
public:
  BplusTreeIndex() = default;
  virtual ~BplusTreeIndex() noexcept;

  RC create(const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta);
//add bzb [multi index] 20211107:b
  RC create_multi(const char *file_name, const IndexMeta &index_meta, const FieldMeta *field_meta[], int field_count);
//20211107:e
  RC open(const char *file_name, const IndexMeta &index_meta, const FieldMeta &field_meta);
  RC close();
  //RC insert_entry(const char *record, const RID *rid) override;
  //add bzb [unique index] 20211103:b
  RC insert_entry(const char *record, const RID *rid, int is_unique_index) override;
  //20211103:e

  //add bzb [multi index] 20211107:b
  RC insert_multi_entry(const char *record, const RID *rid, int is_unique_index) override;
  //20211107:e
  RC delete_entry(const char *record, const RID *rid) override;

  IndexScanner *create_scanner(CompOp comp_op, const char *value) override;

  RC sync() override;

private:
  bool inited_ = false;
  BplusTreeHandler index_handler_;
};

class BplusTreeIndexScanner : public IndexScanner {
public:
  BplusTreeIndexScanner(BplusTreeScanner *tree_scanner);
  ~BplusTreeIndexScanner() noexcept override;

  RC next_entry(RID *rid) override;
  RC destroy() override;
private:
  BplusTreeScanner * tree_scanner_;
};

#endif //__OBSERVER_STORAGE_COMMON_BPLUS_TREE_INDEX_H_
