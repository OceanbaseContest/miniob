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
// Created by Wangyunlai on 2021/5/12.
//

#ifndef __OBSERVER_STORAGE_COMMON_INDEX_META_H__
#define __OBSERVER_STORAGE_COMMON_INDEX_META_H__

#include <string>
#include "rc.h"

class TableMeta;
class FieldMeta;

namespace Json {
class Value;
} // namespace Json

class IndexMeta {
public:
  IndexMeta() = default;
  //add bzb [unique index] 20211103:b
  RC init(const char *name, const FieldMeta &field, const int is_unique_index);
  //20211103:e
  //add bzb [multi index] 20211107:b
  RC init_for_multi(const char *name, const FieldMeta *field[], const int is_unique_index, size_t attribute_count);
  //20211107:e
public:
  const char *name() const;
  const char *field() const;
  //add bzb [multi index] 20211107:b
  //0 不匹配， 1 匹配
  int equal_multi_field(char* const* attribute_name, size_t attribute_count) const;
  //20211107:e

  void desc(std::ostream &os) const;
  int get_is_unique_index();
public:
  void to_json(Json::Value &json_value) const;
  static RC from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index);

private:
  std::string       name_;
  std::string       field_;
  //add bzb [unique index] 20211103:b
  int is_unique_index_;
  //20211103:e
  //add bzb [multi index] 20211107:b
  size_t attribute_count_;
  std::string       multi_field_[50];
  //20211107:e
};
#endif // __OBSERVER_STORAGE_COMMON_INDEX_META_H__