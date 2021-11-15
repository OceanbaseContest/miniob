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
// Created by wangyunlai.wyl on 2021/5/18.
//

#include "storage/common/index_meta.h"
#include "storage/common/field_meta.h"
#include "storage/common/table_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "rc.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_name");
//add bzb [unique index] 20211103:b
const static Json::StaticString IS_UNIQUE_INDEX("is_unique_index");
//20211103:e
//add bzb [multi index] 20211107:b
const static Json::StaticString ATTR_COUNT("attribute_count");
//20211107:e

RC IndexMeta::init(const char *name, const FieldMeta &field, const int is_unique_index) {
  if (nullptr == name || common::is_blank(name)) {
    return RC::INVALID_ARGUMENT;
  }

  name_ = name;
  field_ = field.name();
  //add bzb [unique index] 20211103:b
  is_unique_index_ = is_unique_index;
  //20211103:e
  attribute_count_ = 1;
  return RC::SUCCESS;
}

//add bzb [multi index] 20211107:b
RC IndexMeta::init_for_multi(const char *name, const FieldMeta *field[], const int is_unique_index, size_t attribute_count) {
  if (nullptr == name || common::is_blank(name)) {
    return RC::INVALID_ARGUMENT;
  }
  name_ = name;
  LOG_INFO("name_ = %s", name_.c_str());
  attribute_count_ = attribute_count;
  LOG_INFO("attribute_count_ = %d", attribute_count_);
  for (int i = 0; i < attribute_count_; i++) {
    multi_field_[i] = field[i]->name();
    LOG_INFO("i = %d, field_name = %s", i, multi_field_[i].c_str());
  }
  
  is_unique_index_ = is_unique_index;
  LOG_INFO("is_unique_index_ = %d", is_unique_index_);
  
  return RC::SUCCESS;
}
//20211107:e

void IndexMeta::to_json(Json::Value &json_value) const {
  json_value[FIELD_NAME] = name_;
  //add bzb [unique index] 20211103:b
  json_value[IS_UNIQUE_INDEX] = is_unique_index_;
  //20211103:e
  if (attribute_count_ == 1) {
    json_value[FIELD_FIELD_NAME] = field_;
    json_value[ATTR_COUNT] = attribute_count_;
  }
  //add bzb [multi index] 20211107:b
  else {
    for (int i = 0; i < attribute_count_; i++) {
      std::string field_name = "field_name" + std::to_string(i);
      LOG_INFO("field_name = %s", field_name.c_str());
      Json::String MULTI_FIELD_NAME_I(field_name.c_str());
      json_value[MULTI_FIELD_NAME_I] = multi_field_[i].c_str();
    }
    json_value[ATTR_COUNT] = (int)attribute_count_;
  }
  //20211107:e
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index) {
  const Json::Value &name_value = json_value[FIELD_NAME];
  //const Json::Value &field_value = json_value[FIELD_FIELD_NAME];
  //add bzb [unique index] 20211103:b
  const Json::Value &is_unique_index_value = json_value[IS_UNIQUE_INDEX];
  int is_unique_index = std::stoi(is_unique_index_value.toStyledString().c_str());
  LOG_INFO("is_unique_index = %d", is_unique_index);
  //20211103:e

  //add bzb [multi index] 20211107:b
  const Json::Value &attribute_count = json_value[ATTR_COUNT];
  size_t attribute_count_t = std::stoi(attribute_count.toStyledString().c_str());
  LOG_INFO("attribute_count = %d", attribute_count_t);

  if (attribute_count_t == 1) {
    LOG_INFO("attribute_count == 1");
    const Json::Value &field_value = json_value[FIELD_FIELD_NAME];
    if (!name_value.isString()) {
      LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
      return RC::GENERIC_ERROR;
    }

    if (!field_value.isString()) {
      LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
                name_value.asCString(), field_value.toStyledString().c_str());
      return RC::GENERIC_ERROR;
    }

    const FieldMeta *field = table.field(field_value.asCString());
    if (nullptr == field) {
      LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString());
      return RC::SCHEMA_FIELD_MISSING;
    }
    //add bzb [unique index] 20211103:b
    return index.init(name_value.asCString(), *field, is_unique_index);
    //20211103:e
  }
  else {
    LOG_INFO("attribute_count == else");
    const FieldMeta* multi_field[attribute_count_t];
    for (int i = 0; i < attribute_count_t; i++) {
      std::string field_name = "field_name" + std::to_string(i);
      const Json::String MULTI_FIELD_NAME_I(field_name.c_str());
      const Json::Value &field_value = json_value[MULTI_FIELD_NAME_I];
      const FieldMeta *field = table.field(field_value.asCString());
      multi_field[i] = field;
    }
    return index.init_for_multi(name_value.asCString(), multi_field, is_unique_index, attribute_count_t);
  }
  //20211107:e
}

const char *IndexMeta::name() const {
  return name_.c_str();
}

const char *IndexMeta::field() const {
  return field_.c_str();
}

//add bzb [multi index] 20211107:b
int IndexMeta::equal_multi_field(char* const* attribute_name, size_t attribute_count) const {
  LOG_INFO("IN EQUAL MULTI");
  int equal_field_count = 0;
  if (attribute_count != attribute_count_) {    //先判断属性个数
    LOG_INFO("attribute_count is not equal");
    return 0;
  }
  LOG_INFO("2");
  for (int i = 0; i < attribute_count; i++) {   //判断属性名
    const std::string field_name_cmp = attribute_name[i];
    //const std::string field_name_cmp(attribute_name[i]);
    LOG_INFO("3");
    for (int j = 0; j < attribute_count_; j++) {
      LOG_INFO("4");
      if (strcmp(field_name_cmp.c_str(), multi_field_[j].c_str()) == 0) { //记录属性相等个数
        LOG_INFO("5");
        equal_field_count++;
        break;
      }
    }
  }
  LOG_INFO("equal_field_count = %d", equal_field_count);
  if (equal_field_count == attribute_count_) {
    return 1;
  }
  else {
    return 0;
  }
}
//20211107:e

void IndexMeta::desc(std::ostream &os) const {
  os << "index name=" << name_
      << ", field=" << field_;
}

int IndexMeta::get_is_unique_index() {
  int i = is_unique_index_;
  return i;
}