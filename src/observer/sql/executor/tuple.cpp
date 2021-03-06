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
// Created by Wangyunlai on 2021/5/14.
//

#include "sql/executor/tuple.h"
#include "storage/common/table.h"
#include "common/log/log.h"
#include <algorithm> //add zjx[order by]b:20211108

Tuple::Tuple(const Tuple &other) {
  LOG_PANIC("Copy constructor of tuple is not supported");
  exit(1);
}

Tuple::Tuple(Tuple &&other) noexcept : values_(std::move(other.values_)) {
}

Tuple & Tuple::operator=(Tuple &&other) noexcept {
  if (&other == this) {
    return *this;
  }

  values_.clear();
  values_.swap(other.values_);
  return *this;
}

Tuple::~Tuple() {
}

// add (Value && value)
void Tuple::add(TupleValue *value) {
  values_.emplace_back(value);
}
void Tuple::add(const std::shared_ptr<TupleValue> &other) {
  values_.emplace_back(other);
}

void Tuple::add(int value) {
  add(new IntValue(value));
}

void Tuple::add(float value) {
  add(new FloatValue(value));
}

void Tuple::add(const char *s, int len) {
  add(new StringValue(s, len));
}

void Tuple::add(const Tuple &tuple) { //add zjx[select]b:20211028
  for(int i = 0; i < tuple.size(); i++){
	values_.emplace_back(std::move(tuple.get_pointer(i)));
   }
}

////////////////////////////////////////////////////////////////////////////////

std::string TupleField::to_string() const {
  return std::string(table_name_) + "." + field_name_ + std::to_string(type_);
}

////////////////////////////////////////////////////////////////////////////////
void TupleSchema::from_table(const Table *table, TupleSchema &schema) {
  const char *table_name = table->name();
  const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num();
  for (int i = 0; i < field_num; i++) {
    const FieldMeta *field_meta = table_meta.field(i);
    if (field_meta->visible()) {
      schema.add(field_meta->type(), table_name, field_meta->name());
    }
  }
}

void TupleSchema::add(AttrType type, const char *table_name, const char *field_name) {
  fields_.emplace_back(type, table_name, field_name);
}

void TupleSchema::add_if_not_exists(AttrType type, const char *table_name, const char *field_name) {
  for (const auto &field: fields_) {
    if (0 == strcmp(field.table_name(), table_name) &&
        0 == strcmp(field.field_name(), field_name)) {
      return;
    }
  }

  add(type, table_name, field_name);
}

void TupleSchema::append(const TupleSchema &other) {
  fields_.reserve(fields_.size() + other.fields_.size());
  for (const auto &field: other.fields_) {
    fields_.emplace_back(field);
  }
}

int TupleSchema::index_of_field(const char *table_name, const char *field_name) const {
  LOG_INFO("Get into index_of_field!!!");
  const int size = fields_.size();
  for (int i = 0; i < size; i++) {
    LOG_INFO("Looping in index_of_field!!!");
    const TupleField &field = fields_[i];
    if (0 == strcmp(field.table_name(), table_name) && 0 == strcmp(field.field_name(), field_name)) {
      LOG_INFO("strike in filed!!!!!!");
      return i;
    }
  }
  LOG_INFO("Out of index_of_field!!!");
  return -1;
}

void TupleSchema::print(std::ostream &os) const {
  if (fields_.empty()) {
    os << "No schema";
    return;
  }

  // ???????????????????????????????????????
  std::set<std::string> table_names;
  for (const auto &field: fields_) {
    table_names.insert(field.table_name());
  }

  for (std::vector<TupleField>::const_iterator iter = fields_.begin(), end = --fields_.end();
       iter != end; ++iter) {
    if (table_names.size() > 1) {
      // add szj [select aggregate support]20211106
      if (strlen(iter->field_name()) > 3) {
        char tmp[4];
        strncpy(tmp, iter->field_name(), 3);
        LOG_INFO("here is third_tmp_str %s", tmp);
        if (strcmp(tmp, "AVG") == 0 || strcmp(tmp, "MAX") == 0 || strcmp(tmp, "SUM") == 0 || strcmp(tmp, "COU") == 0) {
        } else {
          os << iter->table_name() << ".";
        }
      }
      // add:e
      // os << iter->table_name() << ".";
    }
    os << iter->field_name() << " | ";
  }

  if (table_names.size() > 1) {
    // add szj [select aggregate support]20211106
    if (strlen(fields_.back().field_name()) > 3) {
      char tmp[4];
      strncpy(tmp, fields_.back().field_name(), 3);
      LOG_INFO("here is third_tmp_str %s", tmp);
      if (strcmp(tmp, "AVG") == 0 || strcmp(tmp, "MAX") == 0 || strcmp(tmp, "SUM") == 0 || strcmp(tmp, "COU") == 0) {
      } else {
        os << fields_.back().table_name() << ".";
      }
    }
    // add:e 
    // os << fields_.back().table_name() << ".";
  }
  os << fields_.back().field_name() << std::endl;
}

/////////////////////////////////////////////////////////////////////////////
TupleSet::TupleSet(TupleSet &&other) : tuples_(std::move(other.tuples_)), schema_(other.schema_){
  other.schema_.clear();
}

TupleSet &TupleSet::operator=(TupleSet &&other) {
  if (this == &other) {
    return *this;
  }

  schema_.clear();
  schema_.append(other.schema_);
  other.schema_.clear();

  tuples_.clear();
  tuples_.swap(other.tuples_);
  return *this;
}

void TupleSet::add(Tuple &&tuple) {
  tuples_.emplace_back(std::move(tuple));
}

void TupleSet::add(const Tuple &tuple) { //add zjx[select]b:20211028
  tuples_.emplace_back(std::move(tuple));
}

void TupleSet::clear() {
  tuples_.clear();
  schema_.clear();
}

//add zjx[order by]b:20211103
int index_;//??????????????????????????????????????????

bool compare_asc(const Tuple &a, const Tuple &b){
  return a.get(index_).compare(b.get(index_)) <= 0?true:false;
}

bool compare_desc(const Tuple &a, const Tuple &b){
  return a.get(index_).compare(b.get(index_)) >= 0?true:false;
}

/**
 * @name: sort
 * @test: 
 * @msg: ?????????
 * @param {int} index_f
 * @param {int} order_type_f
 * @return {*}
 */
void TupleSet::sort(int index_f, int order_type_f){
  index_ = index_f;
  if(order_type_f == 1){
    std::sort( tuples_.begin(), tuples_.end(), compare_asc);
  } else {
    std::sort( tuples_.begin(), tuples_.end(), compare_desc);
  }
}

/**
 * @name: double_sort
 * @test: 
 * @msg: ????????????
 * @param {int} index_f
 * @param {int} index_s
 * @param {int} order_type_f
 * @param {int} order_type_s
 * @return {*}
 */
void TupleSet::double_sort(int index_f, int index_s, int order_type_f, int order_type_s){
  index_ = index_f;
  if (order_type_f == 1) {
    std::sort( tuples_.begin(), tuples_.end(), compare_asc);
  } else {
    std::sort( tuples_.begin(), tuples_.end(), compare_desc);
  }

  if( index_f == index_s && order_type_f == order_type_s ) {
    return;
  } else {
    index_ = index_s;
    int tmp_count = 1;
    std::vector<Tuple>::iterator iter = tuples_.begin();
    for( int i = 1;i < tuples_.size(); i++ ) {
	    tmp_count++;
      if ((tuples_.at(i).get(index_f)).compare(tuples_.at(i-1).get(index_f)) != 0) {
        if(tmp_count > 1) {
          if (order_type_s == 1) {
            std::sort( iter+i-tmp_count+1, iter+i, compare_asc);
          } else {
            std::sort( iter+i-tmp_count+1, iter+i, compare_desc);
             }
          } 
        tmp_count = 1;
       }
    }
    if ( order_type_s == 1 ) {
      std::sort( tuples_.end()-tmp_count, tuples_.end(), compare_asc);
    } else {
      std::sort( tuples_.end()-tmp_count, tuples_.end(), compare_desc);
    }
  } 
}
//e:20211103

void TupleSet::print(std::ostream &os) const {
  if (schema_.fields().empty()) {
    LOG_WARN("Got empty schema");
    return;
  }

  schema_.print(os);

  for (const Tuple &item : tuples_) {
    const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
    for (std::vector<std::shared_ptr<TupleValue>>::const_iterator iter = values.begin(), end = --values.end();
          iter != end; ++iter) {
      (*iter)->to_string(os);
      os << " | ";
    }
    values.back()->to_string(os);
    os << std::endl;
  }
}

void TupleSet::set_schema(const TupleSchema &schema) {
  schema_ = schema;
}

const TupleSchema &TupleSet::get_schema() const {
  return schema_;
}

bool TupleSet::is_empty() const {
  return tuples_.empty();
}

int TupleSet::size() const {
  return tuples_.size();
}

const Tuple &TupleSet::get(int index) const {
  return tuples_[index];
}

const std::vector<Tuple> &TupleSet::tuples() const {
  return tuples_;
}

/////////////////////////////////////////////////////////////////////////////
TupleRecordConverter::TupleRecordConverter(Table *table, TupleSet &tuple_set) :
      table_(table), tuple_set_(tuple_set){
}

void TupleRecordConverter::add_record(const char *record) {
  const TupleSchema &schema = tuple_set_.schema();
  Tuple tuple;
  const TableMeta &table_meta = table_->table_meta();
  for (const TupleField &field : schema.fields()) {
    const FieldMeta *field_meta = table_meta.field(field.field_name());
    assert(field_meta != nullptr);
    switch (field_meta->type()) {
      case INTS: {
        int value = *(int*)(record + field_meta->offset());
        tuple.add(value);
      }
      break;
      case FLOATS: {
        float value = *(float *)(record + field_meta->offset());
        tuple.add(value);
      }
        break;
      case DATES: //add zjx[date]b:20211027
      case CHARS: {
        const char *s = record + field_meta->offset();  // ????????????Cstring?????????
        tuple.add(s, strlen(s));
      }
      break;
      default: {
        LOG_PANIC("Unsupported field type. type=%d", field_meta->type());
      }
    }
  }

  tuple_set_.add(std::move(tuple));
}
