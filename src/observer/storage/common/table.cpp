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
// Created by Wangyunlai on 2021/5/13.
//

#include <limits.h>
#include <string.h>
#include <algorithm>

#include "storage/common/table.h"
#include "storage/common/table_meta.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/default/disk_buffer_pool.h"
#include "storage/common/record_manager.h"
#include "storage/common/condition_filter.h"
#include "storage/common/meta_util.h"
#include "storage/common/index.h"
#include "storage/common/bplus_tree_index.h"
#include "storage/trx/trx.h"

// add szj [insert multi values]20211029:b
#include <vector>
// add:e

Table::Table() : 
    data_buffer_pool_(nullptr),
    file_id_(-1),
    record_handler_(nullptr) {
}

Table::~Table() {
  delete record_handler_;
  record_handler_ = nullptr;

  if (data_buffer_pool_ != nullptr && file_id_ >= 0) {
    data_buffer_pool_->close_file(file_id_);
    data_buffer_pool_ = nullptr;
  }

  LOG_INFO("Table has been closed: %s", name());
}

RC Table::create(const char *path, const char *name, const char *base_dir, int attribute_count, const AttrInfo attributes[]) {

  if (nullptr == name || common::is_blank(name)) {
    LOG_WARN("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }
  LOG_INFO("Begin to create table %s:%s", base_dir, name);

  if (attribute_count <= 0 || nullptr == attributes) {
    LOG_WARN("Invalid arguments. table_name=%s, attribute_count=%d, attributes=%p",
        name, attribute_count, attributes);
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;

  // ?????? table_name.table???????????????????????????
  // ?????????????????????????????????

  int fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (-1 == fd) {
    if (EEXIST == errno) {
      LOG_ERROR("Failed to create table file, it has been created. %s, EEXIST, %s",
                path, strerror(errno));
      return RC::SCHEMA_TABLE_EXIST;
    }
    LOG_ERROR("Create table file failed. filename=%s, errmsg=%d:%s", 
       path, errno, strerror(errno));
    return RC::IOERR;
  }

  close(fd);

  // ????????????
  if ((rc = table_meta_.init(name, attribute_count, attributes)) != RC::SUCCESS) {
    LOG_ERROR("Failed to init table meta. name:%s, ret:%d", name, rc);
    return rc; // delete table file
  }

  std::fstream fs;
  fs.open(path, std::ios_base::out | std::ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", path, strerror(errno));
    return RC::IOERR;
  }

  // ???????????????????????????
  table_meta_.serialize(fs);
  fs.close();

  std::string data_file = std::string(base_dir) + "/" + name + TABLE_DATA_SUFFIX;
  data_buffer_pool_ = theGlobalDiskBufferPool();
  rc = data_buffer_pool_->create_file(data_file.c_str());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create disk buffer pool of data file. file name=%s", data_file.c_str());
    return rc;
  }

  rc = init_record_handler(base_dir);

  base_dir_ = base_dir;
  
  //add bzb [LRU+LFU] 20211031:b
  LOG_INFO("Successfully create table %s:%s", base_dir, name);
  BPFileHandle *file_handle;
  rc = data_buffer_pool_->get_FileHandle(file_id_, file_handle);
  LOG_INFO("file_id_ = %d", file_id_);
  LOG_INFO("pin_count1 =  %d", file_handle->hdr_frame->pin_count);
  file_handle->hdr_frame->pin_count--;
  //file_handle->hdr_frame->open = 0;
  file_handle->hdr_frame->LFU_count++;
  LOG_INFO("pin_count2 =  %d", file_handle->hdr_frame->pin_count);
  //20211031:e

  return rc;
}

//add bzb [drop table] 20211022:b
RC Table::drop(const char *path, const char *name, const char *base_dir) {

  if (nullptr == name || common::is_blank(name)) {
    LOG_WARN("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }
  LOG_INFO("Begin to drop table %s:%s", base_dir, name);

  RC rc = RC::SUCCESS;

  data_buffer_pool_->force_all_pages(file_id_);

  table_meta_.drop_all_index_file(base_dir_, std::string(name));

  std::string data_file = std::string(base_dir) + "/" + name + TABLE_DATA_SUFFIX;
  std::string meta_file = std::string(base_dir) + "/" + name + TABLE_META_SUFFIX;
  //std::string index_file = std::string(base_dir) + "/" + name + TABLE_META_SUFFIX;

  std::remove(data_file.c_str());
  std::remove(meta_file.c_str());

  LOG_INFO("Successfully drop table %s:%s", base_dir, name);
  return rc;
}
//20211022:e

RC Table::open(const char *meta_file, const char *base_dir) {
  // ?????????????????????
  std::fstream fs;
  std::string meta_file_path = std::string(base_dir) + "/" + meta_file;
  fs.open(meta_file_path, std::ios_base::in | std::ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open meta file for read. file name=%s, errmsg=%s", meta_file, strerror(errno));
    return RC::IOERR;
  }
  if (table_meta_.deserialize(fs) < 0) {
    LOG_ERROR("Failed to deserialize table meta. file name=%s", meta_file);
    return RC::GENERIC_ERROR;
  }
  fs.close();

  // ??????????????????
  RC rc = init_record_handler(base_dir);

  base_dir_ = base_dir;

  const int index_num = table_meta_.index_num();
  for (int i = 0; i < index_num; i++) {
    const IndexMeta *index_meta = table_meta_.index(i);
    const FieldMeta *field_meta = table_meta_.field(index_meta->field());
    if (field_meta == nullptr) {
      LOG_PANIC("Found invalid index meta info which has a non-exists field. table=%s, index=%s, field=%s",
                name(), index_meta->name(), index_meta->field());
      return RC::GENERIC_ERROR;
    }

    BplusTreeIndex *index = new BplusTreeIndex();
    std::string index_file = index_data_file(base_dir, name(), index_meta->name());
    rc = index->open(index_file.c_str(), *index_meta, *field_meta);
    if (rc != RC::SUCCESS) {
      delete index;
      LOG_ERROR("Failed to open index. table=%s, index=%s, file=%s, rc=%d:%s",
                name(), index_meta->name(), index_file.c_str(), rc, strrc(rc));
      return rc;
    }
    indexes_.push_back(index);
  }
  return rc;
}

RC Table::commit_insert(Trx *trx, const RID &rid) {
  Record record;
  RC rc = record_handler_->get_record(&rid, &record);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return trx->commit_insert(this, record);
}

RC Table::rollback_insert(Trx *trx, const RID &rid) {

  Record record;
  RC rc = record_handler_->get_record(&rid, &record);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // remove all indexes
  rc = delete_entry_of_indexes(record.data, rid, false);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to delete indexes of record(rid=%d.%d) while rollback insert, rc=%d:%s",
              rid.page_num, rid.slot_num, rc, strrc(rc));
  } else {
    rc = record_handler_->delete_record(&rid);
  }
  return rc;
}

RC Table::insert_record(Trx *trx, Record *record) {
  RC rc = RC::SUCCESS;

  if (trx != nullptr) {
    trx->init_trx_info(this, *record);
  }
  rc = record_handler_->insert_record(record->data, table_meta_.record_size(), &record->rid);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%d:%s", table_meta_.name(), rc, strrc(rc));
    return rc;
  }

  if (trx != nullptr) {
    rc = trx->insert_record(this, record);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to log operation(insertion) to trx");

      RC rc2 = record_handler_->delete_record(&record->rid);
      if (rc2 != RC::SUCCESS) {
        LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                  name(), rc2, strrc(rc2));
      }
      return rc;
    }
  }

  rc = insert_entry_of_indexes(record->data, record->rid);
  if (rc != RC::SUCCESS) {
    RC rc2 = delete_entry_of_indexes(record->data, record->rid, true);
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
    rc2 = record_handler_->delete_record(&record->rid);
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
    return rc;
  }
  return rc;
}
// add szj [insert multi values]20211029:b
RC Table::insert_record(Trx *trx, int value_num, const Value *values, int record_num) {
  if (value_num <= 0 || nullptr == values ) {
    LOG_ERROR("Invalid argument. value num=%d, values=%p", value_num, values);
    return RC::INVALID_ARGUMENT;
  }
  // ????????????????????????
  // char *record_data;
  RC rc = RC::SUCCESS;
  // std::vector<char *> record_data(record_num);
  std::vector<char *> record_data;
  if (record_num == 1) {
    LOG_INFO("Record_num=1, get into first branch!!!!!!");
    rc = make_record(value_num, values, record_data);
  } else {
    LOG_INFO("Record_num=%d, get into first branch!!!!!!", record_num);
    rc = make_record(value_num, values, record_data, record_num);
  }
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create a record. rc=%d:%s", rc, strrc(rc));
    return rc;
  }
  LOG_INFO("Fucking out or Not?");
  //char *record_data;
  
  for (int i = 0; i < record_num; i++) {
    Record record;
    record.data = record_data[i];
    LOG_INFO("%s Fucking out?", record_data[i]);
    //memcpy(record.data, record_data[i], strlen(record_data[i]));
    // record.data = record_data[i];
    rc = insert_record(trx, &record);
  }
  // record.valid = true;
  // delete[] record_data;
  return rc;
}
// add:e

const char *Table::name() const {
  return table_meta_.name();
}

const TableMeta &Table::table_meta() const {
  return table_meta_;
}

RC Table::make_record(int value_num, const Value *values, std::vector<char *> &record_out) { // add szj [insert multi values]20211029
  // ??????????????????????????????
  LOG_INFO("Get into the fucking make record!!");
  if (value_num + table_meta_.sys_field_num() != table_meta_.field_num()) {
    return RC::SCHEMA_FIELD_MISSING;
  }

  const int normal_field_start_index = table_meta_.sys_field_num();
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field = table_meta_.field(i + normal_field_start_index);
    const Value &value = values[i];

    //mod zjx[dates]b:20211031
    if (field->type() != value.type) {
      if(field->type() == DATES && value.type == CHARS){
        continue;
      }else{
        LOG_ERROR("Invalid value type. field name=%s, type=%d, but given=%d",
        field->name(), field->type(), value.type);
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }    
    }
  }
  LOG_INFO("Start to Copy Record String!!!!!!");
  // ????????????????????????
  int record_size = table_meta_.record_size();
  char *record = new char [record_size];
  int sum = 1;
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field = table_meta_.field(i + normal_field_start_index);
    const Value &value = values[i];
    LOG_INFO("%d value is %s?????", sum, value.data);
    std::cout << value.data << std::endl;
    LOG_INFO("!!!!!!!!!!! %d", *((int*)value.data));
    //mod zjx[dates]b:20211031
    if (field->type() == DATES && value.type == CHARS){
      if (!check_date((char*)value.data) ) return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      refactor_date((char*)value.data);
    }
    //e:20211031
    memcpy(record + field->offset(), value.data, field->len());
  }
  LOG_INFO("Succeed to Copy Record String!!!!!!");
  LOG_INFO("Copy value is %s", record);
  // printf("%s\n",record);
  std::cout << record << std::endl;
  // ??????????????????????????????????????????
  record_out.push_back(record);
  //strcpy(record_out[0], record);
  LOG_INFO("Are you fuckingg in ?");
  // LOG_INFO("%s?????", record_out[0]);
  // std::cout << record_out[0] << std::endl;
  return RC::SUCCESS;
}

// add szj [insert multi values]20211029:b
RC Table::make_record(int value_num, const Value *values, std::vector<char *> &record_out, int record_num) {
  // ??????????????????????????????
  if (value_num + record_num * table_meta_.sys_field_num() != record_num * table_meta_.field_num()) {
    return RC::SCHEMA_FIELD_MISSING;
  }
  LOG_INFO("Pass num check Woohuu!!!!!!");
  int single_value_num = value_num / record_num;
  const int normal_field_start_index = table_meta_.sys_field_num();
  for (int j = 0; j < record_num; j++) {
    int index = j * single_value_num;
    for (int i = 0; i < single_value_num; i++) {
      const FieldMeta *field = table_meta_.field(i + normal_field_start_index);
      const Value &value = values[index++];
      LOG_INFO("field type %d", field->type());
      LOG_INFO("value type %d", value.type);
      //mod zjx[dates]b:20211031
      if (field->type() != value.type) {
        if(field->type() == DATES && value.type == CHARS){
          continue;
        }else{
          LOG_ERROR("Invalid value type. field name=%s, type=%d, but given=%d",
          field->name(), field->type(), value.type);
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }    
      }
    }
  }

  LOG_INFO("Pass attr type check Woohuu!!!!!!");
  // ????????????????????????
  int record_size = table_meta_.record_size();
  for (int j = 0; j < record_num; j++) {
    char *record = new char [record_size];
    int index2 = j * single_value_num;
    for (int i = 0; i < single_value_num; i++) {
      const FieldMeta *field = table_meta_.field(i + normal_field_start_index);
      const Value &value = values[index2++];
      //mod zjx[dates]b:20211031
      if (field->type() == DATES && value.type == CHARS){
        if (!check_date((char*)value.data) ) return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        refactor_date((char*)value.data);
      }
      //e:20211031
      memcpy(record + field->offset(), value.data, field->len());
    }
    LOG_INFO("Combine the whole tuple row!!!!!!");
    // record_out[j] = record;
    // strcpy(record_out[j], record);
    record_out.push_back(record);
    LOG_INFO("Transport the whole tuple row!!!!!!");
  }
  
  return RC::SUCCESS;
}
// add:e

RC Table::init_record_handler(const char *base_dir) {
  std::string data_file = std::string(base_dir) + "/" + table_meta_.name() + TABLE_DATA_SUFFIX;
  if (nullptr == data_buffer_pool_) {
    data_buffer_pool_ = theGlobalDiskBufferPool();
  }

  int data_buffer_pool_file_id;
  RC rc = data_buffer_pool_->open_file(data_file.c_str(), &data_buffer_pool_file_id);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open disk buffer pool for file:%s. rc=%d:%s",
              data_file.c_str(), rc, strrc(rc));
    return rc;
  }

  record_handler_ = new RecordFileHandler();
  rc = record_handler_->init(*data_buffer_pool_, data_buffer_pool_file_id);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to init record handler. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  file_id_ = data_buffer_pool_file_id;
  return rc;
}

/**
 * ????????????Record???????????????????????????
 */
class RecordReaderScanAdapter {
public:
  explicit RecordReaderScanAdapter(void (*record_reader)(const char *data, void *context), void *context)
      : record_reader_(record_reader), context_(context){
  }

  void consume(const Record *record) {
    record_reader_(record->data, context_);
  }
private:
  void (*record_reader_)(const char *, void *);
  void *context_;
};
static RC scan_record_reader_adapter(Record *record, void *context) {
  RecordReaderScanAdapter &adapter = *(RecordReaderScanAdapter *)context;
  adapter.consume(record);
  return RC::SUCCESS;
}

RC Table::scan_record(Trx *trx, ConditionFilter *filter, int limit, void *context, void (*record_reader)(const char *data, void *context)) {
  RecordReaderScanAdapter adapter(record_reader, context);
  return scan_record(trx, filter, limit, (void *)&adapter, scan_record_reader_adapter);
}

RC Table::scan_record(Trx *trx, ConditionFilter *filter, int limit, void *context, RC (*record_reader)(Record *record, void *context)) {
  if (nullptr == record_reader) {
    return RC::INVALID_ARGUMENT;
  }

  if (0 == limit) {
    return RC::SUCCESS;
  }

  if (limit < 0) {
    limit = INT_MAX;
  }

  IndexScanner *index_scanner = find_index_for_scan(filter);
  if (index_scanner != nullptr) {
    return scan_record_by_index(trx, index_scanner, filter, limit, context, record_reader);
  }

  RC rc = RC::SUCCESS;
  RecordFileScanner scanner;
  rc = scanner.open_scan(*data_buffer_pool_, file_id_, filter);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. file id=%d. rc=%d:%s", file_id_, rc, strrc(rc));
    return rc;
  }

  int record_count = 0;
  Record record;
  rc = scanner.get_first_record(&record);
  for ( ; RC::SUCCESS == rc && record_count < limit; rc = scanner.get_next_record(&record)) {
    if (trx == nullptr || trx->is_visible(this, &record)) {
      rc = record_reader(&record, context);
      if (rc != RC::SUCCESS) {
        break;
      }
      record_count++;
    }
  }

  if (RC::RECORD_EOF == rc) {
    rc = RC::SUCCESS;
  } else {
    LOG_ERROR("failed to scan record. file id=%d, rc=%d:%s", file_id_, rc, strrc(rc));
  }
  scanner.close_scan();
  // add szj [update sql execute]20211024:b
  if (record_count == 0) return RECORD_RECORD_NOT_EXIST;
  // add:e
  return rc;
}

RC Table::scan_record_by_index(Trx *trx, IndexScanner *scanner, ConditionFilter *filter, int limit, void *context,
                               RC (*record_reader)(Record *, void *)) {
  RC rc = RC::SUCCESS;
  RID rid;
  Record record;
  int record_count = 0;
  while (record_count < limit) {
    rc = scanner->next_entry(&rid);
    if (rc != RC::SUCCESS) {
      if (RC::RECORD_EOF == rc) {
        rc = RC::SUCCESS;
        break;
      }
      LOG_ERROR("Failed to scan table by index. rc=%d:%s", rc, strrc(rc));
      break;
    }

    rc = record_handler_->get_record(&rid, &record);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to fetch record of rid=%d:%d, rc=%d:%s", rid.page_num, rid.slot_num, rc, strrc(rc));
      break;
    }

    if ((trx == nullptr || trx->is_visible(this, &record)) && (filter == nullptr || filter->filter(record))) {
      rc = record_reader(&record, context);
      if (rc != RC::SUCCESS) {
        LOG_TRACE("Record reader break the table scanning. rc=%d:%s", rc, strrc(rc));
        break;
      }
    }

    record_count++;
  }

  scanner->destroy();
  return rc;
}

class IndexInserter {
public:
  explicit IndexInserter(Index *index) : index_(index) {
  }

  RC insert_index(const Record *record) {
    return index_->insert_entry(record->data, &record->rid);
  }
private:
  Index * index_;
};

static RC insert_index_record_reader_adapter(Record *record, void *context) {
  IndexInserter &inserter = *(IndexInserter *)context;
  return inserter.insert_index(record);
}

RC Table::create_index(Trx *trx, const char *index_name, const char *attribute_name) {
  if (index_name == nullptr || common::is_blank(index_name) ||
      attribute_name == nullptr || common::is_blank(attribute_name)) {
    return RC::INVALID_ARGUMENT;
  }
  if (table_meta_.index(index_name) != nullptr ||
      table_meta_.find_index_by_field((attribute_name))) {
    return RC::SCHEMA_INDEX_EXIST;
  }

  const FieldMeta *field_meta = table_meta_.field(attribute_name);
  if (!field_meta) {
    return RC::SCHEMA_FIELD_MISSING;
  }

  IndexMeta new_index_meta;
  RC rc = new_index_meta.init(index_name, *field_meta);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // ????????????????????????
  BplusTreeIndex *index = new BplusTreeIndex();
  std::string index_file = index_data_file(base_dir_.c_str(), name(), index_name);
  rc = index->create(index_file.c_str(), new_index_meta, *field_meta);
  if (rc != RC::SUCCESS) {
    delete index;
    LOG_ERROR("Failed to create bplus tree index. file name=%s, rc=%d:%s", index_file.c_str(), rc, strrc(rc));
    return rc;
  }

  // ????????????????????????????????????????????????
  IndexInserter index_inserter(index);
  rc = scan_record(trx, nullptr, -1, &index_inserter, insert_index_record_reader_adapter);
  if (rc != RC::SUCCESS) {
    // rollback
    delete index;
    LOG_ERROR("Failed to insert index to all records. table=%s, rc=%d:%s", name(), rc, strrc(rc));
    return rc;
  }
  indexes_.push_back(index);

  TableMeta new_table_meta(table_meta_);
  rc = new_table_meta.add_index(new_index_meta);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to add index (%s) on table (%s). error=%d:%s", index_name, name(), rc, strrc(rc));
    return rc;
  }
  // ???????????????????????????
  std::string tmp_file = table_meta_file(base_dir_.c_str(), name()) + ".tmp";
  std::fstream fs;
  fs.open(tmp_file, std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", tmp_file.c_str(), strerror(errno));
    return RC::IOERR; // ?????????????????????????????????????????????
  }
  if (new_table_meta.serialize(fs) < 0) {
    LOG_ERROR("Failed to dump new table meta to file: %s. sys err=%d:%s", tmp_file.c_str(), errno, strerror(errno));
    return RC::IOERR;
  }
  fs.close();

  // ???????????????????????????
  std::string meta_file = table_meta_file(base_dir_.c_str(), name());
  int ret = rename(tmp_file.c_str(), meta_file.c_str());
  if (ret != 0) {
    LOG_ERROR("Failed to rename tmp meta file (%s) to normal meta file (%s) while creating index (%s) on table (%s). " \
              "system error=%d:%s", tmp_file.c_str(), meta_file.c_str(), index_name, name(), errno, strerror(errno));
    return RC::IOERR;
  }

  table_meta_.swap(new_table_meta);

  LOG_INFO("add a new index (%s) on the table (%s)", index_name, name());

  return rc;
}


// add szj [update sql execute]20211024:b
class RecordUpdater {
public:
  RecordUpdater(Table &table, Trx *trx, const char *attribute_name, const Value *value) : table_(table), trx_(trx), attribute_name_(attribute_name), value_(value) {
  }

  RC update_record(Record *record) {
    // ??????tuple????????????????????????!!

    // ??????????????????????????????
    //if (value_num + table_meta_.sys_field_num() != table_meta_.field_num()) {
    //    return RC::SCHEMA_FIELD_MISSING;
    //}

    // ????????????????????????,????????????????????????????????????
    //LOG_INFO("update attr name is %s", attribute_name_);
    int record_size = table_.table_meta_.record_size();
    // char *record_replaced = new char [record_size];
    // int is_update_success = 0;
    const int value_num = table_.table_meta_.field_num() - table_.table_meta_.sys_field_num();
    const int normal_field_start_index = table_.table_meta_.sys_field_num();
    for (int i = 0; i < value_num; i++) {
      const FieldMeta *field = table_.table_meta_.field(i + normal_field_start_index);
      // ?????????????????????????????????slot??????
      // string.c_str() == const char ** 
      //LOG_INFO("Give me the sttr name %s!", p, field->name());
      //LOG_INFO("print the if result %d", (field->name() == attribute_name_));
      // if (field->name() == attribute_name_) {
      // ?????????????????????????????????????????????????????????
      if (!strcmp(field->name(), attribute_name_)) {
          memcpy(record->data + field->offset(), value_->data, field->len());
          // is_update_success = 1;
      }
    }

    // ????????????????????????????????????tuple???????????????
    // if (is_update_success = 0)  return RECORD_RECORD_NOT_EXIST;
    RC rc = RC::SUCCESS;
    rc = table_.update_record(trx_, record);
    if (rc == RC::SUCCESS) {
      update_count_++;
    }
    return rc;
  }

  int updated_count() const {
    return update_count_;
  }

private:
  Table & table_;
  Trx *trx_;
  int update_count_ = 0;
  const char *attribute_name_;
  const Value *value_;
};
// add:e

class RecordDeleter {
public:
  RecordDeleter(Table &table, Trx *trx) : table_(table), trx_(trx) {
  }

  RC delete_record(Record *record) {
    RC rc = RC::SUCCESS;
    rc = table_.delete_record(trx_, record);
    if (rc == RC::SUCCESS) {
      deleted_count_++;
    }
    return rc;
  }

  int deleted_count() const {
    return deleted_count_;
  }

private:
  Table & table_;
  Trx *trx_;
  int deleted_count_ = 0;
};

// add szj [update sql execute]20211024:b
static RC record_reader_update_adapter(Record *record, void *context) {
  RecordUpdater &record_updater = *(RecordUpdater *)context;
  return record_updater.update_record(record);
}
// add:e

static RC record_reader_delete_adapter(Record *record, void *context) {
  RecordDeleter &record_deleter = *(RecordDeleter *)context;
  return record_deleter.delete_record(record);
}

// add szj [update sql execute]20211024:b
// ????????????????????????
RC Table::update_record(Trx *trx, const char *attribute_name, const Value *value, ConditionFilter *filter, int *updated_count) {
  RecordUpdater updater(*this, trx, attribute_name, value);
  RC rc = scan_record(trx, filter, -1, &updater, record_reader_update_adapter);
  if (updated_count != nullptr) {
    *updated_count = updater.updated_count();
  }
  return rc;
  // return RC::GENERIC_ERROR;
}
// add:e

RC Table::delete_record(Trx *trx, ConditionFilter *filter, int *deleted_count) {
  RecordDeleter deleter(*this, trx);
  RC rc = scan_record(trx, filter, -1, &deleter, record_reader_delete_adapter);
  if (deleted_count != nullptr) {
    *deleted_count = deleter.deleted_count();
  }
  return rc;
}

// add szj [update sql execute]20211024:b
RC Table::update_record(Trx *trx, Record *record) {
  RC rc = RC::SUCCESS;
  // trx???????????????
  /*
  if (trx != nullptr) {
    rc = trx->delete_record(this, record);
  } else {
    rc = delete_entry_of_indexes(record->data, record->rid, false);// ???????????? refer to commit_delete
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to delete indexes of record (rid=%d.%d). rc=%d:%s",
                record->rid.page_num, record->rid.slot_num, rc, strrc(rc));
    } else {
      rc = record_handler_->update_record(&record);
    }
  }
  */
  rc = record_handler_->update_record(record);
  return rc;
}
// add:e

RC Table::delete_record(Trx *trx, Record *record) {
  RC rc = RC::SUCCESS;
  if (trx != nullptr) {
    rc = trx->delete_record(this, record);
  } else {
    rc = delete_entry_of_indexes(record->data, record->rid, false);// ???????????? refer to commit_delete
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to delete indexes of record (rid=%d.%d). rc=%d:%s",
                record->rid.page_num, record->rid.slot_num, rc, strrc(rc));
    } else {
      rc = record_handler_->delete_record(&record->rid);
    }
  }
  return rc;
}

RC Table::commit_delete(Trx *trx, const RID &rid) {
  RC rc = RC::SUCCESS;
  Record record;
  rc = record_handler_->get_record(&rid, &record);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  rc = delete_entry_of_indexes(record.data, record.rid, false);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to delete indexes of record(rid=%d.%d). rc=%d:%s",
              rid.page_num, rid.slot_num, rc, strrc(rc));// panic?
  }

  rc = record_handler_->delete_record(&rid);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return rc;
}

RC Table::rollback_delete(Trx *trx, const RID &rid) {
  RC rc = RC::SUCCESS;
  Record record;
  rc = record_handler_->get_record(&rid, &record);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return trx->rollback_delete(this, record); // update record in place
}

RC Table::insert_entry_of_indexes(const char *record, const RID &rid) {
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->insert_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      break;
    }
  }
  return rc;
}

RC Table::delete_entry_of_indexes(const char *record, const RID &rid, bool error_on_not_exists) {
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->delete_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      if (rc != RC::RECORD_INVALID_KEY || !error_on_not_exists) {
        break;
      }
    }
  }
  return rc;
}

Index *Table::find_index(const char *index_name) const {
  for (Index *index: indexes_) {
    if (0 == strcmp(index->index_meta().name(), index_name)) {
      return index;
    }
  }
  return nullptr;
}

IndexScanner *Table::find_index_for_scan(const DefaultConditionFilter &filter) {
  const ConDesc *field_cond_desc = nullptr;
  const ConDesc *value_cond_desc = nullptr;
  if (filter.left().is_attr && !filter.right().is_attr) {
    field_cond_desc = &filter.left();
    value_cond_desc = &filter.right();
  } else if (filter.right().is_attr && !filter.left().is_attr) {
    field_cond_desc = &filter.right();
    value_cond_desc = &filter.left();
  }
  if (field_cond_desc == nullptr || value_cond_desc == nullptr) {
    return nullptr;
  }

  const FieldMeta *field_meta = table_meta_.find_field_by_offset(field_cond_desc->attr_offset);
  if (nullptr == field_meta) {
    LOG_PANIC("Cannot find field by offset %d. table=%s",
              field_cond_desc->attr_offset, name());
    return nullptr;
  }

  const IndexMeta *index_meta = table_meta_.find_index_by_field(field_meta->name());
  if (nullptr == index_meta) {
    return nullptr;
  }

  Index *index = find_index(index_meta->name());
  if (nullptr == index) {
    return nullptr;
  }

  return index->create_scanner(filter.comp_op(), (const char *)value_cond_desc->value);
}

IndexScanner *Table::find_index_for_scan(const ConditionFilter *filter) {
  if (nullptr == filter) {
    return nullptr;
  }

  // remove dynamic_cast
  const DefaultConditionFilter *default_condition_filter = dynamic_cast<const DefaultConditionFilter *>(filter);
  if (default_condition_filter != nullptr) {
    return find_index_for_scan(*default_condition_filter);
  }

  const CompositeConditionFilter *composite_condition_filter = dynamic_cast<const CompositeConditionFilter *>(filter);
  if (composite_condition_filter != nullptr) {
    int filter_num = composite_condition_filter->filter_num();
    for (int i = 0; i < filter_num; i++) {
      IndexScanner *scanner= find_index_for_scan(&composite_condition_filter->filter(i));
      if (scanner != nullptr) {
        return scanner; // ???????????????????????????????????????????????????=
      }
    }
  }
  return nullptr;
}

RC Table::sync() {
  RC rc = data_buffer_pool_->flush_all_pages(file_id_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to flush table's data pages. table=%s, rc=%d:%s", name(), rc, strrc(rc));
    return rc;
  }

  for (Index *index: indexes_) {
    rc = index->sync();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to flush index's pages. table=%s, index=%s, rc=%d:%s",
                name(), index->index_meta().name(), rc, strrc(rc));
      return rc;
    }
  }
  LOG_INFO("Sync table over. table=%s", name());
  return rc;
}
