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

#include <mutex>
#include "sql/parser/parse.h"
#include "rc.h"
#include "common/log/log.h"
#include <string.h> // add szj [select aggregate support]20211106
#include <vector> //add zjx[date]b:20211102

RC parse(char *st, Query *sqln);

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus
void relation_attr_init(RelAttr *relation_attr, const char *relation_name, const char *attribute_name) {
  if (relation_name != nullptr) {
    relation_attr->relation_name = strdup(relation_name);
  } else {
    relation_attr->relation_name = nullptr;
  }
  relation_attr->attribute_name = strdup(attribute_name);
}

// add szj [select aggregate support]20211106:b
bool judge_one(int v) {
  LOG_INFO("Let's fucking go!!!!!! judge_one");
  bool flag = v == 1 ? true : false;
  return flag;
}
// add:e

// add szj [select aggregate support]20211106:b
void relation_col_attr_init(ColAttr *relation_attr, const char *relation_name, const char *attribute_name, const char *aggregate_name) {
  LOG_INFO("get into relation_col_attr_init!!!!");
  if (relation_name != nullptr) {
    relation_attr->relAttr.relation_name = strdup(relation_name);
  } else {
    relation_attr->relAttr.relation_name = nullptr;
  }
  relation_attr->relAttr.attribute_name = strdup(attribute_name);
  LOG_INFO("fINISH entering relation name!!!!");
  LOG_INFO("Give me the fucking aggre_name %s", aggregate_name);
  // 判断聚集类别
  // if (strcasecmp(aggregate_name, "SUM") == 0) {
  if (aggregate_name == nullptr) {
    LOG_INFO("NONE!!!!ONE");
    relation_attr->aggreType = NONE;
  }
  else if (strcasecmp(aggregate_name, "SUM") == 0) {
    LOG_INFO("SUM!!!!");
    relation_attr->aggreType = SUM;
  } 
  else if (strcasecmp(aggregate_name, "MAX") == 0) {
    LOG_INFO("MAX!!!!");
    relation_attr->aggreType = MAX;
  }
  else if (strcasecmp(aggregate_name, "AVG") == 0) {
    LOG_INFO("AVG!!!!");
    relation_attr->aggreType = AVG;
  }
  else if (strcasecmp(aggregate_name, "COUNT") == 0) {
    LOG_INFO("COUNT!!!!");
    relation_attr->aggreType = COUNTS;
  }
  //else if (strcasecmp(strdup(aggregate_name), "COUNT") == 0) {
  //  relation_attr->aggreType = COUNT;
  // } 
  else {
    LOG_INFO("NONE!!!!");
    relation_attr->aggreType = NONE;
  }
  // relation_attr->aggreType = NONE;
  // LOG_INFO("NONE!!!!");
}
// add:e

void relation_attr_destroy(RelAttr *relation_attr) {
  free(relation_attr->relation_name);
  free(relation_attr->attribute_name);
  relation_attr->relation_name = nullptr;
  relation_attr->attribute_name = nullptr;
}

// add szj [select aggregate support]20211106:b
void relation_col_attr_destroy(ColAttr *relation_attr) {
  free(relation_attr->relAttr.attribute_name);
  free(relation_attr->relAttr.relation_name);
  // free(relation_attr->aggreType);
  relation_attr->relAttr.relation_name = nullptr;
  relation_attr->relAttr.attribute_name = nullptr;
  relation_attr->aggreType = NONE;
  // free(relation_attr->relation_name);
  // free(relation_attr->attribute_name);
  // relation_attr->relation_name = nullptr;
  // relation_attr->attribute_name = nullptr;
}
// add:e

void value_init_integer(Value *value, int v) {
  value->type = INTS;
  value->data = malloc(sizeof(v));
  memcpy(value->data, &v, sizeof(v));
}
void value_init_float(Value *value, float v) {
  value->type = FLOATS;
  value->data = malloc(sizeof(v));
  memcpy(value->data, &v, sizeof(v));
}
void value_init_string(Value *value, const char *v) {
  value->type = CHARS;
  value->data = strdup(v);
}
//add zjx[date]b:20211027
void value_init_date(Value *value, const char *v) {
  value->type = DATES;
  value->data = strdup(v);
}
//e:20211027
void value_destroy(Value *value) {
  value->type = UNDEFINED;
  free(value->data);
  value->data = nullptr;
}

void condition_init(Condition *condition, CompOp comp, 
                    int left_is_attr, RelAttr *left_attr, Value *left_value,
                    int right_is_attr, RelAttr *right_attr, Value *right_value) {
  condition->comp = comp;
  condition->left_is_attr = left_is_attr;
  if (left_is_attr) {
    condition->left_attr = *left_attr;
  } else {
    condition->left_value = *left_value;
  }

  condition->right_is_attr = right_is_attr;
  if (right_is_attr) {
    condition->right_attr = *right_attr;
  } else {
    condition->right_value = *right_value;
  }
}
void condition_destroy(Condition *condition) {
  if (condition->left_is_attr) {
    relation_attr_destroy(&condition->left_attr);
  } else {
    value_destroy(&condition->left_value);
  }
  if (condition->right_is_attr) {
    relation_attr_destroy(&condition->right_attr);
  } else {
    value_destroy(&condition->right_value);
  }
}

void attr_info_init(AttrInfo *attr_info, const char *name, AttrType type, size_t length) {
  attr_info->name = strdup(name);
  attr_info->type = type;
  attr_info->length = length;
}
void attr_info_destroy(AttrInfo *attr_info) {
  free(attr_info->name);
  attr_info->name = nullptr;
}

void selects_init(Selects *selects, ...);

// add szj [select aggregate support]20211106:b
void selects_append_attribute(Selects *selects, ColAttr *rel_attr) {
  LOG_INFO("SUCCESS COMPLETE SELECT CONTEXT!!");
  selects->attributes[selects->attr_num++] = *rel_attr;
  if (rel_attr->aggreType == SUM || rel_attr->aggreType == MAX || rel_attr->aggreType == AVG || rel_attr->aggreType == COUNTS)
    selects->aggr_flag = true;
  LOG_INFO("Giving ColAttr Value Success!!");
}
// add:e

void selects_append_relation(Selects *selects, const char *relation_name) {
  selects->relations[selects->relation_num++] = strdup(relation_name);
}

//add zjx[select]b:20211028
//生成JoinNode算子
void selects_append_joinnode(Selects *selects, const char *relation_name, int judge){
  JoinNode* new_node = new JoinNode();
  new_node->table_name_ = strdup(relation_name);
  new_node->join_type_ = true;
  new_node->done_ = true; // 叶子节点设置未已完成join
  new_node->left_node_ = nullptr;
  new_node->right_node_ = nullptr;

  if(judge == 0){  //product access
    selects->joinnodes[selects->joinnode_num++] = new_node;
  } else if(judge == 1){  //join access
    JoinNode* new_f_node = new JoinNode();
    new_f_node->join_type_ = false; // 针对join算法
    new_f_node->done_ = false;
    new_f_node->left_node_ = selects->joinnodes[--selects->joinnode_num];
    new_f_node->right_node_ = new_node;
    selects->joinnodes[selects->joinnode_num++] = new_f_node;
  } 
}
//e:20211028

//add zjx[order by]b:20211103
void ordercondition_destroy(OrderCondition *order_relation_attr) {
  free(order_relation_attr->relAttr.relation_name);
  free(order_relation_attr->relAttr.attribute_name);
  order_relation_attr->relAttr.relation_name = nullptr;
  order_relation_attr->relAttr.attribute_name = nullptr;
}


void selects_append_orders(Selects *selects, RelAttr *relattr, int order_type) { 
  OrderCondition order_condition;
  order_condition.relAttr = *relattr;
  if( order_type == 2 ) { 
	order_condition.ordertype = 0;
  } else {
	order_condition.ordertype = 1;
   }
  selects->orders[selects->order_num++] = order_condition;
}
//e:20211103

void selects_append_conditions(Selects *selects, Condition conditions[], size_t condition_num) {
  assert(condition_num <= sizeof(selects->conditions)/sizeof(selects->conditions[0]));
  for (size_t i = 0; i < condition_num; i++) {
    selects->conditions[i] = conditions[i];
  }
  selects->condition_num = condition_num;
}


void selects_destroy(Selects *selects) {
  for (size_t i = 0; i < selects->attr_num; i++) {
    relation_col_attr_destroy(&selects->attributes[i]);
  }
  selects->attr_num = 0;

  for (size_t i = 0; i < selects->relation_num; i++) {
    free(selects->relations[i]);
    selects->relations[i] = NULL;
  }
  selects->relation_num = 0;

  for (size_t i = 0; i < selects->condition_num; i++) {
    condition_destroy(&selects->conditions[i]);
  }
  selects->condition_num = 0;

  //add zjx[order by]b:20211109
  for (size_t i = 0; i < selects->order_num; i++) {
    ordercondition_destroy(&selects->orders[i]);
  }
  selects->order_num = 0;
}

// add szj [insert multi values]20211029:b
void inserts_init(Inserts *inserts, const char *relation_name, Value values[], size_t value_num, size_t record_num) {
  assert(value_num <= sizeof(inserts->values)/sizeof(inserts->values[0]));

  inserts->relation_name = strdup(relation_name);
  for (size_t i = 0; i < value_num; i++) {
    inserts->values[i] = values[i];
    LOG_INFO("!!!!!!!!!!! %d", *((int*)inserts->values[i].data));
  }
  LOG_INFO("!!!!!!!!!!! %d", record_num);
  LOG_INFO("!!!!!!!!!!! %d", value_num);
  inserts->value_num = value_num;
  inserts->record_num = record_num;
}
// add:e

void inserts_destroy(Inserts *inserts) {
  free(inserts->relation_name);
  inserts->relation_name = nullptr;

  for (size_t i = 0; i < inserts->value_num; i++) {
    value_destroy(&inserts->values[i]);
  }
  inserts->value_num = 0;
}

void deletes_init_relation(Deletes *deletes, const char *relation_name) {
  deletes->relation_name = strdup(relation_name);
}

void deletes_set_conditions(Deletes *deletes, Condition conditions[], size_t condition_num) {
  assert(condition_num <= sizeof(deletes->conditions)/sizeof(deletes->conditions[0]));
  for (size_t i = 0; i < condition_num; i++) {
    deletes->conditions[i] = conditions[i];
  }
  deletes->condition_num = condition_num;
}
void deletes_destroy(Deletes *deletes) {
  for (size_t i = 0; i < deletes->condition_num; i++) {
    condition_destroy(&deletes->conditions[i]);
  }
  deletes->condition_num = 0;
  free(deletes->relation_name);
  deletes->relation_name = nullptr;
}

void updates_init(Updates *updates, const char *relation_name, const char *attribute_name,
                  Value *value, Condition conditions[], size_t condition_num) {
  updates->relation_name = strdup(relation_name);
  updates->attribute_name = strdup(attribute_name);
  updates->value = *value;

  assert(condition_num <= sizeof(updates->conditions)/sizeof(updates->conditions[0]));
  for (size_t i = 0; i < condition_num; i++) {
    updates->conditions[i] = conditions[i];
  }
  updates->condition_num = condition_num;
}

void updates_destroy(Updates *updates) {
  free(updates->relation_name);
  free(updates->attribute_name);
  updates->relation_name = nullptr;
  updates->attribute_name = nullptr;

  value_destroy(&updates->value);

  for (size_t i = 0; i < updates->condition_num; i++) {
    condition_destroy(&updates->conditions[i]);
  }
  updates->condition_num = 0;
}

void create_table_append_attribute(CreateTable *create_table, AttrInfo *attr_info) {
  create_table->attributes[create_table->attribute_count++] = *attr_info;
}
void create_table_init_name(CreateTable *create_table, const char *relation_name) {
  create_table->relation_name = strdup(relation_name);
}
void create_table_destroy(CreateTable *create_table) {
  for (size_t i = 0; i < create_table->attribute_count; i++) {
    attr_info_destroy(&create_table->attributes[i]);
  }
  create_table->attribute_count = 0;
  free(create_table->relation_name);
  create_table->relation_name = nullptr;
}

void drop_table_init(DropTable *drop_table, const char *relation_name) {
  drop_table->relation_name = strdup(relation_name);
}
void drop_table_destroy(DropTable *drop_table) {
  free(drop_table->relation_name);
  drop_table->relation_name = nullptr;
}

void create_index_init(CreateIndex *create_index, const char *index_name, 
                       const char *relation_name, const char *attr_name) {
  create_index->index_name = strdup(index_name);
  create_index->relation_name = strdup(relation_name);
  create_index->attribute_name = strdup(attr_name);
}
void create_index_destroy(CreateIndex *create_index) {
  free(create_index->index_name);
  free(create_index->relation_name);
  free(create_index->attribute_name);

  create_index->index_name = nullptr;
  create_index->relation_name = nullptr;
  create_index->attribute_name = nullptr;
}

void drop_index_init(DropIndex *drop_index, const char *index_name) {
  drop_index->index_name = strdup(index_name);
}
void drop_index_destroy(DropIndex *drop_index) {
  free((char *)drop_index->index_name);
  drop_index->index_name = nullptr;
}

void desc_table_init(DescTable *desc_table, const char *relation_name) {
  desc_table->relation_name = strdup(relation_name);
}

void desc_table_destroy(DescTable *desc_table) {
  free((char *)desc_table->relation_name);
  desc_table->relation_name = nullptr;
}

void load_data_init(LoadData *load_data, const char *relation_name, const char *file_name) {
  load_data->relation_name = strdup(relation_name);

  if (file_name[0] == '\'' || file_name[0] == '\"') {
    file_name++;
  }
  char *dup_file_name = strdup(file_name);
  int len = strlen(dup_file_name);
  if (dup_file_name[len - 1] == '\'' || dup_file_name[len - 1] == '\"') {
    dup_file_name[len - 1] = 0;
  }
  load_data->file_name = dup_file_name;
}

void load_data_destroy(LoadData *load_data) {
  free((char *)load_data->relation_name);
  free((char *)load_data->file_name);
  load_data->relation_name = nullptr;
  load_data->file_name = nullptr;
}

//mod zjx[select]b:20211024
void query_init(Query *query) {
  query->flag = SCF_ERROR;
  memset(&query->sstr, 0, sizeof(query->sstr));
  query->next_sql = nullptr;
}
//e:20211024

Query *query_create() {
  Query *query = (Query *)malloc(sizeof(Query));
  if (nullptr == query) {
    LOG_ERROR("Failed to alloc memroy for query. size=%ld", sizeof(Query));
    return nullptr;
  }

  query_init(query);
  return query;
}

void query_reset(Query *query) {
  switch (query->flag) {
    case SCF_SELECT: {
      selects_destroy(&query->sstr.selection);
    }
    break;
    case SCF_INSERT: {
      inserts_destroy(&query->sstr.insertion);
    }
    break;
    case SCF_DELETE: {
      deletes_destroy(&query->sstr.deletion);
    }
    break;
    case SCF_UPDATE: {
      updates_destroy(&query->sstr.update);
    }
    break;
    case SCF_CREATE_TABLE: {
      create_table_destroy(&query->sstr.create_table);
    }
    break;
    case SCF_DROP_TABLE: {
      drop_table_destroy(&query->sstr.drop_table);
    }
    break;
    case SCF_CREATE_INDEX: {
      create_index_destroy(&query->sstr.create_index);
    }
    break;
    case SCF_DROP_INDEX: {
      drop_index_destroy(&query->sstr.drop_index);
    }
    break;
    case SCF_SYNC: {

    }
    break;
    case SCF_SHOW_TABLES:
    break;

    case SCF_DESC_TABLE: {
      desc_table_destroy(&query->sstr.desc_table);
    }
    break;

    case SCF_LOAD_DATA: {
      load_data_destroy(&query->sstr.load_data);
    }
    break;
    case SCF_BEGIN:
    case SCF_COMMIT:
    case SCF_ROLLBACK:
    case SCF_HELP:
    case SCF_EXIT:
    case SCF_ERROR:
    break;
  }
}

void query_destroy(Query *query) {
  query_reset(query);
  free(query);
}

//add zjx[date&check]b:20211027
bool check_date(char * datedata)
{
  RC rc = RC::SUCCESS;
  std::vector<int> pos;
  char* pch = strchr(datedata,'-');
  while(pch != NULL){
    pos.push_back(pch-datedata);
    pch = strchr(pch+1,'-');     
  }
  if(pos.size()!=2){return false;}

  if(0 == pos[0] 
    || pos[0] == pos[1]
    || pos[1] == strlen(datedata)-1){return false;}
  
  int days_in_month[]= {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  
  int year = atoi(inner_substr(datedata, 0, pos[0]-1));
  int month = atoi(inner_substr(datedata, pos[0]+1, pos[1]-1));
  if (year < 1 || year > 9999 ||
    month < 1 || month > 12)
  {
    return false;
  }
  else
  {
    int mday = days_in_month[month];
    if (1 == is_leap_year(year) && 2 == month)
    {
      mday += 1;
    }
    if (atoi(inner_substr(datedata, pos[1]+1, strlen(datedata)-1)) < 1 || atoi(inner_substr(datedata, pos[1]+1, strlen(datedata)-1)) > mday)
    {
      return false;
    }
  }
  return true;
}

bool is_leap_year(int year){
    if ((year & 3) == 0 && (year%100 || (year%400 == 0 && year)))
    {
        return true;
    }
    return false;
}

void refactor_date(char* datedata){
  int pos[2];
  int j =0;
  char* pch = strchr(datedata,'-');
  while(pch != NULL){
    pos[j++] = (pch-datedata);
    pch = strchr(pch+1,'-');
  }

  char new_datedata[10];
  memset(new_datedata,'0',4-pos[0]);
  memcpy(new_datedata+(4-pos[0]), datedata, pos[0]);
  new_datedata[4] = '-';
  memset(new_datedata+5,'0',3-pos[1]+pos[0]);
  memcpy(new_datedata+8-pos[1]+pos[0], datedata+pos[0]+1, pos[1]-pos[0]-1);
  new_datedata[7] = '-';
  memset(new_datedata+8,'0',3- strlen(datedata) + pos[1]);
  memcpy(new_datedata+11-strlen(datedata)+pos[1], datedata+pos[1]+1, strlen(datedata)-pos[1]-1);
  new_datedata[10] = '\0';
  memcpy(datedata, new_datedata, 10);
}

//获取子串
char * inner_substr(const char *s,int n1,int n2)/*从s中提取下标为n1~n2的字符组成一个新字符串，然后返回这个新串的首地址*/
{
  char *sp = (char*)(malloc(sizeof(char) * (n2 - n1 + 2)));
  int i, j = 0;
  for (i = n1; i <= n2; i++) {
    sp[j++] = s[i];
  }
  sp[j] = 0;
  return sp;
}
//e:20211027

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

////////////////////////////////////////////////////////////////////////////////

extern "C" int sql_parse(const char *st, Query  *sqls);

RC parse(const char *st, Query *sqln) {
  sql_parse(st, sqln);

  if (sqln->flag == SCF_ERROR)
    return SQL_SYNTAX;
  else
    return SUCCESS;
}
