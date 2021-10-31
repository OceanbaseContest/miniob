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

#include <string>
#include <sstream>
//add zjx[select]b:20211022
#include <map>
#include <vector>
//e:20211022


#include "execute_stage.h"

#include "common/io/io.h"
#include "common/log/log.h"
#include "common/seda/timer_stage.h"
#include "common/lang/string.h"
#include "session/session.h"
#include "event/storage_event.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "event/execution_plan_event.h"
#include "sql/executor/execution_node.h"
#include "sql/executor/tuple.h"
#include "storage/common/table.h"
#include "storage/default/default_handler.h"
#include "storage/common/condition_filter.h"
#include "storage/trx/trx.h"

using namespace common;

RC create_selection_executor(Trx *trx, const Selects &selects, const char *db, const char *table_name, SelectExeNode &select_node);

//! Constructor
ExecuteStage::ExecuteStage(const char *tag) : Stage(tag) {}

//! Destructor
ExecuteStage::~ExecuteStage() {}

//! Parse properties, instantiate a stage object
Stage *ExecuteStage::make_stage(const std::string &tag) {
  ExecuteStage *stage = new (std::nothrow) ExecuteStage(tag.c_str());
  if (stage == nullptr) {
    LOG_ERROR("new ExecuteStage failed");
    return nullptr;
  }
  stage->set_properties();
  return stage;
}

//! Set properties for this object set in stage specific properties
bool ExecuteStage::set_properties() {
  //  std::string stageNameStr(stageName);
  //  std::map<std::string, std::string> section = theGlobalProperties()->get(
  //    stageNameStr);
  //
  //  std::map<std::string, std::string>::iterator it;
  //
  //  std::string key;

  return true;
}

//! Initialize stage params and validate outputs
bool ExecuteStage::initialize() {
  LOG_TRACE("Enter");

  std::list<Stage *>::iterator stgp = next_stage_list_.begin();
  default_storage_stage_ = *(stgp++);
  mem_storage_stage_ = *(stgp++);

  LOG_TRACE("Exit");
  return true;
}

//! Cleanup after disconnection
void ExecuteStage::cleanup() {
  LOG_TRACE("Enter");

  LOG_TRACE("Exit");
}

void ExecuteStage::handle_event(StageEvent *event) {
  LOG_TRACE("Enter\n");

  handle_request(event);

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::callback_event(StageEvent *event, CallbackContext *context) {
  LOG_TRACE("Enter\n");

  // here finish read all data from disk or network, but do nothing here.
  ExecutionPlanEvent *exe_event = static_cast<ExecutionPlanEvent *>(event);
  SQLStageEvent *sql_event = exe_event->sql_event();
  sql_event->done_immediate();

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::handle_request(common::StageEvent *event) {
  ExecutionPlanEvent *exe_event = static_cast<ExecutionPlanEvent *>(event);
  SessionEvent *session_event = exe_event->sql_event()->session_event();
  Query *sql = exe_event->sqls();
  const char *current_db = session_event->get_client()->session->get_current_db().c_str();

  CompletionCallback *cb = new (std::nothrow) CompletionCallback(this, nullptr);
  if (cb == nullptr) {
    LOG_ERROR("Failed to new callback for ExecutionPlanEvent");
    exe_event->done_immediate();
    return;
  }
  exe_event->push_callback(cb);

  switch (sql->flag) {
    case SCF_SELECT: { // select
    //add zjx[select]b:20211023
    TupleSet * res_tuple_set = new TupleSet();
    SQLStageEvent * sql_event = exe_event->sql_event();
    do{
      /*当存在多个SQL语句时，在此进入循环，由于目前尚不支持union，此处接口待完善
        修改后，do_select仅承担运算功能，不再对数据进行打印输出，对总结果的打印输出将统一完成。
        */
      do_select(current_db, sql, sql_event->session_event());
      //do_union(res_tuple_set, sql_event->session_event()->get_tupleset());
      sql_event->session_event()->get_tupleset(*res_tuple_set);
      sql = sql->next_sql;
      sql_event = sql_event->next_sql_event();
    } while(sql);
    std::stringstream ss;
    SessionEvent * session_event = exe_event->sql_event()->session_event();
    session_event->set_tupleset(res_tuple_set); 
    session_event->print_to_stream(ss);
    exe_event->done_immediate();
    }
    break;
    //e:20211023

    case SCF_INSERT:
    case SCF_UPDATE:
    case SCF_DELETE:
    case SCF_CREATE_TABLE:
    case SCF_SHOW_TABLES:
    case SCF_DESC_TABLE:
    case SCF_DROP_TABLE:
    case SCF_CREATE_INDEX:
    case SCF_DROP_INDEX: 
    case SCF_LOAD_DATA: {
      StorageEvent *storage_event = new (std::nothrow) StorageEvent(exe_event);
      if (storage_event == nullptr) {
        LOG_ERROR("Failed to new StorageEvent");
        event->done_immediate();
        return;
      }

      default_storage_stage_->handle_event(storage_event);
    }
    break;
    case SCF_SYNC: {
      RC rc = DefaultHandler::get_default().sync();
      session_event->set_response(strrc(rc));
      exe_event->done_immediate();
    }
    break;
    case SCF_BEGIN: {
      session_event->get_client()->session->set_trx_multi_operation_mode(true);
      session_event->set_response(strrc(RC::SUCCESS));
      exe_event->done_immediate();
    }
    break;
    case SCF_COMMIT: {
      Trx *trx = session_event->get_client()->session->current_trx();
      RC rc = trx->commit();
      session_event->get_client()->session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
      exe_event->done_immediate();
    }
    break;
    case SCF_ROLLBACK: {
      Trx *trx = session_event->get_client()->session->current_trx();
      RC rc = trx->rollback();
      session_event->get_client()->session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
      exe_event->done_immediate();
    }
    break;
    case SCF_HELP: {
      const char *response = "show tables;\n"
          "desc `table name`;\n"
          "create table `table name` (`column name` `column type`, ...);\n"
          "create index `index name` on `table` (`column`);\n"
          "insert into `table` values(`value1`,`value2`);\n"
          "update `table` set column=value [where `column`=`value`];\n"
          "delete from `table` [where `column`=`value`];\n"
          "select [ * | `columns` ] from `table`;\n";
      session_event->set_response(response);
      exe_event->done_immediate();
    }
    break;
    case SCF_EXIT: {
      // do nothing
      const char *response = "Unsupported\n";
      session_event->set_response(response);
      exe_event->done_immediate();
    }
    break;
    default: {
      exe_event->done_immediate();
      LOG_ERROR("Unsupported command=%d\n", sql->flag);
    }
  }
}

void end_trx_if_need(Session *session, Trx *trx, bool all_right) {
  if (!session->is_trx_multi_operation_mode()) {
    if (all_right) {
      trx->commit();
    } else {
      trx->rollback();
    }
  }
}

//mod zjx[select]b:20211022
/**
 * @name: 
 * @test:select运算
 * @msg: 
 * @param {char} *db
 * @param {Query} *sql
 * @param {SessionEvent} *session_event
 * @return {*}
 */
RC ExecuteStage::do_select(const char *db, Query *sql, SessionEvent *session_event) {

  RC rc = RC::SUCCESS;
  Session *session = session_event->get_client()->session;
  Trx *trx = session->current_trx();
  const Selects &selects = sql->sstr.selection;
  // 把所有的表和只跟这张表关联的condition都拿出来，生成最底层的select 执行节点
  std::vector<SelectExeNode *> select_nodes;
  for (size_t i = 0; i < selects.relation_num; i++) {
    const char *table_name = selects.relations[i];
    SelectExeNode *select_node = new SelectExeNode;
    rc = create_selection_executor(trx, selects, db, table_name, *select_node);
    if (rc != RC::SUCCESS) {
      delete select_node;
      for (SelectExeNode *& tmp_node: select_nodes) {
        delete tmp_node;
      }
      end_trx_if_need(session, trx, false);
      return rc;
    }
    select_nodes.push_back(select_node);
  }

  if (select_nodes.empty()) {
    LOG_ERROR("No table given");
    end_trx_if_need(session, trx, false);
    return RC::SQL_SYNTAX;
  }

  //保存上一阶段得到的各个原生表的tupleset信息
  TupleSet* tuple_sets[select_nodes.size()];
  std::map<std::string, TupleSet*> name_tupleset_map;  //map用以保存各个表的信息和TupleSet指针的联系
  int count = 0; 
  for (SelectExeNode *&node: select_nodes) {
    TupleSet* tuple_set = new TupleSet();
    rc = node->execute(*tuple_set);
    if (rc != RC::SUCCESS) {
      for (SelectExeNode *& tmp_node: select_nodes) {
        delete tmp_node;
      }
      end_trx_if_need(session, trx, false);
      return rc;
    } else {
      std::string s1(selects.relations[count], selects.relations[count] + strlen(selects.relations[count]));
      tuple_sets[count] = tuple_set;
      name_tupleset_map[s1] = tuple_sets[count]; 
    }
    count++; 
  }

  JoinNode* optimized_join_tree = nullptr;
  do_optimize(sql, select_nodes.size(), &name_tupleset_map, optimized_join_tree);

  if (select_nodes.size() > 1) {
    // 本次查询了多张表，需要做join操作 

    //多表情况下optimized_join_tree不应该为空
    if( optimized_join_tree == nullptr){
      return RC::JOINNODE_ERROR;
    } else if( rc == RC::SUCCESS )
    {
      rc = do_join_product(optimized_join_tree);
    } else {
      return rc;
    }
  } 

  if(rc == RC::SUCCESS){
//    if(sql->aggret_flag){
//      rc = do_aggregate();
	rc = RC::SUCCESS;
//    }
  }else{
    return rc;
  }

  for (SelectExeNode *& tmp_node: select_nodes) {
    delete tmp_node;
  }

  if(select_nodes.size() > 1){
	  session_event->set_tupleset((TupleSet*)optimized_join_tree->tupleset_);	
  } else {
	  session_event->set_tupleset(tuple_sets[0]);
  }
  end_trx_if_need(session, trx, true);
  return rc;
}
//e:20211020

//add zjx[select]b:20211020
/**
 * @name: do_optimize
 * @test: 
 * @msg: 将在语法分析阶段得到的JoinNode节点链表连接成可运算的树，挂载TupleSet指针
 * @param {Query*} sql
 * @param {int} size
 * @param {JoinNode*} &res_node
 * @return {*}
 */
RC ExecuteStage::do_optimize(Query* sql, int size, std::map<std::string, TupleSet*>* name_tupleset_map, JoinNode* &res_node){
  RC rc = RC::SUCCESS;
  if(size == 1){
    return rc;
  }

  for(int i = 0; rc == RC::SUCCESS && i<sql->sstr.selection.joinnode_num; i++){ 
    rc = load_tupleset(sql->sstr.selection.joinnodes[i], name_tupleset_map);
    }
  
  JoinNode** joinnodes = sql->sstr.selection.joinnodes;
  JoinNode* new_node_f = new JoinNode();
  new_node_f->done_ = false;
  new_node_f->join_type_ = true;
  new_node_f->left_node_ = joinnodes[0];

  for(int i = 1;i < sql->sstr.selection.joinnode_num; i++){
    new_node_f->right_node_ = joinnodes[i];
    if(i == sql->sstr.selection.joinnode_num - 1){
      break;
    }
    JoinNode* new_node_s = new JoinNode();
    new_node_s->done_ = false;
    new_node_s->join_type_ = true;
    new_node_s->left_node_ = new_node_f;
    new_node_f = new_node_s;
  }

  res_node = new_node_f;
  return rc;
}
/**
 * @name: do_join_product
 * @test: 
 * @msg: 判别当前节点的运算模式
 * @param {JoinNode*} &optimized_join_tree
 * @return {*}
 */
RC ExecuteStage::do_join_product(JoinNode* &optimized_join_tree){
  RC rc = RC::SUCCESS;
  //空节点不应该到达此函数
  if(optimized_join_tree == nullptr){
    return RC::JOINNODE_ERROR;
  }else if (optimized_join_tree->done_) //传入节点done_为true，代表此节点为基本表节点或已完成运算，可直接返回
  {
    return rc;
  }

  if(!optimized_join_tree->join_type_)
  {
    rc = do_join(optimized_join_tree);
  } else {
    rc = do_product(optimized_join_tree);
  }

  return rc;
}

/**
 * @name: do_join
 * @test: 
 * @msg: 对当前节点进行join运算
 * @param {JoinNode*} &optimized_join_tree
 * @return {*}
 */
RC ExecuteStage::do_join(JoinNode* &optimized_join_tree){
  RC rc = RC::SUCCESS;
  return rc;
}

/**
 * @name: do_product
 * @test: 
 * @msg: 对当前节点进行prodect运算
 * @param {JoinNode*} &optimized_join_tree
 * @return {*}
 */
RC ExecuteStage::do_product(JoinNode* &optimized_join_tree){
  RC rc = RC::SUCCESS;

  //判别左右节点的情况
  if((rc = do_join_product(optimized_join_tree->left_node_)) != RC::SUCCESS)
  {
    return RC::JOINNODE_ERROR;
  } else if ((rc = do_join_product(optimized_join_tree->right_node_)) != RC::SUCCESS)
  {
    return RC::JOINNODE_ERROR;
  }

  TupleSet* left_node_tuple = (TupleSet*)optimized_join_tree->left_node_->tupleset_;
  TupleSet* right_node_tuple = (TupleSet*)optimized_join_tree->right_node_->tupleset_;
  TupleSet* new_tupleset = new TupleSet();

  if(left_node_tuple != nullptr && right_node_tuple != nullptr){
      TupleSchema* new_schema = new TupleSchema();
	    new_schema->clear();
      new_schema->append(left_node_tuple->get_schema());
      new_schema->append(right_node_tuple->get_schema());
      new_tupleset->set_schema(*new_schema);
      for(int i = 0;i < left_node_tuple->size(); i++)
      {
        for(int j = 0; j < right_node_tuple->size(); j++)
        {
	        Tuple new_tuple;
          new_tuple.add(left_node_tuple->get(i));
          new_tuple.add(right_node_tuple->get(j));
          new_tupleset->add(std::move(new_tuple));
        }
      }
  }

  optimized_join_tree->tupleset_ = new_tupleset; 
  optimized_join_tree->done_ = true;
  return rc;
}

/**
 * @name: do_aggregate
 * @test: 
 * @msg: 聚合操作接口
 * @param {*}
 * @return {*}
 */
RC ExecuteStage::do_aggregate(std::vector<TupleSet>* &tuple_sets){
  RC rc = RC::SUCCESS;

  return rc;
}

/**
 * @name: do_union
 * @test: 
 * @msg: union操作接口
 * @param {TupleSet*} &res_tuple_set
 * @param {TupleSet*} &new_tupleset
 * @return {*}
 */
RC ExecuteStage::do_union(TupleSet* &res_tuple_set, TupleSet* &new_tupleset){
  RC rc = RC::SUCCESS;
  if(new_tupleset == nullptr){
  	return rc;
   }

  for(int i = 0; i < new_tupleset->size(); i++){
    res_tuple_set->add(new_tupleset->get(i));
  }
  return rc;
}
/**
 * @name: load_tupleset
 * @test: 
 * @msg: 对JoinNode节点添加TupleSet指针
 * @param {JoinNode*} &node
 * @return {*}
 */
RC ExecuteStage::load_tupleset(JoinNode* &node, std::map<std::string, TupleSet*>* name_tupleset_map){
  RC rc = RC::SUCCESS;
  if(node == nullptr)
  {
    return rc;
  }
  std::string tmp(node->table_name_, node->table_name_ + strlen(node->table_name_));

  //子节点done_为true，代表这是在语法分析阶段得到的基本表节点，需要添加指向已生成的TupleSet的指针
  if(node->done_){
    node->tupleset_ = name_tupleset_map->at(tmp);
    return rc;
  }

  rc = load_tupleset(node->left_node_, name_tupleset_map);
  rc = load_tupleset(node->right_node_, name_tupleset_map);
  return rc;
}
//e:20211020

bool match_table(const Selects &selects, const char *table_name_in_condition, const char *table_name_to_match) {
  if (table_name_in_condition != nullptr) {
    return 0 == strcmp(table_name_in_condition, table_name_to_match);
  }

  return selects.relation_num == 1;
}

static RC schema_add_field(Table *table, const char *field_name, TupleSchema &schema) {
  const FieldMeta *field_meta = table->table_meta().field(field_name);
  if (nullptr == field_meta) {
    LOG_WARN("No such field. %s.%s", table->name(), field_name);
    return RC::SCHEMA_FIELD_MISSING;
  }

  schema.add_if_not_exists(field_meta->type(), table->name(), field_meta->name());
  return RC::SUCCESS;
}

// 把所有的表和只跟这张表关联的condition都拿出来，生成最底层的select 执行节点
RC create_selection_executor(Trx *trx, const Selects &selects, const char *db, const char *table_name, SelectExeNode &select_node) {
  // 列出跟这张表关联的Attr
  TupleSchema schema;
  Table * table = DefaultHandler::get_default().find_table(db, table_name);
  if (nullptr == table) {
    LOG_WARN("No such table [%s] in db [%s]", table_name, db);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  for (int i = selects.attr_num - 1; i >= 0; i--) {
    const RelAttr &attr = selects.attributes[i];
    if (nullptr == attr.relation_name || 0 == strcmp(table_name, attr.relation_name)) {
      if (0 == strcmp("*", attr.attribute_name)) {
        // 列出这张表所有字段
        TupleSchema::from_table(table, schema);
        break; // 没有校验，给出* 之后，再写字段的错误
      } else {
        // 列出这张表相关字段
        RC rc = schema_add_field(table, attr.attribute_name, schema);
        if (rc != RC::SUCCESS) {
          return rc;
        }
      }
    }
  }

  // 找出仅与此表相关的过滤条件, 或者都是值的过滤条件
  std::vector<DefaultConditionFilter *> condition_filters;
  for (size_t i = 0; i < selects.condition_num; i++) {
    const Condition &condition = selects.conditions[i];
    if ((condition.left_is_attr == 0 && condition.right_is_attr == 0) || // 两边都是值
        (condition.left_is_attr == 1 && condition.right_is_attr == 0 && match_table(selects, condition.left_attr.relation_name, table_name)) ||  // 左边是属性右边是值
        (condition.left_is_attr == 0 && condition.right_is_attr == 1 && match_table(selects, condition.right_attr.relation_name, table_name)) ||  // 左边是值，右边是属性名
        (condition.left_is_attr == 1 && condition.right_is_attr == 1 &&
            match_table(selects, condition.left_attr.relation_name, table_name) && match_table(selects, condition.right_attr.relation_name, table_name)) // 左右都是属性名，并且表名都符合
        ) {
      DefaultConditionFilter *condition_filter = new DefaultConditionFilter();
      RC rc = condition_filter->init(*table, condition);
      if (rc != RC::SUCCESS) {
        delete condition_filter;
        for (DefaultConditionFilter * &filter : condition_filters) {
          delete filter;
        }
        return rc;
      }
      condition_filters.push_back(condition_filter);
    }
  }

  return select_node.init(trx, table, std::move(schema), std::move(condition_filters));
}
