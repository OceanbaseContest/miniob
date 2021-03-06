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
//add zjx[order by]b:20211108
#include<algorithm> 
#include<string.h>

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
      /*???????????????SQL?????????????????????????????????????????????????????????union????????????????????????
        ????????????do_select?????????????????????????????????????????????????????????????????????????????????????????????????????????
        */
       LOG_INFO("Get into select function!!!");
      RC rc = do_select(current_db, sql, sql_event->session_event()); //mod zjx[check]b:20211102
      //do_union(res_tuple_set, sql_event->session_event()->get_tupleset());
      //add zjx[check]b:20211103
      if(rc == RC::SCHEMA_TABLE_NOT_EXIST 
            || rc == RC::SCHEMA_FIELD_MISSING
            || rc == RC::RECORD_INVALID_KEY
            || rc == RC::CONDITION_ERROR){
        SessionEvent * session_event = exe_event->sql_event()->session_event();
        session_event->set_response("FAILURE\n");
        exe_event->done_immediate();
        break;
      }
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
 * @test:select??????
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
  Selects &selects = sql->sstr.selection;

  //??????condition
  //add zjx[check]b:20211102
  if( (rc = do_check(selects, db, session_event)) != RC::SUCCESS) {
    return rc;
  }
  //e:20211102

  // ??????????????????????????????????????????condition?????????????????????????????????select ????????????
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

  //?????????????????????????????????????????????tupleset??????
  TupleSet* tuple_sets[select_nodes.size()];
  std::map<std::string, TupleSet*> name_tupleset_map;  //map?????????????????????????????????TupleSet???????????????
  int count = 0; 
  for (SelectExeNode *&node: select_nodes) {
    TupleSet* tuple_set = new TupleSet();
    LOG_INFO("Get into execute function!!!");
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
  LOG_INFO("Get into optimize function!!!");
  do_optimize(sql, select_nodes.size(), &name_tupleset_map, optimized_join_tree);

  if (select_nodes.size() > 1) {
    // ????????????????????????????????????join?????? 

    //???????????????optimized_join_tree???????????????
    if( optimized_join_tree == nullptr){
      return RC::JOINNODE_ERROR;
    } else if( rc == RC::SUCCESS )
    {
      rc = do_join_product(optimized_join_tree);
    } else {
      return rc;
    }
  } 

  // if( rc == RC::SUCCESS ) { //add zjx[group by]b:20211104
  //   if(selects.group_num > 0) {
  //     if( optimized_join_tree != nullptr ) {
  //       TupleSet* tmp_schema = (TupleSet*)optimized_join_tree->tupleset_;
  //       rc = do_group_by(group_by_relattr, tmp_schema, );
  //       optimized_join_tree->tupleset_ = tmp_schema;
	//     } else {
  //       TupleSet* tmp_schema = tuple_sets[0];
  //       rc = do_group_by(group_by_relattr, tmp_schema);
  //       tuple_sets[0] = tmp_schema;
  //     }
  //   }
  // } else {
  //   return rc;
  // }

  LOG_INFO("MotherFucker!!!the aggre_flag is %d", selects.aggr_flag);
  if (selects.aggr_flag == false) {
    if (select_nodes.size() > 1) {
	    TupleSet* tmp_tuple = (TupleSet*)optimized_join_tree->tupleset_;
      if((rc = do_filter(selects, db, tmp_tuple)) != RC::SUCCESS) {
        return rc;
      }
      optimized_join_tree->tupleset_ = tmp_tuple;
    } else {
    TupleSet* tmp_tuple = tuple_sets[0];
    if((rc = do_filter(selects, db, tmp_tuple)) != RC::SUCCESS) {
        return rc;
      }
      tuple_sets[0] = tmp_tuple;
    }
  } else {
    if (select_nodes.size() > 1) {
	    TupleSet* tmp_tuple = (TupleSet*)optimized_join_tree->tupleset_;
      if((rc = do_aggregate(selects, db, tmp_tuple)) != RC::SUCCESS) {
        return rc;
      }
      optimized_join_tree->tupleset_ = tmp_tuple;
    } else {
    TupleSet* tmp_tuple = tuple_sets[0];
    LOG_INFO("DIong dp_aggregate 10000 times????");
    if((rc = do_aggregate(selects, db, tmp_tuple)) != RC::SUCCESS) {
        return rc;
      }
      tuple_sets[0] = tmp_tuple;
    }
  } 

  if(selects.order_num > 0) { //add zjx[order by]b:20211104
    if( optimized_join_tree != nullptr ) {
      TupleSet* tmp_schema = (TupleSet*)optimized_join_tree->tupleset_;
      rc = do_order_by(selects, tmp_schema);
      optimized_join_tree->tupleset_ = tmp_schema;
    } else {
      TupleSet* tmp_schema = tuple_sets[0];
      rc = do_order_by(selects, tmp_schema);
      tuple_sets[0] = tmp_schema;
    }
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
 * @msg: ?????????????????????????????????JoinNode?????????????????????????????????????????????TupleSet??????
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
 * @msg: ?????????????????????????????????
 * @param {JoinNode*} &optimized_join_tree
 * @return {*}
 */
RC ExecuteStage::do_join_product(JoinNode* &optimized_join_tree){
  RC rc = RC::SUCCESS;
  //?????????????????????????????????
  if(optimized_join_tree == nullptr){
    return RC::JOINNODE_ERROR;
  }else if (optimized_join_tree->done_) //????????????done_???true????????????????????????????????????????????????????????????????????????
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
 * @msg: ?????????????????????join??????
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
 * @msg: ?????????????????????prodect??????
 * @param {JoinNode*} &optimized_join_tree
 * @return {*}
 */
RC ExecuteStage::do_product(JoinNode* &optimized_join_tree){
  RC rc = RC::SUCCESS;

  //???????????????????????????
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
 * @name: do_union
 * @test: 
 * @msg: union????????????
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
 * @msg: ???JoinNode????????????TupleSet??????
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

  //?????????done_???true?????????????????????????????????????????????????????????????????????????????????????????????TupleSet?????????
  if(node->done_){
    node->tupleset_ = name_tupleset_map->at(tmp);
    return rc;
  }

  rc = load_tupleset(node->left_node_, name_tupleset_map);
  rc = load_tupleset(node->right_node_, name_tupleset_map);
}

//add zjx[check]b:20211114
/**
 * @name: do_check_condition
 * @test: 
 * @msg: ????????????
 * @param {Selects} &selects, {const char*} db
 * @return {*}
 */
RC ExecuteStage::do_check(Selects &selects, const char* db, SessionEvent * &session_event)  {
  RC rc = RC::SUCCESS;

  //??????join???condition
  //add zjx[join]b:20211104
  if( (rc = do_join_condition_check(selects, db)) != RC::SUCCESS) {
    return rc;
  }

  //select_attr check //add zjx[check]b:20211116
  if ((rc = do_select_attr_check(selects, db, session_event)) != RC::SUCCESS) {
    return rc;
  }

  //where condtion check
  if ((rc = do_where_condition_check(selects, db)) != RC::SUCCESS) {
    return rc;
  }

  //order_condition check
  //add zjx[order by]b:20211103
  if( selects.order_num > 0 ) {
    if((rc = do_order_check(selects, db)) != RC::SUCCESS){
      return rc;
     }
  }

  //group_by_condition check
  //add zjx[group by]b:20211103
  // if( selects.group_num > 0 ) {
  //   if((rc = do_group_by_check(selects, db, group_by_relattr)) != RC::SUCCESS) {
  //     return rc;
  //    }
  // }
  return rc;
}


//add zjx[check]b:20211116
/**
 * @name: 
 * @test: test font
 * @msg: 
 * @param {Selects} &selects
 * @param {char*} db
 * @param {SessionEvent *} session_event
 * @return {*}
 */
RC ExecuteStage::do_select_attr_check(Selects &selects, const char* db, SessionEvent * &session_event){
  RC rc = RC::SUCCESS;
  TupleSet* tmp_tupleset = new TupleSet();
  TupleSchema* tmp_schema = new TupleSchema();
  RelAttr tmp_relAttr;

  for(size_t i = 0; i < selects.attr_num; i++){
    if( 0 == strcmp(selects.attributes[i].relAttr.attribute_name,"*")) {
      tmp_schema->append(*(TupleSchema*)selects.joinnodes[0]->tupleset_);
	continue;
    }
    if((rc = check_and_fix(selects, db, selects.attributes[i].relAttr)) != RC::SUCCESS) {
      return rc;
    }
    tmp_relAttr = selects.attributes[i].relAttr;
    tmp_schema->add(UNDEFINED, tmp_relAttr.relation_name, tmp_relAttr.attribute_name);
  }
  
  tmp_tupleset->set_schema(*tmp_schema);
  session_event->set_tupleset(tmp_tupleset);
  return rc;
}
//e:20211116


//add zjx[check]b:20211102
/**
 * @name: do_check_condition
 * @test: 
 * @msg: ??????condition???????????????????????????????????????
 * @param {Selects} &selects
 * @param {char*} db
 * @return {*}
 */
RC ExecuteStage::do_where_condition_check(Selects &selects, const char* db){
  RC rc = RC::SUCCESS;

  for(size_t i = 0; i < selects.condition_num; i++){
    Condition condition = selects.conditions[i];
    if(condition.left_is_attr == 1) {
      if((rc = check_and_fix(selects, db, selects.conditions[i].left_attr)) != RC::SUCCESS) {
        return rc;
      }
    } 

    if(condition.right_is_attr == 1) {
      if((rc = check_and_fix(selects, db, selects.conditions[i].right_attr)) != RC::SUCCESS) {
        return rc;
      }
    } 
  }
  return rc;
}
//e:20211102

//add zjx[check]b:20211102
/**
 * @name: 
 * @test: test font
 * @msg: 
 * @param {Selects} &selects
 * @param {char*} db
 * @param {RelAttr} &attr
 * @return {*}
 */
RC ExecuteStage::check_and_fix(Selects &selects, const char* db, RelAttr &attr) {//add zjx[select]b:20211114
  RC rc =RC::SUCCESS;

  if(attr.relation_name == nullptr) {
    if ( attr.attribute_name == nullptr) {
      return RC::CONDITION_ERROR;
    }

    int count = 0;
    const char* table_name;
    for( int i = 0; i < selects.relation_num && count < 2; i++) {
      Table * table = DefaultHandler::get_default().find_table(db, selects.relations[i]);
      if(table->table_meta().field(attr.attribute_name) != nullptr){
        count++;
        table_name = selects.relations[i];
      }
    }

    if(count != 1){
      return RC::CONDITION_ERROR;
    } else {
      attr.relation_name = const_cast<char*>(table_name);
    }
  } else if(attr.attribute_name == nullptr) {
    return RC::SCHEMA_FIELD_MISSING;
  } else {
    bool flag = false;
    for(int i = 0; i < selects.relation_num; i++){
      if(((std::string)attr.relation_name).compare((std::string)selects.relations[i]) == 0){
        flag = true;
        break;
      }
    }

    if ( !flag ) {
      return RC::SCHEMA_TABLE_NOT_EXIST;
    } else {
      Table * table = DefaultHandler::get_default().find_table(db, attr.relation_name);
      if (nullptr == table->table_meta().field(attr.attribute_name)) {
        LOG_WARN("No such field %s.%s", table->name(), attr.attribute_name);
        return RC::SCHEMA_FIELD_MISSING;
      }
    } 
  }
  return rc;
}

//add zjx[order by]b:20211103
RC ExecuteStage::do_order_check(Selects &selects, const char* db) {
  RC rc = RC::SUCCESS;

  for(size_t i = 0; i < selects.order_num; i++){
    if((rc = check_and_fix(selects, db, selects.orders[i].relAttr)) != RC::SUCCESS) {
      return rc;
    }
  }
  return rc;
} 

//add zjx[join]b:202111010
RC ExecuteStage::do_join_condition_check(Selects &selects, const char* db) {
  RC rc = RC::SUCCESS;
  // TupleSet* tmp_tupleset = new TupleSet();
  TupleSchema* tmp_schema = new TupleSchema();
  for( int i = 0;i < selects.joinnode_num; i++ ) {
    if( selects.joinnodes[i]->done_ ) {
      Table * table = DefaultHandler::get_default().find_table(db, selects.joinnodes[i]->table_name_);
      if (nullptr == table) {
        LOG_WARN("No such table [%s] in db [%s]", selects.joinnodes[i]->table_name_, db);
        return RC::SCHEMA_TABLE_NOT_EXIST;
      }
      TupleSchema::from_table(table, *tmp_schema);
      continue;
    } else {
      if((rc = do_joinnode_condition_check(selects.joinnodes[i] ,db)) != RC::SUCCESS) {
        return rc;
      }
      tmp_schema->append(*(TupleSchema*)selects.joinnodes[i]->tupleset_);
    }
  }
  selects.joinnodes[0]->tupleset_ = tmp_schema;
  return rc;
}

RC ExecuteStage::do_joinnode_condition_check(JoinNode* &joinnode ,const char* db) {
  RC rc = RC::SUCCESS;
  if( joinnode == nullptr ){
    return rc;
  }

  //????????????????????????,??????????????????????????????
  if( joinnode->done_ ) {
    Table * table = DefaultHandler::get_default().find_table(db, joinnode->table_name_);
    if (nullptr == table) {
      LOG_WARN("No such table [%s] in db [%s]", joinnode->table_name_, db);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    TupleSchema* tmp = (TupleSchema*)joinnode->tupleset_;
    TupleSchema::from_table(table, *tmp);
    joinnode->tupleset_ = tmp;
    return rc;
  }

  if((rc = do_joinnode_condition_check(joinnode->left_node_ , db)) != RC::SUCCESS ) {
    return rc;
  } else if((rc = do_joinnode_condition_check(joinnode->right_node_ , db)) != RC::SUCCESS ) {
    return rc;
  }

  TupleSchema* tmp_schema = (TupleSchema*)joinnode->left_node_->tupleset_;
  tmp_schema->append(*(TupleSchema*)joinnode->right_node_->tupleset_);
  joinnode->tupleset_ = tmp_schema;

  //??????,??????joinnode???join condition????????????
  if( joinnode->join_type_ ) {  //product??????,????????????
    return rc;
  } else {
    joinnode->join_condition;
    RelAttr left_attr = joinnode->join_condition.left_attr;
    RelAttr right_attr = joinnode->join_condition.right_attr;
    if( joinnode->join_condition.left_is_attr == 0 ) { //condition????????????
      int l_index = -1;
      if((rc = do_inner_joinnode_condition_check(tmp_schema, left_attr, l_index)) != RC::SUCCESS) {
        return rc;
      }
      const TupleField l_tmp_feild = tmp_schema->field(l_index);
      if( joinnode->join_condition.right_is_attr == 1 ) { //condition????????????,?????????
        if(!condition_type_check(joinnode->join_condition.right_value.type, l_tmp_feild.type(), joinnode->join_condition.comp_type)) {
	return RC::CONDITION_ERROR; 
	}
      } else { //condition????????????,????????????
        int r_index = -1;
        if((rc = do_inner_joinnode_condition_check(tmp_schema, left_attr, r_index)) != RC::SUCCESS) {
          return rc;
        }
	const TupleField r_tmp_feild = tmp_schema->field(r_index);
        if(!condition_type_check(l_tmp_feild.type(), r_tmp_feild.type(), joinnode->join_condition.comp_type)){
	 return RC::CONDITION_ERROR; 	
	}
      }
    } else if( joinnode->join_condition.right_is_attr == 0 ){ //condition?????????,????????????
      int r_index = -1;
      if((rc = do_inner_joinnode_condition_check(tmp_schema, right_attr, r_index)) != RC::SUCCESS) {
        return rc;
      }
	const TupleField r_tmp_feild = tmp_schema->field(r_index);
      if(!condition_type_check(joinnode->join_condition.left_value.type, r_tmp_feild.type(), joinnode->join_condition.comp_type, 1, 0)) {
      return RC::CONDITION_ERROR;
      } 
    } else {
      if(!condition_type_check(joinnode->join_condition.left_value.type, joinnode->join_condition.right_value.type,joinnode->join_condition.comp_type, 1, 1)) {
      return RC::CONDITION_ERROR;
      }
    }
  }
  return rc;
}


RC ExecuteStage::do_inner_joinnode_condition_check(TupleSchema *tmp_schema, RelAttr attr, int &index) {
  RC rc = RC::SUCCESS;
  if ( attr.relation_name == nullptr 
        || attr.attribute_name == nullptr) {
          return RC::CONDITION_ERROR;
        }
  if((index = tmp_schema->index_of_field(attr.relation_name, attr.attribute_name)) == -1) {
    return RC::CONDITION_ERROR; 
  }
  return rc;
}

bool ExecuteStage::condition_type_check( AttrType left_type, AttrType right_type, AttrType &comp_type, int l_flag, int r_flag) {
  if( left_type == right_type ) { 
    comp_type = left_type;
    return true; 
  }
  if( (left_type == INTS && right_type == FLOATS)
      ||(right_type == INTS && left_type == FLOATS) ) { 
    comp_type = FLOATS;
    return true;
  }

  if( (left_type == CHARS && right_type == DATES && r_flag == 0 && l_flag == 1 ) 
  || (right_type == CHARS && left_type == DATES && l_flag == 0 && r_flag == 1)) {
    comp_type = DATES;
    return true;
  }
  return false;
}

bool ExecuteStage::condition_judge( Condition join_condition )
{
  char *left_value = nullptr;
  char *right_value = nullptr;

  left_value = (char *)join_condition.left_value.data;
  right_value = (char *)join_condition.right_value.data;

  if(left_value == nullptr || right_value == nullptr) {return false;}

  int cmp_result = 0;
  switch (join_condition.comp_type) {
    case CHARS: {  // ???????????????????????????????????????
      // ??????C?????????????????????
      cmp_result = strcmp(left_value, right_value);
    } break;
    case INTS: {
      // ???????????????????????????
      // ???int???float??????????????????????????????,???????????????????????????????????????
      int left = *(int *)left_value;
      int right = *(int *)right_value;
      cmp_result = left - right;
    } break;
    case FLOATS: {
      float left = *(float *)left_value;
      float right = *(float *)right_value;
      cmp_result = (int)(left - right);
    } break;
    default: {
    }
  }

  switch (join_condition.comp) {
    case EQUAL_TO:
      return 0 == cmp_result;
    case LESS_EQUAL:
      return cmp_result <= 0;
    case NOT_EQUAL:
      return cmp_result != 0;
    case LESS_THAN:
      return cmp_result < 0;
    case GREAT_EQUAL:
      return cmp_result >= 0;
    case GREAT_THAN:
      return cmp_result > 0;

    default:
      break;
  }

  LOG_PANIC("Never should print this.");
  return cmp_result;  // should not go here
}

//add zjx[order by]b:20211103
RC ExecuteStage::do_order_by(Selects &selects, TupleSet* &tmp_tuple) {
  RC rc = RC::SUCCESS;

  int index_f,index_s;
  TupleSchema tmp_schema = tmp_tuple->get_schema();
  if( selects.order_num == 1){  
    if((index_f = tmp_schema.index_of_field( selects.orders[0].relAttr.relation_name, selects.orders[0].relAttr.attribute_name )) == -1) {return RC::CONDITION_ERROR; }
    tmp_tuple->sort(index_f, selects.orders[0].ordertype);
  } else if( selects.order_num == 2 ) {
    if((index_s = tmp_schema.index_of_field( selects.orders[1].relAttr.relation_name, selects.orders[1].relAttr.attribute_name )) == -1) {return RC::CONDITION_ERROR; }
    if((index_f = tmp_schema.index_of_field( selects.orders[0].relAttr.relation_name, selects.orders[0].relAttr.attribute_name )) == -1) {return RC::CONDITION_ERROR; }
    tmp_tuple->double_sort(index_f, index_s, selects.orders[0].ordertype, selects.orders[1].ordertype);
  }
  return rc;
}

// add szj [select aggregate support]20211106:b
RC ExecuteStage::get_item_value(const ColAttr* colAttr, Tuple& item, int pos, int &res_out) {
  IntValue* tmp = (IntValue*)const_cast<TupleValue*>(&(item.get(pos)));
  res_out = tmp->get();
  return RC::SUCCESS;
}
// add:e

// add szj [select aggregate support]20211106:b
// ?????????aggr_flag???true????????????????????????
RC ExecuteStage::do_aggregate( Selects selects, const char* db, TupleSet* &restuple) {
  RC rc = RC::SUCCESS;
  // ??????aggregate?????????schema
  TupleSchema schema;
  // TupleSet* final_tupleset = new TupleSet();
  // add szj [select aggregate support]20211106
  TupleSet* aggr_tupleset = new TupleSet();
  TupleSchema aggr_schema;
  // add:e
  // ?????????????????????????????????tuple
  Tuple tmp_tuple;
  LOG_INFO("Tell me the fucking attr_num %d", selects.attr_num);
  for (int i = selects.attr_num - 1; i >= 0; i--) {
    const ColAttr &attr = selects.attributes[i]; 
    if (nullptr == attr.relAttr.relation_name) {
      if ( nullptr == attr.relAttr.attribute_name) {
        return RC::SCHEMA_FIELD_MISSING;
      }
      if (0 == strcmp("*", attr.relAttr.attribute_name) || 0 == strcmp("1", attr.relAttr.attribute_name)) {
        // ???????????????????????????
        schema.append(restuple->get_schema());
        // ????????????????????????
        const char* aggr_attr_name;
        get_aggr_attr_name(&attr, aggr_attr_name);
        LOG_INFO("combined string is %s", aggr_attr_name);
        AttrType type = INTS;
        char* random_name = "fuck";
        aggr_schema.add(type, random_name, aggr_attr_name);
        LOG_INFO("adding schema success!!!");
        // ????????????
        switch(attr.aggreType) {
          case COUNTS:
          {
            int count = 0;
            for (const Tuple &item : restuple->tuples()) {
              // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
              // ??????field_meta->type()????????????????????????????????????get??????
              // IntValue* tmp = (IntValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
              count++;
            }
            tmp_tuple.add(count);
          }
        }
        
      } else {
        LOG_INFO("IN this BRANCH BaBY!!!!");
        int count = 0;
	      const char* table_name;
        for( int i = 0; i < selects.relation_num; i++) {
          Table * table = DefaultHandler::get_default().find_table(db, selects.relations[i]);
          if(table->table_meta().field(attr.relAttr.attribute_name) != nullptr){
            count++;
	        table_name = selects.relations[i];
          }
        }
        
        if(count != 1){
          return RC::SCHEMA_FIELD_MISSING;
        } else {
          LOG_INFO("Check count result wuhoo!!!!");
          Table * table = DefaultHandler::get_default().find_table(db, table_name);
          const FieldMeta *field_meta = table->table_meta().field(attr.relAttr.attribute_name);
          // schema.add(field_meta->type(), table_name, attr.relAttr.attribute_name);
          const char* aggr_attr_name;
          get_aggr_attr_name(&attr, aggr_attr_name);
          LOG_INFO("combined string is %s", aggr_attr_name);
          aggr_schema.add(field_meta->type(), table_name, aggr_attr_name);
          // ??????????????????
          int tmp_pos = restuple->get_schema().index_of_field(table_name, field_meta->name());
          LOG_INFO("Fuck pos %d!!!!", tmp_pos);
          // !!!!!!!!!!!!!!!!???????????????????????????????????????????????????INT??????!!!!!!!!!!!!!!!!!!!! //
          switch(attr.aggreType) {
            case NONE:
              break;
            case SUM:
            {
              if (field_meta->type() == INTS) {
                int sum = 0;
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  IntValue* tmp = (IntValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  sum = sum + tmp->get();
                }
                tmp_tuple.add(sum);
              }
              else if (field_meta->type() == FLOATS) {
                float sum = 0;
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  FloatValue* tmp = (FloatValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  sum = sum + tmp->get();
                }
                tmp_tuple.add(sum);
              } else if (field_meta->type() == CHARS) {
                std::string sum = "0";
                tmp_tuple.add(sum.c_str(), strlen(sum.c_str()));
              } else if (field_meta->type() == DATES) {
                std::string sum = "0";
                tmp_tuple.add(sum.c_str(), strlen(sum.c_str()));
              } else {
                // ???UNDEFINE????????????????????????
              }
              break;
            }
            case MAX:
            {
              if (field_meta->type() == INTS) {
                int max_num = -9999999;
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  IntValue* tmp = (IntValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  max_num = tmp->get() > max_num ? tmp->get() : max_num;
                }
                tmp_tuple.add(max_num);
              }
              else if (field_meta->type() == FLOATS) {
                float max_num = -999999;
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  FloatValue* tmp = (FloatValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  max_num = tmp->get() > max_num ? tmp->get() : max_num;
                }
                tmp_tuple.add(max_num);
              } else if (field_meta->type() == CHARS) {
                // ??????????????????????????????
                std::string max_str = "0";
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  StringValue* tmp = (StringValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  std::string tmp_str = tmp->get();

                  max_str = strcmp(max_str.c_str(), tmp_str.c_str()) < 0 ? tmp_str : max_str;
                }
                tmp_tuple.add(max_str.c_str(), strlen(max_str.c_str()));
              } else if (field_meta->type() == DATES) {
                // ??????????????????????????????
                std::string max_str = "0";
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  StringValue* tmp = (StringValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  std::string tmp_str = tmp->get();

                  max_str = strcmp(max_str.c_str(), tmp_str.c_str()) < 0 ? tmp_str : max_str;
                }
                tmp_tuple.add(max_str.c_str(), strlen(max_str.c_str()));
              } else {
                // ???UNDEFINE????????????????????????!!
              }
              break;
            }
            case AVG:
            {
              if (field_meta->type() == INTS) {
                // ????????????????????????????????????????????????????????????????????????
                float sum = 0;
                int count = 0;
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  IntValue* tmp = (IntValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  sum = sum + tmp->get();
                  count++;
                }
                sum = sum / count;
                LOG_INFO("Hey Boy!this is cal sum %f", sum);
                LOG_INFO("judege the dot num %f",(sum - (int)sum));
                if ((sum - (int)sum) == 0)
                  tmp_tuple.add((int)sum);
                else  
                  tmp_tuple.add(sum);
              }
              else if (field_meta->type() == FLOATS) {
                float sum = 0;
                int count = 0;
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  FloatValue* tmp = (FloatValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  sum = sum + tmp->get();
                  count++;
                }
                tmp_tuple.add(sum / count);
              } else if (field_meta->type() == CHARS) {
                std::string sum = "0";
                tmp_tuple.add(sum.c_str(), strlen(sum.c_str()));
              } else if (field_meta->type() == DATES) {
                std::string sum = "0";
                tmp_tuple.add(sum.c_str(), strlen(sum.c_str()));
              } else {
                // ???UNDEFINE????????????????????????
              }
              break;
            }
            case COUNTS:
            {
              int count = 0;
              for (const Tuple &item : restuple->tuples()) {
                // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                // ??????field_meta->type()????????????????????????????????????get??????
                count++;
              }
              tmp_tuple.add(count);
            }
          }
          
          /*
          if (attr.aggreType == SUM) {
            LOG_INFO("Do it one more time");
            do_aggr_sum(restuple, field_meta, tmp_pos, tmp_tuple);

          }
          */

          /*
          switch(attr.aggreType) {
            case MAX:
            case AVG:
            // case COUNT:
            case SUM:
              do_aggr_sum(restuple, field_meta, tmp_pos, &tmp_tuple);
              break;
            case NONE:
              break;
              //rc = ERROR;
          }
          */
        }
      }
      // add:e
    } else {
      // ???????????????????????????
      Table * table = DefaultHandler::get_default().find_table(db, attr.relAttr.relation_name);
      const FieldMeta *field_meta = table->table_meta().field(attr.relAttr.attribute_name);
      if (nullptr == field_meta) {
        LOG_WARN("No such field. %s.%s", table->name(), attr.relAttr.attribute_name);
        return RC::SCHEMA_FIELD_MISSING;
      }
      const char* aggr_attr_name;
      get_aggr_attr_name(&attr, aggr_attr_name);
      LOG_INFO("combined string is %s", aggr_attr_name);
      aggr_schema.add(field_meta->type(), attr.relAttr.relation_name, aggr_attr_name);
      // ??????????????????
      int tmp_pos = restuple->get_schema().index_of_field(attr.relAttr.relation_name, field_meta->name());

      // !!!!!!!!!!!!!!!!???????????????????????????????????????????????????INT??????!!!!!!!!!!!!!!!!!!!!
      switch(attr.aggreType) {
            case NONE:
              break;
            case SUM:
            {
              if (field_meta->type() == INTS) {
                int sum = 0;
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  IntValue* tmp = (IntValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  sum = sum + tmp->get();
                }
                tmp_tuple.add(sum);
              }
              else if (field_meta->type() == FLOATS) {
                float sum = 0;
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  FloatValue* tmp = (FloatValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  sum = sum + tmp->get();
                }
                tmp_tuple.add(sum);
              } else if (field_meta->type() == CHARS) {
                std::string sum = "0";
                tmp_tuple.add(sum.c_str(), strlen(sum.c_str()));
              } else if (field_meta->type() == DATES) {
                std::string sum = "0";
                tmp_tuple.add(sum.c_str(), strlen(sum.c_str()));
              } else {
                // ???UNDEFINE????????????????????????
              }
              break;
            }
            case MAX:
            {
              if (field_meta->type() == INTS) {
                int max_num = -9999999;
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  IntValue* tmp = (IntValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  max_num = tmp->get() > max_num ? tmp->get() : max_num;
                }
                tmp_tuple.add(max_num);
              }
              else if (field_meta->type() == FLOATS) {
                float max_num = -999999;
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  FloatValue* tmp = (FloatValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  max_num = tmp->get() > max_num ? tmp->get() : max_num;
                }
                tmp_tuple.add(max_num);
              } else if (field_meta->type() == CHARS) {
                // ??????????????????????????????
                std::string max_str = "0";
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  StringValue* tmp = (StringValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  std::string tmp_str = tmp->get();

                  max_str = strcmp(max_str.c_str(), tmp_str.c_str()) < 0 ? tmp_str : max_str;
                }
                tmp_tuple.add(max_str.c_str(), strlen(max_str.c_str()));
              } else if (field_meta->type() == DATES) {
                // ??????????????????????????????
                std::string max_str = "0";
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  StringValue* tmp = (StringValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  std::string tmp_str = tmp->get();

                  max_str = strcmp(max_str.c_str(), tmp_str.c_str()) < 0 ? tmp_str : max_str;
                }
                tmp_tuple.add(max_str.c_str(), strlen(max_str.c_str()));
              } else {
                // ???UNDEFINE????????????????????????!!
              }
              break;
            }
            case AVG:
            {
              if (field_meta->type() == INTS) {
                // ????????????????????????????????????????????????????????????????????????
                float sum = 0;
                int count = 0;
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  IntValue* tmp = (IntValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  sum = sum + tmp->get();
                  count++;
                }
                sum = sum / count;
                LOG_INFO("Hey Boy!this is cal sum %f", sum);
                LOG_INFO("judege the dot num %f",(sum - (int)sum));
                if ((sum - (int)sum) == 0)
                  tmp_tuple.add((int)sum);
                else  
                  tmp_tuple.add(sum);
              }
              else if (field_meta->type() == FLOATS) {
                float sum = 0;
                int count = 0;
                for (const Tuple &item : restuple->tuples()) {
                  // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                  // ??????field_meta->type()????????????????????????????????????get??????
                  FloatValue* tmp = (FloatValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
                  sum = sum + tmp->get();
                  count++;
                }
                tmp_tuple.add(sum / count);
              } else if (field_meta->type() == CHARS) {
                std::string sum = "0";
                tmp_tuple.add(sum.c_str(), strlen(sum.c_str()));
              } else if (field_meta->type() == DATES) {
                std::string sum = "0";
                tmp_tuple.add(sum.c_str(), strlen(sum.c_str()));
              } else {
                // ???UNDEFINE????????????????????????
              }
              break;
            }
            case COUNTS:
            {
              int count = 0;
              for (const Tuple &item : restuple->tuples()) {
                // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
                // ??????field_meta->type()????????????????????????????????????get??????
                count++;
              }
              tmp_tuple.add(count);
            }
          }
      /*
      switch(attr.aggreType) {
        case NONE:
          break;
        case SUM:
        {
          int sum = 0;
          for (const Tuple &item : restuple->tuples()) {
            // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
            // ??????field_meta->type()????????????????????????????????????get??????
            IntValue* tmp = (IntValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
            sum = sum + tmp->get();
          }
          tmp_tuple.add(sum);
          break;
        }
        case MAX:
        {
          // int max_num = INT_MIN;
          int max_num = -9999;
          for (const Tuple &item : restuple->tuples()) {
            // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
            // ??????field_meta->type()????????????????????????????????????get??????
            IntValue* tmp = (IntValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
            max_num = tmp->get() > max_num ? tmp->get() : max_num;
          }
          tmp_tuple.add(max_num);
          break;
        }
        case AVG:
        {
          float sum = 0;
          int count = 0;
          for (const Tuple &item : restuple->tuples()) {
            // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
            // ??????field_meta->type()????????????????????????????????????get??????
            IntValue* tmp = (IntValue*)const_cast<TupleValue*>(&(item.get(tmp_pos)));
            sum = sum + tmp->get();
            count++;
          }
          sum = sum / count;
          tmp_tuple.add(sum);
          break;
        }
      }
      */

    }      
  } 
  LOG_INFO("BEGIN to SET aGGRE_tupleset!!!");
  // ???tuple?????????tuple_set???
  aggr_tupleset->add(std::move(tmp_tuple));
  // printf("%d\n",schema.fields().size());
  // add szj [select aggregate support]20211106
  LOG_INFO("HEy this IS aggr_schema size %d\n",aggr_schema.fields().size());
  aggr_tupleset->set_schema(aggr_schema);
  // add:e
  // final_tupleset->set_schema(schema);

  restuple = aggr_tupleset;
  return rc;
}
// add:e

// add szj [select aggregate support]20211106:b
RC ExecuteStage::do_aggr_sum(TupleSet* restuple, const FieldMeta *field_meta, int pos, Tuple &tuple) {
  int sum = 0;
            // ????????????????????????index??????
            // ??????????????????table_NAME
            LOG_INFO("restuple.size is %d", restuple->size());
            if (restuple->size() < 0) {
              tuple.add(sum);
              return RC::SUCCESS;
            }
              
            return RC::GENERIC_ERROR;
            LOG_INFO("GETTING do_aggr_sum!!!!");
            
            /*
            for (const Tuple &item : restuple->tuples()) {
              LOG_INFO("Keep going?????");
              // const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
              // sum += *(int*)item.get_pointer(tmp_pos);
              // ??????field_meta->type()????????????????????????????????????get??????
               IntValue* tmp = (IntValue*)const_cast<TupleValue*>(&(item.get(pos)));
               sum = sum + tmp->get();
              // sum?????????????????????????????????!!!!!!!!!!!!!!
              // sum++;
            }
            */
            // ??????????????????aggr_tuple???
            // tuple.add(sum);
}
// add:e

// add szj [select aggregate support]20211106:b
RC ExecuteStage::get_aggr_attr_name(const ColAttr* colAttr, const char* &str_out) {
  LOG_INFO("Get into get_aggre_attr_name!!");
  const char* endBrace = ")";
  switch (colAttr->aggreType)
  {
    case SUM:
    {
      char tmp_str[25] = "SUM(";
      if (colAttr->relAttr.relation_name != nullptr) {
        strcat(tmp_str, colAttr->relAttr.relation_name);
        strcat(tmp_str, ".");
      }
      strcat(tmp_str, colAttr->relAttr.attribute_name);
      strcat(tmp_str, endBrace);
      LOG_INFO("combined string is %s", tmp_str);
      str_out = tmp_str;
      /* code */
      break;
    }
    case MAX:
    {
      char tmp_str[25] = "MAX(";
      if (colAttr->relAttr.relation_name != nullptr) {
        strcat(tmp_str, colAttr->relAttr.relation_name);
        strcat(tmp_str, ".");
      }
      strcat(tmp_str, colAttr->relAttr.attribute_name);
      strcat(tmp_str, endBrace);
      LOG_INFO("combined string is %s", tmp_str);
      str_out = tmp_str;
      /* code */
      break;
    }
    case AVG:
    {
      char tmp_str[25] = "AVG(";
      if (colAttr->relAttr.relation_name != nullptr) {
        strcat(tmp_str, colAttr->relAttr.relation_name);
        strcat(tmp_str, ".");
      }
      strcat(tmp_str, colAttr->relAttr.attribute_name);
      strcat(tmp_str, endBrace);
      LOG_INFO("combined string is %s", tmp_str);
      str_out = tmp_str;
      /* code */
      break;
    }
    case COUNTS:
    {
      char tmp_str[25] = "COUNT(";
      if (colAttr->relAttr.relation_name != nullptr) {
        strcat(tmp_str, colAttr->relAttr.relation_name);
        strcat(tmp_str, ".");
      }
      strcat(tmp_str, colAttr->relAttr.attribute_name);
      strcat(tmp_str, endBrace);
      LOG_INFO("combined string is %s", tmp_str);
      str_out = tmp_str;
      /* code */
      break;
    }
  }
  return RC::SUCCESS;
}
// add:e

RC ExecuteStage::do_filter( Selects selects, const char* db, TupleSet* &restuple) {
  RC rc = RC::SUCCESS;
  TupleSchema schema;
  TupleSet* final_tupleset = new TupleSet();
  for (int i = selects.attr_num - 1; i >= 0; i--) {
    const ColAttr &attr = selects.attributes[i]; // add szj [select aggregate support]20211106
    if (nullptr == attr.relAttr.relation_name) 
    {
      if ( nullptr == attr.relAttr.attribute_name) {
        return RC::SCHEMA_FIELD_MISSING;
      }
      if (0 == strcmp("*", attr.relAttr.attribute_name)) {
        // ???????????????????????????
        schema.append(restuple->get_schema());
        // ??????*??????????????????aggre_schema????????????????
      } else {
        int count = 0;
	      const char* table_name;
        for( int i = 0; i < selects.relation_num; i++) {
          Table * table = DefaultHandler::get_default().find_table(db, selects.relations[i]);
          if(table->table_meta().field(attr.relAttr.attribute_name) != nullptr){
            count++;
	        table_name = selects.relations[i];
          }
        }

        if(count != 1){
          return RC::SCHEMA_FIELD_MISSING;
        } else {
          Table * table = DefaultHandler::get_default().find_table(db, table_name);
          const FieldMeta *field_meta = table->table_meta().field(attr.relAttr.attribute_name);
          schema.add(field_meta->type(), table_name, attr.relAttr.attribute_name);
        }
      }
    } else {
      // ???????????????????????????
      Table * table = DefaultHandler::get_default().find_table(db, attr.relAttr.relation_name);
      const FieldMeta *field_meta = table->table_meta().field(attr.relAttr.attribute_name);
      if (nullptr == field_meta) {
        LOG_WARN("No such field. %s.%s", table->name(), attr.relAttr.attribute_name);
        return RC::SCHEMA_FIELD_MISSING;
      }
      schema.add(field_meta->type(), attr.relAttr.relation_name, attr.relAttr.attribute_name);
    }
  }

  final_tupleset->set_schema(schema);

  std::vector<int> pos_record;
  int tmp_pos;
  for (int i = 0; i < schema.fields().size(); i++){
    tmp_pos = restuple->get_schema().index_of_field(schema.field(i).table_name(), schema.field(i).field_name());
    pos_record.push_back(tmp_pos);
  }

  for (const Tuple &item : restuple->tuples()) {
    Tuple tmp_tuple;
    const std::vector<std::shared_ptr<TupleValue>> &values = item.values();
    for (std::vector<int>::iterator iter = pos_record.begin(); iter != pos_record.end(); iter++) {
      tmp_tuple.add(std::move(item.get_pointer(*iter)));
    }
    final_tupleset->add(std::move(tmp_tuple));
  }

  restuple = final_tupleset;
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

// ??????????????????????????????????????????condition?????????????????????????????????select ????????????
RC create_selection_executor(Trx *trx, const Selects &selects, const char *db, const char *table_name, SelectExeNode &select_node) {
  // ???????????????????????????Attr
  TupleSchema schema;
  Table * table = DefaultHandler::get_default().find_table(db, table_name);
  if (nullptr == table) {
    LOG_WARN("No such table [%s] in db [%s]", table_name, db);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  for (int i = selects.attr_num - 1; i >= 0; i--) {
    // add szj [select aggregate support]20211106:b
    const ColAttr &attr = selects.attributes[i]; 
    // const RelAttr &attr = selects.attributes[i]; 
    // if (nullptr == attr.relation_name || 0 == strcmp(table_name, attr.relation_name)) {
    if (nullptr == attr.relAttr.relation_name || 0 == strcmp(table_name, attr.relAttr.relation_name)) {
      if (0 == strcmp("*", attr.relAttr.attribute_name) || 0 == strcmp("1", attr.relAttr.attribute_name)) {
        break; 
      } else {
        if (nullptr == table->table_meta().field(attr.relAttr.attribute_name)) {
          LOG_WARN("No such field. %s.%s", table->name(), attr.relAttr.attribute_name);
          // add:e
          return RC::SCHEMA_FIELD_MISSING;
        }
      }
    }
  }
  TupleSchema::from_table(table, schema);

  // ???????????????????????????????????????, ??????????????????????????????
  std::vector<DefaultConditionFilter *> condition_filters;
  for (size_t i = 0; i < selects.condition_num; i++) {
    const Condition &condition = selects.conditions[i];
    if ((condition.left_is_attr == 0 && condition.right_is_attr == 0) || // ???????????????
        (condition.left_is_attr == 1 && condition.right_is_attr == 0 && match_table(selects, condition.left_attr.relation_name, table_name)) ||  // ???????????????????????????
        (condition.left_is_attr == 0 && condition.right_is_attr == 1 && match_table(selects, condition.right_attr.relation_name, table_name)) ||  // ?????????????????????????????????
        (condition.left_is_attr == 1 && condition.right_is_attr == 1 &&
            match_table(selects, condition.left_attr.relation_name, table_name) && match_table(selects, condition.right_attr.relation_name, table_name)) // ?????????????????????????????????????????????
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
