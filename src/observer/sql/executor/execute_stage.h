/*
 * @Description: 
 * @version: 
 * @Author: MoonKnight
 * @Date: 2021-10-28 20:56:08
 * @LastEditors: MoonKnight
 * @LastEditTime: 2021-11-18 10:38:49
 */
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

#ifndef __OBSERVER_SQL_EXECUTE_STAGE_H__
#define __OBSERVER_SQL_EXECUTE_STAGE_H__

#include "common/seda/stage.h"
#include "sql/parser/parse.h"
#include "rc.h"
#include <vector>
#include <string.h>
// add szj [select aggregate support]20211106:b
#include "sql/executor/tuple.h"
#include "storage/common/field_meta.h"
// add:e

#include <algorithm> //add zjx[check]b:20211102

class TupleSet;

class SessionEvent;

class ExecuteStage : public common::Stage {
public:
  ~ExecuteStage();
  static Stage *make_stage(const std::string &tag);

protected:
  // common function
  ExecuteStage(const char *tag);
  bool set_properties() override;

  bool initialize() override;
  void cleanup() override;
  void handle_event(common::StageEvent *event) override;
  void callback_event(common::StageEvent *event,
                     common::CallbackContext *context) override;

  void handle_request(common::StageEvent *event);
  RC do_select(const char *db, Query *sql, SessionEvent *session_event);

  //add zjx[select]b:20211020
  RC do_join_product(JoinNode* &optimized_join_tree);
  RC do_join(JoinNode* &optimized_join_tree);
  RC do_product(JoinNode* &optimized_join_tree);
  // RC do_aggregate(ColAttr *&colAttr, TupleSet *&tuple_sets); // add szj [select aggregate support]20211106
  RC do_union(TupleSet* &res_tuple_set, TupleSet* &new_tupleset);
  RC do_optimize(Query* sql, int size, std::map<std::string, TupleSet*>* name_tupleset_map, JoinNode* &res_node);
  RC do_filter(const Selects selects, const char *db, TupleSet* &restuple);
  // add szj [select aggregate support]20211106:b
  RC do_aggregate(const Selects selects, const char *db, TupleSet* &restuple);
  RC do_aggr_sum(TupleSet* restuple, const FieldMeta *field_meta, int pos, Tuple &tuple);
  RC get_aggr_attr_name(const ColAttr* colAttr, const char* &str_out);
  RC get_item_value(const ColAttr* colAttr, Tuple &item, int pos, int& res_out);
  // add:e
  //e:20211020
  //add bzb [drop table] 20211022:b
  RC do_drop_table(const char *db, Query *sql, SessionEvent *session_event);
  //20211022:e
private:
  RC load_tupleset(JoinNode* &node, std::map<std::string, TupleSet*>* name_tupleset_map);
  RC do_check(Selects &selects, const char* db, SessionEvent *&session_event); //add zjx[check]b:20211114
  RC do_select_attr_check(Selects &selects, const char* db, SessionEvent *&session_event);//add zjx[check]b:20211116
  RC do_where_condition_check(Selects &selects, const char* db);//add zjx[check]b:20211114
  RC check_and_fix(Selects &selects, const char* db, RelAttr &left_attr);//add zjx[select]b:20211114
  //add zjx[join]b:20211110
  RC do_join_condition_check(Selects &selects, const char* db);
  RC do_joinnode_condition_check(JoinNode* &joinnode ,const char* db);
  RC do_inner_joinnode_condition_check(TupleSchema *tmp_schema, RelAttr attr, int &index);
  bool condition_type_check( AttrType left_type, AttrType right_type, AttrType &comp_type, int l_flag = 0, int r_flag = 0);
  bool condition_judge(Condition join_condition);
  //add zjx[order by]b:20211103
  RC do_order_check(Selects &selects, const char* db); 
  RC do_order_by(Selects &selects, TupleSet* &tmp_tuple);
protected:
private:
  Stage *default_storage_stage_ = nullptr;
  Stage *mem_storage_stage_ = nullptr;
};

#endif //__OBSERVER_SQL_EXECUTE_STAGE_H__
