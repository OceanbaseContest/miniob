/*
 * @Description: 
 * @version: 
 * @Author: MoonKnight
 * @Date: 2021-10-28 20:56:08
 * @LastEditors: MoonKnight
 * @LastEditTime: 2021-10-29 10:56:19
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
  RC do_aggregate(std::vector<TupleSet>* &tuple_sets);
  RC do_union(TupleSet* &res_tuple_set, TupleSet* &new_tupleset);
  RC do_optimize(Query* sql, int size, std::map<std::string, TupleSet*>* name_tupleset_map, JoinNode* &res_node);
  RC load_tupleset(JoinNode* &node, std::map<std::string, TupleSet*>* name_tupleset_map);
  //e:20211020
protected:
private:
  Stage *default_storage_stage_ = nullptr;
  Stage *mem_storage_stage_ = nullptr;
};

#endif //__OBSERVER_SQL_EXECUTE_STAGE_H__
