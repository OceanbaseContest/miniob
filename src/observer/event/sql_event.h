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
// Created by Longda on 2021/4/14.
//

#ifndef __OBSERVER_SQL_EVENT_SQLEVENT_H__
#define __OBSERVER_SQL_EVENT_SQLEVENT_H__

#include "common/seda/stage_event.h"
#include <string>

class SessionEvent;

class SQLStageEvent : public common::StageEvent {
public:
  SQLStageEvent(SessionEvent *event, std::string &sql);
  virtual ~SQLStageEvent() noexcept;

  const std::string &get_sql() const {
    return sql_;
  }

  SessionEvent * session_event() const {
    return session_event_;
  }
  //add zjx[select]b:20211023
  SQLStageEvent * next_sql_event() {return next_sqlstageevent_;}
  
private:
  SessionEvent *session_event_;
  std::string & sql_;
  // void *context_;
  //add zjx[select]b:20211023
  //存在多个sql语句时，比如uniion的情况，需要多个SQLStageEvent保存对应的TupleSet
  SQLStageEvent * next_sqlstageevent_;
};

#endif //__SRC_OBSERVER_SQL_EVENT_SQLEVENT_H__
