/*
  Copyright 2019 www.dev5.cn, Inc. dev5@qq.com
 
  This file is part of X-MSG-IM.
 
  X-MSG-IM is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  X-MSG-IM is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
 
  You should have received a copy of the GNU Affero General Public License
  along with X-MSG-IM.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <libmisc-mysql-c.h>
#include "XmsgImHlrSysEventCollOper.h"
#include "XmsgImHlrDb.h"

XmsgImHlrSysEventCollOper* XmsgImHlrSysEventCollOper::inst = new XmsgImHlrSysEventCollOper();

XmsgImHlrSysEventCollOper::XmsgImHlrSysEventCollOper()
{

}

XmsgImHlrSysEventCollOper* XmsgImHlrSysEventCollOper::instance()
{
	return XmsgImHlrSysEventCollOper::inst;
}

bool XmsgImHlrSysEventCollOper::load(void (*loadCb)(shared_ptr<XmsgImHlrSysEventColl> coll))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s where ets < '%s' order by ver asc" ,XmsgImHlrDb::xmsgImHlrSysEventColl.c_str(), DateMisc::to_yyyy_mm_dd_hh_mi_ss(Xsc::clock / 1000L).c_str())
	bool ret = MysqlMisc::query(conn, sql, [loadCb](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row) 
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImHlrDb::xmsgImHlrSysEventColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImHlrDb::xmsgImHlrSysEventColl.c_str())
			return true;
		}
		auto coll = XmsgImHlrSysEventCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgImHlrDb::xmsgImHlrSysEventColl.c_str(), row->toString().c_str())
			return false; 
		}
		loadCb(coll);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImHlrSysEventCollOper::insert(shared_ptr<XmsgImHlrSysEventColl> coll)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	bool ret = this->insert(conn, coll);
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImHlrSysEventCollOper::insert(void* conn, shared_ptr<XmsgImHlrSysEventColl> coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?)", XmsgImHlrDb::xmsgImHlrSysEventColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addLong((ullong) coll->ent) 
	->addLong(coll->ver) 
	->addVarchar(coll->msg) 
	->addBlob(coll->dat) 
	->addDateTime(coll->gts) 
	->addDateTime(coll->ets);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrSysEventColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrSysEventColl.c_str(), coll->toString().c_str())
	});
}

bool XmsgImHlrSysEventCollOper::maxVer(ullong& ver)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select max(ver) from %s", XmsgImHlrDb::xmsgImHlrSysEventColl.c_str())
	bool ret = MysqlMisc::longVal(conn, sql, ver);
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

shared_ptr<XmsgImHlrSysEventColl> XmsgImHlrSysEventCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
	uint ent;
	if (!row->getInt("ent", ent))
	{
		LOG_ERROR("can not found field: ent")
		return nullptr;
	}
	if (!XmsgImHlrUsrEventColl::isValidEnt(ent))
	{
		LOG_ERROR("XmsgUsrEventNoticeType field format error, ent: %d", ent)
		return nullptr;
	}
	ullong ver;
	if (!row->getLong("ver", ver))
	{
		LOG_ERROR("can not found field: ver")
		return nullptr;
	}
	string msg;
	if (!row->getStr("msg", msg))
	{
		LOG_ERROR("can not found field: msg")
		return nullptr;
	}
	string dat;
	if (!row->getBin("dat", dat))
	{
		LOG_ERROR("can not found field: dat")
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts")
		return nullptr;
	}
	ullong ets;
	if (!row->getLong("ets", ets))
	{
		LOG_ERROR("can not found field: ets")
		return nullptr;
	}
	shared_ptr<XmsgImHlrSysEventColl> coll(new XmsgImHlrSysEventColl());
	coll->ent = (XmsgUsrEventNoticeType) ent;
	coll->ver = ver;
	coll->msg = msg;
	coll->dat = dat;
	coll->gts = gts;
	coll->ets = ets;
	return coll;
}

XmsgImHlrSysEventCollOper::~XmsgImHlrSysEventCollOper()
{

}

