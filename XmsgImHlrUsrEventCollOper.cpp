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
#include "XmsgImHlrUsrEventCollOper.h"
#include "XmsgImHlrDb.h"

XmsgImHlrUsrEventCollOper* XmsgImHlrUsrEventCollOper::inst = new XmsgImHlrUsrEventCollOper();

XmsgImHlrUsrEventCollOper::XmsgImHlrUsrEventCollOper()
{

}

XmsgImHlrUsrEventCollOper* XmsgImHlrUsrEventCollOper::instance()
{
	return XmsgImHlrUsrEventCollOper::inst;
}

bool XmsgImHlrUsrEventCollOper::load(void (*loadCb)(shared_ptr<XmsgImHlrUsrEventColl> coll))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s where isRead != 0 and ets < '%s' order by ver asc" ,XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str(), DateMisc::to_yyyy_mm_dd_hh_mi_ss(Xsc::clock / 1000L).c_str())
	bool ret = MysqlMisc::query(conn, sql, [loadCb](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row) 
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str())
			return true;
		}
		auto coll = XmsgImHlrUsrEventCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str(), row->toString().c_str())
			return false; 
		}
		loadCb(coll);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImHlrUsrEventCollOper::loadUsrMaxVer(void (*loadCb)(SptrCgt cgt, ullong ver))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select cgt, max(ver) from %s group by cgt", XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str())
	bool ret = MysqlMisc::query(conn, sql, [loadCb](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row) 
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str())
			return true;
		}
		string str;
		if (!row->getStr(0, str))
		{
			LOG_ERROR("can not found field cgt, table: %s", XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str())
			return false;
		}
		SptrCgt cgt = ChannelGlobalTitle::parse(str);
		if (cgt == nullptr)
		{
			LOG_ERROR("channel global title format error, cgt: %s, table: %s", str.c_str(), XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str())
			return false;
		}
		ullong ver;
		if (!row->getLong(1, ver))
		{
			LOG_ERROR("can not found field ver, table: %s", XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str())
			return false;
		}
		loadCb(cgt, ver);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImHlrUsrEventCollOper::insert(shared_ptr<XmsgImHlrUsrEventColl> coll)
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

bool XmsgImHlrUsrEventCollOper::insert(void* conn, shared_ptr<XmsgImHlrUsrEventColl> coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?, ?, ?, ?)", XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->cgt->toString()) 
	->addBool(coll->isRead) 
	->addLong((ullong) coll->ent) 
	->addLong(coll->ver) 
	->addVarchar(coll->msg) 
	->addBlob(coll->dat) 
	->addDateTime(coll->gts) 
	->addDateTime(coll->uts) 
	->addDateTime(coll->ets);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str(), coll->toString().c_str())
	});
}

bool XmsgImHlrUsrEventCollOper::eventRead(SptrCgt cgt, ullong ver)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, cgt: %s, ver: %llu", cgt->toString().c_str(), ver)
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "update %s set isRead = ? where cgt = ? and ver <= ?", XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addBool(true) 
	->addVarchar(cgt->toString()) 
	->addLong(ver);
	bool ret = MysqlMisc::sql((MYSQL*) conn, req, [cgt, ver](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("update %s.%s failed, cgt: %s, ver: %llu, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str(), cgt->toString().c_str(), ver, ret, desc.c_str())
			return;
		}
		LOG_TRACE("update %s.%s successful, cgt: %s, ver: %llu", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str(), cgt->toString().c_str(), ver)
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

shared_ptr<XmsgImHlrUsrEventColl> XmsgImHlrUsrEventCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
	string str;
	if (!row->getStr("cgt", str))
	{
		LOG_ERROR("can not found field: cgt")
		return nullptr;
	}
	SptrCgt cgt = ChannelGlobalTitle::parse(str);
	if (cgt == nullptr)
	{
		LOG_ERROR("cgt format error, cgt: %s", str.c_str())
		return nullptr;
	}
	bool isRead;
	if (!row->getBool("isRead", isRead))
	{
		LOG_ERROR("can not found field: isRead, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	uint ent;
	if (!row->getInt("ent", ent))
	{
		LOG_ERROR("can not found field: ent, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	if (!XmsgImHlrUsrEventColl::isValidEnt(ent))
	{
		LOG_ERROR("XmsgUsrEventNoticeType field format error, ent: %d, cgt: %s", ent, cgt->toString().c_str())
		return nullptr;
	}
	ullong ver;
	if (!row->getLong("ver", ver))
	{
		LOG_ERROR("can not found field: ver, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	string msg;
	if (!row->getStr("msg", msg))
	{
		LOG_ERROR("can not found field: msg, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	string dat;
	if (!row->getBin("dat", dat))
	{
		LOG_ERROR("can not found field: dat, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	ullong uts;
	if (!row->getLong("uts", uts))
	{
		LOG_ERROR("can not found field: uts, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	ullong ets;
	if (!row->getLong("ets", ets))
	{
		LOG_ERROR("can not found field: ets, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgImHlrUsrEventColl> coll(new XmsgImHlrUsrEventColl());
	coll->cgt = cgt;
	coll->isRead = isRead;
	coll->ent = (XmsgUsrEventNoticeType) ent;
	coll->ver = ver;
	coll->msg = msg;
	coll->dat = dat;
	coll->gts = gts;
	coll->uts = uts;
	coll->ets = ets;
	return coll;
}

XmsgImHlrUsrEventCollOper::~XmsgImHlrUsrEventCollOper()
{

}

