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
#include "XmsgImHlrContractsBlackListCollOper.h"
#include "XmsgImHlrDb.h"

XmsgImHlrContractsBlackListCollOper* XmsgImHlrContractsBlackListCollOper::inst = new XmsgImHlrContractsBlackListCollOper();

XmsgImHlrContractsBlackListCollOper::XmsgImHlrContractsBlackListCollOper()
{

}

XmsgImHlrContractsBlackListCollOper* XmsgImHlrContractsBlackListCollOper::instance()
{
	return XmsgImHlrContractsBlackListCollOper::inst;
}

bool XmsgImHlrContractsBlackListCollOper::load(void (*loadCb)(shared_ptr<XmsgImHlrContractsBlackListColl> coll))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s", XmsgImHlrDb::xmsgImHlrContractsBlackListColl.c_str())
	bool ret = MysqlMisc::query(conn, sql, [loadCb](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImHlrDb::xmsgImHlrContractsBlackListColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImHlrDb::xmsgImHlrContractsBlackListColl.c_str())
			return true;
		}
		auto coll = XmsgImHlrContractsBlackListCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgImHlrDb::xmsgImHlrContractsBlackListColl.c_str(), row->toString().c_str())
			return false; 
		}
		loadCb(coll);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImHlrContractsBlackListCollOper::insert(shared_ptr<XmsgImHlrContractsBlackListColl> coll)
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

bool XmsgImHlrContractsBlackListCollOper::insert(void* conn, shared_ptr<XmsgImHlrContractsBlackListColl> coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?)", XmsgImHlrDb::xmsgImHlrContractsBlackListColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->cgt->toString()) 
	->addVarchar(coll->ctp->toString()) 
	->addBlob(coll->info->SerializeAsString()) 
	->addDateTime(coll->gts);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrContractsBlackListColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrContractsBlackListColl.c_str(), coll->toString().c_str())
	});
}

shared_ptr<XmsgImHlrContractsBlackListColl> XmsgImHlrContractsBlackListCollOper::loadOneFromIter(void* it)
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
	if (!row->getStr("ctp", str))
	{
		LOG_ERROR("can not found field: ctp, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	SptrCgt ctp = ChannelGlobalTitle::parse(str);
	if (ctp == nullptr)
	{
		LOG_ERROR("cpt format error, cgt: %s, cgt: %s", str.c_str(), cgt->toString().c_str())
		return nullptr;
	}
	if (!row->getBin("info", str))
	{
		LOG_ERROR("can not found field: info, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgKv> info(new XmsgKv());
	if (!info->ParseFromString(str))
	{
		LOG_ERROR("info format error, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgImHlrContractsBlackListColl> coll(new XmsgImHlrContractsBlackListColl());
	coll->cgt = cgt;
	coll->ctp = ctp;
	coll->info = info;
	coll->gts = gts;
	return coll;
}

XmsgImHlrContractsBlackListCollOper::~XmsgImHlrContractsBlackListCollOper()
{

}

