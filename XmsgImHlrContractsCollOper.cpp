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
#include "XmsgImHlrContractsCollOper.h"
#include "XmsgImHlrDb.h"

XmsgImHlrContractsCollOper* XmsgImHlrContractsCollOper::inst = new XmsgImHlrContractsCollOper();

XmsgImHlrContractsCollOper::XmsgImHlrContractsCollOper()
{

}

XmsgImHlrContractsCollOper* XmsgImHlrContractsCollOper::instance()
{
	return XmsgImHlrContractsCollOper::inst;
}

bool XmsgImHlrContractsCollOper::load(void (*loadCb)(shared_ptr<XmsgImHlrContractsColl> coll))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s", XmsgImHlrDb::xmsgImHlrContractsColl.c_str())
	bool ret = MysqlMisc::query(conn, sql, [loadCb](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImHlrDb::xmsgImHlrContractsColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImHlrDb::xmsgImHlrContractsColl.c_str())
			return true;
		}
		auto coll = XmsgImHlrContractsCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgImHlrDb::xmsgImHlrContractsColl.c_str(), row->toString().c_str())
			return false; 
		}
		loadCb(coll);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImHlrContractsCollOper::insert(shared_ptr<XmsgImHlrContractsColl> coll)
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

bool XmsgImHlrContractsCollOper::insert(void* conn, shared_ptr<XmsgImHlrContractsColl> coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?)", XmsgImHlrDb::xmsgImHlrContractsColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->cgt->toString()) 
	->addVarchar(coll->ctp->toString()) 
	->addBlob(coll->info->SerializeAsString()) 
	->addDateTime(coll->gts) 
	->addDateTime(coll->uts);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrContractsColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrContractsColl.c_str(), coll->toString().c_str())
	});
}

bool XmsgImHlrContractsCollOper::update(shared_ptr<XmsgImHlrContractsColl> coll)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, coll: %s", coll->toString().c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "update %s set info = ?, uts = ? where cgt = ? and ctp = ?", XmsgImHlrDb::xmsgImHlrContractsColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addBlob(coll->info->SerializeAsString()) 
	->addDateTime(coll->uts) 
	->addVarchar(coll->cgt->toString()) 
	->addVarchar(coll->ctp->toString());
	bool ret = MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("update %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrContractsColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("update %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrContractsColl.c_str(), coll->toString().c_str())
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImHlrContractsCollOper::del(SptrCgt cgt, SptrCgt ctp)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, cgt: %s, ctp: %s", cgt->toString().c_str(), ctp->toString().c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "delete from %s where cgt = ? and ctp = ?", XmsgImHlrDb::xmsgImHlrContractsColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(cgt->toString()) 
	->addVarchar(ctp->toString());
	bool ret = MysqlMisc::sql((MYSQL*) conn, req, [cgt, ctp](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("delete from %s.%s failed, cgt: %s, ctp: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrContractsColl.c_str(), cgt->toString().c_str(), ctp->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("delete from %s.%s successful, cgt: %s, ctp: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrContractsColl.c_str(), cgt->toString().c_str(), ctp->toString().c_str())
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

shared_ptr<XmsgImHlrContractsColl> XmsgImHlrContractsCollOper::loadOneFromIter(void* it)
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
	ullong uts;
	if (!row->getLong("uts", uts))
	{
		LOG_ERROR("can not found field: uts, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgImHlrContractsColl> coll(new XmsgImHlrContractsColl());
	coll->cgt = cgt;
	coll->ctp = ctp;
	coll->info = info;
	coll->gts = gts;
	coll->uts = uts;
	return coll;
}

XmsgImHlrContractsCollOper::~XmsgImHlrContractsCollOper()
{

}

