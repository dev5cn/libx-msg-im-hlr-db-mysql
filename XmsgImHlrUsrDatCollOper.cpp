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
#include "XmsgImHlrUsrDatCollOper.h"
#include "XmsgImHlrDb.h"

XmsgImHlrUsrDatCollOper* XmsgImHlrUsrDatCollOper::inst = new XmsgImHlrUsrDatCollOper();

XmsgImHlrUsrDatCollOper::XmsgImHlrUsrDatCollOper()
{

}

XmsgImHlrUsrDatCollOper* XmsgImHlrUsrDatCollOper::instance()
{
	return XmsgImHlrUsrDatCollOper::inst;
}

bool XmsgImHlrUsrDatCollOper::load(void (*loadCb)(shared_ptr<XmsgImHlrUsrDatColl> dat))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s", XmsgImHlrDb::xmsgImHlrUsrDatColl.c_str())
	bool ret = MysqlMisc::query(conn, sql, [loadCb](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImHlrDb::xmsgImHlrUsrDatColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImHlrDb::xmsgImHlrUsrDatColl.c_str())
			return true;
		}
		auto coll = XmsgImHlrUsrDatCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgImHlrDb::xmsgImHlrUsrDatColl.c_str(), row->toString().c_str())
			return false; 
		}
		loadCb(coll);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImHlrUsrDatCollOper::insert(shared_ptr<XmsgImHlrUsrDatColl> coll)
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

bool XmsgImHlrUsrDatCollOper::insert(void* conn, shared_ptr<XmsgImHlrUsrDatColl> coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?)", XmsgImHlrDb::xmsgImHlrUsrDatColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->cgt->toString()) 
	->addLong(coll->ver) 
	->addBlob(coll->pri->SerializeAsString()) 
	->addBlob(coll->pub->SerializeAsString()) 
	->addDateTime(coll->gts) 
	->addDateTime(coll->uts);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrUsrDatColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImHlrDb::xmsgImHlrUsrDatColl.c_str(), coll->toString().c_str())
	});
}

shared_ptr<XmsgImHlrUsrDatColl> XmsgImHlrUsrDatCollOper::loadOneFromIter(void* it)
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
	ullong ver;
	if (!row->getLong("ver", ver))
	{
		LOG_ERROR("can not found field: ver, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	if (!row->getBin("pri", str))
	{
		LOG_ERROR("can not found field: pri, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgImHlrUsrDatPri> pri(new XmsgImHlrUsrDatPri());
	if (!pri->ParseFromString(str))
	{
		LOG_ERROR("pri format error, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	if (!row->getBin("pub", str))
	{
		LOG_ERROR("can not found field: pub, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgImHlrUsrDatPub> pub(new XmsgImHlrUsrDatPub());
	if (!pub->ParseFromString(str))
	{
		LOG_ERROR("pub format error, cgt: %s", cgt->toString().c_str())
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
	shared_ptr<XmsgImHlrUsrDatColl> coll(new XmsgImHlrUsrDatColl());
	coll->cgt = cgt;
	coll->ver = ver;
	coll->pri = pri;
	coll->pub = pub;
	coll->gts = gts;
	coll->uts = uts;
	return coll;
}

shared_ptr<XmsgImHlrUsrDatColl> XmsgImHlrUsrDatCollOper::initXmsgImHlrUsrDatColl(SptrCgt cgt)
{
	shared_ptr<XmsgImHlrUsrDatColl> coll(new XmsgImHlrUsrDatColl());
	coll->cgt = cgt;
	coll->ver = 0ULL;
	coll->gts = DateMisc::nowGmt0();
	coll->uts = coll->gts;
	coll->pri.reset(new XmsgImHlrUsrDatPri());
	coll->pri->set_enable(true);
	coll->pub.reset(new XmsgImHlrUsrDatPub());
	return coll;
}

XmsgImHlrUsrDatCollOper::~XmsgImHlrUsrDatCollOper()
{

}

