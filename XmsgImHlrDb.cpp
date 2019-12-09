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
#include "XmsgImHlrCfgCollOper.h"
#include "XmsgImHlrContractsBlackListCollOper.h"
#include "XmsgImHlrContractsCollOper.h"
#include "XmsgImHlrDb.h"
#include "XmsgImHlrSysEventCollOper.h"
#include "XmsgImHlrUsrDatCollOper.h"
#include "XmsgImHlrUsrEventCollOper.h"

XmsgImHlrDb* XmsgImHlrDb::inst = new XmsgImHlrDb();
string XmsgImHlrDb::xmsgImHlrCfgColl = "tb_x_msg_im_hlr_cfg"; 
string XmsgImHlrDb::xmsgImHlrContractsBlackListColl = "tb_x_msg_im_hlr_contracts_black_list"; 
string XmsgImHlrDb::xmsgImHlrContractsColl = "tb_x_msg_im_hlr_contracts"; 
string XmsgImHlrDb::xmsgImHlrUsrDatColl = "tb_x_msg_im_hlr_usr_dat"; 
string XmsgImHlrDb::xmsgImHlrUsrEventColl = "tb_x_msg_im_hlr_usr_event"; 
string XmsgImHlrDb::xmsgImHlrSysEventColl = "tb_x_msg_im_hlr_sys_event"; 

XmsgImHlrDb::XmsgImHlrDb()
{

}

XmsgImHlrDb* XmsgImHlrDb::instance()
{
	return XmsgImHlrDb::inst;
}

bool XmsgImHlrDb::load()
{
	auto& cfg = XmsgImHlrCfg::instance()->cfgPb->mysql();
	if (!MysqlConnPool::instance()->init(cfg.host(), cfg.port(), cfg.db(), cfg.usr(), cfg.password(), cfg.poolsize()))
		return false;
	LOG_INFO("init mysql connection pool successful, host: %s:%d, db: %s", cfg.host().c_str(), cfg.port(), cfg.db().c_str())
	if ("mysql" == XmsgImHlrCfg::instance()->cfgPb->cfgtype() && !this->initCfg())
		return false;
	ullong sts = DateMisc::dida();
	if (!XmsgImHlrUsrDatCollOper::instance()->load(XmsgImUsrMgr::loadCb))
		return false;
	LOG_INFO("load %s.%s successful, count: %zu, elap: %dms", cfg.db().c_str(), XmsgImHlrDb::xmsgImHlrUsrDatColl.c_str(), XmsgImUsrMgr::instance()->size4usr(), DateMisc::elapDida(sts))
	XmsgImSysEvent::init();
	sts = DateMisc::dida();
	if (!XmsgImHlrSysEventCollOper::instance()->load(XmsgImSysEvent::loadCb))
		return false;
	LOG_INFO("load %s.%s successful, elap: %dms", cfg.db().c_str(), XmsgImHlrDb::xmsgImHlrSysEventColl.c_str(), DateMisc::elapDida(sts))
	ullong ver = 0ULL;
	if (!XmsgImHlrSysEventCollOper::instance()->maxVer(ver)) 
		return false;
	XmsgImSysEvent::instance()->setMaxVersion(ver);
	sts = DateMisc::dida();
	if (!XmsgImHlrUsrEventCollOper::instance()->load(XmsgImUsrEvent::loadCb))
		return false;
	LOG_INFO("load %s.%s successful, elap: %dms", cfg.db().c_str(), XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str(), DateMisc::elapDida(sts))
	sts = DateMisc::dida();
	if (!XmsgImHlrUsrEventCollOper::instance()->loadUsrMaxVer(XmsgImUsrEvent::loadCb4maxVer)) 
		return false;
	LOG_INFO("load %s.%s usr event max version successful, elap: %dms", cfg.db().c_str(), XmsgImHlrDb::xmsgImHlrUsrEventColl.c_str(), DateMisc::elapDida(sts))
	sts = DateMisc::dida();
	if (!XmsgImHlrContractsCollOper::instance()->load(XmsgImHlrContractsMgr::loadCb4contracts))
		return false;
	LOG_INFO("load %s.%s successful, elap: %dms", cfg.db().c_str(), XmsgImHlrDb::xmsgImHlrContractsColl.c_str(), DateMisc::elapDida(sts))
	sts = DateMisc::dida();
	if (!XmsgImHlrContractsBlackListCollOper::instance()->load(XmsgImHlrContractsMgr::loadCb4contractsBlackList))
		return false;
	LOG_INFO("load %s.%s successful, elap: %dms", cfg.db().c_str(), XmsgImHlrDb::xmsgImHlrContractsBlackListColl.c_str(), DateMisc::elapDida(sts))
	this->abst.reset(new ActorBlockingSingleThread("hlr-db"));
	return true;
}

void XmsgImHlrDb::future(function<void()> cb)
{
	this->abst->future(cb);
}

bool XmsgImHlrDb::initCfg()
{
	shared_ptr<XmsgImHlrCfgColl> coll = XmsgImHlrCfgCollOper::instance()->load();
	if (coll == NULL)
		return false;
	LOG_INFO("got a x-msg-im-hlr config from db: %s", coll->toString().c_str())
	XmsgImHlrCfg::instance()->cfgPb = coll->cfg;
	return true;
}

XmsgImHlrDb::~XmsgImHlrDb()
{

}

