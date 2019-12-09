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

#ifndef XMSGIMHLRDB_H_
#define XMSGIMHLRDB_H_

#include <libx-msg-im-hlr-core.h>
#include <libx-msg-im-hlr-pb.h>

class XmsgImHlrDb
{
public:
	bool load(); 
	void future(function<void()> cb); 
	static XmsgImHlrDb* instance();
public:
	static string xmsgImHlrCfgColl; 
	static string xmsgImHlrContractsBlackListColl; 
	static string xmsgImHlrContractsColl; 
	static string xmsgImHlrUsrDatColl; 
	static string xmsgImHlrUsrEventColl; 
	static string xmsgImHlrSysEventColl; 
private:
	shared_ptr<ActorBlockingSingleThread> abst; 
	static XmsgImHlrDb* inst;
	bool initCfg(); 
	XmsgImHlrDb();
	virtual ~XmsgImHlrDb();
};

#endif 
