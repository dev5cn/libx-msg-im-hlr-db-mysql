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

#ifndef XMSGIMHLRUSREVENTCOLLOPER_H_
#define XMSGIMHLRUSREVENTCOLLOPER_H_

#include <libx-msg-im-hlr-core.h>

class XmsgImHlrUsrEventCollOper
{
public:
	bool load(void (*loadCb)(shared_ptr<XmsgImHlrUsrEventColl> coll)); 
	bool loadUsrMaxVer(void (*loadCb)(SptrCgt cgt, ullong ver)); 
	bool insert(shared_ptr<XmsgImHlrUsrEventColl> coll); 
	bool insert(void* conn, shared_ptr<XmsgImHlrUsrEventColl> coll); 
	bool eventRead(SptrCgt cgt, ullong ver); 
	static XmsgImHlrUsrEventCollOper* instance();
private:
	static XmsgImHlrUsrEventCollOper* inst;
	shared_ptr<XmsgImHlrUsrEventColl> loadOneFromIter(void* it); 
	XmsgImHlrUsrEventCollOper();
	virtual ~XmsgImHlrUsrEventCollOper();
};

#endif 
