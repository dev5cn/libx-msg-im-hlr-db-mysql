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

#ifndef XMSGIMHLRCFGCOLLOPER_H_
#define XMSGIMHLRCFGCOLLOPER_H_

#include <libx-msg-im-hlr-core.h>
#include <libx-msg-common-dat-struct-cpp.h>

class XmsgImHlrCfgCollOper
{
public:
	shared_ptr<XmsgImHlrCfgColl> load(); 
	bool insert(const string& cgt, shared_ptr<XmsgImHlrCfgColl> cfg); 
	static XmsgImHlrCfgCollOper* instance();
private:
	static XmsgImHlrCfgCollOper* inst;
	shared_ptr<XmsgImHlrCfgColl> loadOneFromIter(void* it); 
	XmsgImHlrCfgCollOper();
	virtual ~XmsgImHlrCfgCollOper();
};

#endif 
