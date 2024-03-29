/************************************************************************************
   Copyright (C) 2013 SkySQL AB
   Copyright (c) 2022 OceanBase
   
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.
   
   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.
   
   You should have received a copy of the GNU Library General Public
   License along with this library; if not see <http://www.gnu.org/licenses>
   or write to the Free Software Foundation, Inc., 
   51 Franklin St., Fifth Floor, Boston, MA 02110, USA
*************************************************************************************/
#ifndef _ma_environment_h_
#define _ma_environment_h_

MADB_Env *MADB_EnvInit();
SQLRETURN MADB_EnvFree(MADB_Env *Env);

SQLRETURN MADB_EnvSetAttr(MADB_Env* Env, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength);
SQLRETURN MADB_EnvGetAttr(MADB_Env *Env, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER BufferLength, SQLINTEGER *StringLengthPtr);

extern Client_Charset SourceAnsiCs;

#endif /* _ma_environment_h_ */
