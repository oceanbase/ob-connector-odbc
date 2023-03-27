/************************************************************************************
   Copyright (C) 2017 MariaDB Corporation AB
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

#pragma once
#ifndef _ma_typeconv_h
#define _ma_typeconv_h

/* Argument should be pointer to SQL_TIMESTAMP_STRUCT or MYSQL_TIME */
#define VALID_TIME(PTR2TM_OR_TS) (PTR2TM_OR_TS->hour < 24 && PTR2TM_OR_TS->minute < 60 && PTR2TM_OR_TS->second < 60)
/* 39 - max 16byte number lenght + 1 for sign + 1 for dot + 1 for + scale up to 38 \0 */
#define MADB_CHARSIZE_FOR_NUMERIC 80
BOOL      MADB_ConversionSupported(MADB_DescRecord *From, MADB_DescRecord *To);
size_t    MADB_ConvertNumericToChar(SQL_NUMERIC_STRUCT *Numeric, char *Buffer, int *ErrorCode);
SQLLEN    MADB_CalculateLength(MADB_Stmt *Stmt, SQLLEN *OctetLengthPtr, MADB_DescRecord *CRec, void* DataPtr);
SQLRETURN MADB_C2SQL(MADB_Stmt* Stmt, MADB_DescRecord *CRec, MADB_DescRecord *SqlRec, SQLULEN ParamSetIdx, MYSQL_BIND *bind);
SQLRETURN MADB_C2RefCursor(MADB_Stmt* Stmt, MADB_DescRecord* IrdRec, SQLULEN ParamSetIdx, MYSQL_BIND *bind);

SQLRETURN MADB_Wchar2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                         MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr);
SQLRETURN MADB_Char2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                        MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr);
SQLRETURN MADB_Char2SqlBin_Oracle(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                        MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr);
SQLRETURN MADB_Char2Sql_Oracle(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                        MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr);
SQLRETURN MADB_Numeric2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                        MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr);
SQLRETURN MADB_Timestamp2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                        MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr);
SQLRETURN MADB_Timestamp2Sql_Oracle(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                        MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr);
SQLRETURN MADB_Time2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                        MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr);
SQLRETURN MADB_Time2Sql_Oracle(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                        MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr);
SQLRETURN MADB_IntervalHtoMS2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                        MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr);
SQLRETURN MADB_Date2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                        MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr);

SQLRETURN MADB_ConvertC2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                            MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr);

SQLRETURN MADB_TsConversionIsPossible(SQL_TIMESTAMP_STRUCT *ts, SQLSMALLINT SqlType, MADB_Error *Error, enum enum_madb_error SqlState, int isTime);
SQLRETURN MADB_Str2Ts(const char *Str, size_t Length, MYSQL_TIME *Tm, BOOL Interval, MADB_Error *Error, BOOL *isTime);
SQLRETURN MADB_Str2Ts_Oracle(const char *Str, size_t Length, ORACLE_TIME *Tm, BOOL Interval, MADB_Error *Error, BOOL *isTime);
SQLRETURN MADB_DateTime2Str_Oracle(char *Str, size_t* Length, ORACLE_TIME *Tm, char* fmt, MADB_Error *Error);
SQLRETURN MADB_Date2Str_Oracle(char *Str, size_t* Length, ORACLE_TIME *Tm, char* fmt, MADB_Error *Error);

SQLLEN    MADB_CalculateLength2Str(MADB_Stmt *Stmt, SQLLEN *OctetLengthPtr, MADB_DescRecord *CRec, void* DataPtr);
SQLRETURN MADB_ConvertType2Str(MADB_Stmt *Stmt, SQLSMALLINT ctype, MADB_DescRecord *CRec, char *dataPtr, long *length, char *buff, uint buff_max);

#endif
