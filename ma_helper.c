/************************************************************************************
   Copyright (C) 2013, 2018 MariaDB Corporation AB
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
#include <ma_odbc.h>
#include <stdint.h>

#define MADB_FIELD_IS_BINARY(_field) ((_field)->charsetnr == BINARY_CHARSETNR)
#define MADB_TRIM_SPACE(p) while(*p == ' ' || *p == '\t')p++
#define MADB_TRIM_RIGHT_SPACE(p) while(*p == ' ' || *p == '\t' || *p == '\0'){*p=0;p--;}

void CloseMultiStatements(MADB_Stmt *Stmt)
{
  unsigned int i;

  for (i=0; i < STMT_COUNT(Stmt->Query); ++i)
  {
    MDBUG_C_PRINT(Stmt->Connection, "-->closing %0x", Stmt->MultiStmts[i]);
    if (Stmt->MultiStmts[i] != NULL)
    {
      mysql_stmt_close(Stmt->MultiStmts[i]);
    }
  }
  MADB_FREE(Stmt->MultiStmts);
  Stmt->stmt= NULL;
}


MYSQL_STMT* MADB_NewStmtHandle(MADB_Stmt *Stmt)
{
  static const my_bool UpdateMaxLength= 1;
  MYSQL_STMT* stmt= mysql_stmt_init(Stmt->Connection->mariadb);

  if (stmt != NULL)
  {
    mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, &UpdateMaxLength);
  }
  else
  {
    MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
  }

  return stmt;
}

/* Required, but not sufficient condition */
BOOL QueryIsPossiblyMultistmt(MADB_QUERY *Query)
{
  return Query->QueryType != MADB_QUERY_CREATE_PROC && Query->QueryType != MADB_QUERY_CREATE_FUNC &&
         Query->QueryType != MADB_QUERY_CREATE_DEFINER && Query->QueryType != MADB_NOT_ATOMIC_BLOCK;
}


/* Trims spaces and/or ';' at the end of query */
int SqlRtrim(char *StmtStr, int Length)
{
  if (Length > 0)
  {
    char *end= StmtStr + Length - 1;
    while (end > StmtStr && (isspace(0x000000ff & *end) || *end == ';'))
    {
      *end= '\0';
      --end;
      --Length;
    }
  }

  return Length;
}

/* Function assumes that the query is multistatement. And, e.g. STMT_COUNT(Stmt->Query) > 1 */
unsigned int GetMultiStatements(MADB_Stmt *Stmt, BOOL ExecDirect)
{
  int          i= 0;
  unsigned int MaxParams= 0;
  char        *p= Stmt->Query.RefinedText;

  Stmt->MultiStmtNr= 0;
  Stmt->MultiStmts= (MYSQL_STMT **)MADB_CALLOC(sizeof(MYSQL_STMT) * STMT_COUNT(Stmt->Query));

  while (p < Stmt->Query.RefinedText + Stmt->Query.RefinedLength && (i >=0 && i < STMT_COUNT(Stmt->Query)))
  {
    Stmt->MultiStmts[i]= i == 0 ? Stmt->stmt : MADB_NewStmtHandle(Stmt);
    MDBUG_C_PRINT(Stmt->Connection, "-->inited&preparing %0x(%d,%s)", Stmt->MultiStmts[i], i, p);

    if (mysql_stmt_prepare(Stmt->MultiStmts[i], p, (unsigned long)strlen(p)))
    {
      MADB_SetNativeError(&Stmt->Error, SQL_HANDLE_STMT, Stmt->MultiStmts[i]);
      CloseMultiStatements(Stmt);

      /* Last paranoid attempt make sure that we did not have a parsing error.
         More to preserve "backward-compatibility" - we did this before, but before trying to
         prepare "multi-statement". */
      if (i == 0 && Stmt->Error.NativeError !=1295 /*ER_UNSUPPORTED_PS*/)
      {
        Stmt->stmt= MADB_NewStmtHandle(Stmt);
        if (mysql_stmt_prepare(Stmt->stmt, STMT_STRING(Stmt), (unsigned long)strlen(STMT_STRING(Stmt))))
        {
          MADB_STMT_CLOSE_STMT(Stmt);
        }
        else
        {
          MADB_DeleteSubqueries(&Stmt->Query);
          return 0;
        }
      }
      return 1;
    }
    if (mysql_stmt_param_count(Stmt->MultiStmts[i]) > MaxParams)
    {
      MaxParams= mysql_stmt_param_count(Stmt->MultiStmts[i]);
    }
    p+= strlen(p) + 1;
    ++i;
  }

  if (MaxParams)
  {
    Stmt->params= (MYSQL_BIND *)MADB_CALLOC(sizeof(MYSQL_BIND) * MaxParams);
  }

  return 0;
}


my_bool MADB_CheckPtrLength(SQLINTEGER MaxLength, char *Ptr, SQLINTEGER NameLen)
{
  if(!Ptr)
    return TRUE;
  if ((NameLen == SQL_NTS && strlen(Ptr) >(size_t) MaxLength) || NameLen > MaxLength)
    return FALSE;
  return TRUE;
}

int  MADB_GetWCharType(int Type)
{
  switch (Type) {
  case SQL_CHAR:
    return SQL_WCHAR;
  case SQL_VARCHAR:
    return SQL_WVARCHAR;
  case SQL_LONGVARCHAR:
    return SQL_WLONGVARCHAR;
  default:
    return Type;
  }
}

int MADB_KeyTypeCount(MADB_Dbc *Connection, char *TableName, int KeyFlag)
{
  int          Count= 0;
  unsigned int i;
  char         StmtStr[1024];
  char         *p= StmtStr;
  char         Database[65]= {'\0'};
  MADB_Stmt    *Stmt= NULL;
  MYSQL_FIELD  *Field;
  
  if (Connection->OracleMode){
    Connection->Methods->GetAttr(Connection, SQL_ATTR_CURRENT_CATALOG, Database, 65, NULL, FALSE);
    p += _snprintf(p, 1024, "SELECT * FROM ");
    if (Database[0] != '\0')
    {
      p += _snprintf(p, sizeof(StmtStr) - strlen(p), "%s.", Database);
    }
    p += _snprintf(p, sizeof(StmtStr) - strlen(p), "%s where rownum = 0", TableName);
    if (MA_SQLAllocHandle(SQL_HANDLE_STMT, (SQLHANDLE)Connection, (SQLHANDLE*)&Stmt) == SQL_ERROR ||
      Stmt->Methods->ExecDirect(Stmt, (char *)StmtStr, SQL_NTS) == SQL_ERROR ||
      Stmt->Methods->Fetch(Stmt) == SQL_ERROR)
    {
      goto end;
    }
  } else {
    Connection->Methods->GetAttr(Connection, SQL_ATTR_CURRENT_CATALOG, Database, 65, NULL, FALSE);
    p += _snprintf(p, 1024, "SELECT * FROM ");
    if (Database[0] != '\0')
    {
      p += _snprintf(p, sizeof(StmtStr) - strlen(p), "`%s`.", Database);
    }
    p += _snprintf(p, sizeof(StmtStr) - strlen(p), "%s LIMIT 0", TableName);
    if (MA_SQLAllocHandle(SQL_HANDLE_STMT, (SQLHANDLE)Connection, (SQLHANDLE*)&Stmt) == SQL_ERROR ||
      Stmt->Methods->ExecDirect(Stmt, (char *)StmtStr, SQL_NTS) == SQL_ERROR ||
      Stmt->Methods->Fetch(Stmt) == SQL_ERROR)
    {
      goto end;
    }
  }

  if (IsStmtNossps(Stmt)){
    if (Stmt->result2 && Stmt->fields2){
      for (i = 0; i < Stmt->result2->field_count; i++)
      {
        Field = &(Stmt->fields2[i]);
        if (Field->flags & KeyFlag)
        {
          ++Count;
        }
      }
    }
  } else {
    for (i = 0; i < mysql_stmt_field_count(Stmt->stmt); i++)
    {
      Field = mysql_fetch_field_direct(Stmt->metadata, i);
      if (Field->flags & KeyFlag)
      {
        ++Count;
      }
    }
  }
  
end:
  if (Stmt)
  {
    Stmt->Methods->StmtFree(Stmt, SQL_DROP);
  }
  return Count;
}


/* {{{ MADB_CheckODBCType */
BOOL MADB_CheckODBCType(SQLSMALLINT Type)
{
  switch(Type)
  {
  case SQL_C_CHAR:
  case SQL_C_WCHAR:
  case SQL_C_SSHORT:
  case SQL_C_SHORT:
  case SQL_C_USHORT:
  case SQL_C_SLONG:
  case SQL_C_LONG:
  case SQL_C_ULONG:
  case SQL_C_FLOAT:
  case SQL_C_DOUBLE:
  case SQL_C_BIT:
  case SQL_C_STINYINT:
  case SQL_C_TINYINT:
  case SQL_C_UTINYINT:
  case SQL_C_SBIGINT:
  case SQL_C_UBIGINT:
  case SQL_C_BINARY:
  case SQL_C_TYPE_DATE:
  case SQL_C_TYPE_TIME:
  case SQL_C_TYPE_TIMESTAMP:
  case SQL_C_NUMERIC:
#if (ODBCVER>=0x0350)
  case SQL_C_GUID:
#endif
  case SQL_C_DEFAULT:
    return TRUE;
  default:
    return FALSE;
  }
}

/* {{{ MADB_GetTypeFromConciseType */
SQLSMALLINT MADB_GetTypeFromConciseType(SQLSMALLINT ConciseType)
{
  switch (ConciseType)
  {
    /* todo: support for interval. currently we map only date/time types */
  case SQL_C_DATE:
  case SQL_C_TIME:
  case SQL_C_TIMESTAMP:
  case SQL_TYPE_DATE:
  case SQL_TYPE_TIME:
  case SQL_TYPE_TIMESTAMP:
    return SQL_DATETIME;
  case SQL_C_INTERVAL_YEAR:
  case SQL_C_INTERVAL_YEAR_TO_MONTH:
  case SQL_C_INTERVAL_MONTH:
  case SQL_C_INTERVAL_DAY:
  case SQL_C_INTERVAL_DAY_TO_HOUR:
  case SQL_C_INTERVAL_DAY_TO_MINUTE:
  case SQL_C_INTERVAL_DAY_TO_SECOND:
  case SQL_C_INTERVAL_HOUR:
  case SQL_C_INTERVAL_HOUR_TO_MINUTE:
  case SQL_C_INTERVAL_HOUR_TO_SECOND:
  case SQL_C_INTERVAL_MINUTE:
  case SQL_C_INTERVAL_MINUTE_TO_SECOND:
  case SQL_C_INTERVAL_SECOND:
      return SQL_INTERVAL;
  default:
    return ConciseType;
  }
}
/* }}} */

/* {{{ MADB_GetTypeName */
char *MADB_GetTypeName(MYSQL_FIELD *Field, BOOL IsOracle)
{
  switch(Field->type) {
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL:
    return IsOracle? "NUMBER":"decimal";
  case MYSQL_TYPE_NULL:
    return "null";
  case MYSQL_TYPE_TINY:
    return (Field->flags & NUM_FLAG) ? "tinyint" : "char";
  case MYSQL_TYPE_SHORT:
    return "smallint";
  case MYSQL_TYPE_LONG:
    return "integer";
  case MYSQL_TYPE_FLOAT:
    return IsOracle?"BINARY_FLOAT":"float";
  case MYSQL_TYPE_DOUBLE:
    return IsOracle?"BINARY_DOUBLE":"double";
  case MYSQL_TYPE_TIMESTAMP:
    return "timestamp";
  case MYSQL_TYPE_LONGLONG:
    return "bigint";
  case MYSQL_TYPE_INT24:
    return "mediumint";
  case MYSQL_TYPE_DATE:
    return "date";
  case MYSQL_TYPE_TIME:
    return "time";
  case MYSQL_TYPE_DATETIME:
    return IsOracle? "DATE":"datetime";
  case MYSQL_TYPE_YEAR:
    return "year";
  case MYSQL_TYPE_NEWDATE:
    return "date";
  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_VAR_STRING:
    if (IsOracle){
      return MADB_FIELD_IS_BINARY(Field) ? "VARBINARY" : "VARCHAR2";
    } else {
      return MADB_FIELD_IS_BINARY(Field) ? "varbinary" : "varchar";
    }
  case MYSQL_TYPE_BIT:
    return "bit";
  case MYSQL_TYPE_ENUM:
    return "enum";
  case MYSQL_TYPE_SET:
    return "set";
  case MYSQL_TYPE_TINY_BLOB:
    return MADB_FIELD_IS_BINARY(Field) ? "tinyblob" : "tinytext";
  case MYSQL_TYPE_MEDIUM_BLOB:
    return MADB_FIELD_IS_BINARY(Field) ? "mediumblob" : "mediumtext";
  case MYSQL_TYPE_LONG_BLOB:
    if (IsOracle) {
      return MADB_FIELD_IS_BINARY(Field) ? "BLOB" : "CLOB";
    } else {
      return MADB_FIELD_IS_BINARY(Field) ? "longblob" : "longtext";
    }
  case MYSQL_TYPE_BLOB:
    return MADB_FIELD_IS_BINARY(Field) ? "blob" : "text";
  case MYSQL_TYPE_STRING:
    if (IsOracle){
      return MADB_FIELD_IS_BINARY(Field) ? "BINARY" : "CHAR";
    } else {
      return MADB_FIELD_IS_BINARY(Field) ? "binary" : "char";
    }
  case MYSQL_TYPE_GEOMETRY:
    return "geometry";
  case MYSQL_TYPE_ORA_BLOB:    return "BLOB";
  case MYSQL_TYPE_ORA_CLOB:    return "CLOB";
  case MYSQL_TYPE_OB_RAW:      return "RAW";
  case MYSQL_TYPE_OB_INTERVAL_YM:  return "INTERVAL YEAR TO MONTH";
  case MYSQL_TYPE_OB_INTERVAL_DS:  return "INTERVAL DAY TO SECOND";
  case MYSQL_TYPE_OB_NUMBER_FLOAT: return "NUMBER";
  case MYSQL_TYPE_OB_NVARCHAR2:    return "NVARCHAR2";
  case MYSQL_TYPE_OB_NCHAR:        return "NCHAR";
  case MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:         return "TIMESTAMP WITH LOCAL TIME ZONE";
  case MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:   return "TIMESTAMP WITH LOCAL TIME ZONE";
  case MYSQL_TYPE_OB_TIMESTAMP_NANO:                   return "TIMESTAMP";
  case MYSQL_TYPE_OB_UROWID:   return "UROWID";
  default:
    return "";
  }
}
/* }}} */

MYSQL_RES *MADB_GetDefaultColumnValues(MADB_Stmt *Stmt, MYSQL_FIELD *fields)
{
  MADB_DynString DynStr;
  unsigned int i;
  MYSQL_RES *result= NULL;
  if(IS_ORACLE_MODE(Stmt)) {
    MADB_InitDynamicString(&DynStr, "SELECT COLUMN_NAME,DATA_DEFAULT AS  COLUMN_DEFAULT FROM ALL_TAB_COLUMNS  WHERE OWNER='", 512, 512);
    if (MADB_DynstrAppend(&DynStr, fields[0].db) ||
        MADB_DynstrAppend(&DynStr, "' AND TABLE_NAME='") ||
        MADB_DynstrAppend(&DynStr, fields[0].org_table) ||
        MADB_DynstrAppend(&DynStr, "' AND COLUMN_NAME IN ("))
      goto error;
  } else {
    MADB_InitDynamicString(&DynStr, "SELECT COLUMN_NAME, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='", 512, 512);
    if (MADB_DynstrAppend(&DynStr, fields[0].db) ||
        MADB_DynstrAppend(&DynStr, "' AND TABLE_NAME='") ||
        MADB_DynstrAppend(&DynStr, fields[0].org_table) ||
        MADB_DynstrAppend(&DynStr, "' AND COLUMN_NAME IN ("))
      goto error;
  }

  for (i=0; i < mysql_stmt_field_count(Stmt->stmt); i++)
  {
    MADB_DescRecord *Rec= MADB_DescGetInternalRecord(Stmt->Ard, i, MADB_DESC_READ);

    if (!Rec->inUse || MADB_ColumnIgnoredInAllRows(Stmt->Ard, Rec) == TRUE)
    {
      continue;
    }
    if (MADB_DynstrAppend(&DynStr, i > 0 ? ",'" : "'") ||
      MADB_DynstrAppend(&DynStr, fields[i].org_name) ||
      MADB_DynstrAppend(&DynStr, "'"))
    {
      goto error;
    }
  }
  if(IS_ORACLE_MODE(Stmt)){
    if (MADB_DynstrAppend(&DynStr, ") AND DATA_DEFAULT IS NOT NULL AND OWNER != '__recyclebin'"))
      goto error;
  } else {
    if (MADB_DynstrAppend(&DynStr, ") AND COLUMN_DEFAULT IS NOT NULL"))
      goto error;
  }
  
  LOCK_MARIADB(Stmt->Connection);
  if (mysql_query(Stmt->Connection->mariadb, DynStr.str)){
    UNLOCK_MARIADB(Stmt->Connection);
    goto error;
  }
  result= mysql_store_result(Stmt->Connection->mariadb);
  UNLOCK_MARIADB(Stmt->Connection);

error:
    MADB_DynstrFree(&DynStr);
    return result;
}

MYSQL_RES *MADB_GetDefaultColumnValues_oracle(MADB_Stmt *Stmt, char* db, char* table)
{
  MADB_DynString DynStr;
  unsigned int i;
  MYSQL_RES *result = NULL;
  if (IS_ORACLE_MODE(Stmt)) {
    MADB_InitDynamicString(&DynStr, "SELECT COLUMN_NAME,DATA_DEFAULT AS  COLUMN_DEFAULT FROM ALL_TAB_COLUMNS  WHERE OWNER='", 512, 512);
    if (MADB_DynstrAppend(&DynStr, db) ||
      MADB_DynstrAppend(&DynStr, "' AND TABLE_NAME='") ||
      MADB_DynstrAppend(&DynStr, table) ||
      MADB_DynstrAppend(&DynStr, "' AND COLUMN_NAME IN ("))
      goto error;
  } else {
    MADB_InitDynamicString(&DynStr, "SELECT COLUMN_NAME, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='", 512, 512);
    if (MADB_DynstrAppend(&DynStr, db) ||
      MADB_DynstrAppend(&DynStr, "' AND TABLE_NAME='") ||
      MADB_DynstrAppend(&DynStr, table) ||
      MADB_DynstrAppend(&DynStr, "' AND COLUMN_NAME IN ("))
      goto error;
  }

  for (i = 0; i < MADB_STMT_COLUMN_COUNT(Stmt); i++)
  {
    MADB_DescRecord *Rec = MADB_DescGetInternalRecord(Stmt->Ard, i, MADB_DESC_READ);

    if (!Rec->inUse || MADB_ColumnIgnoredInAllRows(Stmt->Ard, Rec) == TRUE)
    {
      continue;
    }

    MADB_DescRecord *RecordField = MADB_DescGetInternalRecord(Stmt->Ird, i, MADB_DESC_READ);
    if (!RecordField || MADB_DynstrAppend(&DynStr, i > 0 ? ",'" : "'") ||
      MADB_DynstrAppend(&DynStr, RecordField->BaseColumnName) ||
      MADB_DynstrAppend(&DynStr, "'")) {
      goto error;
    }
  }
  if (IS_ORACLE_MODE(Stmt)) {
    if (MADB_DynstrAppend(&DynStr, ") AND DATA_DEFAULT IS NOT NULL AND OWNER != '__recyclebin'"))
      goto error;
  } else {
    if (MADB_DynstrAppend(&DynStr, ") AND COLUMN_DEFAULT IS NOT NULL"))
      goto error;
  }

  LOCK_MARIADB(Stmt->Connection);
  if (mysql_query(Stmt->Connection->mariadb, DynStr.str)){
    UNLOCK_MARIADB(Stmt->Connection);
    goto error;
  }
  result= mysql_store_result(Stmt->Connection->mariadb);
  UNLOCK_MARIADB(Stmt->Connection);

error:
    MADB_DynstrFree(&DynStr);
    return result;
}

char *MADB_GetDefaultColumnValue(MYSQL_RES *res, const char *Column)
{
  MYSQL_ROW row;

  if (res == NULL || !res->row_count)
    return NULL;
  mysql_data_seek(res, 0);
  while ((row= mysql_fetch_row(res)))
  {
    if (_stricmp(row[0], Column) == 0)
     return _strdup(row[1]);
  }
  return NULL;
}

SQLLEN MADB_GetDataSize(SQLSMALLINT SqlType, SQLLEN OctetLength, BOOL Unsigned,
                        SQLSMALLINT Precision, SQLSMALLINT Scale, unsigned int CharMaxLen, BOOL IsOracle)
{
  switch(SqlType)
  {
  case SQL_BIT:
    return 1;
  case SQL_TINYINT:
    return 3;
  case SQL_SMALLINT:
    return 5;
  case SQL_INTEGER:
    return 10;
  case SQL_BIGINT:
    return 20 - test(Unsigned != FALSE);
  case SQL_REAL:
    return 7;
  case SQL_DOUBLE:
    return 15;
  case SQL_FLOAT:
    return IsOracle ? (int)(Precision*0.30103)+1:15;
  case SQL_DECIMAL:
  case SQL_NUMERIC:
    return Precision;
  case SQL_TYPE_DATE:
    return SQL_DATE_LEN;
  case SQL_TYPE_TIME:
    return SQL_TIME_LEN + MADB_FRACTIONAL_PART(Scale);
  case SQL_TYPE_TIMESTAMP:
    return SQL_TIMESTAMP_LEN + MADB_FRACTIONAL_PART(Scale);
  case SQL_BINARY:
  case SQL_VARBINARY:
  case SQL_LONGVARBINARY:
    return OctetLength;
  case SQL_GUID:
    return 36;;
  default:
    if (IsOracle) {
      return OctetLength;
    } else {
      if (CharMaxLen < 2/*i.e.0||1*/) {
        return OctetLength;
      } else {
        return OctetLength / CharMaxLen;
      }
    }
  }
}

/* {{{ MADB_GetDisplaySize */
size_t MADB_GetDisplaySize(MYSQL_FIELD *Field, MARIADB_CHARSET_INFO *charset, BOOL IsOracle)
{
  /* Todo: check these values with output from mysql --with-columntype-info */
  switch (Field->type) {
  case MYSQL_TYPE_NULL:
    return 1;
  case MYSQL_TYPE_BIT:
    return (Field->length == 1) ? 1 : (Field->length + 7) / 8 * 2;
  case MYSQL_TYPE_TINY:
    return 4 - test(Field->flags & UNSIGNED_FLAG);
  case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_YEAR:
    return 6 - test(Field->flags & UNSIGNED_FLAG);
  case MYSQL_TYPE_INT24:
    return 9 - test(Field->flags & UNSIGNED_FLAG);
  case MYSQL_TYPE_LONG:
    return 11 - test(Field->flags & UNSIGNED_FLAG);
  case MYSQL_TYPE_LONGLONG:
    return 20;
  case MYSQL_TYPE_DOUBLE:
    return IsOracle ? 15 + 2 : 15;
  case MYSQL_TYPE_FLOAT:
    return IsOracle ? 7 + 2 : 7;
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL:
  {
    if (IsOracle){
      if (Field->length>40){
        return 40;
      } else {
        size_t Precision = Field->length - test((Field->flags & UNSIGNED_FLAG) == 0) - test(Field->decimals != 0);
        //return Field->length + test(Precision == Field->decimals);
        return Precision + 2;
      }
    } else {
      /* The edge case like decimal(1,1)*/
      size_t Precision= Field->length - test((Field->flags & UNSIGNED_FLAG) == 0) - test(Field->decimals != 0);
      return Field->length + test(Precision == Field->decimals);
    }
  }
  case MYSQL_TYPE_DATE:
    return SQL_DATE_LEN; /* YYYY-MM-DD */
  case MYSQL_TYPE_TIME:
    return SQL_TIME_LEN + MADB_FRACTIONAL_PART(Field->decimals); /* HH:MM:SS.ffffff */
  case MYSQL_TYPE_NEWDATE:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_DATETIME:
    return SQL_TIMESTAMP_LEN + MADB_FRACTIONAL_PART(Field->decimals);
  case MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
  case MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
  case MYSQL_TYPE_OB_TIMESTAMP_NANO:
    return SQL_TIMESTAMP_LEN + MADB_FRACTIONAL_PART(Field->decimals);
  case MYSQL_TYPE_OB_NUMBER_FLOAT:
    return (int)(Field->precision*0.30103)+1+2;
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_ENUM:
  case MYSQL_TYPE_GEOMETRY:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_SET:
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_OB_RAW:
  case MYSQL_TYPE_OB_NVARCHAR2:
  case MYSQL_TYPE_OB_NCHAR:
  case MYSQL_TYPE_OB_UROWID:
  case MYSQL_TYPE_ORA_BLOB:
  case MYSQL_TYPE_ORA_CLOB:
  {
    if (IsOracle){
      if (MADB_FIELD_IS_BINARY(Field)) {
        return Field->length * 2; /* ODBC specs says we should give 2 characters per byte to display binaray data in hex form */
      } else {
        return Field->length;
      }
    } else {
      if (MADB_FIELD_IS_BINARY(Field)) {
        return Field->length * 2; /* ODBC specs says we should give 2 characters per byte to display binaray data in hex form */
      } else if (charset == NULL || charset->char_maxlen < 2/*i.e.0||1*/) {
        return Field->length;
      } else {
        return Field->length / charset->char_maxlen;
      }
    }
  }
  default:
    return SQL_NO_TOTAL;
  }
}
/* }}} */

/* {{{ MADB_GetOctetLength */
size_t MADB_GetOctetLength(MYSQL_FIELD *Field, unsigned short MaxCharLen, BOOL IsOracle)
{
  size_t Length= MIN(MADB_INT_MAX32, Field->length);

  switch (Field->type) {
  case MYSQL_TYPE_NULL:
    return 1;
  case MYSQL_TYPE_BIT:
    return (Field->length + 7) / 8;
  case MYSQL_TYPE_TINY:
    return 1;
  case MYSQL_TYPE_YEAR:
  case MYSQL_TYPE_SHORT:
    return 2;
  case MYSQL_TYPE_INT24:
  case MYSQL_TYPE_LONG:
    return 4;
  case MYSQL_TYPE_LONGLONG:
    return 8;
  case MYSQL_TYPE_DOUBLE:
    return 8;
  case MYSQL_TYPE_FLOAT:
    return 4;
  case MYSQL_TYPE_DECIMAL:
  case MYSQL_TYPE_NEWDECIMAL:
  {
    if (IsOracle){
      if (Field->length > 40) {
        return 38;
      } else {
        size_t Precision = Field->length - test((Field->flags & UNSIGNED_FLAG) == 0) - test(Field->decimals != 0);
        return Precision;
        //return Field->length + test(Precision == Field->decimals);
      }
    } else {
      /* The edge case like decimal(1,1)*/
      size_t Precision = Field->length - test((Field->flags & UNSIGNED_FLAG) == 0) - test(Field->decimals != 0);
      return Field->length + test(Precision == Field->decimals);
    }
  }
  case MYSQL_TYPE_DATE:
    return sizeof(SQL_DATE_STRUCT);
  case MYSQL_TYPE_TIME:
    return sizeof(SQL_TIME_STRUCT);
   case MYSQL_TYPE_NEWDATE:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_DATETIME:
    return sizeof(SQL_TIMESTAMP_STRUCT);
  case MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
  case MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
  case MYSQL_TYPE_OB_TIMESTAMP_NANO:
    return sizeof(SQL_TIMESTAMP_STRUCT);
  case MYSQL_TYPE_OB_NUMBER_FLOAT:
    return (int)(Field->precision*0.30103) + 1;
  case MYSQL_TYPE_BLOB:
  case MYSQL_TYPE_ENUM:
  case MYSQL_TYPE_GEOMETRY:
  case MYSQL_TYPE_LONG_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB:
  case MYSQL_TYPE_TINY_BLOB:
    return Length;
  case MYSQL_TYPE_SET:
  case MYSQL_TYPE_STRING:
  case MYSQL_TYPE_VARCHAR:
  case MYSQL_TYPE_VAR_STRING:
  case MYSQL_TYPE_OB_RAW:
  case MYSQL_TYPE_OB_NVARCHAR2:
  case MYSQL_TYPE_OB_NCHAR:
  case MYSQL_TYPE_OB_UROWID:
  case MYSQL_TYPE_ORA_BLOB:
  case MYSQL_TYPE_ORA_CLOB:
    return Length; /* Field->length is calculated using current charset */
  default:
    return SQL_NO_TOTAL;
  }
}
/* }}} */

/* {{{ MADB_GetDefaultType */
int MADB_GetDefaultType(int SQLDataType)
{
  switch(SQLDataType)
  {
  case SQL_BIGINT:
    return SQL_C_SBIGINT;
  case SQL_BINARY:
    return SQL_C_BINARY;
  case SQL_BIT:
    return SQL_C_BIT;
  case SQL_CHAR:
    return SQL_C_CHAR;
  case SQL_DATE:
  case SQL_TYPE_DATE:
    return SQL_C_DATE;
  case SQL_DECIMAL:
    return SQL_C_CHAR;
  case SQL_DOUBLE:
    return SQL_C_DOUBLE; 
  case SQL_FLOAT:
    return SQL_C_FLOAT;
  case SQL_INTEGER:
    return SQL_C_LONG;
  case SQL_LONGVARBINARY:
    return SQL_C_BINARY;
  case SQL_LONGVARCHAR:
    return SQL_C_CHAR;
  case SQL_NUMERIC:
    return SQL_C_CHAR;
  case SQL_REAL:
    return SQL_C_FLOAT;
  case SQL_SMALLINT:
    return SQL_C_SHORT;
  case SQL_TIME:
  case SQL_TYPE_TIME:
    return SQL_C_TIME;
  case SQL_TIMESTAMP:
  case SQL_TYPE_TIMESTAMP:
    return SQL_C_TIMESTAMP;
  case SQL_TINYINT:
    return SQL_C_TINYINT;
  case SQL_VARBINARY:
    return SQL_C_BINARY;
  case SQL_VARCHAR:
    return SQL_C_CHAR;
  default:
    return SQL_C_CHAR;
  }
}
/* }}} */

/* {{{ MapMariadDbToOdbcType */
       /* It's not quite right to mix here C and SQL types, even though constants are sort of equal */
SQLSMALLINT MapMariadDbToOdbcType(MYSQL_FIELD *field)
{
  switch (field->type) {
    case MYSQL_TYPE_BIT:
      if (field->length > 1)
        return SQL_BINARY;
      return SQL_BIT;
    case MYSQL_TYPE_NULL:
      return SQL_VARCHAR;
    case MYSQL_TYPE_TINY:
      return field->flags & NUM_FLAG ? SQL_TINYINT : SQL_CHAR;
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_SHORT:
      return SQL_SMALLINT;
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
      return SQL_INTEGER;
    case MYSQL_TYPE_FLOAT:
      return SQL_REAL;
    case MYSQL_TYPE_DOUBLE:
      return SQL_DOUBLE;
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
      return SQL_TYPE_TIMESTAMP;
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_DATE:
      return SQL_TYPE_DATE;
    case MYSQL_TYPE_TIME:
       return SQL_TYPE_TIME;
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
      return MADB_FIELD_IS_BINARY(field) ? SQL_LONGVARBINARY : SQL_LONGVARCHAR;
    case MYSQL_TYPE_LONGLONG:
      return SQL_BIGINT;
    case MYSQL_TYPE_STRING:
      return MADB_FIELD_IS_BINARY(field) ? SQL_BINARY : SQL_CHAR;
    case MYSQL_TYPE_VAR_STRING:
      return MADB_FIELD_IS_BINARY(field) ? SQL_VARBINARY : SQL_VARCHAR;
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_ENUM:
      return SQL_CHAR;
    case MYSQL_TYPE_GEOMETRY:
      return SQL_LONGVARBINARY;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      return SQL_DECIMAL;
    case MYSQL_TYPE_OB_TIMESTAMP_NANO:
    case MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
      return SQL_TYPE_TIMESTAMP;
    case MYSQL_TYPE_ORA_CLOB:
      return SQL_VARCHAR;
    case MYSQL_TYPE_ORA_BLOB:
      return SQL_VARBINARY;
    case MYSQL_TYPE_OB_NCHAR:
      return SQL_WCHAR;
    case MYSQL_TYPE_OB_NVARCHAR2:
      return SQL_WVARCHAR;
    case MYSQL_TYPE_OB_RAW:
      return SQL_VARBINARY;
    case MYSQL_TYPE_OB_NUMBER_FLOAT:
      return SQL_FLOAT;
    default:
      return SQL_UNKNOWN_TYPE;
  }
}

/* }}} */
/* {{{ MADB_GetTypeLength */
size_t MADB_GetTypeLength(SQLINTEGER SqlDataType, size_t Length, BOOL IsOracle)
{
  switch(SqlDataType)
  {
  case SQL_C_BIT:
  case SQL_C_TINYINT:
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
    return 1;
  case SQL_C_SHORT:
  case SQL_C_SSHORT:
  case SQL_C_USHORT:
    return 2;
  case SQL_C_LONG:
  case SQL_C_SLONG:
  case SQL_C_ULONG:
    return sizeof(SQLINTEGER);
  case SQL_C_UBIGINT:
  case SQL_C_SBIGINT:
    return sizeof(long long);
  case SQL_C_DOUBLE:
    return sizeof(SQLDOUBLE);
  case SQL_C_FLOAT:
    return IsOracle ? 4 : sizeof(SQLFLOAT);
  case SQL_C_NUMERIC:
    return sizeof(SQL_NUMERIC_STRUCT);
  case SQL_C_TYPE_TIME:
  case SQL_C_TIME:
    return sizeof(SQL_TIME_STRUCT);
  case SQL_C_TYPE_DATE:
  case SQL_C_DATE:
    return sizeof(SQL_DATE_STRUCT);
  case SQL_C_TYPE_TIMESTAMP:
  case SQL_C_TIMESTAMP:
    return sizeof(SQL_TIMESTAMP_STRUCT);
  default:
    return Length;
  }
}
/* }}} */

/* {{{ MADB_GetMaDBTypeAndLength */
int MADB_GetMaDBTypeAndLength(SQLINTEGER SqlDataType, my_bool *Unsigned, unsigned long *Length)
{
  *Unsigned= 0;
  switch(SqlDataType)
  {
  case SQL_C_BIT:
  case SQL_C_TINYINT:
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
    *Length= 1;
    *Unsigned= (SqlDataType == SQL_C_UTINYINT);

    return MYSQL_TYPE_TINY;

  case SQL_C_SHORT:
  case SQL_C_SSHORT:
  case SQL_C_USHORT:
    *Length= 2;
    *Unsigned= (SqlDataType == SQL_C_USHORT);

    return MYSQL_TYPE_SHORT;
 
  case SQL_C_LONG:
  case SQL_C_SLONG:
  case SQL_C_ULONG:
    *Length= sizeof(SQLINTEGER);
    *Unsigned= (SqlDataType == SQL_C_ULONG);
    return MYSQL_TYPE_LONG;
  case SQL_C_UBIGINT:
  case SQL_C_SBIGINT:
    *Length= sizeof(long long);
    *Unsigned= (SqlDataType == SQL_C_UBIGINT);
    return MYSQL_TYPE_LONGLONG;
  case SQL_C_DOUBLE:
    *Length= sizeof(SQLDOUBLE);
    return MYSQL_TYPE_DOUBLE;
  case SQL_C_FLOAT:
    *Length =sizeof(SQLFLOAT);
    return MYSQL_TYPE_FLOAT;
  case SQL_C_NUMERIC:
    /**Length= sizeof(SQL_NUMERIC_STRUCT);*/
    return MYSQL_TYPE_DECIMAL;
  case SQL_C_TYPE_TIME:
  case SQL_C_TIME:
    *Length= sizeof(SQL_TIME_STRUCT);
    return MYSQL_TYPE_TIME;
  case SQL_C_TYPE_DATE:
  case SQL_C_DATE:
    *Length= sizeof(SQL_DATE_STRUCT);
    return MYSQL_TYPE_DATE;
  case SQL_C_TYPE_TIMESTAMP:
  case SQL_C_TIMESTAMP:
    *Length= sizeof(SQL_TIMESTAMP_STRUCT);
    return MYSQL_TYPE_TIMESTAMP;
  case SQL_C_INTERVAL_HOUR_TO_MINUTE:
  case SQL_C_INTERVAL_HOUR_TO_SECOND:
    *Length= sizeof(SQL_INTERVAL_STRUCT);
    return MYSQL_TYPE_TIME;
  case SQL_C_CHAR:
    return MYSQL_TYPE_STRING;
  default:
    return MYSQL_TYPE_BLOB;
  }
}
/* }}} */

void MADB_CopyOdbcTsToMadbTime(SQL_TIMESTAMP_STRUCT *Src, MYSQL_TIME *Dst)
{
  Dst->year=        Src->year;
  Dst->month=       Src->month;
  Dst->day=         Src->day;
  Dst->hour=        Src->hour;
  Dst->minute=      Src->minute;
  Dst->second=      Src->second;
  Dst->second_part= Src->fraction / 1000;
}

void MADB_CopyOdbcTsToMadbTime_Oracle(SQL_TIMESTAMP_STRUCT *Src, ORACLE_TIME *Dst)
{
  Dst->century = Src->year / 100;
  Dst->year = Src->year % 100;
  Dst->month = Src->month;
  Dst->day = Src->day;
  Dst->hour = Src->hour;
  Dst->minute = Src->minute;
  Dst->second = Src->second;
  Dst->second_part = Src->fraction / 1000;
}

void MADB_CopyOdbcTimeToMadbTime_Oracle(SQL_TIME_STRUCT *Src, ORACLE_TIME *Dst)
{
  /*oracle mode year/month/day use current time*/
  time_t sec_time;
  struct tm * cur_tm;

  sec_time = time(NULL);
  cur_tm = localtime(&sec_time);

  Dst->century = (1900 + cur_tm->tm_year) / 100;
  Dst->year = (1900 + cur_tm->tm_year) % 100;
  Dst->month = (cur_tm->tm_mon + 1);
  Dst->day = (cur_tm->tm_mday);
  Dst->hour = Src->hour;
  Dst->minute = Src->minute;
  Dst->second = Src->second;
  Dst->second_part = 0;
}

void MADB_CopyMadbTimeToOdbcTs(MYSQL_TIME *Src, SQL_TIMESTAMP_STRUCT *Dst)
{
  Dst->year=        Src->year;
  Dst->month=       Src->month;
  Dst->day=         Src->day;
  Dst->hour=        Src->hour;
  Dst->minute=      Src->minute;
  Dst->second=      Src->second;
  Dst->fraction=    Src->second_part*1000;
}

SQLRETURN MADB_CopyMadbTimestamp(MADB_Stmt *Stmt, MYSQL_TIME *tm, SQLPOINTER DataPtr, SQLLEN *Length, SQLLEN *Ind,
                                 SQLSMALLINT CType, SQLSMALLINT SqlType)
{
  SQLLEN Dummy;

  Length= Length == NULL ? &Dummy : Length;

  switch(CType)
  {
    case SQL_C_TIMESTAMP:
    case SQL_C_TYPE_TIMESTAMP:
    {
      SQL_TIMESTAMP_STRUCT *ts= (SQL_TIMESTAMP_STRUCT *)DataPtr;

      if (ts != NULL)
      {
        /* If time converted to timestamp - fraction is set to 0, date is set to current date */
        if (SqlType == SQL_TIME || SqlType == SQL_TYPE_TIME)
        {
          time_t sec_time;
          struct tm * cur_tm;

          sec_time= time(NULL);
          cur_tm= localtime(&sec_time);

          ts->year= 1900 + cur_tm->tm_year;
          ts->month= cur_tm->tm_mon + 1;
          ts->day= cur_tm->tm_mday;
          ts->fraction= 0;
        }
        else
        {
          ts->year= tm->year;
          ts->month= tm->month;
          ts->day= tm->day;
          ts->fraction= tm->second_part * 1000;
        }
        ts->hour= tm->hour;
        ts->minute= tm->minute;
        ts->second= tm->second;

        if (ts->year + ts->month + ts->day + ts->hour + ts->minute + ts->fraction + ts->second == 0)
        {
          if (Ind != NULL)
          {
            *Ind= SQL_NULL_DATA;
          }
          else
          {
            return MADB_SetError(&Stmt->Error, MADB_ERR_22002, NULL, 0);
          }
          break;
        }
      }
      *Length= sizeof(SQL_TIMESTAMP_STRUCT);
    }
    break;
    case SQL_C_TIME:
    case SQL_C_TYPE_TIME:
    {
      SQL_TIME_STRUCT *ts= (SQL_TIME_STRUCT *)DataPtr;

      if (ts != NULL)
      {
        /* tm(buffer from MYSQL_BIND) can be NULL. And that happens if ts(app's buffer) is null */
        if (!VALID_TIME(tm))
        {
          return MADB_SetError(&Stmt->Error, MADB_ERR_22007, NULL, 0);
        }

        ts->hour= tm->hour;
        ts->minute= tm->minute;
        ts->second= tm->second;

        *Length= sizeof(SQL_TIME_STRUCT);

        if (tm->second_part)
        {
          return MADB_SetError(&Stmt->Error, MADB_ERR_01S07, NULL, 0);
        }
      }
    }
    break;
    case SQL_C_DATE:
    case SQL_TYPE_DATE:
    {
      SQL_DATE_STRUCT *ts= (SQL_DATE_STRUCT *)DataPtr;

      if (ts != NULL)
      {
        ts->year= tm->year;
        ts->month= tm->month;
        ts->day= tm->day;
        if (ts->year + ts->month + ts->day == 0)
        {
          if (Ind != NULL)
          {
            *Ind= SQL_NULL_DATA;
          }
          else
          {
            return MADB_SetError(&Stmt->Error, MADB_ERR_22002, NULL, 0);
          }
          break;
        }
      }
      *Length= sizeof(SQL_DATE_STRUCT);
    }
    break;
  }
    
  return SQL_SUCCESS;
}

SQLRETURN MADB_CopyMadbTimestamp_Oracle(MADB_Stmt *Stmt, ORACLE_TIME *tm, SQLPOINTER DataPtr, SQLLEN *Length, SQLLEN *Ind,
  SQLSMALLINT CType, SQLSMALLINT SqlType)
{
  SQLLEN Dummy;

  Length = Length == NULL ? &Dummy : Length;

  switch (CType)
  {
  case SQL_C_TIMESTAMP:
  case SQL_C_TYPE_TIMESTAMP:
  {
    SQL_TIMESTAMP_STRUCT *ts = (SQL_TIMESTAMP_STRUCT *)DataPtr;

    if (ts != NULL)
    {
      /* If time converted to timestamp - fraction is set to 0, date is set to current date */
      if (SqlType == SQL_TIME || SqlType == SQL_TYPE_TIME)
      {
        time_t sec_time;
        struct tm * cur_tm;

        sec_time = time(NULL);
        cur_tm = localtime(&sec_time);

        ts->year = 1900 + cur_tm->tm_year;
        ts->month = cur_tm->tm_mon + 1;
        ts->day = cur_tm->tm_mday;
        ts->fraction = 0;
      }
      else
      {
        ts->year = tm->year+tm->century*100;
        ts->month = tm->month;
        ts->day = tm->day;
        ts->fraction = tm->second_part;
      }
      ts->hour = tm->hour;
      ts->minute = tm->minute;
      ts->second = tm->second;

      if (ts->year + ts->month + ts->day + ts->hour + ts->minute + ts->fraction + ts->second == 0)
      {
        if (Ind != NULL)
        {
          *Ind = SQL_NULL_DATA;
        }
        else
        {
          return MADB_SetError(&Stmt->Error, MADB_ERR_22002, NULL, 0);
        }
        break;
      }
    }
    *Length = sizeof(SQL_TIMESTAMP_STRUCT);
  }
  break;
  case SQL_C_TIME:
  case SQL_C_TYPE_TIME:
  {
    SQL_TIME_STRUCT *ts = (SQL_TIME_STRUCT *)DataPtr;

    if (ts != NULL)
    {
      /* tm(buffer from MYSQL_BIND) can be NULL. And that happens if ts(app's buffer) is null */
      if (!VALID_TIME(tm))
      {
        return MADB_SetError(&Stmt->Error, MADB_ERR_22007, NULL, 0);
      }

      ts->hour = tm->hour;
      ts->minute = tm->minute;
      ts->second = tm->second;

      *Length = sizeof(SQL_TIME_STRUCT);

      if (tm->second_part)
      {
        return MADB_SetError(&Stmt->Error, MADB_ERR_01S07, NULL, 0);
      }
    }
  }
  break;
  case SQL_C_DATE:
  case SQL_TYPE_DATE:
  {
    SQL_DATE_STRUCT *ts = (SQL_DATE_STRUCT *)DataPtr;

    if (ts != NULL)
    {
      ts->year = tm->year+tm->century*100;
      ts->month = tm->month;
      ts->day = tm->day;
      if (ts->year + ts->month + ts->day == 0)
      {
        if (Ind != NULL)
        {
          *Ind = SQL_NULL_DATA;
        }
        else
        {
          return MADB_SetError(&Stmt->Error, MADB_ERR_22002, NULL, 0);
        }
        break;
      }
    }
    *Length = sizeof(SQL_DATE_STRUCT);
  }
  break;
  }

  return SQL_SUCCESS;
}

void *GetBindOffset(MADB_Desc *Desc, MADB_DescRecord *Record, void *Ptr, SQLULEN RowNumber, size_t PtrSize)
{
  size_t BindOffset= 0;

  /* This is not quite clear - I'd imagine, that if BindOffset is set, then Ptr can be NULL.
     Makes perfect sense in case of row-based binding - setting pointers to offset in structure, and BindOffset to the begin of array.
     One of members would have 0 offset then. But specs are rather against that, and other drivers also don't support such interpretation */
  if (Ptr == NULL)
  {
    return NULL;
  }
  if (Desc->Header.BindOffsetPtr != NULL)
  {
    BindOffset= (size_t)*Desc->Header.BindOffsetPtr;
  }

  /* row wise binding */
  if (Desc->Header.BindType == SQL_BIND_BY_COLUMN ||
    Desc->Header.BindType == SQL_PARAM_BIND_BY_COLUMN)
  {
    BindOffset+= PtrSize * RowNumber;
  }
  else
  {
    BindOffset+= Desc->Header.BindType * RowNumber;
  }

  return (char *)Ptr + BindOffset;
}

/* Checking if column ignored in all bound rows. Should hel*/
BOOL MADB_ColumnIgnoredInAllRows(MADB_Desc *Desc, MADB_DescRecord *Rec)
{
  SQLULEN row;
  SQLLEN *IndicatorPtr;

  for (row= 0; row < Desc->Header.ArraySize; ++row)
  {
    IndicatorPtr= (SQLLEN *)GetBindOffset(Desc, Rec, Rec->IndicatorPtr, row, sizeof(SQLLEN));

    if (IndicatorPtr == NULL || *IndicatorPtr != SQL_COLUMN_IGNORE)
    {
      return FALSE;
    }
  }

  return TRUE;
}


void MADB_NumericInit(SQL_NUMERIC_STRUCT *number, MADB_DescRecord *Ard)
{
  if (!number)
    return;
  number->precision= (SQLCHAR)Ard->Precision;
  number->scale= (SQLCHAR)Ard->Scale;
  memset(number->val, 0, sizeof(number->val));
}

/* {{{ MADB_CharToSQLNumeric */
int MADB_CharToSQLNumeric(char *buffer, MADB_Desc *Ard, MADB_DescRecord *ArdRecord, SQL_NUMERIC_STRUCT *dst_buffer, unsigned long RowNumber, BOOL IsOracle)
{
  char *p;
  SQL_NUMERIC_STRUCT *number= dst_buffer != NULL ? dst_buffer :
    (SQL_NUMERIC_STRUCT *)GetBindOffset(Ard, ArdRecord, ArdRecord->DataPtr, RowNumber, ArdRecord->OctetLength);
  int ret= 0;

  if (!buffer || !number)
  {
    return ret;
  }

  p= trim(buffer);
  MADB_NumericInit(number, ArdRecord);

  /* Determining the sign of the number. From now on we dean with unsigned number */
  if (!(number->sign = (*p == '-') ? 0 : 1))
  {
    p++;
  }
  /* Empty string - nothing to do*/
  if (!*p)
  {
    return ret;
  }

  if (number->precision == 0)
  {
    number->precision= MADB_DEFAULT_PRECISION;
  }

  /* Skipping leading zeroes */
  while (*p == '0')
  {
    ++p;
  }
  if (*p)
  {
    int i;
    unsigned int bit, hval, tv, dig, sta, olen;
    int leading_zeros= 0;
    char *dot= strchr(p, '.');
    char digits[100];
    unsigned short digits_count= 0; /* integer part digits count*/

    if (dot == NULL)
    {
      char* end= p;
      while (*end && isdigit(0x000000ff & *end))
        ++end;

      digits_count= (unsigned short)(end - p);
    }
    else
    {
      digits_count= (unsigned short)(dot - p);
    }
    /* Overflow checks */
    if (digits_count > MADB_DEFAULT_PRECISION + 1 ) /* 16 bytes of FF make up 39 digits number */
    {
      return MADB_ERR_22003;
    }
    if (number->precision > 0 &&  digits_count > number->precision)
    {
      /* if scale is negative, and we have just enough zeroes at the end - we are fine, there is no overflow */
      if (number->scale < 0 && (number->precision - number->scale) >= digits_count)
      {
        /* Checking that all digits past presision are '0'. Otherwise - overflow */
        for (i = digits_count - number->precision; i > 0; --i)
        {
          if (*(p + digits_count - i) != '0')
          {
            return MADB_ERR_22003;
          }
        }
      }
      else
      {
        return MADB_ERR_22003;
      }
    }

    memcpy(digits, p, digits_count);

    if (IsOracle){
      if (!dot && number->scale > 0){
        /*keep oracle*/
        dot = ".00000000000000000000000000000000000000";
      }
    }

    if (dot && number->scale > 0)
    {
      short digits_total= 0,       /* fractional part total digits */
            digits_significant= 0; /* fractional part significant digits(not counting 0 at the end) */

      p= dot + 1;
      while (*p)
      {
        /* ignore non numbers */
        if (!isdigit(0x000000ff & *p))
          break;
        ++digits_total;
        /* ignore trailing zeros */
        if (*p != '0')
        {
          digits_significant= digits_total;
        }
        ++p;
      }

      /* Kinda tricky. let's say precision is 5.2. 1234.5 is fine, 1234.56 is overflow, 123.456 fractional overflow with rounding and warning */
      if (digits_count + digits_significant > number->precision && digits_significant <= number->scale)
      {
        return MADB_ERR_22003;
        /* if digits are zero there is no overflow */
        /*for (p= dot + 1; p <= dot + digits_significant; ++p)
        {
          if (*p != '0')
            
        }*/
      }
      
      if (digits_significant > number->scale)
      {
        ret= MADB_ERR_01S07;
        memcpy(digits + digits_count, dot + 1, number->scale);
      }
      else
      {
        memcpy(digits + digits_count, dot + 1, digits_significant);
      
        for (i= digits_count + digits_significant; i < digits_count + number->scale; ++i)
        {
          digits[i]= '0';
        }
      }
      digits_count+= number->scale;
    }

    /* Rounding */
    if (number->scale < 0)
    {
      int64_t OldVal, Val;
      int64_t RoundNumber= (int64_t)pow(10.0, -number->scale);

      //if (digits_count <= number->precision)
      {
        digits[digits_count/*number->precision*/]= 0;
      }
      Val= _atoi64(digits);

      OldVal= Val;
      Val= (Val + RoundNumber / 2) / RoundNumber * RoundNumber;
      if (OldVal != Val)
      {
        return MADB_ERR_22003;
      }
      _snprintf(digits, sizeof(digits), "%lld", Val/RoundNumber);
      digits_count= (short)strlen(digits);
      if (digits_count > number->precision)
        return MADB_ERR_22003;
    }

    digits_count= MIN(digits_count, MADB_DEFAULT_PRECISION + 1);
    for (hval = 0, bit = 1L, sta = 0, olen = 0; sta < digits_count;)
    {
      for (dig = 0, i = sta; i < digits_count; i++)
      {
        tv = dig * 10 + digits[i] - '0';
        dig = tv % 2;
        digits[i] = tv / 2 + '0';
        if (i == sta && tv < 2)
          sta++;
      }
      if (dig > 0)
        hval |= bit;
      bit <<= 1;
      if (bit >= (1L << 8))
      {
        if (olen >= SQL_MAX_NUMERIC_LEN)
        {
          //number->scale = sta - number->precision;
          ret= MADB_ERR_22003;
          break;
        }
        number->val[olen++] = hval;
        hval = 0;
        bit = 1L;

      } 
    }
    if (hval != 0)
    {
      if (olen < SQL_MAX_NUMERIC_LEN)
      {
        number->val[olen++] = hval;
      }
      else
      {
        ret= MADB_ERR_22003;
      }
    }
  } 
  return ret;
}

/* {{{ MADB_GetHexString */
size_t MADB_GetHexString(char *BinaryBuffer, size_t BinaryLength,
                          char *HexBuffer, size_t HexLength)
{
  const char HexDigits[]= "0123456789ABCDEF";
  char *Start= HexBuffer;
  size_t CurrentLength= HexLength;

  if (!HexBuffer || !BinaryBuffer)
    return 0;
   
  while (BinaryLength-- && CurrentLength > 2)
  {
    *HexBuffer++=HexDigits[*BinaryBuffer >> 4];
    *HexBuffer++=HexDigits[*BinaryBuffer & 0x0F];
    BinaryBuffer++;
    CurrentLength-= 2;
  }
  *HexBuffer= 0;
  return (HexBuffer - Start);
}


SQLRETURN MADB_DaeStmt(MADB_Stmt *Stmt, SQLUSMALLINT Operation)
{
  char          *TableName=   MADB_GetTableName(Stmt);
  char          *CatalogName= MADB_GetCatalogName(Stmt);
  MADB_DynString DynStmt;

  MADB_CLEAR_ERROR(&Stmt->Error);
  memset(&DynStmt, 0, sizeof(MADB_DynString));

  if (Stmt->DaeStmt)
    Stmt->Methods->StmtFree(Stmt->DaeStmt, SQL_DROP);
  Stmt->DaeStmt= NULL;

  if (!SQL_SUCCEEDED(MA_SQLAllocHandle(SQL_HANDLE_STMT, (SQLHANDLE)Stmt->Connection, (SQLHANDLE *)&Stmt->DaeStmt)))
  {
    MADB_CopyError(&Stmt->Error, &Stmt->Connection->Error);
    goto end;
  }

  switch(Operation)
  {
  case SQL_ADD:
    if (IS_ORACLE_MODE(Stmt)){
      if (MADB_InitDynamicString(&DynStmt, "INSERT INTO ", 1024, 1024) ||
        MADB_DynstrAppend(&DynStmt, CatalogName) ||
        MADB_DynstrAppend(&DynStmt, ".") ||
        MADB_DynstrAppend(&DynStmt, TableName)) {
        MADB_DynstrFree(&DynStmt);
        MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
        return Stmt->Error.ReturnValue;
      }
    } else {
      if (MADB_InitDynamicString(&DynStmt, "INSERT INTO ", 1024, 1024) ||
        MADB_DynStrAppendQuoted(&DynStmt, CatalogName) ||
        MADB_DynstrAppend(&DynStmt, ".") ||
        MADB_DynStrAppendQuoted(&DynStmt, TableName)) {
        MADB_DynstrFree(&DynStmt);
        MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
        return Stmt->Error.ReturnValue;
      }
    }
    if (MADB_DynStrUpdateSet(Stmt, &DynStmt)){
      MADB_DynstrFree(&DynStmt);
      return Stmt->Error.ReturnValue;
    }
    Stmt->DataExecutionType= MADB_DAE_ADD;
    break;
  case SQL_DELETE:
    if (IS_ORACLE_MODE(Stmt)) {
      if (MADB_InitDynamicString(&DynStmt, "DELETE FROM ", 1024, 1024) ||
        MADB_DynstrAppend(&DynStmt, CatalogName) ||
        MADB_DynstrAppend(&DynStmt, ".") ||
        MADB_DynstrAppend(&DynStmt, TableName)) {
        MADB_DynstrFree(&DynStmt);
        MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
        return Stmt->Error.ReturnValue;
      }
    } else {
      if (MADB_InitDynamicString(&DynStmt, "DELETE FROM ", 1024, 1024) ||
        MADB_DynStrAppendQuoted(&DynStmt, CatalogName) ||
        MADB_DynstrAppend(&DynStmt, ".") ||
        MADB_DynStrAppendQuoted(&DynStmt, TableName)) {
        MADB_DynstrFree(&DynStmt);
        MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
        return Stmt->Error.ReturnValue;
      }
    }
    if (MADB_DynStrGetWhere(Stmt, &DynStmt, TableName, FALSE)){
      MADB_DynstrFree(&DynStmt);
      return Stmt->Error.ReturnValue;
    }
    Stmt->DataExecutionType= MADB_DAE_DELETE;
    break;
  case SQL_UPDATE:
    if (IS_ORACLE_MODE(Stmt)) {
      if (MADB_InitDynamicString(&DynStmt, "UPDATE ", 1024, 1024) ||
        MADB_DynstrAppend(&DynStmt, CatalogName) ||
        MADB_DynstrAppend(&DynStmt, ".") ||
        MADB_DynstrAppend(&DynStmt, TableName)) {
        MADB_DynstrFree(&DynStmt);
        MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
        return Stmt->Error.ReturnValue;
      }
    } else {
      if (MADB_InitDynamicString(&DynStmt, "UPDATE ", 1024, 1024) ||
        MADB_DynStrAppendQuoted(&DynStmt, CatalogName) ||
        MADB_DynstrAppend(&DynStmt, ".") ||
        MADB_DynStrAppendQuoted(&DynStmt, TableName)) {
        MADB_DynstrFree(&DynStmt);
        MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
        return Stmt->Error.ReturnValue;
      }
    }
    if (MADB_DynStrUpdateSet(Stmt, &DynStmt) ||
      MADB_DynStrGetWhere(Stmt, &DynStmt, TableName, FALSE)){
      MADB_DynstrFree(&DynStmt);
      return Stmt->Error.ReturnValue;
    }
    Stmt->DataExecutionType= MADB_DAE_UPDATE;
    break;
  }
  
  if (!SQL_SUCCEEDED(Stmt->DaeStmt->Methods->Prepare(Stmt->DaeStmt, DynStmt.str, SQL_NTS, FALSE)))
  {
    MADB_CopyError(&Stmt->Error, &Stmt->DaeStmt->Error);
    Stmt->Methods->StmtFree(Stmt->DaeStmt, SQL_DROP);
    Stmt->DaeStmt = NULL;
  }
   
end:
  MADB_DynstrFree(&DynStmt);
  return Stmt->Error.ReturnValue;

}


int MADB_FindNextDaeParam(MADB_Desc *Desc, int InitialParam, SQLSMALLINT RowNumber)
{
  int             i;
  MADB_DescRecord *Record;

  for (i= InitialParam > -1 ? InitialParam + 1 : 0; i < Desc->Header.Count; i++)
  {
    if ((Record= MADB_DescGetInternalRecord(Desc, i, MADB_DESC_READ)))
    {
      if (Record->OctetLengthPtr)
      {
        /* Stmt->DaeRowNumber is 1 based */
        SQLLEN *OctetLength = (SQLLEN *)GetBindOffset(Desc, Record, Record->OctetLengthPtr, RowNumber > 1 ? RowNumber - 1 : 0, sizeof(SQLLEN));
        if (PARAM_IS_DAE(OctetLength))
        {
          return i;
        }
      }
    }
  }

  return MADB_NOPARAM;
}


BOOL MADB_IsNumericType(SQLSMALLINT ConciseType)
{
  switch (ConciseType)
  {
    case SQL_C_DOUBLE:
    case SQL_C_FLOAT:
    case SQL_DECIMAL:
      return TRUE;
  }

  return MADB_IsIntType(ConciseType);
}


BOOL MADB_IsIntType(SQLSMALLINT ConciseType)
{
  switch (ConciseType)
  {
  case SQL_C_TINYINT:
  case SQL_C_STINYINT:
  case SQL_C_UTINYINT:
  case SQL_C_SHORT:
  case SQL_C_SSHORT:
  case SQL_C_USHORT:
  case SQL_C_LONG:
  case SQL_C_SLONG:
  case SQL_C_ULONG:
  case SQL_C_UBIGINT:
  case SQL_C_SBIGINT:
  case SQL_BIGINT:
    return TRUE;
  }
  return FALSE;
}

/* Now it's more like installing result */
void MADB_InstallStmt(MADB_Stmt *Stmt, MYSQL_STMT *stmt)
{
  Stmt->stmt= stmt;

  if (mysql_stmt_field_count(Stmt->stmt) == 0)
  {
    MADB_DescFree(Stmt->Ird, TRUE);
    Stmt->AffectedRows= mysql_stmt_affected_rows(Stmt->stmt);
  }
  else
  {
    Stmt->AffectedRows= 0;
    MADB_StmtResetResultStructures(Stmt);
    MADB_DescSetIrdMetadata(Stmt, mysql_fetch_fields(FetchMetadata(Stmt)), mysql_stmt_field_count(Stmt->stmt));
  }
}

BOOL IsStmtRefCursor(MADB_Stmt *Stmt)
{
  BOOL bRet = FALSE;
  if (Stmt && IS_ORACLE_MODE(Stmt) && Stmt->Connection && Stmt->lastRefCursor>=0 && Stmt->maxRefCursor>0) {
    bRet = TRUE;
  }
  return bRet;
}

BOOL IsStmtNossps(MADB_Stmt *Stmt)
{
  BOOL bRet = FALSE;
  if (Stmt && Stmt->Connection && Stmt->Connection->Dsn->NoSsps){
    bRet = TRUE;
  }
  return bRet;
}

unsigned int StmtFieldCount(MADB_Stmt *Stmt)
{
  if (IsStmtRefCursor(Stmt)) {
    if (Stmt->stmtRefCursor) {
      return mysql_stmt_field_count(Stmt->stmtRefCursor);
    } else {
      return 0;
    }
  } else if (IsStmtNossps(Stmt)){
    if (Stmt->result2){
      return Stmt->result2->field_count;
    } else {
      return mysql_field_count(Stmt->Connection->mariadb);
    }
  } else {
    return mysql_stmt_field_count(Stmt->stmt);
  }
}

unsigned long long StmtNumRows(MADB_Stmt *Stmt)
{
  if (IsStmtRefCursor(Stmt)) {
    if (Stmt->stmtRefCursor)
      return mysql_stmt_num_rows(Stmt->stmtRefCursor);
    else
      return 0;
  } else if (IsStmtNossps(Stmt)){
    if (Stmt->result2)
      return mysql_num_rows(Stmt->result2);
    else
      return 0;
  } else {
    return mysql_stmt_num_rows(Stmt->stmt);
  }
}

BOOL StmtMoreResults(MADB_Stmt *Stmt)
{
  if (IsStmtNossps(Stmt)) {
    return mysql_more_results(Stmt->Connection->mariadb);
  } else {
    return mysql_stmt_more_results(Stmt->stmt);
  }
}

void StmtFreeResults(MADB_Stmt *Stmt)
{
  if (IsStmtRefCursor(Stmt)) {
    if (Stmt->stmtRefCursor)
      mysql_stmt_free_result(Stmt->stmtRefCursor);
  } else if (IsStmtNossps(Stmt)) {
    if (Stmt->result2)
      mysql_free_result(Stmt->result2);
    Stmt->result2 = NULL;
  } else {
    mysql_stmt_free_result(Stmt->stmt);
  }
}

int StmtNextResult(MADB_Stmt *Stmt)
{
  if (IsStmtNossps(Stmt)) {
    return mysql_next_result(Stmt->Connection->mariadb);
  } else {
    return mysql_stmt_next_result(Stmt->stmt);
  }
}
unsigned long long StmtAffectRows(MADB_Stmt *Stmt)
{
  if (IsStmtNossps(Stmt)) {
    return mysql_affected_rows(Stmt->Connection->mariadb);
  } else {
    return mysql_stmt_affected_rows(Stmt->stmt);
  }
}

void StmtStoreResult(MADB_Stmt *Stmt)
{
  if (IsStmtNossps(Stmt)) {
    Stmt->result2 = mysql_store_result(Stmt->Connection->mariadb);
  } else {
    mysql_stmt_store_result(Stmt->stmt);
  }
}

void StmtDataSeek(MADB_Stmt *Stmt, unsigned long long offset)
{
  if (IsStmtNossps(Stmt)) {
    mysql_data_seek(Stmt->result2, offset);
  } else {
    mysql_stmt_data_seek(Stmt->stmt, offset);
  }
}

long double MyStrtold(const char *nptr, char **endptr)
{
  /*
   * Experienced odd compilation errors on one of windows build hosts -
   * cmake reported there is strold function. Since double and long double on windows
   * are of the same size - we are using strtod on those platforms regardless
   * to the HAVE_FUNCTION_STRTOLD value
   */
#ifdef _WIN32
  return strtod(nptr, endptr);
#else
# ifndef HAVE_FUNCTION_STRTOLD
  return strtod(nptr, endptr);
# else
#  if defined(__hpux) && defined(_LONG_DOUBLE)
  union {
    long_double l_d;
    long double ld;
  } u;
  u.l_d = strtold(nptr, endptr);
  return u.ld;
#  else
  return strtold(nptr, endptr);
#  endif
# endif
#endif
}

char * Myll2str(long long val, char* dst, int radix)
{
#if defined(HAVE_LONG_LONG) && SIZEOF_LONG != 8
  typedef unsigned long long int ulonglong; /* ulong or unsigned long long */
  typedef long long int longlong;
#else
  typedef unsigned long	ulonglong;	/* ulong or unsigned long long */
  typedef long		longlong;
#endif
#define LONG_MAX      2147483647L   // maximum (signed) long value
  static char _dig_vec[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  char buffer[65];
  register char *p;
  long long_val;

  if (radix < 0)
  {
    if (radix < -36 || radix > -2) return (char*)0;
    if (val < 0) {
      *dst++ = '-';
      val = 0ULL - val;
    }
    radix = -radix;
  }
  else
  {
    if (radix > 36 || radix < 2) return (char*)0;
  }
  if (val == 0)
  {
    *dst++ = '0';
    *dst = '\0';
    return dst;
  }
  p = &buffer[sizeof(buffer) - 1];
  *p = '\0';

  while ((ulonglong)val > (ulonglong)LONG_MAX)
  {
    ulonglong quo = (ulonglong)val / (uint)radix;
    uint rem = (uint)(val - quo * (uint)radix);
    *--p = _dig_vec[rem];
    val = quo;
  }
  long_val = (long)val;
  while (long_val != 0)
  {
    long quo = long_val / radix;
    *--p = _dig_vec[(unsigned char)(long_val - quo * radix)];
    long_val = quo;
  }
  while ((*dst++ = *p++) != 0);
  return dst - 1;
}

BOOL IsDataTruncate(MADB_Stmt *Stmt, MADB_DescRecord *ArdRecord, void *Src, long SrcLen)
{
  if (ArdRecord->ConciseType == SQL_CHAR){
    return ArdRecord->OctetLength < SrcLen;
  }
  switch(ArdRecord->ConciseType){
  case SQL_C_BIT:
    return ArdRecord->OctetLength < 1;
  case SQL_C_TYPE_TIMESTAMP:
  case SQL_C_TIMESTAMP:
    return ArdRecord->OctetLength < sizeof(SQL_TIMESTAMP_STRUCT);
  case SQL_C_TYPE_TIME:
  case SQL_C_TIME:
    return ArdRecord->OctetLength < sizeof(SQL_TIME_STRUCT);
  case SQL_C_TYPE_DATE:
  case SQL_C_DATE:
    return ArdRecord->OctetLength < sizeof(SQL_DATE_STRUCT);
  case SQL_C_INTERVAL_HOUR_TO_MINUTE:
  case SQL_C_INTERVAL_HOUR_TO_SECOND:
    return ArdRecord->OctetLength < sizeof(SQL_INTERVAL_STRUCT);
  case SQL_C_NUMERIC:
    //return ArdRecord->OctetLength < sizeof(SQL_NUMERIC_STRUCT);
    return FALSE;
  case SQL_C_WCHAR:
    return ArdRecord->OctetLength < SrcLen * sizeof(SQLWCHAR);
  case SQL_C_CHAR:
    return ArdRecord->OctetLength < SrcLen;
  case SQL_C_TINYINT:
  case SQL_C_STINYINT:
    if (Src != NULL){
      long long val = strtoll(Src, NULL, 10);
      return NUMERIC_TRUNCATION(val, INT_MIN8, INT_MAX8);
    } else {
      return ArdRecord->OctetLength < 1;
    }
  case SQL_C_UTINYINT:
    if (Src != NULL) {
      long long val = strtoll(Src, NULL, 10);
      return NUMERIC_TRUNCATION(val, 0, UINT_MAX8);
    } else {
      return ArdRecord->OctetLength < 1;
    }
  case SQL_C_SHORT:
  case SQL_C_SSHORT:
    if (Src != NULL) {
      long long val = strtoll(Src, NULL, 10);
      return NUMERIC_TRUNCATION(val, INT_MIN16, INT_MAX16);
    } else {
      return ArdRecord->OctetLength < 2;
    }
  case SQL_C_USHORT:
    if (Src != NULL) {
      long long val = strtoll(Src, NULL, 10);
      return NUMERIC_TRUNCATION(val, 0, UINT_MAX16);
    } else {
      return ArdRecord->OctetLength < 2;
    }
  case SQL_C_LONG:
  case SQL_C_SLONG:
    if (Src != NULL){
      long long val = strtoll(Src, NULL, 10);
      return NUMERIC_TRUNCATION(val, INT_MIN32, INT_MAX32);
    } else {
      return ArdRecord->OctetLength < sizeof(SQLINTEGER);
    }
  case SQL_C_ULONG:
    if (Src != NULL) {
      long long val = strtoll(Src, NULL, 10);
      return NUMERIC_TRUNCATION(val, 0, UINT_MAX32);
    } else {
      return ArdRecord->OctetLength < sizeof(SQLINTEGER);
    }
  case SQL_C_FLOAT:
    return ArdRecord->OctetLength < sizeof(float);
  case SQL_C_DOUBLE:
    return ArdRecord->OctetLength < sizeof(double);
  case SQL_DEFAULT:
    return FALSE;
  default:
    return ArdRecord->OctetLength < SrcLen;
  }
}

BOOL GetDataByString(MADB_Stmt *Stmt, MADB_DescRecord *ArdRecord, void* Src, SQLLEN SrcLen, void* Dst, SQLLEN DstLen)
{
  switch(ArdRecord->ConciseType){
  case SQL_C_WCHAR:
    if (Src != NULL && Dst != NULL) {
      MADB_SetString(&Stmt->Connection->Charset, Dst, DstLen, (char *)Src, SrcLen, &Stmt->Error);
      return TRUE;
    }
	return FALSE;
  case SQL_C_CHAR:
    if (Src != NULL && Dst != NULL) {
      memcpy(Dst, Src, MIN(SrcLen, DstLen));
      return TRUE;
    } 
    return FALSE;
  case SQL_C_TINYINT:
  case SQL_C_STINYINT:
    if (Src != NULL && Dst != NULL) {
      long long val = strtoll(Src, NULL, 10);
      *(SQLSCHAR*)Dst = (SQLSCHAR)val;
      return TRUE;
    }
    return FALSE;
  case SQL_C_UTINYINT:
    if (Src != NULL && Dst != NULL) {
      long long val = strtoll(Src, NULL, 10);
      *(SQLCHAR*)Dst = (SQLCHAR)val;
      return TRUE;
    }
    return FALSE;
  case SQL_C_SHORT:
  case SQL_C_SSHORT:
    if (Src != NULL && Dst != NULL) {
      long long val = strtoll(Src, NULL, 10);
      *(SQLSMALLINT*)Dst = (SQLSMALLINT)val;
      return TRUE;
    }
    return FALSE;
  case SQL_C_USHORT:
    if (Src != NULL && Dst != NULL) {
      long long val = strtoll(Src, NULL, 10);
      *(SQLUSMALLINT*)Dst = (SQLUSMALLINT)val;
      return TRUE;
    }
    return FALSE;
  case SQL_C_LONG:
  case SQL_C_SLONG:
    if (Src != NULL && Dst != NULL) {
      long long val = strtoll(Src, NULL, 10);
      *(SQLINTEGER*)Dst = (SQLINTEGER)val;
      return TRUE;
    }
    return FALSE;
  case SQL_C_ULONG:
    if (Src != NULL && Dst != NULL) {
      long long val = strtoll(Src, NULL, 10);
      *(SQLUINTEGER*)Dst = (SQLUINTEGER)val;
      return TRUE;
    }
    return FALSE;
  case SQL_C_FLOAT:
    if (Src != NULL && Dst != NULL) {
      *(float*)Dst = (float)MyStrtold(Src, NULL);
      return TRUE;
    }
    return FALSE;
  case SQL_C_DOUBLE:
    if (Src != NULL && Dst != NULL) {
      *(double*)Dst = (double)MyStrtold(Src, NULL);
      return TRUE;
    }
  default:
    if (Src != NULL && Dst != NULL) {
      memcpy(Dst, Src, MIN(SrcLen, DstLen));
      return TRUE;
    }
    return FALSE;
  }
  return FALSE;
}

char getHexChar(char uc){
  if (uc >= '0' && uc <= '9'){
    return uc - '0';
  } else if (uc >= 'A' && uc <= 'Z'){
    return uc - 'A';
  } else if (uc >= 'a' && uc <= 'z'){
    return uc - 'a';
  } else {
    return -1;
  }
}
BOOL GetBinaryData(MADB_Stmt *Stmt, unsigned char* Dst, unsigned long DstLen, unsigned char* Src, unsigned long SrcLen, unsigned long *offset)
{
  unsigned long i = 0, j = 0, maxSrc = 0;
  maxSrc = DstLen * 2 > SrcLen ? SrcLen: DstLen*2;
  for(i = 0, j = 0; i < maxSrc; ){
    if (i == 0 && SrcLen % 2 == 1) {
      char c = getHexChar(Src[i]);
      if (c < 0){
        return FALSE;
      }
      Dst[j] = c;
      i++;
      j++;
    } else {
      char c1 = getHexChar(Src[i]);
      char c2 = getHexChar(Src[i+1]);
      if (c1 < 0 || c2 < 0){
        return FALSE;
      }
      Dst[j] = c1 * 16 + c2;
      i += 2;
      j++;
    }
  }
  *offset = j;
  return TRUE;
}

char* GetInsertQueryHead(MADB_QUERY* Query)
{
  char *pQuery = Query->RefinedText;
  char *head = strstr(pQuery, "(");
  if (head) {
    char *tmp = strstr(head + 1, "(");
    if (tmp) {
      head = tmp;
    }
  }
  return head;
}

int FormatCallSQL(char* sql, SQLINTEGER len, MADB_DynString* str)
{
  //example: {call TEST.TEST1.MY_PROC_NAME()}  'TEST1' is package, then must delete 'TEST.'
  char *start = NULL;
  char *end = NULL;
  char *pCall = NULL;
  SQLINTEGER i = 0;
  BOOL hasParentheses = 0;  //'()'
  BOOL hasBraces = 0;   //'{}'
  BOOL hasReturn = 0;   //?=call xxx
  BOOL useBlock = 1;

  char *p = NULL;
  char *buf = calloc(1, len + 32);
  if (NULL == buf)
    return -1;

  p = buf;
  memcpy(p, sql, len);
  p = buf + len;
  MADB_TRIM_RIGHT_SPACE(p);
  p = buf;
  MADB_TRIM_SPACE(p);
  if (*p == '{') {
    p++; MADB_TRIM_SPACE(p);
  }
  if (*p == '?') { //for function
    hasReturn = 1;
    p++; MADB_TRIM_SPACE(p);
    if (*p == '=') {
      p++; MADB_TRIM_SPACE(p);
    }
  }
  if (strncasecmp(p, "call", 4)==0) {
    pCall = p + 4;
    start = p + 4;
    if (NULL != (p = strchr(start, '='))) {
      //function->{call :1=TEST.packet()}
      start = p+1;
    }
    if (NULL != (p = strchr(start, '.'))) {
      end = p + 1;
      if (NULL == (p=strchr(end, '.'))){
        start = NULL;
        end = NULL;
      }
    } else {
      start = NULL;
      end = NULL;
    }

    //{call "TEST"."TEST1.MY_PROC_NAME"()} -> {call      "TEST1.MY_PROC_NAME"()}
    for (; start && end && end - start > 0; start++) {
      *start = ' ';
    }

    //{call      "TEST1.MY_PROC_NAME"()} -> {call       TEST1.MY_PROC_NAME ()}
    if (pCall) {
      p = pCall;
      while (*p != '\0') {
        if (*p == '"')
          *p = ' ';
        p++;
      }
    }

    //{call      TEST1.MY_PROC_NAME} -> {call      TEST1.MY_PROC_NAME()}
    p = buf;
    i = len - 1;
    while(i>0 && !isalpha(p[i])){
      if (p[i]==')')
        hasParentheses = 1;
      if (p[i] == '}')
        hasBraces = 1;
      i--;
    }
    if (!hasParentheses) {
      for(i = len -1; i>0; i--) {
        if (p[i] != ' ' && p[i] != '\t' && p[i] !='\r' && p[i] != '\n' && p[i] != '}')
          break;
      }
      p[++i] = '(';
      p[++i] = ')';
      if (hasBraces){
        p[++i] = '}';
      }
      p[++i] = 0;
    }

    if (useBlock){
      MADB_DynstrAppendMem(str, "begin \n ", 8);

      if (hasReturn)
        MADB_DynstrAppendMem(str, " ?:=", 4);
      if (hasBraces)
        MADB_DynstrAppendMem(str, pCall, strlen(pCall) - 1); //-1 -> delete '}'
      else
        MADB_DynstrAppendMem(str, pCall, strlen(pCall));

      MADB_DynstrAppendMem(str, "; \nend;", 7);
    } else {
      MADB_DynstrAppendMem(str, buf, strlen(buf));
    }
    free(buf);
  }
  return 0;
}

int FnConvert(char** pos, MADB_DynString* str) {
  int ret = 0;
  if (strncasecmp(*pos, "ifnull", 6) == 0) {
    MADB_DynstrAppendMem(str, "NVL", 3);
    *pos += 6;
  } else if (strncasecmp(*pos, "substring", 9) == 0) {
    MADB_DynstrAppendMem(str, "SUBSTR", 6);
    *pos += 9;
  } else if (strncasecmp(*pos, "left", 4) == 0) {
    char *tmp = NULL;
    MADB_DynstrAppendMem(str, "SUBSTR", 6);
    *pos += 4;
    tmp = strstr(*pos, ",");
    if (tmp) {
      int len = tmp - *pos;
      MADB_DynstrAppendMem(str, *pos, len);
      MADB_DynstrAppendMem(str, ",1", 2);
      *pos += len;
    }
  }
  return ret;
}
int FormatNativeSQL(char* sql, SQLINTEGER len, MADB_DynString* str)
{
  //select {fn IFNULL(NULL, 'aaa')} from dual
  BOOL inString = 0;
  BOOL inComment = 0;
  BOOL isFn = 0;
  SQLINTEGER i = 0;
  char *pos = sql;
  char *end = sql + len;

  while(*pos != 0 && pos < end){
    if (*pos == '/' && *(pos + 1) == '*') {
      MADB_DynstrAppendMem(str, pos, 2);
      pos += 2;
      inComment = 1;
    } else if (*pos == '*' && *(pos + 1) == '/') {
      MADB_DynstrAppendMem(str, pos, 2);
      pos += 2;
      inComment = 0;
    } else if (*pos == '\'' || *pos == '\"') {
      MADB_DynstrAppendMem(str, pos, 1);
      pos += 1;
      inString = inString ? 0 : 1;
    } else if (*pos == '{' && inString == 0 && inComment == 0) {
      //{fn xxxx}
      pos++;
      while (*pos == ' ')pos++;
      if (strncasecmp(pos, "fn", 2) == 0) {
        isFn = 1;
        pos += 2;
      }

      while (*pos == ' ') {
        MADB_DynstrAppendMem(str, pos, 1);
        pos++;
      }

      //{ISNULL/LEFT/SUBSTRING}
      if (isFn) {
        FnConvert(&pos, str);
      }
    } else if (*pos == '}' && isFn && inString == 0 && inComment == 0){
      MADB_DynstrAppendMem(str, " ", 1);
      pos++;
      isFn = 0;
    } else {
      MADB_DynstrAppendMem(str, pos, 1);
      pos += 1;
    }
  }
  return 0;
}

int FormatSQL(char* sql, SQLINTEGER len, MADB_DynString* str, BOOL* isCall)
{
  char *p = sql;
  *isCall = 0;

  MADB_TRIM_SPACE(p);
  if (*p == '{')
    p++; MADB_TRIM_SPACE(p);

  if (*p == '?') { //for function
    p++; MADB_TRIM_SPACE(p);
    if (*p == '=') {
      p++; MADB_TRIM_SPACE(p);
    }
  }
  
  if (strncasecmp(p, "call", 4) == 0){
    *isCall = 1;
  }

  if (*isCall) {
    //call sql
    return FormatCallSQL(sql, len, str);
  } else {
    //native sql
    return FormatNativeSQL(sql, len, str);
  }
  return -1;
}