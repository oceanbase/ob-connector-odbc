/************************************************************************************
   Copyright (c) 2000, 2018, Oracle and/or its affiliates.

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
#include "ma_catalog.h"

#define MAX_CATALOG_SQLLEN 2048
/* {{{ MADB_StmtColumnPrivileges */
SQLRETURN MADB_StmtColumnPrivileges(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                                    char *SchemaName, SQLSMALLINT NameLength2, char *TableName,
                                    SQLSMALLINT NameLength3, char *ColumnName, SQLSMALLINT NameLength4)
{
  char StmtStr[MAX_CATALOG_SQLLEN] = {0};
  char *p = StmtStr;

  MADB_CLEAR_ERROR(&Stmt->Error);

  /* TableName is mandatory */
  if (!TableName || !NameLength3)
  {
    MADB_SetError(&Stmt->Error, MADB_ERR_HY009, "Tablename is required", 0);
    return Stmt->Error.ReturnValue;
  }
  
  if(IS_ORACLE_MODE(Stmt)) {
    p+= _snprintf(p, MAX_CATALOG_SQLLEN,
      "SELECT AO.OWNER AS TABLE_SCHEMA, NULL AS TABLE_CAT,AO.OBJECT_NAME AS TABLE_NAME, NULL AS COLUMN_NAME, "
      " NULL AS GRANTOR, NULL AS GRANTEE, NULL AS PRIVILEGE, AO.GENERATED AS IS_GRANTABLE "
      "FROM ALL_OBJECTS AO WHERE AO.OBJECT_TYPE= 'TABLE' ");
    
    p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " AND AO.OWNER IN SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') ");
    if (SchemaName && SchemaName[0])
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " AND  AO.OWNER LIKE '%s' ", SchemaName);

    if (TableName && TableName[0])
      p += _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " AND  AO.OBJECT_NAME = '%s' ", TableName);

    p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr)," ORDER BY TABLE_SCHEMA, TABLE_NAME");
  } else {
    p+= _snprintf(p, MAX_CATALOG_SQLLEN, "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL as TABLE_SCHEM, TABLE_NAME,"
      "COLUMN_NAME, NULL AS GRANTOR, GRANTEE, PRIVILEGE_TYPE AS PRIVILEGE,"
      "IS_GRANTABLE FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES WHERE ");
    
    if (CatalogName && CatalogName[0])
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "TABLE_SCHEMA LIKE '%s' ", CatalogName);
    else
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "TABLE_SCHEMA LIKE DATABASE() ");
    if (TableName && TableName[0])
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "AND TABLE_NAME LIKE '%s' ", TableName);
    if (ColumnName && ColumnName[0])
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "AND COLUMN_NAME LIKE '%s' ", ColumnName);
    p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "ORDER BY TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, PRIVILEGE");
  } 
  return Stmt->Methods->ExecDirect(Stmt, StmtStr, (SQLINTEGER)strlen(StmtStr));
}
/* }}} */

/* {{{ MADB_StmtTablePrivileges */
SQLRETURN MADB_StmtTablePrivileges(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                                    char *SchemaName, SQLSMALLINT NameLength2,
                                    char *TableName, SQLSMALLINT NameLength3)
{
  char StmtStr[MAX_CATALOG_SQLLEN] = {0};
  char *p = StmtStr;

  MADB_CLEAR_ERROR(&Stmt->Error);
  if(IS_ORACLE_MODE(Stmt)) {
    p += _snprintf(p, MAX_CATALOG_SQLLEN,"SELECT NULL AS TABLE_CAT, AO.OWNER AS TABLE_SCHEM, AO.OBJECT_NAME AS TABLE_NAME, "
                  "NULL AS GRANTOR, NULL as GRANTEE, NULL as PRIVILEGE, AO.GENERATED AS IS_GRANTABLE "
                  "FROM ALL_OBJECTS AO WHERE AO.OBJECT_TYPE = 'TABLE' ");
    
    p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " AND AO.OWNER IN SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') ");
    if (SchemaName && SchemaName[0])
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " AND  AO.OWNER LIKE '%s' ", SchemaName);

    //if (TableName &&TableName[0])
    //  p += _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " AND  AO.OBJECT_NAME = '%s' ", TableName);

    p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " ORDER BY TABLE_SCHEM, TABLE_NAME");
  } else {
    p += _snprintf(p, MAX_CATALOG_SQLLEN, "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, "
                  "NULL AS GRANTOR, GRANTEE, PRIVILEGE_TYPE AS PRIVILEGE, IS_GRANTABLE "
                  "FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES WHERE ");
    
    if (CatalogName)
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "TABLE_SCHEMA LIKE '%s' ", CatalogName);
    else
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "TABLE_SCHEMA LIKE IF(DATABASE(), DATABASE(), '%%') ");
    if (TableName)
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "AND TABLE_NAME LIKE '%s' ", TableName);
  
    p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "ORDER BY TABLE_SCHEM, TABLE_NAME, PRIVILEGE");
  } 

  return Stmt->Methods->ExecDirect(Stmt, StmtStr, (SQLINTEGER)strlen(StmtStr));
}
/* }}} */

/* {{{ MADB_StmtTables */
SQLRETURN MADB_StmtTables(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT CatalogNameLength,
                          char *SchemaName, SQLSMALLINT SchemaNameLength, char *TableName,
                          SQLSMALLINT TableNameLength, char *TableType, SQLSMALLINT TableTypeLength)
{
  MADB_DynString StmtStr;
  char Quote[2];
  SQLRETURN ret;

  /*
  METADATA_ID       CatalogName     SchemaName       TableName           TableType
  ---------------------------------------------------------------------------------
  ODBC_V3:
  SQL_FALSE         Pattern         Pattern          Pattern             ValueList
  SQL_TRUE          Identifier      Identifier       Identifier          ValueList
  ODBC_V2:
                    Identifier      Identifier       Identifier          ValueList
  --------------------------------------------------------------------------------
  */

  MDBUG_C_ENTER(Stmt->Connection, "MADB_StmtTables");

  ADJUST_LENGTH(CatalogName, CatalogNameLength);
  ADJUST_LENGTH(SchemaName, SchemaNameLength);
  ADJUST_LENGTH(TableName, TableNameLength);
  ADJUST_LENGTH(TableType, TableTypeLength);

  if(IS_ORACLE_MODE(Stmt)) {
    MADB_InitDynamicString(&StmtStr, "SELECT NULL AS TABLE_CAT, AO.OWNER AS TABLE_SCHEM,AO.OBJECT_NAME AS TABLE_NAME,AO.OBJECT_TYPE AS TABLE_TYPE,ATC.COMMENTS AS REMARKS FROM ALL_OBJECTS AO, ALL_TAB_COMMENTS ATC WHERE AO.OWNER LIKE ",8192,521);
    MADB_DynstrAppend(&StmtStr, "'");
    if(SchemaName != NULL && SchemaNameLength > 0) {
      MADB_DynstrAppend(&StmtStr, SchemaName);
    } else {
      MADB_DynstrAppend(&StmtStr, "%");
    }
    MADB_DynstrAppend(&StmtStr, "'");
    MADB_DynstrAppend(&StmtStr, " ESCAPE '/' AND AO.OBJECT_NAME LIKE ");
    MADB_DynstrAppend(&StmtStr, "'");
    if(TableName != NULL && TableNameLength > 0) {
      MADB_DynstrAppend(&StmtStr,TableName);
    } else {
      MADB_DynstrAppend(&StmtStr, "%");
    }
    MADB_DynstrAppend(&StmtStr, "'");
    MADB_DynstrAppend(&StmtStr, " ESCAPE '/' AND AO.OWNER = ATC.OWNER (+) AND AO.OBJECT_NAME = ATC.TABLE_NAME (+) AND AO.OWNER != '__RECYCLEBIN' ");
    if (TableType && TableTypeLength && strcmp(TableType, SQL_ALL_TABLE_TYPES) != 0)
    {
      unsigned int i;
      char *myTypes[3] = { "TABLE", "VIEW", "SYNONYM" };
      MADB_DynstrAppend(&StmtStr, " AND AO.OBJECT_TYPE IN (''");
      for (i = 0; i < 3; i++)
      {
        if (strstr(TableType, myTypes[i]))
        {
          MADB_DynstrAppend(&StmtStr, ", '");
          MADB_DynstrAppend(&StmtStr, myTypes[i]);
          MADB_DynstrAppend(&StmtStr, "'");
        }
      }
      MADB_DynstrAppend(&StmtStr, ") ");
    } else {
      MADB_DynstrAppend(&StmtStr, " AND AO.OBJECT_TYPE IN ('SYNONYM','TABLE','VIEW') ");
    }
    MADB_DynstrAppend(&StmtStr, " ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME");
  } else {
    if (CatalogNameLength > 64 || TableNameLength > 64)
    {
      MADB_SetError(&Stmt->Error, MADB_ERR_HY090, "Table and catalog names are limited to 64 chars", 0);
      return Stmt->Error.ReturnValue;
    }

    /* SQL_ALL_CATALOGS 
       If CatalogName is SQL_ALL_CATALOGS and SchemaName and TableName are empty strings, 
       the result set contains a list of valid catalogs for the data source. 
       (All columns except the TABLE_CAT column contain NULLs
    */
    if (CatalogName && CatalogNameLength && TableName != NULL && !TableNameLength &&
      SchemaName != NULL && SchemaNameLength == 0 && !strcmp(CatalogName, SQL_ALL_CATALOGS))
    {
      MADB_InitDynamicString(&StmtStr, "SELECT SCHEMA_NAME AS TABLE_CAT, CONVERT(NULL,CHAR(64)) AS TABLE_SCHEM, "
                                    "CONVERT(NULL,CHAR(64)) AS TABLE_NAME, NULL AS TABLE_TYPE, NULL AS REMARKS "
                                    "FROM INFORMATION_SCHEMA.SCHEMATA "
                                    "GROUP BY SCHEMA_NAME ORDER BY SCHEMA_NAME",
                                    8192, 512);
    }
    /* SQL_ALL_TABLE_TYPES
       If TableType is SQL_ALL_TABLE_TYPES and CatalogName, SchemaName, and TableName are empty strings, 
       the result set contains a list of valid table types for the data source. 
       (All columns except the TABLE_TYPE column contain NULLs.)
    */
    else if (CatalogName != NULL && !CatalogNameLength && TableName != NULL && !TableNameLength &&
      SchemaName != NULL && SchemaNameLength == 0 && TableType && TableTypeLength &&
              !strcmp(TableType, SQL_ALL_TABLE_TYPES))
    {
      MADB_InitDynamicString(&StmtStr, "SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, "
                                    "NULL AS TABLE_NAME, 'TABLE' AS TABLE_TYPE, NULL AS REMARKS "
                                    "FROM DUAL "
                                    "UNION "
                                    "SELECT NULL, NULL, NULL, 'VIEW', NULL FROM DUAL "
                                    "UNION "
                                    "SELECT NULL, NULL, NULL, 'SYSTEM VIEW', NULL FROM DUAL",
                                    8192, 512); 
    }
    /* Since we treat our databases as catalogs, the only acceptable value for schema is NULL or "%"
       if that is not the special case of call for schemas list. Otherwise we return empty resultset*/
    else if (SchemaName &&
      ((!strcmp(SchemaName,SQL_ALL_SCHEMAS) && CatalogName && CatalogNameLength == 0 && TableName && TableNameLength == 0) ||
        strcmp(SchemaName, SQL_ALL_SCHEMAS)))
    {
      MADB_InitDynamicString(&StmtStr, "SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, "
        "NULL AS TABLE_NAME, NULL AS TABLE_TYPE, NULL AS REMARKS "
        "FROM DUAL WHERE 1=0", 8192, 512);
    }
    else
    {
      MADB_InitDynamicString(&StmtStr, "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, "
                                    "if(TABLE_TYPE='BASE TABLE','TABLE',TABLE_TYPE) AS TABLE_TYPE ,"
                                    "TABLE_COMMENT AS REMARKS FROM INFORMATION_SCHEMA.TABLES WHERE 1=1 ",
                                    8192, 512);
      if (Stmt->Options.MetadataId== SQL_TRUE)
      {
        strcpy(Quote, "`");
      }
      else
      {
        strcpy(Quote, "'");
      }

      if (CatalogName != NULL)
      {
        MADB_DynstrAppend(&StmtStr, " AND TABLE_SCHEMA ");
        MADB_DynstrAppend(&StmtStr, "LIKE ");
        MADB_DynstrAppend(&StmtStr, Quote);
        MADB_DynstrAppend(&StmtStr, CatalogName);
        MADB_DynstrAppend(&StmtStr, Quote);
      }
      else if (Stmt->Connection->Environment->AppType == ATypeMSAccess)
      {
        MADB_DynstrAppend(&StmtStr, " AND TABLE_SCHEMA=DATABASE()");
      }

      if (TableName && TableNameLength)
      {
        MADB_DynstrAppend(&StmtStr, " AND TABLE_NAME LIKE ");
        MADB_DynstrAppend(&StmtStr, Quote);
        MADB_DynstrAppend(&StmtStr, TableName);
        MADB_DynstrAppend(&StmtStr, Quote);
      }
      if (TableType && TableTypeLength && strcmp(TableType, SQL_ALL_TABLE_TYPES) != 0)
      {
        unsigned int i;
        char *myTypes[3]= {"TABLE", "VIEW", "SYNONYM"};
        MADB_DynstrAppend(&StmtStr, " AND TABLE_TYPE IN (''");
        for (i= 0; i < 3; i++)
        {
          if (strstr(TableType, myTypes[i]))
          {
            if (strstr(myTypes[i], "TABLE"))
              MADB_DynstrAppend(&StmtStr, ", 'BASE TABLE'");
            else
            {
              MADB_DynstrAppend(&StmtStr, ", '");
              MADB_DynstrAppend(&StmtStr, myTypes[i]);
              MADB_DynstrAppend(&StmtStr, "'");
            }
          }
        }
        MADB_DynstrAppend(&StmtStr, ") ");
      }
      MADB_DynstrAppend(&StmtStr, " ORDER BY TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE");
    }
  }
  MDBUG_C_PRINT(Stmt->Connection, "SQL Statement: %s", StmtStr.str);
  ret = Stmt->Methods->ExecDirect(Stmt, StmtStr.str, SQL_NTS);
  MADB_DynstrFree(&StmtStr);
  MDBUG_C_RETURN(Stmt->Connection, ret, &Stmt->Error);
}
/* }}} */

static MADB_ShortTypeInfo SqlStatsColType[13]=
                               /*1*/    {{SQL_VARCHAR, 0, SQL_NULLABLE, 0}, {SQL_VARCHAR, 0, SQL_NULLABLE, 0}, {SQL_VARCHAR, 0, SQL_NO_NULLS, 0}, {SQL_SMALLINT, 0, SQL_NULLABLE, 0},
                               /*5*/     {SQL_VARCHAR, 0, SQL_NULLABLE, 0}, {SQL_VARCHAR, 0, SQL_NULLABLE, 0}, {SQL_SMALLINT, 0, SQL_NO_NULLS, 0}, {SQL_SMALLINT, 0, SQL_NULLABLE, 0},
                               /*9*/     {SQL_VARCHAR, 0, SQL_NULLABLE, 0}, {SQL_CHAR, 0, SQL_NULLABLE, 2}, {SQL_INTEGER, 0, SQL_NULLABLE, 0}, {SQL_INTEGER, 0, SQL_NULLABLE, 0},
                               /*13*/    {SQL_VARCHAR, 0, SQL_NULLABLE, 0}};

/* {{{ MADB_StmtStatistics */
SQLRETURN MADB_StmtStatistics(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                              char *SchemaName, SQLSMALLINT NameLength2,
                              char *TableName, SQLSMALLINT NameLength3,
                              SQLUSMALLINT Unique, SQLUSMALLINT Reserved)
{
  char StmtStr[MAX_CATALOG_SQLLEN] = {0};
  char *p = StmtStr;
  SQLRETURN ret;

  MADB_CLEAR_ERROR(&Stmt->Error);

  /* TableName is mandatory */
  if (!TableName || !NameLength3)
  {
    MADB_SetError(&Stmt->Error, MADB_ERR_HY009, "Tablename is required", 0);
    return Stmt->Error.ReturnValue;
  }

  if(IS_ORACLE_MODE(Stmt)) {
    p+= _snprintf(p, MAX_CATALOG_SQLLEN,"SELECT NULL AS TABLE_CAT,AIC.TABLE_OWNER AS TABLE_SCHEM,AIC.TABLE_NAME, 0 AS NON_UNIQUE,(SELECT USER FROM DUAL) AS INDEX_QUALIFIER,AIC.INDEX_NAME, %d  AS TYPE, AIC.COLUMN_POSITION AS ORDINAL_POSITION,"
      " COLUMN_NAME,DECODE(DESCEND,'ASC','A','DESC','D') AS ASC_OR_DESC,0 AS CARDINALITY, NULL AS PAGES,NULL AS FILTER_CONDITION FROM ALL_IND_COLUMNS AIC ",SQL_INDEX_OTHER);
    
    if (SchemaName && SchemaName[0])
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "WHERE AIC.TABLE_OWNER LIKE '%s' ", SchemaName);
    else
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "WHERE AIC.TABLE_OWNER IN SYS_CONTEXT('USERENV','CURRENT_SCHEMA') ");

    if (TableName && TableName[0])
      p += _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "AND AIC.TABLE_NAME = '%s' ", TableName);
  } else {
    p+= _snprintf(p, MAX_CATALOG_SQLLEN, "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, "
      "NON_UNIQUE, NULL AS INDEX_QUALIFIER, INDEX_NAME, "
      "%d AS TYPE, "
      "SEQ_IN_INDEX AS ORDINAL_POSITION, COLUMN_NAME, COLLATION AS ASC_OR_DESC, "
      "CARDINALITY, NULL AS PAGES, NULL AS FILTER_CONDITION "
      "FROM INFORMATION_SCHEMA.STATISTICS ",
      SQL_INDEX_OTHER);
    
    if (CatalogName && CatalogName[0])
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "WHERE TABLE_SCHEMA LIKE '%s' ", CatalogName);
    else
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "WHERE TABLE_SCHEMA LIKE IF(DATABASE() IS NOT NULL, DATABASE(), '%%') ");

    if (TableName)
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "AND TABLE_NAME LIKE '%s' ", TableName);

    if (Unique == SQL_INDEX_UNIQUE)
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "AND NON_UNIQUE=0 ");

    _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "ORDER BY NON_UNIQUE, INDEX_NAME, ORDINAL_POSITION");
  }
  ret= Stmt->Methods->ExecDirect(Stmt, StmtStr, SQL_NTS);

  if (SQL_SUCCEEDED(ret))
  {
    MADB_FixColumnDataTypes(Stmt, SqlStatsColType);
  }
  return ret;
}
/* }}} */


static MADB_ShortTypeInfo SqlColumnsColType[18]=
/*1*/    {{SQL_VARCHAR, 0, SQL_NO_NULLS, 0}, {SQL_VARCHAR, 0, SQL_NO_NULLS, 0}, {SQL_VARCHAR, 0, SQL_NULLABLE, 0}, {SQL_VARCHAR, 0, SQL_NULLABLE, 0},
/*5*/     {SQL_SMALLINT, 0, SQL_NO_NULLS, 0}, {SQL_VARCHAR, 0, SQL_NO_NULLS, 0}, {SQL_INTEGER, 0, SQL_NULLABLE, 0}, {SQL_INTEGER, 0, SQL_NULLABLE, 0},
/*9*/     {SQL_SMALLINT, 0, SQL_NULLABLE, 0}, {SQL_SMALLINT, 0, SQL_NULLABLE, 0}, {SQL_SMALLINT, 0, SQL_NO_NULLS, 0}, {SQL_VARCHAR, 0, SQL_NULLABLE, 0},
/*13*/    {SQL_VARCHAR, 0, SQL_NULLABLE, 0}, {SQL_SMALLINT, 0, SQL_NO_NULLS, 0}, {SQL_SMALLINT, 0, SQL_NULLABLE, 0},
/*16*/    {SQL_INTEGER, 0, SQL_NULLABLE, 0}, {SQL_INTEGER, 0, SQL_NO_NULLS, 0}, {SQL_VARCHAR, 0, SQL_NULLABLE, 0}};

/* {{{ MADB_StmtColumns */
SQLRETURN MADB_StmtColumns(MADB_Stmt *Stmt,
                           char *CatalogName, SQLSMALLINT NameLength1,
                           char *SchemaName,  SQLSMALLINT NameLength2,
                           char *TableName,   SQLSMALLINT NameLength3,
                           char *ColumnName,  SQLSMALLINT NameLength4)
{
  MADB_DynString StmtStr;
  SQLRETURN ret;
  size_t Length= strlen(MADB_CATALOG_COLUMNSp3);
  char *ColumnsPart= MADB_CALLOC(Length);
  unsigned int OctetsPerChar= Stmt->Connection->Charset.cs_info->char_maxlen > 0 && Stmt->Connection->Charset.cs_info->char_maxlen < 10 ? Stmt->Connection->Charset.cs_info->char_maxlen : 1;

  MDBUG_C_ENTER(Stmt->Connection, "StmtColumns");

  if (ColumnsPart == NULL)
  {
    return MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
  }

 if(IS_ORACLE_MODE(Stmt)) {
#if 1
    char  tmp[] = "SELECT  NULL AS table_cat,ATC.owner AS table_schem,\
		   ATC.table_name AS table_name,\
		   ATC.column_name AS column_name,\
		   DECODE (ATC.data_type, 'VARCHAR2', 12, 'NUMBER', 3,'CHAR', 1,\
		       'LONG', -1, 'RAW', -3,'DATE', 93, 'LONG RAW', -4,\
		       'BLOB', -4, 'BFILE', -13,'CLOB', -1,'FLOAT', 6, \
		       'BINARY_DOUBLE', 8,'BINARY_FLOAT', 7,  \
		       'NCHAR',-8, 'NVARCHAR2', -9, 'XMLTYPE', 2009, \
		       DECODE(substr(ATC.data_type,0,9), 'TIMESTAMP', DECODE(length(ATC.data_type), 12, 93, -4), -4)) \
           AS data_type,\
		   ATC.data_type AS type_name,\
		   DECODE (ATC.data_precision, null, \
           DECODE (ATC.data_type, 'CHAR', ATC.char_length, \
		           'VARCHAR', ATC.char_length, 'VARCHAR2', ATC.char_length, \
		           'NCHAR', ATC.char_length, 'NVARCHAR2', ATC.char_length,  \
               'NUMBER', 38,'DATE', 19,  'BINARY_FLOAT', 7, 'FLOAT', 38, 'BINARY_DOUBLE', 15, \
               DECODE (substr(ATC.data_type,14), 'WITH TIME ZONE', 1111, 'WITH LOCAL TIME ZONE', 1111, \
                   DECODE (substr(ATC.data_type,0,12),'INTERVAL YEA', 1111,'INTERVAL DAY', 1111, \
                       DECODE (substr(ATC.data_type,0,9), 'TIMESTAMP', DECODE(length(ATC.data_type), 12, 19, ATC.data_length),ATC.data_length)))), \
           DECODE (ATC.data_type, 'FLOAT', floor(ATC.data_precision*0.30103)+1, ATC.data_precision) ) \
           AS column_size,\
		   DECODE (ATC.data_type, 'FLOAT', 8,'DATE', 16, 'BINARY_FLOAT', 4, 'BINARY_DOUBLE', 8, \
           'NUMBER', DECODE (ATC.data_precision, null, 40, ATC.data_precision+2),\
           DECODE (substr(ATC.data_type,14), 'WITH TIME ZONE', 13, 'WITH LOCAL TIME ZONE', 11, \
               DECODE (substr(ATC.data_type,0,12),'INTERVAL YEA',5,'INTERVAL DAY',11, \
                   DECODE (substr(ATC.data_type,0,9), 'TIMESTAMP', DECODE(length(ATC.data_type), 12, 11, ATC.data_length),ATC.data_length)))) \
           AS buffer_length,\
		   DECODE (ATC.data_type, 'NUMBER', DECODE (ATC.data_precision, null, -127, ATC.data_scale), ATC.data_scale) AS decimal_digits, \
		   10 AS num_prec_radix,\
		   DECODE (ATC.nullable, 'N', 0, 1) AS nullable,\
		   NULL AS remarks,\
		   ATC.data_default AS column_def, \
		   DECODE (ATC.data_type, 'CHAR', 1, 'VARCHAR2', 12, 'NUMBER', 3,\
		        'DATE', 9, 'RAW', -3, 'LONG RAW', -4,'LONG', -1,\
		       'FLOAT', 6,'BLOB', -4, 'CLOB', -1, 'BFILE', -13,  \
		       'BINARY_FLOAT', 7, 'BINARY_DOUBLE', 8, \
		       'NVARCHAR2', -9,'NCHAR',-8, 'XMLTYPE', 2009, \
		       DECODE(SUBSTR(ATC.data_type,0,9), 'TIMESTAMP', DECODE(LENGTH(ATC.data_type), 12, 9, -4), -4)) \
           AS sql_data_type,\
       DECODE (ATC.data_type, 'DATE', 3, DECODE(SUBSTR(ATC.data_type,0,9), 'TIMESTAMP', DECODE(LENGTH(ATC.data_type), 12, 3, NULL), NULL))\
		       AS sql_datetime_sub, \
		   ATC.data_length AS char_octet_length,\
		   ATC.column_id AS ordinal_position,\
		   DECODE (ATC.nullable, 'N', 'NO', 'YES') AS is_nullable\
		   FROM ALL_TAB_COLUMNS ATC";
    char tmp2[] = "AND ATC.owner != '__recyclebin'\
		   ORDER BY table_schem, table_name, ordinal_position";
#endif 
    MADB_InitDynamicString(&StmtStr, "", 8192, 1024);

    MADB_CLEAR_ERROR(&Stmt->Error);
    if (MADB_DynstrAppend(&StmtStr, tmp))
      goto dynerror; 

    ADJUST_LENGTH(CatalogName, NameLength1);
    ADJUST_LENGTH(SchemaName, NameLength2);
    ADJUST_LENGTH(TableName, NameLength3);
    ADJUST_LENGTH(ColumnName, NameLength4);
    MADB_DynstrAppend(&StmtStr, " WHERE ATC.owner LIKE ") ;
    if (SchemaName)
    {
      if (MADB_DynstrAppend(&StmtStr, "'") ||
        MADB_DynstrAppendMem(&StmtStr, SchemaName, NameLength2) ||
        MADB_DynstrAppend(&StmtStr, "' "))
        goto dynerror;
    } else {
      if (MADB_DynstrAppend(&StmtStr, "'%' "))
        goto dynerror;
    }

    MADB_DynstrAppend(&StmtStr, " ESCAPE '/' AND ATC.table_name LIKE ") ;

    if (TableName && NameLength3) {
      if(MADB_DynstrAppend(&StmtStr, "'") ||
        MADB_DynstrAppendMem(&StmtStr, TableName, NameLength3) ||
        MADB_DynstrAppend(&StmtStr, "' "))
        goto dynerror;
    } else {
      if (MADB_DynstrAppend(&StmtStr, "'%' "))
        goto dynerror;
    }


    MADB_DynstrAppend(&StmtStr, " ESCAPE '/' AND ATC.column_name LIKE ") ;
    if (ColumnName && NameLength4) {
      if (MADB_DynstrAppend(&StmtStr, "'") ||
        MADB_DynstrAppendMem(&StmtStr, ColumnName, NameLength4) ||
        MADB_DynstrAppend(&StmtStr, "' "))
        goto dynerror;
    } else {
      if (MADB_DynstrAppend(&StmtStr, "'%' "))
        goto dynerror;
    }
    if (MADB_DynstrAppend(&StmtStr, tmp2))
      goto dynerror; 
  } else {
    _snprintf(ColumnsPart, Length, MADB_CATALOG_COLUMNSp3, OctetsPerChar);

    MADB_InitDynamicString(&StmtStr, "", 8192, 1024);

    MADB_CLEAR_ERROR(&Stmt->Error);
    if (MADB_DynstrAppend(&StmtStr, MADB_CATALOG_COLUMNSp1))
      goto dynerror;
    if (MADB_DynstrAppend(&StmtStr, MADB_SQL_DATATYPE(Stmt)))
      goto dynerror;
    if (MADB_DynstrAppend(&StmtStr, ColumnsPart))
      goto dynerror;
    if (MADB_DynstrAppend(&StmtStr, MADB_DEFAULT_COLUMN(Stmt->Connection)))
      goto dynerror;

    if (MADB_DynstrAppend(&StmtStr, MADB_CATALOG_COLUMNSp4))
      goto dynerror;

    ADJUST_LENGTH(CatalogName, NameLength1);
    ADJUST_LENGTH(SchemaName, NameLength2);
    ADJUST_LENGTH(TableName, NameLength3);
    ADJUST_LENGTH(ColumnName, NameLength4);

    if(MADB_DynstrAppend(&StmtStr, "TABLE_SCHEMA = "))
      goto dynerror;

    if (CatalogName)
    {
      if (MADB_DynstrAppend(&StmtStr, "'") ||
        MADB_DynstrAppendMem(&StmtStr, CatalogName, NameLength1) ||
        MADB_DynstrAppend(&StmtStr, "' "))
        goto dynerror;
    } else
      if (MADB_DynstrAppend(&StmtStr, "DATABASE()"))
        goto dynerror;

    if (TableName && NameLength3)
      if (MADB_DynstrAppend(&StmtStr, "AND TABLE_NAME LIKE '") ||
        MADB_DynstrAppendMem(&StmtStr, TableName, NameLength3) ||
        MADB_DynstrAppend(&StmtStr, "' "))
        goto dynerror;

    if (ColumnName && NameLength4)
      if (MADB_DynstrAppend(&StmtStr, "AND COLUMN_NAME LIKE '") ||
        MADB_DynstrAppendMem(&StmtStr, ColumnName, NameLength4) ||
        MADB_DynstrAppend(&StmtStr, "' "))
        goto dynerror;

    if (MADB_DynstrAppend(&StmtStr, " ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"))
      goto dynerror;
  }

  MDBUG_C_DUMP(Stmt->Connection, StmtStr.str, s);

  ret= Stmt->Methods->ExecDirect(Stmt, StmtStr.str, SQL_NTS);

  if (SQL_SUCCEEDED(ret))
  {
    MADB_FixColumnDataTypes(Stmt, SqlColumnsColType);
  }

  MADB_FREE(ColumnsPart);
  MADB_DynstrFree(&StmtStr);
  MDBUG_C_DUMP(Stmt->Connection, ret, d);

  return ret;

dynerror:
  MADB_FREE(ColumnsPart);
  MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
  return Stmt->Error.ReturnValue;
}
/* }}} */

/* {{{ MADB_StmtProcedureColumns */
SQLRETURN MADB_StmtProcedureColumns(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                                char *SchemaName, SQLSMALLINT NameLength2, char *ProcName,
                                SQLSMALLINT NameLength3, char *ColumnName, SQLSMALLINT NameLength4)
{
  char *StmtStr;
  char *p;
  SQLRETURN ret;
  
  MADB_CLEAR_ERROR(&Stmt->Error);
  if(IS_ORACLE_MODE(Stmt)) {
    size_t Length = strlen(MADB_PROCEDURE_COLUMNS_ORACLE) + MAX_CATALOG_SQLLEN;
    if (!(StmtStr= MADB_CALLOC(Length))) {
      return MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
    }
    memset(StmtStr, 0, Length);
    p = StmtStr;
    
    p+=  _snprintf(p, MAX_CATALOG_SQLLEN, MADB_PROCEDURE_COLUMNS_ORACLE);
    
    if (SchemaName)
      p+= _snprintf(p, Length - strlen(StmtStr), "WHERE  AG.OWNER  = '%s' ", SchemaName);
    else
      p+= _snprintf(p, Length - strlen(StmtStr), "WHERE  AG.OWNER = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')   ");

    if (ProcName && ProcName[0])
      p+= _snprintf(p, Length - strlen(StmtStr), "AND AG.PROCEDURE_NAME LIKE  '%s' ESCAPE '\\' ", ProcName);//MADB_PROCEDURE_COLUMNS_ORACLE
    if (ColumnName && ColumnName[0])
      p+= _snprintf(p, Length- strlen(StmtStr), "AND ARGUMENT_NAME  LIKE '%s' ", ColumnName);
  } else {
    size_t Length= strlen(MADB_PROCEDURE_COLUMNS(Stmt)) + MAX_CATALOG_SQLLEN;
    unsigned int OctetsPerChar= Stmt->Connection->Charset.cs_info->char_maxlen > 0 ? Stmt->Connection->Charset.cs_info->char_maxlen: 1;
    if (!(StmtStr= MADB_CALLOC(Length))) {
      return MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
    }
    memset(StmtStr, 0, Length);
    p= StmtStr;

    p+= _snprintf(p, Length, MADB_PROCEDURE_COLUMNS(Stmt), OctetsPerChar);

    if (CatalogName)
      p+= _snprintf(p, Length - strlen(StmtStr), "WHERE SPECIFIC_SCHEMA='%s' ", CatalogName);
    else
      p+= _snprintf(p, Length - strlen(StmtStr), "WHERE SPECIFIC_SCHEMA LIKE DATABASE() ");
    if (ProcName && ProcName[0])
      p+= _snprintf(p, Length - strlen(StmtStr), "AND SPECIFIC_NAME LIKE '%s' ", ProcName);
    if (ColumnName)
    {
      if (ColumnName[0])
      {
        p+= _snprintf(p, Length- strlen(StmtStr), "AND PARAMETER_NAME LIKE '%s' ", ColumnName);
      }
      else
      {
        p+= _snprintf(p, Length- strlen(StmtStr), "AND PARAMETER_NAME IS NULL ");
      }
    }

    p+= _snprintf(p, Length - strlen(StmtStr), " ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION");
  }
  ret= Stmt->Methods->ExecDirect(Stmt, StmtStr, SQL_NTS);

  MADB_FREE(StmtStr);

  return ret;
}
/* }}} */

/* {{{ MADB_StmtPrimaryKeys */
SQLRETURN MADB_StmtPrimaryKeys(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                                char *SchemaName, SQLSMALLINT NameLength2, char *TableName,
                                SQLSMALLINT NameLength3)
{
  char StmtStr[MAX_CATALOG_SQLLEN] = {0};
  char *p = StmtStr;

  MADB_CLEAR_ERROR(&Stmt->Error);

  /* TableName is mandatory */
  if (!TableName || !NameLength3)
  {
    MADB_SetError(&Stmt->Error, MADB_ERR_HY009, "Tablename is required", 0);
    return Stmt->Error.ReturnValue;
  }
  
  if(IS_ORACLE_MODE(Stmt)) {
    p+= _snprintf(p, MAX_CATALOG_SQLLEN,"SELECT NULL AS TABLE_CAT, ACC.OWNER AS TABLE_SCHEM, ACC.TABLE_NAME, "
                  " ACC.COLUMN_NAME , ACC.POSITION  AS KEY_SEQ, ACC.CONSTRAINT_NAME AS PK_NAME  FROM ALL_CONS_COLUMNS  ACC, ALL_CONSTRAINTS AC "
                  " WHERE AC.CONSTRAINT_TYPE  = 'P' ");
    
    if (SchemaName && SchemaName[0])
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " AND AC.OWNER LIKE '%s' ", SchemaName);
    else
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " AND AC.OWNER LIKE '%%' ");

    if (TableName)
      p += _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " AND AC.TABLE_NAME = '%s' ", TableName);

    p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " AND AC.constraint_name = ACC.constraint_name  AND AC.table_name = ACC.table_name AND AC.owner = ACC.owner ORDER BY key_seq");
  } else {
    p+= _snprintf(p, MAX_CATALOG_SQLLEN, "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, "
                  "TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION KEY_SEQ, "
                  "'PRIMARY' PK_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE "
                  "COLUMN_KEY = 'pri' AND ");
    
    if (CatalogName && CatalogName[0])
    {
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "TABLE_SCHEMA LIKE '%s' ", CatalogName);
    }
    else
    {
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "TABLE_SCHEMA LIKE IF(DATABASE() IS NOT NULL, DATABASE(), '%%') ");
    }
    if (TableName)
    {
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "AND TABLE_NAME LIKE '%s' ", TableName);
    }
    p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION");
  }
  return Stmt->Methods->ExecDirect(Stmt, StmtStr, SQL_NTS);
}
/* }}} */

/* {{{ MADB_StmtSpecialColumns */
SQLRETURN MADB_StmtSpecialColumns(MADB_Stmt *Stmt, SQLUSMALLINT IdentifierType,
                                  char *CatalogName, SQLSMALLINT NameLength1, 
                                  char *SchemaName, SQLSMALLINT NameLength2,
                                  char *TableName, SQLSMALLINT NameLength3,
                                  SQLUSMALLINT Scope, SQLUSMALLINT Nullable)
{
  char *StmtStr = NULL;
  char *p = NULL;
  SQLRETURN ret;

  MADB_CLEAR_ERROR(&Stmt->Error);

  /* TableName is mandatory */
  if (!TableName || !NameLength3) {
    MADB_SetError(&Stmt->Error, MADB_ERR_HY009, "Tablename is required", 0);
    return Stmt->Error.ReturnValue;
  }
  if (IdentifierType!= SQL_BEST_ROWID && IdentifierType != SQL_ROWVER){
    MADB_SetError(&Stmt->Error, MADB_ERR_HY097, NULL, 0);
    return Stmt->Error.ReturnValue;
  }
  if (Scope != SQL_SCOPE_CURROW && Scope != SQL_SCOPE_TRANSACTION && Scope != SQL_SCOPE_SESSION){
    MADB_SetError(&Stmt->Error, MADB_ERR_HY098, NULL, 0);
    return Stmt->Error.ReturnValue;
  }
  if (Nullable != SQL_NO_NULLS && Nullable != SQL_NULLABLE){
    MADB_SetError(&Stmt->Error, MADB_ERR_HY099, NULL, 0);
    return Stmt->Error.ReturnValue;
  }
 
  if(IS_ORACLE_MODE(Stmt)) {
    size_t Length = strlen(MADB_SPECIAL_COLUMNS_ORACLE) + MAX_CATALOG_SQLLEN;
    if (!(StmtStr = MADB_CALLOC(Length))) {
      return MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
    }
    memset(StmtStr, 0, Length);
    p = StmtStr;

    if (IdentifierType == SQL_BEST_ROWID){
      p += _snprintf(p, Length, MADB_SPECIAL_COLUMNS_ROWID_ORACLE);
    } else {
      p += _snprintf(p, Length, MADB_SPECIAL_COLUMNS_ORACLE);
    }
    
    if (SchemaName && SchemaName[0])
      p+= _snprintf(p, Length - strlen(StmtStr), " AND ATC.OWNER LIKE '%s' ", CatalogName);

    if (TableName && TableName[0])
      p+= _snprintf(p, Length - strlen(StmtStr), " AND ATC.TABLE_NAME LIKE '%s' ", TableName);

    if (IdentifierType == SQL_BEST_ROWID) {
      if (Scope == SQL_SCOPE_SESSION) {
        p += _snprintf(p, Length - strlen(StmtStr), " AND rownum=0 ");
      } else {
        p += _snprintf(p, Length - strlen(StmtStr), " AND rownum=1 ");  //one row data
      }
    } else {
      if (Nullable == SQL_NO_NULLS) {
        p += _snprintf(p, Length - strlen(StmtStr), " AND ATC.nullable='N' ");
      }
      p += _snprintf(p, Length - strlen(StmtStr), " AND rownum=0 ");  //oracle no data
    }
    p+= _snprintf(p, Length - strlen(StmtStr), "ORDER BY owner, TABLE_NAME");
  } else {
    size_t Length = strlen(MADB_SPECIAL_COLUMNS_ORACLE) + MAX_CATALOG_SQLLEN;
    if (!(StmtStr = MADB_CALLOC(Length))) {
      return MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
    }
    memset(StmtStr, 0, Length);
    p = StmtStr;

    p+= _snprintf(p, Length, "SELECT NULL AS SCOPE, COLUMN_NAME, %s,"
                  "DATA_TYPE TYPE_NAME,"
                  "CASE" 
                  "  WHEN DATA_TYPE in ('bit', 'tinyint', 'smallint', 'year', 'mediumint', 'int',"
                  "'bigint', 'decimal', 'float', 'double') THEN NUMERIC_PRECISION "
                  "  WHEN DATA_TYPE='date' THEN 10"
                  "  WHEN DATA_TYPE='time' THEN 8"
                  "  WHEN DATA_TYPE in ('timestamp', 'datetime') THEN 19 "
                  "END AS COLUMN_SIZE,"
                  "CHARACTER_OCTET_LENGTH AS BUFFER_LENGTH,"
                  "NUMERIC_SCALE DECIMAL_DIGITS, " 
                  XSTR(SQL_PC_UNKNOWN) " PSEUDO_COLUMN "
                  "FROM INFORMATION_SCHEMA.COLUMNS WHERE 1 ", MADB_SQL_DATATYPE(Stmt));

    if (CatalogName && CatalogName[0])
      p+= _snprintf(p, Length - strlen(StmtStr), "AND TABLE_SCHEMA LIKE '%s' ", CatalogName);
    else
      p+= _snprintf(p, Length - strlen(StmtStr), "AND TABLE_SCHEMA LIKE IF(DATABASE() IS NOT NULL, DATABASE(), '%%') ");
    if (TableName && TableName[0])
      p+= _snprintf(p, Length - strlen(StmtStr), "AND TABLE_NAME LIKE '%s' ", TableName);

    if (Nullable == SQL_NO_NULLS)
      p+= _snprintf(p, Length - strlen(StmtStr), "AND IS_NULLABLE <> 'YES' ");

    if (IdentifierType == SQL_BEST_ROWID)
      p+= _snprintf(p, Length - strlen(StmtStr), "AND COLUMN_KEY IN ('PRI', 'UNI') ");
    else if (IdentifierType == SQL_ROWVER)
      p+= _snprintf(p, Length - strlen(StmtStr), "AND DATA_TYPE='timestamp' AND EXTRA LIKE '%%CURRENT_TIMESTAMP%%' ");
    p+= _snprintf(p, Length - strlen(StmtStr), "ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_KEY");
  }

  ret = Stmt->Methods->ExecDirect(Stmt, StmtStr, SQL_NTS);

  MADB_FREE(StmtStr);

  return ret;
}
/* }}} */

/* {{{ MADB_StmtProcedures */
SQLRETURN MADB_StmtProcedures(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                              char *SchemaName, SQLSMALLINT NameLength2, char *ProcName,
                              SQLSMALLINT NameLength3)
{
  char StmtStr[MAX_CATALOG_SQLLEN] = {0};
  char *p = StmtStr;

  MADB_CLEAR_ERROR(&Stmt->Error);

  if(IS_ORACLE_MODE(Stmt)) { 
    p+= _snprintf(p, MAX_CATALOG_SQLLEN, MADB_PROCEDURE_ORACLE);
    
    if (SchemaName && SchemaName[0])
      p += _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "WHERE OWNER LIKE '%s' ", SchemaName);
    else
      p += _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "WHERE OWNER IN SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') ");

    if (ProcName && ProcName[0])
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "AND PROCEDURE_NAME2 LIKE '%s' ", ProcName);

    p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr),"AND (OBJECT_TYPE = 'PROCEDURE' OR OBJECT_TYPE = 'FUNCTION' OR OBJECT_TYPE= 'PACKAGE')") ;
  } else {
    p+= _snprintf(p, MAX_CATALOG_SQLLEN, "SELECT ROUTINE_SCHEMA AS PROCEDURE_CAT, NULL AS PROCEDURE_SCHEM, "
                  "SPECIFIC_NAME PROCEDURE_NAME, NULL NUM_INPUT_PARAMS, "
                  "NULL NUM_OUTPUT_PARAMS, NULL NUM_RESULT_SETS, "
                  "ROUTINE_COMMENT REMARKS, "
                  "CASE ROUTINE_TYPE "
                  "  WHEN 'FUNCTION' THEN " XSTR(SQL_PT_FUNCTION)
                  "  WHEN 'PROCEDURE' THEN " XSTR(SQL_PT_PROCEDURE)
                  "  ELSE " XSTR(SQL_PT_UNKNOWN) " "
                  "END PROCEDURE_TYPE "
                  "FROM INFORMATION_SCHEMA.ROUTINES ");
    
    if (CatalogName && CatalogName[0])
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "WHERE ROUTINE_SCHEMA LIKE '%s' ", CatalogName);
    else
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "WHERE ROUTINE_SCHEMA LIKE DATABASE() ");
    if (ProcName && ProcName[0])
      p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), "AND SPECIFIC_NAME LIKE '%s' ", ProcName);

    p+= _snprintf(p, MAX_CATALOG_SQLLEN - strlen(StmtStr), " ORDER BY ROUTINE_SCHEMA, SPECIFIC_NAME");
  }
  return Stmt->Methods->ExecDirect(Stmt, StmtStr, SQL_NTS);
}
/* }}} */

/* {{{ SQLForeignKeys */
SQLRETURN MADB_StmtForeignKeys(MADB_Stmt *Stmt, char *PKCatalogName, SQLSMALLINT NameLength1,
                               char *PKSchemaName, SQLSMALLINT NameLength2, char *PKTableName,
                               SQLSMALLINT NameLength3, char *FKCatalogName, SQLSMALLINT NameLength4,
                               char *FKSchemaName, SQLSMALLINT NameLength5,  char *FKTableName,
                               SQLSMALLINT NameLength6)
{
  SQLRETURN ret= SQL_ERROR;
  MADB_DynString StmtStr;
  char EscapeBuf[256] = {0};

  MADB_CLEAR_ERROR(&Stmt->Error);

  ADJUST_LENGTH(PKCatalogName, NameLength1);
  ADJUST_LENGTH(PKSchemaName, NameLength2);
  ADJUST_LENGTH(PKTableName, NameLength3);
  ADJUST_LENGTH(FKCatalogName, NameLength4);
  ADJUST_LENGTH(FKSchemaName, NameLength5);
  ADJUST_LENGTH(FKTableName, NameLength6);

  /* PKTableName and FKTableName are mandatory */
  if ((!PKTableName || !NameLength3) && (!FKTableName || !NameLength6))
  {
    MADB_SetError(&Stmt->Error, MADB_ERR_HY009, "PKTableName or FKTableName are required", 0);
    return Stmt->Error.ReturnValue;
  }
  
  if(IS_ORACLE_MODE(Stmt)) {
    MADB_InitDynamicString(&StmtStr, "SELECT NULL AS PKTABLE_CAT, PRIMARY_KEY.OWNER AS PKTABLE_SCHEM, "
      " PRIMARY_KEY.TABLE_NAME AS PKTABLE_NAME, PRIMARY_CONS.COLUMN_NAME AS PKCOLUMN_NAME, NULL AS FKTABLE_CAT, "
      " FOREIGN_KEY.R_OWNER AS FKTABLE_SCHEM, FOREIGN_KEY.TABLE_NAME AS FKTABLE_NAME, FOREIGN_CONS.COLUMN_NAME AS FKCOLUMN_NAME, "
      " 1 AS KEY_SEQ, NULL AS UPDATE_RULE, 3 AS DELETE_RULE, "
      " FOREIGN_KEY.CONSTRAINT_NAME AS FK_NAME, PRIMARY_KEY.CONSTRAINT_NAME AS PK_NAME, NULL as DEFERRABILITY "
      " FROM DBA_CONSTRAINTS PRIMARY_KEY , DBA_CONSTRAINTS FOREIGN_KEY, DBA_CONS_COLUMNS PRIMARY_CONS, DBA_CONS_COLUMNS FOREIGN_CONS "
      " WHERE PRIMARY_KEY.OWNER = FOREIGN_KEY.R_OWNER AND PRIMARY_KEY.CONSTRAINT_NAME = FOREIGN_KEY.R_CONSTRAINT_NAME "
      " AND FOREIGN_KEY.R_OWNER = FOREIGN_CONS.OWNER AND FOREIGN_KEY.CONSTRAINT_NAME = FOREIGN_CONS.CONSTRAINT_NAME "
      " AND PRIMARY_KEY.OWNER = PRIMARY_CONS.OWNER AND PRIMARY_KEY.CONSTRAINT_NAME = PRIMARY_CONS.CONSTRAINT_NAME "
      " AND FOREIGN_KEY.CONSTRAINT_TYPE = 'R' ", 8192, 512);
    if (PKTableName && PKTableName[0])
    {
      MADB_DynstrAppend(&StmtStr, " AND PRIMARY_KEY.owner = ");
      MADB_DynstrAppend(&StmtStr, " SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')");
      MADB_DynstrAppend(&StmtStr, " AND PRIMARY_KEY.table_name = '");

      mysql_real_escape_string(Stmt->Connection->mariadb, EscapeBuf, PKTableName, MIN(255, NameLength3));
      MADB_DynstrAppend(&StmtStr, EscapeBuf);
      MADB_DynstrAppend(&StmtStr, "' ");
    }

    if (FKTableName && FKTableName[0])
    {
      MADB_DynstrAppend(&StmtStr, " AND FOREIGN_KEY.R_OWNER = ");
      MADB_DynstrAppend(&StmtStr, " SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')");
      MADB_DynstrAppend(&StmtStr, " AND FOREIGN_KEY.table_name = '");

      mysql_real_escape_string(Stmt->Connection->mariadb, EscapeBuf, FKTableName, MIN(255, NameLength6));
      MADB_DynstrAppend(&StmtStr, EscapeBuf);
      MADB_DynstrAppend(&StmtStr, "' ");
    }
    MADB_DynstrAppend(&StmtStr, " ORDER BY FKTABLE_NAME, KEY_SEQ, PKTABLE_NAME");
  } else {
    MADB_InitDynamicString(&StmtStr,
      "SELECT A.REFERENCED_TABLE_SCHEMA PKTABLE_CAT, NULL PKTABLE_SCHEM, "
      "A.REFERENCED_TABLE_NAME PKTABLE_NAME, " 
      "A.REFERENCED_COLUMN_NAME PKCOLUMN_NAME, "
      "A.TABLE_SCHEMA FKTABLE_CAT, NULL FKTABLE_SCHEM, "
      "A.TABLE_NAME FKTABLE_NAME, A.COLUMN_NAME FKCOLUMN_NAME, "
      "A.POSITION_IN_UNIQUE_CONSTRAINT KEY_SEQ, "
      "CASE update_rule "
      "  WHEN 'RESTRICT' THEN " XSTR(SQL_RESTRICT)
      "  WHEN 'NO ACTION' THEN " XSTR(SQL_NO_ACTION)
      "  WHEN 'CASCADE' THEN " XSTR(SQL_CASCADE)
      "  WHEN 'SET NULL' THEN " XSTR(SQL_SET_NULL)
      "  WHEN 'SET DEFAULT' THEN " XSTR(SQL_SET_DEFAULT) " "
      "END UPDATE_RULE, "
      "CASE DELETE_RULE" 
      "  WHEN 'RESTRICT' THEN " XSTR(SQL_RESTRICT)
      "  WHEN 'NO ACTION' THEN " XSTR(SQL_NO_ACTION)
      "  WHEN 'CASCADE' THEN " XSTR(SQL_CASCADE)
      "  WHEN 'SET NULL' THEN " XSTR(SQL_SET_NULL)
      "  WHEN 'SET DEFAULT' THEN " XSTR(SQL_SET_DEFAULT) " "
      " END DELETE_RULE,"
      "A.CONSTRAINT_NAME FK_NAME, "
      "'PRIMARY' PK_NAME,"
      XSTR(SQL_NOT_DEFERRABLE) " AS DEFERRABILITY "
      " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE A"
      " JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE B"
      " ON (B.TABLE_SCHEMA = A.REFERENCED_TABLE_SCHEMA"
      " AND B.TABLE_NAME = A.REFERENCED_TABLE_NAME"
      " AND B.COLUMN_NAME = A.REFERENCED_COLUMN_NAME)"
      " JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC"
      " ON (RC.CONSTRAINT_NAME = A.CONSTRAINT_NAME"
      " AND RC.TABLE_NAME = A.TABLE_NAME"
      " AND RC.CONSTRAINT_SCHEMA = A.TABLE_SCHEMA)"
      " WHERE B.CONSTRAINT_NAME= 'PRIMARY'", 8192, 512);

    if (PKTableName && PKTableName[0])
    {
      MADB_DynstrAppend(&StmtStr, " AND A.REFERENCED_TABLE_SCHEMA "); 

      if (PKCatalogName && PKCatalogName[0])
      {
        MADB_DynstrAppend(&StmtStr, "LIKE '");
        mysql_real_escape_string(Stmt->Connection->mariadb, EscapeBuf, PKCatalogName, MIN(NameLength1, 255));
        MADB_DynstrAppend(&StmtStr, EscapeBuf);
        MADB_DynstrAppend(&StmtStr, "' ");
      }
      else 
        MADB_DynstrAppend(&StmtStr, "= DATABASE()");
    
      MADB_DynstrAppend(&StmtStr, " AND A.REFERENCED_TABLE_NAME = '");

      mysql_real_escape_string(Stmt->Connection->mariadb, EscapeBuf, PKTableName, MIN(255, NameLength3));
      MADB_DynstrAppend(&StmtStr, EscapeBuf);
      MADB_DynstrAppend(&StmtStr, "' ");
    }

    if (FKTableName && FKTableName[0])
    {
      MADB_DynstrAppend(&StmtStr, " AND A.TABLE_SCHEMA = "); 

      if (FKCatalogName && FKCatalogName[0])
      {
        MADB_DynstrAppend(&StmtStr, "'");
        mysql_real_escape_string(Stmt->Connection->mariadb, EscapeBuf, FKCatalogName, MIN(NameLength4, 255));
        MADB_DynstrAppend(&StmtStr, EscapeBuf);
        MADB_DynstrAppend(&StmtStr, "' ");
      }
      else
        MADB_DynstrAppend(&StmtStr, "DATABASE() ");
      
      MADB_DynstrAppend(&StmtStr, " AND A.TABLE_NAME = '");

      mysql_real_escape_string(Stmt->Connection->mariadb, EscapeBuf, FKTableName, MIN(255, NameLength6));
      MADB_DynstrAppend(&StmtStr, EscapeBuf);
      MADB_DynstrAppend(&StmtStr, "' ");
    }
    MADB_DynstrAppend(&StmtStr, "ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ, PKTABLE_NAME");

  }
 
  ret= Stmt->Methods->ExecDirect(Stmt, StmtStr.str, SQL_NTS);

  MADB_DynstrFree(&StmtStr);

  return ret;
}
/* }}} */