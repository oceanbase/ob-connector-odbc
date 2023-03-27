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


#ifndef _ma_catalog_h_
#define _ma_catalog_h_

#include <ma_odbc.h>


SQLRETURN MADB_StmtColumnPrivileges(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                                    char *SchemaName, SQLSMALLINT NameLength2, char *TableName,
                                    SQLSMALLINT NameLength3, char *ColumnName, SQLSMALLINT NameLength4);
SQLRETURN MADB_StmtTablePrivileges(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                                    char *SchemaName, SQLSMALLINT NameLength2,
                                    char *TableName, SQLSMALLINT NameLength3);
SQLRETURN MADB_StmtTables(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT CatalogNameLength,
                          char *SchemaName, SQLSMALLINT SchemaNameLength, char *TableName,
                          SQLSMALLINT TableNameLength, char *TableType, SQLSMALLINT TableTypeLength);
SQLRETURN MADB_StmtStatistics(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                              char *SchemaName, SQLSMALLINT NameLength2,
                              char *TableName, SQLSMALLINT NameLength3,
                              SQLUSMALLINT Unique, SQLUSMALLINT Reserved);
SQLRETURN MADB_StmtColumns(MADB_Stmt *Stmt,
                           char *CatalogName, SQLSMALLINT NameLength1,
                           char *SchemaName,  SQLSMALLINT NameLength2,
                           char *TableName,   SQLSMALLINT NameLength3,
                           char *ColumnName,  SQLSMALLINT NameLength4);
SQLRETURN MADB_StmtProcedureColumns(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                                char *SchemaName, SQLSMALLINT NameLength2, char *ProcName,
                                SQLSMALLINT NameLength3, char *ColumnName, SQLSMALLINT NameLength4);
SQLRETURN MADB_StmtPrimaryKeys(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                                char *SchemaName, SQLSMALLINT NameLength2, char *TableName,
                                SQLSMALLINT NameLength3);
SQLRETURN MADB_StmtSpecialColumns(MADB_Stmt *Stmt, SQLUSMALLINT IdentifierType,
                                  char *CatalogName, SQLSMALLINT NameLength1, 
                                  char *SchemaName, SQLSMALLINT NameLength2,
                                  char *TableName, SQLSMALLINT NameLength3,
                                  SQLUSMALLINT Scope, SQLUSMALLINT Nullable);
SQLRETURN MADB_StmtProcedures(MADB_Stmt *Stmt, char *CatalogName, SQLSMALLINT NameLength1,
                              char *SchemaName, SQLSMALLINT NameLength2, char *ProcName,
                              SQLSMALLINT NameLength3);
SQLRETURN MADB_StmtForeignKeys(MADB_Stmt *Stmt, char *PKCatalogName, SQLSMALLINT NameLength1,
                               char *PKSchemaName, SQLSMALLINT NameLength2, char *PKTableName,
                               SQLSMALLINT NameLength3, char *FKCatalogName, SQLSMALLINT NameLength4,
                               char *FKSchemaName, SQLSMALLINT NameLength5,  char *FKTableName,
                               SQLSMALLINT NameLength6);
                               


#define MADB_TRANSFER_OCTET_LENGTH(TYPE_DEF_COL_NAME)\
  "@tol:=CAST(CASE @dt"\
  "  WHEN " XSTR(SQL_BIT) " THEN 1 "\
  "  WHEN " XSTR(SQL_TINYINT) " THEN 1 "\
  "  WHEN " XSTR(SQL_SMALLINT) " THEN 2 "\
  "  WHEN " XSTR(SQL_INTEGER) " THEN IF(DATA_TYPE='mediumint',3,4) "\
  "  WHEN " XSTR(SQL_BIGINT) " THEN 20 "\
  "  WHEN " XSTR(SQL_REAL) " THEN 4 "\
  "  WHEN " XSTR(SQL_FLOAT) " THEN 8 "\
  "  WHEN " XSTR(SQL_DOUBLE) " THEN 8 "\
  "  WHEN " XSTR(SQL_DATE) " THEN 6 "\
  "  WHEN " XSTR(SQL_TYPE_DATE) " THEN 6 "\
  "  WHEN " XSTR(SQL_TIME) " THEN 6 "\
  "  WHEN " XSTR(SQL_TYPE_TIME) " THEN 6 "\
  "  WHEN " XSTR(SQL_TIMESTAMP) " THEN 16 "\
  "  WHEN " XSTR(SQL_TYPE_TIMESTAMP) " THEN 16 "\
  "  WHEN " XSTR(SQL_GUID) " THEN 16 "\
  "  WHEN " XSTR(SQL_DECIMAL) " THEN @ColSize + IF(" #TYPE_DEF_COL_NAME " LIKE '%%unsigned',1,2) "\
  "  WHEN " XSTR(SQL_BINARY) " THEN IF(DATA_TYPE='bit', CAST(((NUMERIC_PRECISION + 7) / 8) AS SIGNED), CHARACTER_OCTET_LENGTH) "\
  "  WHEN " XSTR(SQL_VARBINARY) " THEN CHARACTER_OCTET_LENGTH "\
  "  WHEN " XSTR(SQL_LONGVARBINARY) " THEN CHARACTER_OCTET_LENGTH "\
  "  ELSE CHARACTER_MAXIMUM_LENGTH*%u "\
  "END AS SIGNED)"

/* CASE for DATA_TYPE glued in 2 parts for ODBC v2 or v3 */
#define MADB_SQL_DATATYPEp1\
  "CASE DATA_TYPE"\
  "  WHEN 'bit' THEN @dt:= IF(NUMERIC_PRECISION=1," XSTR(SQL_BIT) ", " XSTR(SQL_BINARY) ")"\
  "  WHEN 'tinyint' THEN @dt:=" XSTR(SQL_TINYINT)\
  "  WHEN 'smallint' THEN @dt:=" XSTR(SQL_SMALLINT)\
  "  WHEN 'year' THEN @dt:= " XSTR(SQL_SMALLINT)\
  "  WHEN 'mediumint' THEN @dt:=" XSTR(SQL_INTEGER)\
  "  WHEN 'int' THEN @dt:=" XSTR(SQL_INTEGER)\
  "  WHEN 'bigint' THEN @dt:=" XSTR(SQL_BIGINT)\
  "  WHEN 'blob' THEN @dt:=" XSTR(SQL_LONGVARBINARY)\
  "  WHEN 'tinyblob' THEN @dt:=" XSTR(SQL_LONGVARBINARY)\
  "  WHEN 'mediumblob' THEN @dt:=" XSTR(SQL_LONGVARBINARY)\
  "  WHEN 'longblob' THEN @dt:=" XSTR(SQL_LONGVARBINARY)\
  "  WHEN 'decimal' THEN @dt:=" XSTR(SQL_DECIMAL)\
  "  WHEN 'float' THEN @dt:=IF(NUMERIC_SCALE IS NULL," XSTR(SQL_REAL) ", "  XSTR(SQL_DECIMAL) ")"\
  "  WHEN 'double' THEN @dt:=IF(NUMERIC_SCALE IS NULL," XSTR(SQL_DOUBLE) ", "  XSTR(SQL_DECIMAL) ")"\
  "  WHEN 'binary' THEN @dt:=" XSTR(SQL_BINARY)\
  "  WHEN 'varbinary' THEN @dt:=" XSTR(SQL_VARBINARY)

#define MADB_SQL_DATATYPEp1U\
  "  WHEN 'text' THEN @dt:=" XSTR(SQL_WLONGVARCHAR)\
  "  WHEN 'tinytext' THEN @dt:=" XSTR(SQL_WLONGVARCHAR)\
  "  WHEN 'mediumtext' THEN @dt:=" XSTR(SQL_WLONGVARCHAR)\
  "  WHEN 'longtext' THEN @dt:=" XSTR(SQL_WLONGVARCHAR)\
  "  WHEN 'char' THEN @dt:=" XSTR(SQL_WCHAR)\
  "  WHEN 'enum' THEN @dt:=" XSTR(SQL_WCHAR)\
  "  WHEN 'set' THEN @dt:=" XSTR(SQL_WCHAR)\
  "  WHEN 'varchar' THEN @dt:=" XSTR(SQL_WVARCHAR)

#define MADB_SQL_DATATYPEp1A\
  "  WHEN 'text' THEN @dt:=" XSTR(SQL_LONGVARCHAR)\
  "  WHEN 'tinytext' THEN @dt:=" XSTR(SQL_LONGVARCHAR)\
  "  WHEN 'mediumtext' THEN @dt:=" XSTR(SQL_LONGVARCHAR)\
  "  WHEN 'longtext' THEN @dt:=" XSTR(SQL_LONGVARCHAR)\
  "  WHEN 'char' THEN @dt:=" XSTR(SQL_CHAR)\
  "  WHEN 'enum' THEN @dt:=" XSTR(SQL_CHAR)\
  "  WHEN 'set' THEN @dt:=" XSTR(SQL_CHAR)\
  "  WHEN 'varchar' THEN @dt:=" XSTR(SQL_VARCHAR)

#define MADB_SQL_DATATYPEp2_ODBC3\
  "  WHEN 'date' THEN @dt:=" XSTR(SQL_TYPE_DATE)\
  "  WHEN 'time' THEN @dt:=" XSTR(SQL_TYPE_TIME)\
  "  WHEN 'datetime' THEN @dt:=" XSTR(SQL_TYPE_TIMESTAMP)\
  "  WHEN 'timestamp' THEN @dt:=" XSTR(SQL_TYPE_TIMESTAMP)\
  "  ELSE @dt:=" XSTR(SQL_LONGVARBINARY)\
  "END AS DATA_TYPE"

#define MADB_SQL_DATATYPEp2_ODBC2\
  "  WHEN 'date' THEN @dt:=" XSTR(SQL_DATE)\
  "  WHEN 'time' THEN @dt:=" XSTR(SQL_TIME)\
  "  WHEN 'datetime' THEN @dt:=" XSTR(SQL_TIMESTAMP)\
  "  WHEN 'timestamp' THEN @dt:=" XSTR(SQL_TIMESTAMP)\
  "  ELSE @dt:=" XSTR(SQL_LONGVARBINARY)\
  "END AS DATA_TYPE"

#define MADB_SQL_DATATYPE_ODBC3U MADB_SQL_DATATYPEp1 MADB_SQL_DATATYPEp1U MADB_SQL_DATATYPEp2_ODBC3
#define MADB_SQL_DATATYPE_ODBC3A MADB_SQL_DATATYPEp1 MADB_SQL_DATATYPEp1A MADB_SQL_DATATYPEp2_ODBC3
#define MADB_SQL_DATATYPE_ODBC2U MADB_SQL_DATATYPEp1 MADB_SQL_DATATYPEp1U MADB_SQL_DATATYPEp2_ODBC2
#define MADB_SQL_DATATYPE_ODBC2A MADB_SQL_DATATYPEp1 MADB_SQL_DATATYPEp1A MADB_SQL_DATATYPEp2_ODBC2

#define MADB_SQL_DATATYPE(StmtHndl) (StmtHndl->Connection->Environment->OdbcVersion >= SQL_OV_ODBC3 ?\
 (StmtHndl->Connection->IsAnsi ? MADB_SQL_DATATYPE_ODBC3A : MADB_SQL_DATATYPE_ODBC3U) :\
 (StmtHndl->Connection->IsAnsi ? MADB_SQL_DATATYPE_ODBC2A : MADB_SQL_DATATYPE_ODBC2U))

/************** End of DATA_TYPE *************/

/************** SQLColumns       *************/
#define MADB_COLUMN_SIZE\
    "CAST(CASE" \
    "  WHEN DATA_TYPE = 'bit' THEN @ColSize:=((NUMERIC_PRECISION + 7) / 8) "\
    "  WHEN DATA_TYPE in ('tinyint', 'smallint', 'mediumint', 'int',"\
                         "'bigint', 'decimal') THEN @ColSize:=NUMERIC_PRECISION "\
    "  WHEN DATA_TYPE = 'float' THEN if(NUMERIC_SCALE IS NULL, @ColSize:=7, @ColSize:=NUMERIC_PRECISION)"\
    "  WHEN DATA_TYPE = 'double' THEN if(NUMERIC_SCALE IS NULL, @ColSize:=15, @ColSize:=NUMERIC_PRECISION)"\
    "  WHEN DATA_TYPE = 'date' THEN @ColSize:=10"\
    "  WHEN DATA_TYPE = 'time' THEN @ColSize:=8"\
    "  WHEN DATA_TYPE = 'year' THEN @ColSize:=4"\
    "  WHEN DATA_TYPE in ('timestamp', 'datetime') THEN @ColSize:=19 "\
    "  ELSE @ColSize:=CHARACTER_MAXIMUM_LENGTH "\
  "END AS UNSIGNED)"

#define MADB_CATALOG_COLUMNSp1 "SELECT TABLE_SCHEMA AS TABLE_CAT, NULL AS TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, "
#define MADB_CATALOG_COLUMNSp3 ", UCASE(IF(COLUMN_TYPE LIKE '%%(%%)%%',  CONCAT(SUBSTRING(COLUMN_TYPE,1, LOCATE('(',COLUMN_TYPE) - 1 ), SUBSTRING(COLUMN_TYPE,1+locate(')',COLUMN_TYPE))), COLUMN_TYPE )) AS TYPE_NAME, "\
  MADB_COLUMN_SIZE " AS COLUMN_SIZE,"\
  MADB_TRANSFER_OCTET_LENGTH(COLUMN_TYPE) " AS BUFFER_LENGTH, "\
  "NUMERIC_SCALE DECIMAL_DIGITS, IF(CHARACTER_OCTET_LENGTH IS NOT NULL, NULL, 10) AS NUM_PREC_RADIX,"\
  "IF(DATA_TYPE='timestamp', 1, IF(IS_NULLABLE='YES',1,IF(EXTRA='auto_increment', 1, 0))) AS NULLABLE, "\
  "COLUMN_COMMENT AS REMARKS,"
#define MADB_DEFAULT_COLUMN_OLD  "IF(COLLATION_NAME IS NOT NULL AND COLUMN_DEFAULT IS NOT NULL, CONCAT(CHAR(39), COLUMN_DEFAULT, CHAR(39)), COLUMN_DEFAULT)"
#define MADB_DEFAULT_COLUMN_NEW  "COLUMN_DEFAULT"
#define MADB_DEFAULT_COLUMN(DbcHndl) (MADB_ServerSupports(DbcHndl,MADB_ENCLOSES_COLUMN_DEF_WITH_QUOTES) ? MADB_DEFAULT_COLUMN_NEW : MADB_DEFAULT_COLUMN_OLD)
#define MADB_CATALOG_TYPE_SUB "CAST(CASE"\
  "  WHEN DATA_TYPE = 'date' THEN " XSTR(SQL_DATETIME)\
  "  WHEN DATA_TYPE = 'time' THEN " XSTR(SQL_DATETIME)\
  "  WHEN DATA_TYPE = 'datetime' THEN " XSTR(SQL_DATETIME)\
  "  WHEN DATA_TYPE = 'timestamp' THEN " XSTR(SQL_DATETIME)\
  " ELSE @dt "\
  "END AS SIGNED) SQL_DATA_TYPE,"\
  "CAST(CASE"\
  "  WHEN DATA_TYPE = 'date' THEN " XSTR(SQL_CODE_DATE)\
  "  WHEN DATA_TYPE = 'time' THEN " XSTR(SQL_CODE_TIME)\
  "  WHEN DATA_TYPE = 'datetime' THEN " XSTR(SQL_CODE_TIMESTAMP)\
  "  WHEN DATA_TYPE = 'timestamp' THEN " XSTR(SQL_CODE_TIMESTAMP)\
  " ELSE NULL "\
  "END AS SIGNED) SQL_DATETIME_SUB,"
#define MADB_CATALOG_COLUMNSp4 " AS COLUMN_DEF," MADB_CATALOG_TYPE_SUB\
  "IF(CHARACTER_OCTET_LENGTH IS NOT NULL, @tol, IF(DATA_TYPE='bit' AND NUMERIC_PRECISION =1, NULL, CAST((NUMERIC_PRECISION + 7)/8 AS SIGNED))) AS CHAR_OCTET_LENGTH, "\
  "ORDINAL_POSITION,"\
  "IF(DATA_TYPE='timestamp', 'YES', IF(IS_NULLABLE='YES','YES',IF(EXTRA='auto_increment', 'YES', 'NO'))) AS IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE "

/************** End of SQLColumns ************/

/************** SQLProcedureColumns **********/
#define MADB_PROCEDURE_COLUMNSp1 \
  "SELECT SPECIFIC_SCHEMA AS PROCEDURE_CAT, NULL AS PROCEDURE_SCHEM, "\
  "SPECIFIC_NAME PROCEDURE_NAME, IF(PARAMETER_NAME IS NULL, '', PARAMETER_NAME) COLUMN_NAME, "\
  "CASE PARAMETER_MODE "\
  "  WHEN 'IN' THEN " XSTR(SQL_PARAM_INPUT)\
  "  WHEN 'OUT' THEN " XSTR(SQL_PARAM_OUTPUT)\
  "  WHEN 'INOUT' THEN " XSTR(SQL_PARAM_INPUT_OUTPUT)\
  "  ELSE IF(PARAMETER_MODE IS NULL, " XSTR(SQL_RETURN_VALUE) ", " XSTR(SQL_PARAM_TYPE_UNKNOWN) ")"\
  "END COLUMN_TYPE, "
#define MADB_PROCEDURE_COLUMNSp3\
  ", DATA_TYPE TYPE_NAME, "\
  MADB_COLUMN_SIZE " AS COLUMN_SIZE, "\
  MADB_TRANSFER_OCTET_LENGTH(DTD_IDENTIFIER) " AS BUFFER_LENGTH, "\
  "NUMERIC_SCALE DECIMAL_DIGITS, IF(NUMERIC_PRECISION IS NULL, NULL, 10) AS NUM_PREC_RADIX,"\
  XSTR(SQL_NULLABLE_UNKNOWN) " NULLABLE,"\
  "NULL REMARKS, NULL COLUMN_DEF," MADB_CATALOG_TYPE_SUB \
  "IF(CHARACTER_MAXIMUM_LENGTH IS NULL, NULL, @tol) CHAR_OCTET_LENGTH, "\
  "ORDINAL_POSITION, 'YES' IS_NULLABLE FROM INFORMATION_SCHEMA.PARAMETERS "

#define MADB_PROCEDURE_COLUMNS_ODBC3U MADB_PROCEDURE_COLUMNSp1 MADB_SQL_DATATYPE_ODBC3U MADB_PROCEDURE_COLUMNSp3
#define MADB_PROCEDURE_COLUMNS_ODBC2U MADB_PROCEDURE_COLUMNSp1 MADB_SQL_DATATYPE_ODBC2U MADB_PROCEDURE_COLUMNSp3
#define MADB_PROCEDURE_COLUMNS_ODBC3A MADB_PROCEDURE_COLUMNSp1 MADB_SQL_DATATYPE_ODBC3A MADB_PROCEDURE_COLUMNSp3
#define MADB_PROCEDURE_COLUMNS_ODBC2A MADB_PROCEDURE_COLUMNSp1 MADB_SQL_DATATYPE_ODBC2A MADB_PROCEDURE_COLUMNSp3

#define MADB_PROCEDURE_COLUMNS(StmtHndl) (StmtHndl->Connection->Environment->OdbcVersion >= SQL_OV_ODBC3 ?\
                                        (StmtHndl->Connection->IsAnsi ? MADB_PROCEDURE_COLUMNS_ODBC3A : MADB_PROCEDURE_COLUMNS_ODBC3U) : \
                                        (StmtHndl->Connection->IsAnsi ? MADB_PROCEDURE_COLUMNS_ODBC2A : MADB_PROCEDURE_COLUMNS_ODBC2U))
/************** SQLProcedureColumns **********/


/************** SQLProcedureColumnsOracle **********/
#define MADB_PROCEDURE_COLUMNS_ORACLE \
  "SELECT NULl as PROCEDURE_CAT, AG.OWNER as PROCEDURE_SCHEM, " \
      " AG.PROCEDURE_NAME AS PROCEDURE_NAME, " \
      " AG.ARGUMENT_NAME as COLUMN_NAME, " \
      " DECODE(AG.IN_OUT,'IN',1,'OUT',4,'INOUT',2) as column_type," \
      " DECODE (AG.data_type, "\
      "   'CHAR', 1, 'VARCHAR2', 12, 'NUMBER', 3, 'LONG', -1, 'DATE', 93,"\
      "   'RAW', -3, 'LONG RAW', -4, 'BLOB', 2004, 'CLOB', 2005, 'BFILE', -13,"\
      "   'FLOAT', 6, 'TIMESTAMP(6)', 93, 'TIMESTAMP(6) WITH TIME ZONE', -101,"\
      "   'TIMESTAMP(6) WITH LOCAL TIME ZONE', -102, 'INTERVAL YEAR(2) TO MONTH', -103,"\
      "   'INTERVAL DAY(2) TO SECOND(6)', -104, 'BINARY_FLOAT', 100, 'BINARY_DOUBLE', 101,"\
      "   'XMLTYPE', 2009, 1111) as data_type ,AG.data_type as type_name , "\
      " DECODE (AG.data_precision, null, DECODE (AG.data_type, 'CHAR', AG.data_length, 'VARCHAR', AG.data_length,'VARCHAR2', AG.data_length, 'NVARCHAR2', AG.data_length, 'NCHAR', AG.data_length, 'NUMBER', 0, AG.data_length), AG.data_precision) as  COLUMN_SIZE, "\
      " 0 AS buffer_length , 0 as DECIMAL_DIGITS,0 as NUM_PREC_RADIX ,0 as NULLABLE ,null as REMARKS ,0 AS sql_data_type ,"\
      " null as SQL_DATETIME_SUB,0 as CHAR_OCTET_LENGTH ,AG.POSITION as ORDINAL_POSITION , 'NO' as  IS_NULLABLE  FROM  "\
      " (select t.*, DECODE (t.PACKAGE_NAME, null, '', t.PACKAGE_NAME||'.') ||t.OBJECT_NAME AS PROCEDURE_NAME from ALL_ARGUMENTS t) AG "
/************** SQLProcedureColumnsOracle **********/

/************** SQLProcedure **************/
#define MADB_PROCEDURE_ORACLE \
  "SELECT SAR.OWNER AS PROCEDURE_CAT,"\
    "NULL AS PROCEDURE_SCHEM,"\
    "SAR.PROCEDURE_NAME2 AS PROCEDURE_NAME,"\
    "NULL AS NUM_INPUT_PARAMS,"\
    "NULL AS NUM_OUTPUT_PARAMS,"\
    "NULL AS NUM_RESULT_SETS,"\
    "NULL AS REMARKS,"\
    "SAR.PROCEDURE_TYPE AS PROCEDURE_TYPE FROM "\
    " (select t.*, "\
    "  CASE WHEN t.OBJECT_TYPE = 'FUNCTION' then 2 WHEN t.OBJECT_TYPE= 'PROCEDURE' THEN 1 WHEN t.OBJECT_TYPE= 'PACKAGE' THEN 1 ELSE 0 END AS PROCEDURE_TYPE,"\
    "  CASE WHEN t.OBJECT_TYPE ='PACKAGE' then t.OBJECT_NAME||'.'||t.PROCEDURE_NAME ELSE t.OBJECT_NAME END AS PROCEDURE_NAME2 "\
    "  FROM SYS.ALL_PROCEDURES t"\
    " ) SAR "
/************** SQLProcedure **************/

/************** SQLSpecialColumnsOracle **********/
#define MADB_SPECIAL_COLUMNS_ROWID_ORACLE \
  "SELECT 2 AS SCOPE, 'ROWID' AS COLUMN_NAME, 1 AS DATA_TYPE, 'ROWID' AS TYPE_NAME, NULL AS COLUMN_SIZE, 18 AS BUFFER_LENGTH, NULL AS DECIMAL_DIGITS, 2 AS PSEUDO_COLUMN " \
   "FROM ALL_TAB_COLUMNS  ATC WHERE 2 = 2 "

#define MADB_SPECIAL_COLUMNS_ORACLE \
   "SELECT NULL AS SCOPE, "\
     "ATC.column_name AS column_name, "\
     "case when ATC.data_type = 'CHAR' then 1 "\
     " when ATC.data_type = 'VARCHAR2' then 12 " \
     " when ATC.data_type = 'NCHAR' then - 8 "\
     " when ATC.data_type = 'NVARCHAR2' then - 9 " \
     " when ATC.data_type = 'XMLTYPE' then 2009 "\
     " when ATC.data_type = 'DATE' then 93 "\
     " when ATC.data_type = 'BFILE' then - 13 "\
     " when ATC.data_type = 'LONG' then - 1 "\
     " when ATC.data_type = 'FLOAT' then 6 "\
     " when ATC.data_type = 'NUMBER' then 3 "\
     " when ATC.data_type = 'BINARY_FLOAT' then 7 "\
     " when ATC.data_type = 'BINARY_DOUBLE' then 8 "\
     " when ATC.data_type = 'CLOB' then - 1 "\
     " when ATC.data_type = 'RAW' then - 3 "\
     " when substr(ATC.data_type, 0, 9) = 'TIMESTAMP' and length(ATC.data_type) = 12 then 93 "\
     " else - 4 END AS data_type, "\
     " ATC.data_type AS type_name, "\
   " case when ATC.data_type in('CHAR', 'VARCHAR', 'VARCHAR2', 'NCHAR','NVARCHAR2') then ATC.char_length "\
   " when ATC.data_type = 'DATE' then 19 "\
   " when ATC.data_type = 'NUMBER' then 38 "\
   " when ATC.data_type = 'BINARY_FLOAT' then 7 "\
   " when ATC.data_type = 'BINARY_DOUBLE' then 15 "\
   " when substr(ATC.data_type, 0, 12) = 'INTERVAL YEA' then 1111 "\
   " when substr(ATC.data_type, 0, 12) = 'INTERVAL DAY' then 1111 "\
   " when substr(ATC.data_type, 0, 9) = 'TIMESTAMP' and length(ATC.data_type) = 27 then 1111 "\
   " when substr(ATC.data_type, 0, 9) = 'TIMESTAMP' and length(ATC.data_type) = 33 then 1111 "\
   " when substr(ATC.data_type, 0, 9) = 'TIMESTAMP' and length(ATC.data_type) = 12 then 19 "\
   " when ATC.data_type = 'FLOAT' and ATC.data_precision != null then floor(ATC.data_precision*0.30103) + 1 "\
   " when ATC.data_type = 'FLOAT' and ATC.data_precision = null then ATC.data_precision "\
   " else ATC.data_length END AS column_size, "\
   " case when ATC.data_type = 'DATE' then 16 "\
   " when ATC.data_type = 'FLOAT' then 8 "\
   " when ATC.data_type = 'BINARY_FLOAT' then 4 "\
   " when ATC.data_type = 'BINARY_DOUBLE' then 8 "\
   " when ATC.data_type = 'NUMBER' and ATC.data_precision = null then 40 "\
   " when ATC.data_type = 'NUMBER' and ATC.data_precision != null then ATC.data_precision + 2 "\
   " when substr(ATC.data_type, 0, 12) = 'INTERVAL YEA' then 5 "\
   " when substr(ATC.data_type, 0, 12) = 'INTERVAL DAY' then 11 "\
   " when substr(ATC.data_type, 0, 9) = 'TIMESTAMP' and length(ATC.data_type) = 27 then 13 "\
   " when substr(ATC.data_type, 0, 9) = 'TIMESTAMP' and length(ATC.data_type) = 33 then 11 "\
   " when substr(ATC.data_type, 0, 9) = 'TIMESTAMP' and length(ATC.data_type) = 12 then 11 "\
   " else ATC.data_length END AS buffer_length, "\
   " case when ATC.data_type = 'DATE' then 16 "\
   " when ATC.data_type = 'FLOAT' then 8 "\
   " when ATC.data_type = 'BINARY_FLOAT' then 4 "\
   " when ATC.data_type = 'BINARY_DOUBLE' then 8 "\
   " when ATC.data_type = 'NUMBER' and ATC.data_precision = null then 40 "\
   " when ATC.data_type = 'NUMBER' and ATC.data_precision != null then ATC.data_precision + 2 "\
   " when substr(ATC.data_type, 0, 12) = 'INTERVAL YEA' then 5 "\
   " when substr(ATC.data_type, 0, 12) = 'INTERVAL DAY' then 11 "\
   " when substr(ATC.data_type, 0, 9) = 'TIMESTAMP' and length(ATC.data_type) = 27 then 13 "\
   " when substr(ATC.data_type, 0, 9) = 'TIMESTAMP' and length(ATC.data_type) = 33 then 11 "\
   " when substr(ATC.data_type, 0, 9) = 'TIMESTAMP' and length(ATC.data_type) = 12 then 11 "\
   " else ATC.data_length END AS buffer_length, "\
   " case when ATC.data_type = 'NUMBER' and ATC.data_precision = null then - 127 "\
   " else ATC.data_scale END AS decimal_digits, "\
   " 0 as PSEUDO_COLUMN FROM ALL_TAB_COLUMNS ATC where 2=2 "
/************** SQLSpecialColumnsOracle **********/

#endif
