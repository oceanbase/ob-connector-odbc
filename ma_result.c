/************************************************************************************
   Copyright (C) 2013,2019 MariaDB Corporation AB
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


/* {{{ MADB_StmtResetResultStructures */
void MADB_StmtResetResultStructures(MADB_Stmt *Stmt)
{
  int fieldcnt = StmtFieldCount(Stmt);
  Stmt->CharOffset = (unsigned long *)MADB_REALLOC((char *)Stmt->CharOffset, sizeof(long) * fieldcnt);
  memset(Stmt->CharOffset, 0, sizeof(long) * fieldcnt);
  Stmt->Lengths = (unsigned long *)MADB_REALLOC((char *)Stmt->Lengths, sizeof(long) * fieldcnt);
  memset(Stmt->Lengths, 0, sizeof(long) * fieldcnt);

  Stmt->LastRowFetched= 0;
  MADB_STMT_RESET_CURSOR(Stmt);
}
/* }}} */

/* {{{ MoveNext - moves C/C cursor forward for Offset positions */
SQLRETURN MoveNext(MADB_Stmt *Stmt, unsigned long long Offset)
{
  SQLRETURN  result= SQL_SUCCESS;

  if (Stmt->result != NULL)
  {
    unsigned int i;
    char        *SavedFlag;

    SavedFlag= (char*)MADB_CALLOC(mysql_stmt_field_count(Stmt->stmt));

    if (SavedFlag == NULL)
    {
      return SQL_ERROR;
    }

    for (i=0; i < mysql_stmt_field_count(Stmt->stmt); i++)
    {
      SavedFlag[i]= Stmt->stmt->bind[i].flags & MADB_BIND_DUMMY;

      Stmt->stmt->bind[i].flags|= MADB_BIND_DUMMY;
    }

    while (Offset--)
    {
      if (mysql_stmt_fetch(Stmt->stmt) == 1)
      {
        result= SQL_ERROR;
        break;
      }
    }

    for (i=0; i < mysql_stmt_field_count(Stmt->stmt); i++)
    {
      Stmt->stmt->bind[i].flags &= (~MADB_BIND_DUMMY | SavedFlag[i]);
    }

    MADB_FREE(SavedFlag);
  }
  return result;
}
/* }}} */

/* {{{ MADB_StmtDataSeek */
SQLRETURN MADB_StmtDataSeek(MADB_Stmt *Stmt, my_ulonglong FetchOffset)
{
  MYSQL_ROWS *tmp= NULL;

  if (IsStmtNossps(Stmt)){
    if (!Stmt->result2)
    {
      return SQL_NO_DATA_FOUND;
    }

    mysql_data_seek(Stmt->result2, FetchOffset);
  } else {
    if (!Stmt->stmt->result.data)
    {
      return SQL_NO_DATA_FOUND;
    }

    mysql_stmt_data_seek(Stmt->stmt, FetchOffset);
  }
  

  return SQL_SUCCESS;  
}
/* }}} */

/* {{{  */
void QuickDropAllPendingResults(MYSQL* Mariadb)
{
  int Next= 0;
  do {
    if (Next == 0)
    {
      if (mysql_field_count(Mariadb) > 0)
      {
        MYSQL_RES *Res= mysql_store_result(Mariadb);

        if (Res)
        {
          mysql_free_result(Res);
        }
      }
    }
  } while ((Next= mysql_next_result(Mariadb)) != -1);
}
/* }}} */

SQLRETURN MADB_StmtMoreResultsNossps(MADB_Stmt *Stmt)
{
  SQLRETURN ret = SQL_SUCCESS;

  if (!mysql_more_results(Stmt->Connection->mariadb))
    return SQL_NO_DATA;

  LOCK_MARIADB(Stmt->Connection);
  if (mysql_next_result(Stmt->Connection->mariadb) > 0){
    UNLOCK_MARIADB(Stmt->Connection);
    return MADB_SetError(&Stmt->Error, MADB_ERR_HY000, mysql_error(Stmt->Connection->mariadb), 0);
  }

  if (mysql_field_count(Stmt->Connection->mariadb) == 0)
  {
    MADB_DescFree(Stmt->Ird, TRUE);
    Stmt->AffectedRows = mysql_affected_rows(Stmt->Connection->mariadb);
  }
  else
  {
    unsigned int ServerStatus;
    mariadb_get_infov(Stmt->Connection->mariadb, MARIADB_CONNECTION_SERVER_STATUS, (void*)&ServerStatus);

    if (Stmt->Query.QueryType == MADB_QUERY_CALL) {
      Stmt->State = MADB_SS_OUTPARAMSFETCHED;
      ret = MADB_GetOutParamsNossps(Stmt, 0);
    } else {
      if (Stmt->result2) {
        mysql_free_result(Stmt->result2);
        Stmt->result2 = NULL;
      }
      Stmt->result2 = mysql_store_result(Stmt->Connection->mariadb);
      if (Stmt->result2 && Stmt->result2->field_count > 0) {
        Stmt->fields2 = mysql_fetch_fields(Stmt->result2);
        MADB_DescSetIrdMetadata(Stmt, mysql_fetch_fields(Stmt->result2), Stmt->result2->field_count);
        MADB_StmtResetResultStructures(Stmt);
      }

      Stmt->AffectedRows = mysql_affected_rows(Stmt->Connection->mariadb);
    }
  }
  UNLOCK_MARIADB(Stmt->Connection);

  return ret;
}

SQLRETURN MADB_StmtMoreResultsRefCursor(MADB_Stmt *Stmt)
{
  SQLRETURN ret = SQL_SUCCESS;
  MADB_FREE(Stmt->result);
  Stmt->LastRowFetched = 0;
  MADB_STMT_RESET_CURSOR(Stmt);

  MADB_StmtCloseRefCursor(Stmt);

  if (Stmt->lastRefCursor < Stmt->maxRefCursor) {
    Stmt->lastRefCursor++;
  }
  if (Stmt->lastRefCursor >= Stmt->maxRefCursor) {
    Stmt->lastRefCursor = -1;
    Stmt->maxRefCursor = 0;
    ret = SQL_NO_DATA_FOUND;
  } else {
    if (Stmt->arrayRefCursor[Stmt->lastRefCursor] == 0) {
      ret = SQL_SUCCESS_WITH_INFO;
    } else {
      //open lastRefCursor 
      ret = MADB_StmtOpenRefCursor(Stmt, SQL_FETCH_NEXT, 1);
    }
  }
  return ret;
}

/* {{{ MADB_StmtMoreResults */
SQLRETURN MADB_StmtMoreResults(MADB_Stmt *Stmt)
{
  SQLRETURN ret= SQL_SUCCESS;

  // RefCursor
  if (IsStmtRefCursor(Stmt)) {
    return MADB_StmtMoreResultsRefCursor(Stmt);
  }

  if (!Stmt->stmt)
  {
    return MADB_SetError(&Stmt->Error, MADB_ERR_08S01, NULL, 0);
  }

  /* We can't have it in MADB_StmtResetResultStructures, as it breaks dyn_cursor functionality.
     Thus we free-ing bind structs on move to new result only */
  MADB_FREE(Stmt->result);

  if (Stmt->MultiStmts)
  {
    if (Stmt->MultiStmtNr == STMT_COUNT(Stmt->Query) - 1)
    {
      return SQL_NO_DATA;
    }

    ++Stmt->MultiStmtNr;

    MADB_InstallStmt(Stmt, Stmt->MultiStmts[Stmt->MultiStmtNr]);

    return SQL_SUCCESS;
  }

  /* in case we executed a multi statement, it was done via mysql_query */
  if (Stmt->State == MADB_SS_EMULATED)
  {
    if (!mysql_more_results(Stmt->Connection->mariadb))
      return SQL_NO_DATA;
    else
    {
      int Next;

      LOCK_MARIADB(Stmt->Connection);
      Next= mysql_next_result(Stmt->Connection->mariadb);

      if (Next > 0)
      {
        ret= MADB_SetError(&Stmt->Error, MADB_ERR_HY000, mysql_error(Stmt->Connection->mariadb), 0);
      }
      else if (mysql_field_count(Stmt->Connection->mariadb) != 0)
      {
        MYSQL_RES *Res= mysql_store_result(Stmt->Connection->mariadb);
        if (Res != NULL)
        {
          mysql_free_result(Res);
        }
        ret= MADB_SetError(&Stmt->Error, MADB_ERR_01000, "Internal error - unexpected text result received", 0);
      }
      else
      {
        Stmt->AffectedRows= mysql_affected_rows(Stmt->Connection->mariadb);
      }
      UNLOCK_MARIADB(Stmt->Connection);
    }
    return ret;
  }

  //nossps
  if (IsStmtNossps(Stmt)) {
    return MADB_StmtMoreResultsNossps(Stmt);
  }

  if (mysql_stmt_more_results(Stmt->stmt))
  {
    mysql_stmt_free_result(Stmt->stmt);
  }
  else
  {
    return SQL_NO_DATA;
  }
  
  LOCK_MARIADB(Stmt->Connection);
  if (mysql_stmt_next_result(Stmt->stmt) > 0)
  {
    UNLOCK_MARIADB(Stmt->Connection);
    return MADB_SetNativeError(&Stmt->Error, SQL_HANDLE_STMT, Stmt->stmt);
  }
  
  MADB_StmtResetResultStructures(Stmt);

  if (mysql_stmt_field_count(Stmt->stmt) == 0)
  {
    MADB_DescFree(Stmt->Ird, TRUE);
    Stmt->AffectedRows= mysql_stmt_affected_rows(Stmt->stmt);
  }
  else
  {
    unsigned int ServerStatus;

    MADB_DescSetIrdMetadata(Stmt, mysql_fetch_fields(FetchMetadata(Stmt)), mysql_stmt_field_count(Stmt->stmt));
    Stmt->AffectedRows= 0;

    mariadb_get_infov(Stmt->Connection->mariadb, MARIADB_CONNECTION_SERVER_STATUS, (void*)&ServerStatus);

    if (ServerStatus & SERVER_PS_OUT_PARAMS)
    {
      Stmt->State= MADB_SS_OUTPARAMSFETCHED;
      ret= Stmt->Methods->GetOutParams(Stmt, 0);
    }
    else
    {
      if (Stmt->Options.CursorType != SQL_CURSOR_FORWARD_ONLY)
      {
        mysql_stmt_store_result(Stmt->stmt);
        mysql_stmt_data_seek(Stmt->stmt, 0);
      }
    }
  }
  UNLOCK_MARIADB(Stmt->Connection);

  return ret;
}
/* }}} */

/* {{{ MADB_RecordsToFetch */
SQLULEN MADB_RowsToFetch(MADB_Cursor *Cursor, SQLULEN ArraySize, unsigned long long RowsInResultst)
{
  SQLULEN  Position= Cursor->Position >= 0 ? Cursor->Position : 0;
  SQLULEN result= ArraySize;

  Cursor->RowsetSize= ArraySize;

  if (Position + ArraySize > RowsInResultst)
  {
    if (Position >= 0 && RowsInResultst > Position)
    {
      result= (SQLULEN)(RowsInResultst - Position);
    }
    else
    {
      result= 1;
    }
  }

  return result;
}
/* }}} */

