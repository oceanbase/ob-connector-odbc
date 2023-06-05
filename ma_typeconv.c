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

/* ODBC C->SQL and SQL->C type conversion functions */

#include <ma_odbc.h>
#include <ma_global.h>
#include <ob_oralce_format_models.h>
#include "ma_string.h"

/* Borrowed from C/C and adapted */
SQLRETURN MADB_Str2Ts(const char *Str, size_t Length, MYSQL_TIME *Tm, BOOL Interval, MADB_Error *Error, BOOL *isTime)
{
  char *localCopy= MADB_ALLOC(Length + 1), *Start= localCopy, *Frac, *End= Start + Length;
  my_bool isDate= 0;

  if (Start == NULL)
  {
    MADB_FREE(localCopy);
    return MADB_SetError(Error, MADB_ERR_HY001, NULL, 0);
  }

  memset(Tm, 0, sizeof(MYSQL_TIME));
  memcpy(Start, Str, Length);
  Start[Length]= '\0';

  while (Length && isspace(*Start)) ++Start, --Length;

  if (Length == 0)
  {
    goto end;//MADB_SetError(Error, MADB_ERR_22008, NULL, 0);
  }  

  /* Determine time type:
  MYSQL_TIMESTAMP_DATE: [-]YY[YY].MM.DD
  MYSQL_TIMESTAMP_DATETIME: [-]YY[YY].MM.DD hh:mm:ss.mmmmmm
  MYSQL_TIMESTAMP_TIME: [-]hh:mm:ss.mmmmmm
  */
  if (strchr(Start, '-'))
  {
    if (sscanf(Start, "%d-%u-%u", &Tm->year, &Tm->month, &Tm->day) < 3)
    {
      MADB_FREE(localCopy);
      return MADB_SetError(Error, MADB_ERR_22008, NULL, 0);
    }
    isDate= 1;
    if (!(Start= strchr(Start, ' ')))
    {
      goto check;
    }
  }
  if (!strchr(Start, ':'))
  {
    goto check;
  }

  if (isDate == 0)
  {
    *isTime= 1;
  }

  if ((Frac= strchr(Start, '.')) != NULL) /* fractional seconds */
  {
    size_t FracMulIdx= End - (Frac + 1) - 1/*to get index array index */;
    /* ODBC - nano-seconds */
    if (sscanf(Start, "%d:%u:%u.%6lu", &Tm->hour, &Tm->minute,
      &Tm->second, &Tm->second_part) < 4)
    {
      MADB_FREE(localCopy);
      return MADB_SetError(Error, MADB_ERR_22008, NULL, 0);
    }
    /* 9 digits up to nano-seconds, and -1 since comparing with arr idx  */
    if (FracMulIdx < 6 - 1)
    {
      static unsigned long Mul[]= {100000, 10000, 1000, 100, 10};
      Tm->second_part*= Mul[FracMulIdx];
    }
  }
  else
  {
    if (sscanf(Start, "%d:%u:%u", &Tm->hour, &Tm->minute,
      &Tm->second) < 3)
    {
      MADB_FREE(localCopy);
      return MADB_SetError(Error, MADB_ERR_22008, NULL, 0);
    }
  }

check:
  if (Interval == FALSE)
  {
    if (isDate)
    {
      if (Tm->year > 0)
      {
        if (Tm->year < 70)
        {
          Tm->year+= 2000;
        }
        else if (Tm->year < 100)
        {
          Tm->year+= 1900;
        }
      }
    }
  }

end:
  MADB_FREE(localCopy);
  return SQL_SUCCESS;
}

SQLRETURN MADB_Str2Ts_Oracle(const char *Str, size_t Length, ORACLE_TIME *Tm, BOOL Interval, MADB_Error *Error, BOOL *isTime)
{
  char *localCopy = MADB_ALLOC(Length + 1), *Start = localCopy, *Frac, *End = Start + Length;
  my_bool isDate = 0;
  my_bool isOffset = 0;
  my_bool isPositive = 1;
  int year = 0;

  if (Start == NULL)
  {
    return MADB_SetError(Error, MADB_ERR_HY001, NULL, 0);
  }

  memset(Tm, 0, sizeof(ORACLE_TIME));
  memcpy(Start, Str, Length);
  Start[Length] = '\0';

  while (Length && isspace(*Start)) ++Start, --Length;

  if (Length == 0)
  {
    goto end;//MADB_SetError(Error, MADB_ERR_22008, NULL, 0);
  }

  /* Determine time type:
  MYSQL_TIMESTAMP_DATE: [-]YY[YY].MM.DD
  MYSQL_TIMESTAMP_DATETIME: [-]YY[YY].MM.DD hh:mm:ss.mmmmmm
  MYSQL_TIMESTAMP_TIME: [-]hh:mm:ss.mmmmmm
  */
  if (strchr(Start, '-'))
  {
    if (sscanf(Start, "%d-%u-%u", &Tm->year, &Tm->month, &Tm->day) < 3)
    {
      MADB_FREE(localCopy);
      return MADB_SetError(Error, MADB_ERR_22008, NULL, 0);
    }
    isDate = 1;
    if (!(Start = strchr(Start, ' ')))
    {
      goto check;
    }
    Start += 1;
  }
  if (!strchr(Start, ':'))
  {
    goto check;
  }

  if (isDate == 0)
  {
    *isTime = 1;
  }

  if ((Frac = strchr(Start, '.')) != NULL) /* fractional seconds */
  {
    size_t FracMulIdx = End - (Frac + 1) - 1/*to get index array index */;
    if (sscanf(Start, "%d:%u:%u.%9lu", &Tm->hour, &Tm->minute, &Tm->second, &Tm->second_part) < 4)
    {
      MADB_FREE(localCopy);
      return MADB_SetError(Error, MADB_ERR_22008, NULL, 0);
    }
    /* 9 digits up to nano-seconds, and -1 since comparing with arr idx  */
    if (FracMulIdx < 9 - 1)
    {
      static unsigned long Mul[] = { 100000000, 10000000, 1000000, 100000, 10000, 1000, 100, 10 };
      Tm->second_part *= Mul[FracMulIdx];
    }
  }
  else
  {
    if (sscanf(Start, "%d:%u:%u", &Tm->hour, &Tm->minute, &Tm->second) < 3)
    {
      MADB_FREE(localCopy);
      return MADB_SetError(Error, MADB_ERR_22008, NULL, 0);
    }
  }

  if (Start = strchr(Start, ' '))
  {
    Start += 1;
    if (strchr(Start, ':'))
    {
      if (*Start == '-') {
        isPositive = 0;
      }
      if (sscanf(Start, "%d:%d", &Tm->offset_hour, &Tm->offset_minute) < 2) {
        MADB_FREE(localCopy);
        return MADB_SetError(Error, MADB_ERR_22008, NULL, 0);
      }
      isOffset = 1;
      if (isPositive == 0) {
        Tm->offset_hour *= -1;
      }
    }
  }
  
check:
  if (Interval == FALSE)
  {
    if (isDate)
    {
      if (Tm->year > 0)
      {
        if (Tm->year < 70)
        {
          Tm->year += 2000;
        }
        else if (Tm->year < 100)
        {
          Tm->year += 1900;
        }
      }
    }
  }

  year = Tm->year;
  Tm->year = year % 100;
  Tm->century = year / 100;

end:
  MADB_FREE(localCopy);
  return SQL_SUCCESS;
}

void GetFsprecFromFmt(unsigned char *fsprec, const char* fmt)
{
  int i = 0;
  char *pFind = NULL;
  char tmp[128] = { 0 };
  size_t len = 128;
  size_t fmt_str_length = strlen(fmt);

  for (i = 0; i < (int)fmt_str_length; i++) {
    tmp[i] = toupper(fmt[i]);
  }
  pFind = strstr(tmp, "FF");//FF1-FF9
  if (pFind != NULL && pFind - tmp + 3 <= (int)fmt_str_length) {
    char c = (char)fmt[pFind - tmp + 2];
    if (c > '0' && c <= '9') {
      *fsprec = (unsigned char)(c - '0');
    }
  }
}

SQLRETURN MADB_DateTime2Str_Oracle(char *Str, size_t* Length, ORACLE_TIME *Tm, char* fmt, MADB_Error *Error)
{
  #define MAX_DATETIME_BUFFER_SIZE 100
  const char *inner_buffer[MAX_DATETIME_BUFFER_SIZE];
  const char *fmt_str = NULL;
  unsigned char fmt_str_length = 0;
  int64_t pos = 0;
  uint nano_max_len = 9;
  int8_t sign = 1;
  ORACLE_TIME* oracle_time = Tm;
  unsigned char fsprec = 6;

  struct ObTime ob_time;
  memset(&ob_time, 0, sizeof(struct ObTime));

  if (NULL != fmt) {
    fmt_str = (char*)fmt;
    fmt_str_length = (unsigned char)strlen(fmt_str);
  } else {
    fmt_str = (char*)"YYYY-MM-DD HH24:MI:SS";
    fmt_str_length = (unsigned char)strlen(fmt_str);
  }

  GetFsprecFromFmt(&fsprec, fmt_str);

  ob_time.mode_ |= DT_TYPE_ORACLE;

  ob_time.nano_scale_ = fsprec;
  if (ob_time.nano_scale_ > nano_max_len) {
    ob_time.nano_scale_ = nano_max_len;
  }

  ob_time.parts_[DT_YEAR] = oracle_time->century * 100 + oracle_time->year;
  ob_time.parts_[DT_MON] = oracle_time->month;
  ob_time.parts_[DT_MDAY] = oracle_time->day;
  ob_time.parts_[DT_HOUR] = oracle_time->hour;
  ob_time.parts_[DT_MIN] = oracle_time->minute;
  ob_time.parts_[DT_SEC] = oracle_time->second;
  ob_time.parts_[DT_USEC] = oracle_time->second_part;
  ob_time.parts_[DT_DATE] = ob_time_to_date(&ob_time);

  ob_time.time_zone_id_ = -1;

  sign = 1;
  if (oracle_time->offset_hour < 0 || oracle_time->offset_minute < 0) {
    sign = -1;
  }

  ob_time.parts_[DT_OFFSET_MIN] = sign * (abs(oracle_time->offset_hour) * 60 + abs(oracle_time->offset_minute));

  if (NULL != oracle_time->tz_name) {
    int32_t tz_length = 0;
    ob_time.time_zone_id_ = 0;

    tz_length = strlen(oracle_time->tz_name);
    if (tz_length + 1 > OB_MAX_TZ_NAME_LEN) {
      return MADB_SetError(Error, MADB_ERR_HY000, NULL, 0);
    } else {
      memcpy(ob_time.tz_name_, oracle_time->tz_name, tz_length);
      ob_time.tz_name_[tz_length] = '\0';

      if (NULL != oracle_time->tz_abbr) {
        tz_length = strlen(oracle_time->tz_abbr);

        if (tz_length + 1 > OB_MAX_TZ_ABBR_LEN) {
          return MADB_SetError(Error, MADB_ERR_HY000, NULL, 0);
        } else {
          memcpy(ob_time.tzd_abbr_, oracle_time->tz_abbr, tz_length);
          ob_time.tzd_abbr_[tz_length] = '\0';
        }
      }
    }
  }

  if (ob_time_to_str_oracle_dfm(&ob_time, fmt_str, fmt_str_length, ob_time.nano_scale_, (char*)inner_buffer, MAX_DATETIME_BUFFER_SIZE, &pos)) {
    return MADB_SetError(Error, MADB_ERR_HY000, NULL, 0);
  } else if (pos > *Length) {
    return MADB_SetError(Error, MADB_ERR_HY000, NULL, 0);
  } else {
    memcpy(Str, inner_buffer, pos);
    if (pos < *Length) {
      Str[pos] = '\0';
    }
    *Length = pos;
  }

  return SQL_SUCCESS;
}

SQLRETURN MADB_Date2Str_Oracle(char *Str, size_t* Length, ORACLE_TIME *Tm, char* fmt, MADB_Error *Error)
{
#define MAX_DATETIME_BUFFER_SIZE 100
  const char *inner_buffer[MAX_DATETIME_BUFFER_SIZE];
  const char *fmt_str = NULL;
  unsigned char fmt_str_length = 0;
  unsigned char fsprec = 6;
  int64_t pos = 0;

  struct ObTime ob_time;
  memset(&ob_time, 0, sizeof(struct ObTime));

  if (NULL != fmt) {
    fmt_str = (char*)fmt;
    fmt_str_length = (unsigned char)strlen(fmt_str);
  }
  else {
    fmt_str = (char*)"DD-MON-YY";
    fmt_str_length = (unsigned char)strlen(fmt_str);
  }

  GetFsprecFromFmt(&fsprec, fmt_str);

  ob_time.mode_ |= DT_TYPE_ORACLE;

  ob_time.parts_[DT_YEAR] = Tm->century * 100 + Tm->year;
  ob_time.parts_[DT_MON] = Tm->month;
  ob_time.parts_[DT_MDAY] = Tm->day;
  ob_time.parts_[DT_HOUR] = Tm->hour;
  ob_time.parts_[DT_MIN] = Tm->minute;
  ob_time.parts_[DT_SEC] = Tm->second;
  ob_time.parts_[DT_USEC] = Tm->second_part;
  ob_time.parts_[DT_DATE] = ob_time_to_date(&ob_time);

  ob_time.time_zone_id_ = -1;

  if (ob_time_to_str_oracle_dfm(&ob_time, fmt_str, fmt_str_length, ob_time.nano_scale_, (char*)inner_buffer, MAX_DATETIME_BUFFER_SIZE, &pos)) {
    return MADB_SetError(Error, MADB_ERR_HY000, NULL, 0);
  } else if (pos > *Length) {
    return MADB_SetError(Error, MADB_ERR_HY000, NULL, 0);
  } else {
    memcpy(Str, inner_buffer, pos);
    if (pos < *Length) {
      Str[pos] = '\0';
    }
    *Length = pos;
  }
  return SQL_SUCCESS;
}

/* {{{ MADB_ConversionSupported */
BOOL MADB_ConversionSupported(MADB_DescRecord *From, MADB_DescRecord *To)
{
  switch (From->ConciseType)
  {
  case SQL_C_TIMESTAMP:
  case SQL_C_TYPE_TIMESTAMP:
  case SQL_C_TIME:
  case SQL_C_TYPE_TIME:
  case SQL_C_DATE:
  case SQL_C_TYPE_DATE:

    if (To->Type == SQL_INTERVAL)
    {
      return FALSE;
    }

  }
  return TRUE;
}
/* }}} */

/* {{{ MADB_ConvertCharToBit */
char MADB_ConvertCharToBit(MADB_Stmt *Stmt, char *src)
{
  char *EndPtr= NULL;
  float asNumber= strtof(src, &EndPtr);

  if (asNumber < 0 || asNumber > 1)
  {
    /* 22003 */
  }
  else if (asNumber != 0 && asNumber != 1)
  {
    /* 22001 */
  }
  else if (EndPtr != NULL && *EndPtr != '\0')
  {
    /* 22018. TODO: check if condition is correct */
  }

  return asNumber != 0 ? '\1' : '\0';
}
/* }}} */

/* {{{ MADB_ConvertNumericToChar */
size_t MADB_ConvertNumericToChar(SQL_NUMERIC_STRUCT *Numeric, char *Buffer, int *ErrorCode)
{
  const double DenominatorTable[]= {1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0, 100000000.0, 1000000000.0/*9*/,
                                    10000000000.0, 100000000000.0, 1000000000000.0, 10000000000000.0, 100000000000000.0, 1000000000000000.0/*15*/,
                                    10000000000000000.0, 100000000000000000.0, 1000000000000000000.0, 10000000000000000000.0, 1e+20 /*20*/, 1e+21,
                                    1e+22, 1e+23, 1e+24, 1e+25, 1e+26, 1e+27, 1e+28, 1e+29, 1e+30, 1e+31, 1e+32, 1e+33, 1e+34, 1e+35, 1e+36, 1e+37, 1e+38 };
  unsigned long long Numerator= 0;
  double Denominator;
  int Scale= 0;
  unsigned long long ByteDenominator= 1;
  int i;
  char* p;
  size_t Length;

  Buffer[0]= 0;
  *ErrorCode= 0;

  Scale+= (Numeric->scale < 0) ? -Numeric->scale : Numeric->scale;

  for (i= 0; i < SQL_MAX_NUMERIC_LEN; ++i)
  {
    if (i > 7 && Numeric->val[i] != '\0')
    {
      *ErrorCode = MADB_ERR_22003;
      return 0;
    }
    Numerator += Numeric->val[i] * ByteDenominator;
    ByteDenominator <<= 8;
  }

  if (Numeric->scale > 0)
  {
    Denominator = DenominatorTable[Scale];// pow(10, Scale);
    char tmp[10 /*1 sign + 1 % + 1 dot + 3 scale + 1f + 1\0 */];
    _snprintf(tmp, sizeof(tmp), "%s%%.%df", Numeric->sign ? "" : "-", Numeric->scale);
    _snprintf(Buffer, MADB_CHARSIZE_FOR_NUMERIC, tmp, Numerator / Denominator);
  }
  else
  {
    _snprintf(Buffer, MADB_CHARSIZE_FOR_NUMERIC, "%s%llu", Numeric->sign ? "" : "-", Numerator);
    /* Checking Truncation for negative/zero scale before adding 0 */
    Length= strlen(Buffer) - (Numeric->sign ? 0 : 1);
    if (Length > Numeric->precision)
    {
      *ErrorCode = MADB_ERR_22003;
      goto end;
    }
    for (i= 0; i < Scale; ++i)
    {
      strcat(Buffer, "0");
    }
  }

  if (Buffer[0] == '-')
  {
    ++Buffer;
  }

  Length= strlen(Buffer);
  /* Truncation checks:
  1st ensure, that the digits before decimal point will fit */
  if ((p= strchr(Buffer, '.')))
  {
    if (Numeric->precision != 0 && (p - Buffer) > Numeric->precision)
    {
      *ErrorCode= MADB_ERR_22003;
      Length= Numeric->precision;
      Buffer[Numeric->precision]= 0;
      goto end;
    }

    /* If scale >= precision, we still can have no truncation */
    if (Length > Numeric->precision + 1/*dot*/ && Scale < Numeric->precision)
    {
      *ErrorCode= MADB_ERR_01S07;
      Length = Numeric->precision + 1/*dot*/;
      Buffer[Length]= 0;
      goto end;
    }
  }

end:
  /* check if last char is decimal point */
  if (Length > 0 && Buffer[Length - 1] == '.')
  {
    Buffer[Length - 1]= 0;
  }
  if (Numeric->sign == 0)
  {
    ++Length;
  }
  return Length;
}
/* }}} */

/* {{{ MADB_ConvertNullValue */
SQLRETURN MADB_ConvertNullValue(MADB_Stmt *Stmt, MYSQL_BIND *MaBind)
{
  MaBind->buffer_type=  MYSQL_TYPE_NULL;
  MaBind->buffer_length= 0;

  return SQL_SUCCESS;
}
/* }}} */

/* {{{ MADB_ProcessIndicator */
/* Returns TRUE if indicator contains some special value, and thus no further type conversion is needed */
BOOL MADB_ProcessIndicator(MADB_Stmt *Stmt, SQLLEN Indicator, char * DefaultValue, MYSQL_BIND *MaBind)
{
  switch (Indicator)
  {
  case SQL_COLUMN_IGNORE:
    if (DefaultValue == NULL)
    {
      MADB_ConvertNullValue(Stmt, MaBind);
    }
    else
    {
      MaBind->buffer=       DefaultValue;
      MaBind->buffer_length= (unsigned long)strlen(DefaultValue);
      MaBind->buffer_type=  MYSQL_TYPE_STRING;
    }
    return TRUE;
  case SQL_NULL_DATA:
    MADB_ConvertNullValue(Stmt, MaBind);
    return TRUE;
  }

  return FALSE;
}
/* }}} */

/* {{{ MADB_CalculateLength */
SQLLEN MADB_CalculateLength(MADB_Stmt *Stmt, SQLLEN *OctetLengthPtr, MADB_DescRecord *CRec, void* DataPtr)
{
  /* If no OctetLengthPtr was specified, or OctetLengthPtr is SQL_NTS character
     are considered to be NULL binary data are null terminated */
  if (!OctetLengthPtr || *OctetLengthPtr == SQL_NTS)
  {
    /* Meaning of Buffer Length is not quite clear in specs. Thus we treat in the way, that does not break
        (old) testcases. i.e. we neglect its value if Length Ptr is specified */
    SQLLEN BufferLen= OctetLengthPtr ? -1 : CRec->OctetLength;

    switch (CRec->ConciseType)
    {
    case SQL_C_WCHAR:
      /* CRec->OctetLength eq 0 means not 0-length buffer, but that this value is not specified. Thus -1, for SqlwcsLen
          and SafeStrlen that means buffer len is not specified */
      return SqlwcsLen((SQLWCHAR *)DataPtr, BufferLen/sizeof(SQLWCHAR) - test(BufferLen == 0)) * sizeof(SQLWCHAR);
      break;
    case SQL_C_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
    case SQL_C_CHAR:
      return SafeStrlen((SQLCHAR *)DataPtr, BufferLen != 0 ? BufferLen : -1);
    }
  }
  else
  {
    if(IS_ORACLE_MODE(Stmt) && CRec->OctetLength > 0 &&
      (CRec->ConciseType == SQL_C_CHAR || CRec->ConciseType == SQL_C_WCHAR)){
      return min(CRec->OctetLength, *OctetLengthPtr);
    } else {
      return *OctetLengthPtr;
    }
  }

  return CRec->OctetLength;
}
/* }}} */

/* {{{ MADB_GetBufferForSqlValue */
void* MADB_GetBufferForSqlValue(MADB_Stmt *Stmt, MADB_DescRecord *CRec, size_t Size)
{
  if (Stmt->RebindParams || CRec->InternalBuffer == NULL)
  {
    MADB_FREE(CRec->InternalBuffer);
    CRec->InternalBuffer= MADB_CALLOC(Size);
    if (CRec->InternalBuffer == NULL)
    {
      MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
      return NULL;
    }
  }

  return (void *)CRec->InternalBuffer;
}
/* }}} */

/* {{{ MADB_Wchar2Sql */
SQLRETURN MADB_Wchar2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
  MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr)
{
  SQLULEN mbLength=0;

  MADB_FREE(CRec->InternalBuffer);

  /* conn cs ? */
  CRec->InternalBuffer= MADB_ConvertFromWChar((SQLWCHAR *)DataPtr, (SQLINTEGER)(Length / sizeof(SQLWCHAR)),
    &mbLength, &Stmt->Connection->Charset, NULL);

  if (CRec->InternalBuffer == NULL)
  {
    return MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
  }

  *LengthPtr= (unsigned long)mbLength;
  *Buffer= CRec->InternalBuffer;

  MaBind->buffer_type=  MYSQL_TYPE_STRING;

  return SQL_SUCCESS;
}
/* }}} */

/* {{{ MADB_Char2Sql */
SQLRETURN MADB_Char2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
  MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr)
{
  switch (SqlRec->Type)
  {
    case SQL_BIT:
      if (*Buffer == NULL)
      {
        CRec->InternalBuffer= (char *)MADB_GetBufferForSqlValue(Stmt, CRec, MaBind->buffer_length);

        if (CRec->InternalBuffer == NULL)
        {
          return Stmt->Error.ReturnValue;
        }
        *Buffer= CRec->InternalBuffer;
      }

      *LengthPtr= 1;
      **(char**)Buffer= MADB_ConvertCharToBit(Stmt, DataPtr);
      MaBind->buffer_type= MYSQL_TYPE_TINY;
      break;
  case SQL_DATETIME:
  {
    MYSQL_TIME Tm;
    SQL_TIMESTAMP_STRUCT Ts;
    BOOL isTime;

    /* Enforcing constraints on date/time values */
    RETURN_ERROR_OR_CONTINUE(MADB_Str2Ts(DataPtr, Length, &Tm, FALSE, &Stmt->Error, &isTime));
    MADB_CopyMadbTimeToOdbcTs(&Tm, &Ts);
    RETURN_ERROR_OR_CONTINUE(MADB_TsConversionIsPossible(&Ts, SqlRec->ConciseType, &Stmt->Error, MADB_ERR_22018, isTime));
    /* To stay on the safe side - still sending as string in the default branch */
  }
  default:
    /* Bulk shouldn't get here, thus logic for single paramset execution */
    *LengthPtr= (unsigned long)Length;
    *Buffer= DataPtr;
    MaBind->buffer_type= MYSQL_TYPE_STRING;
  }

  return SQL_SUCCESS;
}
/* }}} */

SQLRETURN MADB_Char2SqlBin_Oracle(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
  MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr)
{
  /* Bulk shouldn't get here, thus logic for single paramset execution */
  *LengthPtr = (unsigned long)Length;
  *Buffer = DataPtr;
  MaBind->buffer_type = MYSQL_TYPE_OB_RAW;
  return SQL_SUCCESS;
}

SQLRETURN MADB_Char2Sql_Oracle(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
  MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr)
{
  switch (SqlRec->Type)
  {
  case SQL_BIT:
    if (*Buffer == NULL)
    {
      CRec->InternalBuffer = (char *)MADB_GetBufferForSqlValue(Stmt, CRec, MaBind->buffer_length);

      if (CRec->InternalBuffer == NULL)
      {
        return Stmt->Error.ReturnValue;
      }
      *Buffer = CRec->InternalBuffer;
    }

    *LengthPtr = 1;
    **(char**)Buffer = MADB_ConvertCharToBit(Stmt, DataPtr);
    MaBind->buffer_type = MYSQL_TYPE_TINY;
    break;
  case SQL_DATETIME:  //SQL_TYPE_DATE, SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP
  {
    if (SqlRec->ConciseType == SQL_TYPE_DATE || SqlRec->ConciseType == SQL_C_DATE) {
      MYSQL_TIME Tm;
      SQL_TIMESTAMP_STRUCT Ts;
      BOOL isTime;

      /* Enforcing constraints on date/time values */
      RETURN_ERROR_OR_CONTINUE(MADB_Str2Ts(DataPtr, Length, &Tm, FALSE, &Stmt->Error, &isTime));
      MADB_CopyMadbTimeToOdbcTs(&Tm, &Ts);
      char* pFind = strchr(DataPtr, ' '); // oracle this keep year-month-day
      if (pFind) {
        Length = pFind - (char*)DataPtr;
      }

      *LengthPtr = (unsigned long)Length;
      *Buffer = DataPtr;
      MaBind->buffer_type = MYSQL_TYPE_STRING;
    } else if (SqlRec->ConciseType == SQL_TYPE_TIME || SqlRec->ConciseType == SQL_C_TIME){
      ORACLE_TIME Tm;
      BOOL isTime;
      char fmt[128] = { 0 };
      SQLLEN len = 128;

      time_t sec_time;
      struct tm * cur_tm;
      sec_time = time(NULL);
      cur_tm = localtime(&sec_time);

      MADB_FREE(CRec->InternalBuffer);
      CRec->InternalBuffer = (char *)MADB_GetBufferForSqlValue(Stmt, CRec, 128);
      if (CRec->InternalBuffer == NULL) {
        return Stmt->Error.ReturnValue;
      }
      memset(CRec->InternalBuffer, 0, 128);
      *Buffer = CRec->InternalBuffer;
      
      /* Enforcing constraints on date/time values */
      RETURN_ERROR_OR_CONTINUE(MADB_Str2Ts_Oracle(DataPtr, Length, &Tm, FALSE, &Stmt->Error, &isTime));
      Tm.century = (1900 + cur_tm->tm_year) / 100;
      Tm.year = (1900 + cur_tm->tm_year) % 100;
      Tm.month = (cur_tm->tm_mon + 1);
      Tm.day = (cur_tm->tm_mday);
      MADB_DateTime2Str_Oracle(CRec->InternalBuffer, &len, &Tm, "YYYY-MM-DD HH24:MI:SS", &Stmt->Error);

      *LengthPtr = (unsigned long)strlen(CRec->InternalBuffer);
      //*Buffer = DataPtr;
      MaBind->buffer_type = MYSQL_TYPE_STRING;
    } else {
      MYSQL_TIME Tm;
      SQL_TIMESTAMP_STRUCT Ts;
      BOOL isTime;

      /* Enforcing constraints on date/time values */
      RETURN_ERROR_OR_CONTINUE(MADB_Str2Ts(DataPtr, Length, &Tm, FALSE, &Stmt->Error, &isTime));
      MADB_CopyMadbTimeToOdbcTs(&Tm, &Ts);
      RETURN_ERROR_OR_CONTINUE(MADB_TsConversionIsPossible(&Ts, SqlRec->ConciseType, &Stmt->Error, MADB_ERR_22018, isTime));
      *LengthPtr = (unsigned long)Length;
      *Buffer = DataPtr;
      MaBind->buffer_type = MYSQL_TYPE_STRING;
    }
  }
  break;
  default:
    /* Bulk shouldn't get here, thus logic for single paramset execution */
    *LengthPtr = (unsigned long)Length;
    *Buffer = DataPtr;
    MaBind->buffer_type = MYSQL_TYPE_STRING;
  }

  return SQL_SUCCESS;
}

/* {{{ MADB_Numeric2Sql */
SQLRETURN MADB_Numeric2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
  MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr)
{
  SQL_NUMERIC_STRUCT *p;
  int ErrorCode= 0;

  /* We might need to preserve this pointer to be able to later release the memory */
  CRec->InternalBuffer= (char *)MADB_GetBufferForSqlValue(Stmt, CRec, MADB_CHARSIZE_FOR_NUMERIC);

  if (CRec->InternalBuffer == NULL)
  {
    return Stmt->Error.ReturnValue;
  }

  p= (SQL_NUMERIC_STRUCT *)DataPtr;
  p->scale= (SQLSCHAR)SqlRec->Scale;
  p->precision= (SQLSCHAR)SqlRec->Precision;

  *LengthPtr= (unsigned long)MADB_ConvertNumericToChar((SQL_NUMERIC_STRUCT *)p, CRec->InternalBuffer, &ErrorCode);;
  *Buffer= CRec->InternalBuffer;

  MaBind->buffer_type= MYSQL_TYPE_STRING;

  if (ErrorCode)
  {
    /*TODO: I guess this parameters row should be skipped */
    return MADB_SetError(&Stmt->Error, ErrorCode, NULL, 0);
  }

  return SQL_SUCCESS;
}
/* }}} */

/* {{{ MADB_TsConversionIsPossible */
SQLRETURN MADB_TsConversionIsPossible(SQL_TIMESTAMP_STRUCT *ts, SQLSMALLINT SqlType, MADB_Error *Error, enum enum_madb_error SqlState, int isTime)
{
  /* I think instead of MADB_ERR_22008 there should be also SqlState */
  switch (SqlType)
  {
  case SQL_TIME:
  case SQL_TYPE_TIME:
    if (ts->fraction)
    {
      return MADB_SetError(Error, MADB_ERR_22008, NULL, 0);
    }
    break;
  case SQL_DATE:
  case SQL_TYPE_DATE:
    if (ts->hour + ts->minute + ts->second + ts->fraction)
    {
      return MADB_SetError(Error, MADB_ERR_22008, NULL, 0);
    }
    break;
  default:
    /* This only would be good for SQL_TYPE_TIME. If C type is time(isTime!=0), and SQL type is timestamp, date fields may be NULL - driver should set them to current date */
    if ((isTime == 0 && ts->year == 0) || ts->month == 0 || ts->day == 0)
    {
      return MADB_SetError(Error, SqlState, NULL, 0);
    }
  }
  return SQL_SUCCESS;
}
/* }}} */

/* {{{ MADB_Timestamp2Sql */
SQLRETURN MADB_Timestamp2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
  MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr)
{
  MYSQL_TIME           *tm= NULL;
  SQL_TIMESTAMP_STRUCT *ts= (SQL_TIMESTAMP_STRUCT *)DataPtr;

  RETURN_ERROR_OR_CONTINUE(MADB_TsConversionIsPossible(ts, SqlRec->ConciseType, &Stmt->Error, MADB_ERR_22007, 0));

  if (*Buffer == NULL)
  {
    tm= (MYSQL_TIME*)MADB_GetBufferForSqlValue(Stmt, CRec, sizeof(MYSQL_TIME));
    if (tm == NULL)
    {
      /* Error is set in function responsible for allocation */
      return Stmt->Error.ReturnValue;
    }
    *Buffer= tm;
  }
  else
  {
    tm= *Buffer;
  }
  

  /* Default types. Not quite clear if time_type has any effect */
  tm->time_type=       MYSQL_TIMESTAMP_DATETIME;
  MaBind->buffer_type= MYSQL_TYPE_TIMESTAMP;

  switch (SqlRec->ConciseType) {
  case SQL_TYPE_DATE:
    if (ts->hour + ts->minute + ts->second + ts->fraction != 0)
    {
      return MADB_SetError(&Stmt->Error, MADB_ERR_22008, "Time fields are nonzero", 0);
    }

    MaBind->buffer_type = MYSQL_TYPE_DATE;
    tm->time_type = MYSQL_TIMESTAMP_DATE;
    tm->year = ts->year;
    tm->month = ts->month;
    tm->day = ts->day;
    break;
  case SQL_TYPE_TIME:
    if (ts->fraction != 0)
    {
      return MADB_SetError(&Stmt->Error, MADB_ERR_22008, "Fractional seconds fields are nonzero", 0);
    }
    
    if (!VALID_TIME(ts))
    {
      return MADB_SetError(&Stmt->Error, MADB_ERR_22007, "Invalid time", 0);
    }
    MaBind->buffer_type= MYSQL_TYPE_TIME;
    tm->time_type=       MYSQL_TIMESTAMP_TIME;
    tm->hour=   ts->hour;
    tm->minute= ts->minute;
    tm->second= ts->second;
    break;
  default:
    MADB_CopyOdbcTsToMadbTime(ts, tm);
  }

  *LengthPtr= sizeof(MYSQL_TIME);

  return SQL_SUCCESS;
}
/* }}} */

/* {{{ MADB_Timestamp2Sql */
SQLRETURN MADB_Timestamp2Sql_Oracle(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
  MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr)
{
  SQL_TIMESTAMP_STRUCT *ts = (SQL_TIMESTAMP_STRUCT *)DataPtr;
  if (ts->year==0 || ts->month == 0 || ts->day ==0){
    return MADB_SetError(&Stmt->Error, MADB_ERR_22007, NULL, 0);
  }

  switch (SqlRec->ConciseType) {
  case SQL_TYPE_DATE:
  case SQL_C_DATE:
  {
    MYSQL_TIME           *tm = NULL;
    if (*Buffer == NULL)
    {
      tm = (MYSQL_TIME*)MADB_GetBufferForSqlValue(Stmt, CRec, sizeof(MYSQL_TIME));
      if (tm == NULL)
      {
        /* Error is set in function responsible for allocation */
        return Stmt->Error.ReturnValue;
      }
      *Buffer = tm;
    } else {
      tm = *Buffer;
    }

    MaBind->buffer_type = MYSQL_TYPE_DATETIME;
    tm->time_type = MYSQL_TIMESTAMP_DATE;
    tm->year = ts->year;
    tm->month = ts->month;
    tm->day = ts->day;
    tm->hour = ts->hour;
    tm->minute = ts->minute;
    tm->second = ts->second;
    *LengthPtr = sizeof(MYSQL_TIME);
    return SQL_SUCCESS;
  }
  break;
  case SQL_TYPE_TIME:
  case SQL_C_TIME:
  case SQL_TYPE_TIMESTAMP:
  case SQL_C_TIMESTAMP:
  {
    ORACLE_TIME           *tm = NULL;
    if (*Buffer == NULL)
    {
      tm = (ORACLE_TIME*)MADB_GetBufferForSqlValue(Stmt, CRec, sizeof(ORACLE_TIME));
      if (tm == NULL){
        /* Error is set in function responsible for allocation */
        return Stmt->Error.ReturnValue;
      }
      *Buffer = tm;
    } else {
      tm = *Buffer;
    }
    MaBind->buffer_type = MYSQL_TYPE_OB_TIMESTAMP_NANO;
    MADB_CopyOdbcTsToMadbTime_Oracle(ts, tm);
    *LengthPtr = sizeof(ORACLE_TIME);
    return SQL_SUCCESS;
  }
  break;
  default:
    return Stmt->Error.ReturnValue;
  }
  return SQL_SUCCESS;
}
/* }}} */

/* {{{ MADB_Time2Sql */
SQLRETURN MADB_Time2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
  MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr)
{
  MYSQL_TIME      *tm= NULL;
  SQL_TIME_STRUCT *ts= (SQL_TIME_STRUCT *)DataPtr;

  if ((SqlRec->ConciseType == SQL_TYPE_TIME || SqlRec->ConciseType == SQL_TYPE_TIMESTAMP ||
    SqlRec->ConciseType == SQL_TIME || SqlRec->ConciseType == SQL_TIMESTAMP || SqlRec->ConciseType == SQL_DATETIME) &&
    !VALID_TIME(ts))
  {
    return MADB_SetError(&Stmt->Error, MADB_ERR_22007, NULL, 0);
  }

  if (*Buffer == NULL)
  {
    tm= (MYSQL_TIME*)MADB_GetBufferForSqlValue(Stmt, CRec, sizeof(MYSQL_TIME));
    if (tm == NULL)
    {
      /* Error is set in function responsible for allocation */
      return Stmt->Error.ReturnValue;
    }
    *Buffer= tm;
  }
  else
  {
    tm= *Buffer;
  }

  if(SqlRec->ConciseType == SQL_TYPE_TIMESTAMP ||
    SqlRec->ConciseType == SQL_TIMESTAMP || SqlRec->ConciseType == SQL_DATETIME)
  {
    time_t sec_time;
    struct tm * cur_tm;

    sec_time= time(NULL);
    cur_tm= localtime(&sec_time);

    tm->year= 1900 + cur_tm->tm_year;
    tm->month= cur_tm->tm_mon + 1;
    tm->day= cur_tm->tm_mday;
    tm->second_part= 0;
    
    tm->time_type= MYSQL_TIMESTAMP_DATETIME;
    MaBind->buffer_type= MYSQL_TYPE_TIMESTAMP;
  }
  else
  {
    tm->year=  0;
    tm->month= 0;
    tm->day=   0;

    tm->time_type = MYSQL_TIMESTAMP_TIME;
    MaBind->buffer_type= MYSQL_TYPE_TIME;
  }

  tm->hour=   ts->hour;
  tm->minute= ts->minute;
  tm->second= ts->second;

  tm->second_part= 0;

  *LengthPtr= sizeof(MYSQL_TIME);

  return SQL_SUCCESS;
}
/* }}} */

SQLRETURN MADB_Time2Sql_Oracle(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
  MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr)
{
  SQL_TIME_STRUCT *ts = (SQL_TIME_STRUCT *)DataPtr;

  if ((SqlRec->ConciseType == SQL_TYPE_TIME || SqlRec->ConciseType == SQL_TYPE_TIMESTAMP ||
    SqlRec->ConciseType == SQL_TIME || SqlRec->ConciseType == SQL_TIMESTAMP || SqlRec->ConciseType == SQL_DATETIME) &&
    !VALID_TIME(ts))
  {
    return MADB_SetError(&Stmt->Error, MADB_ERR_22007, NULL, 0);
  }

  switch(SqlRec->ConciseType){
  case SQL_TYPE_DATE:
  case SQL_C_DATE:
  {
    MYSQL_TIME      *tm = NULL;
    if (*Buffer == NULL)
    {
      tm = (MYSQL_TIME*)MADB_GetBufferForSqlValue(Stmt, CRec, sizeof(MYSQL_TIME));
      if (tm == NULL)
      {
        /* Error is set in function responsible for allocation */
        return Stmt->Error.ReturnValue;
      }
      *Buffer = tm;
    } else {
      tm = *Buffer;
    }

    time_t sec_time;
    struct tm * cur_tm;

    sec_time = time(NULL);
    cur_tm = localtime(&sec_time);

    tm->year = 1900 + cur_tm->tm_year;
    tm->month = cur_tm->tm_mon + 1;
    tm->day = cur_tm->tm_mday;
    tm->hour = ts->hour;
    tm->minute = ts->minute;
    tm->second = ts->second;
    tm->second_part = 0;

    tm->time_type = MYSQL_TIMESTAMP_DATETIME;
    MaBind->buffer_type = MYSQL_TYPE_TIMESTAMP;
    *LengthPtr = sizeof(MYSQL_TIME);
    return SQL_SUCCESS;
  }
  break;
  case SQL_TYPE_TIMESTAMP:
  case SQL_C_TIMESTAMP:
  case SQL_TYPE_TIME:
  case SQL_C_TIME:
  {
    ORACLE_TIME      *tm = NULL;
    if (*Buffer == NULL)
    {
      tm = (ORACLE_TIME*)MADB_GetBufferForSqlValue(Stmt, CRec, sizeof(ORACLE_TIME));
      if (tm == NULL)
      {
        /* Error is set in function responsible for allocation */
        return Stmt->Error.ReturnValue;
      }
      *Buffer = tm;
    } else {
      tm = *Buffer;
    }

    MaBind->buffer_type = MYSQL_TYPE_OB_TIMESTAMP_NANO;
    MADB_CopyOdbcTimeToMadbTime_Oracle(ts, tm);
    *LengthPtr = sizeof(ORACLE_TIME);
    return SQL_SUCCESS;
  }
  break;
  default:
    return Stmt->Error.ReturnValue;
  }
  return SQL_SUCCESS;
}

/* {{{ MADB_IntervalHtoMS2Sql */
SQLRETURN MADB_IntervalHtoMS2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
  MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr)
{
  MYSQL_TIME          *tm= NULL;
  SQL_INTERVAL_STRUCT *is= (SQL_INTERVAL_STRUCT *)DataPtr;

  if (*Buffer == NULL)
  {
    tm= (MYSQL_TIME*)MADB_GetBufferForSqlValue(Stmt, CRec, sizeof(MYSQL_TIME));
    if (tm == NULL)
    {
      /* Error is set in function responsible for allocation */
      return Stmt->Error.ReturnValue;
    }
    *Buffer= tm;
  }
  else
  {
    tm= *Buffer;
  }

  tm->hour=   is->intval.day_second.hour;
  tm->minute= is->intval.day_second.minute;
  tm->second= CRec->ConciseType == SQL_C_INTERVAL_HOUR_TO_SECOND ? is->intval.day_second.second : 0;

  tm->second_part= 0;

  tm->time_type= MYSQL_TIMESTAMP_TIME;
  MaBind->buffer_type= MYSQL_TYPE_TIME;
  *LengthPtr= sizeof(MYSQL_TIME);

  return SQL_SUCCESS;
}
/* }}} */

/* {{{ MADB_Date2Sql */
SQLRETURN MADB_Date2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
  MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr)
{
  MYSQL_TIME      *tm= NULL, **BuffPtr= (MYSQL_TIME**)Buffer;
  SQL_DATE_STRUCT *ts= (SQL_DATE_STRUCT *)DataPtr;

  if (*BuffPtr == NULL)
  {
    tm= (MYSQL_TIME*)MADB_GetBufferForSqlValue(Stmt, CRec, sizeof(MYSQL_TIME));
    if (tm == NULL)
    {
      /* Error is set in function responsible for allocation */
      return Stmt->Error.ReturnValue;
    }
    *BuffPtr= tm;
  }
  else
  {
    tm= *BuffPtr;
  }

  tm->year=  ts->year;
  tm->month= ts->month;
  tm->day=   ts->day;

  tm->hour= tm->minute= tm->second= tm->second_part= 0;
  tm->time_type= MYSQL_TIMESTAMP_DATE;

  if (IS_ORACLE_MODE(Stmt))
    MaBind->buffer_type = MYSQL_TYPE_DATETIME;
  else
    MaBind->buffer_type= MYSQL_TYPE_DATE;

  *LengthPtr= sizeof(MYSQL_TIME);

  return SQL_SUCCESS;
}
/* }}} */

/* {{{ MADB_ConvertC2Sql */
SQLRETURN MADB_ConvertC2Sql(MADB_Stmt *Stmt, MADB_DescRecord *CRec, void* DataPtr, SQLLEN Length,
                            MADB_DescRecord *SqlRec, MYSQL_BIND *MaBind, void **Buffer, unsigned long *LengthPtr)
{
  if (Buffer == NULL)
  {
    MaBind->buffer= NULL;
    Buffer= &MaBind->buffer;
  }
  if (LengthPtr == NULL)
  {
    LengthPtr= &MaBind->buffer_length;
  }
  /* Switch to fill BIND structures based on C and SQL type */
  switch (CRec->ConciseType)
  {
  case WCHAR_TYPES:
    RETURN_ERROR_OR_CONTINUE(MADB_Wchar2Sql(Stmt, CRec, DataPtr, Length, SqlRec, MaBind, Buffer, LengthPtr));
    break;
  case CHAR_BINARY_TYPES:
    if (IS_ORACLE_MODE(Stmt)){
      if (CRec->ConciseType == SQL_C_BINARY || CRec->ConciseType == SQL_LONGVARBINARY || CRec->ConciseType == SQL_VARBINARY){
        RETURN_ERROR_OR_CONTINUE(MADB_Char2SqlBin_Oracle(Stmt, CRec, DataPtr, Length, SqlRec, MaBind, Buffer, LengthPtr));
      } else {
        RETURN_ERROR_OR_CONTINUE(MADB_Char2Sql_Oracle(Stmt, CRec, DataPtr, Length, SqlRec, MaBind, Buffer, LengthPtr));
      }
    } else {
      RETURN_ERROR_OR_CONTINUE(MADB_Char2Sql(Stmt, CRec, DataPtr, Length, SqlRec, MaBind, Buffer, LengthPtr));
    }
    break;
  case SQL_C_NUMERIC:
    RETURN_ERROR_OR_CONTINUE(MADB_Numeric2Sql(Stmt, CRec, DataPtr, Length, SqlRec, MaBind, Buffer, LengthPtr));
    break;
  case SQL_C_TIMESTAMP:
  case SQL_TYPE_TIMESTAMP:
    if (IS_ORACLE_MODE(Stmt)) {
      RETURN_ERROR_OR_CONTINUE(MADB_Timestamp2Sql_Oracle(Stmt, CRec, DataPtr, Length, SqlRec, MaBind, Buffer, LengthPtr));
    } else {
      RETURN_ERROR_OR_CONTINUE(MADB_Timestamp2Sql(Stmt, CRec, DataPtr, Length, SqlRec, MaBind, Buffer, LengthPtr));
    }
    break;
  case SQL_C_TIME:
  case SQL_C_TYPE_TIME:
    if (IS_ORACLE_MODE(Stmt)){
      RETURN_ERROR_OR_CONTINUE(MADB_Time2Sql_Oracle(Stmt, CRec, DataPtr, Length, SqlRec, MaBind, Buffer, LengthPtr));
    } else {
      RETURN_ERROR_OR_CONTINUE(MADB_Time2Sql(Stmt, CRec, DataPtr, Length, SqlRec, MaBind, Buffer, LengthPtr));
    }
    break;
  case SQL_C_INTERVAL_HOUR_TO_MINUTE:
  case SQL_C_INTERVAL_HOUR_TO_SECOND:
    RETURN_ERROR_OR_CONTINUE(MADB_IntervalHtoMS2Sql(Stmt, CRec, DataPtr, Length, SqlRec, MaBind, Buffer, LengthPtr));
    break;
  case SQL_C_DATE:
  case SQL_TYPE_DATE:
    RETURN_ERROR_OR_CONTINUE(MADB_Date2Sql(Stmt, CRec, DataPtr, Length, SqlRec, MaBind, Buffer, LengthPtr));
    break;
  default:
    /* memset(MaBind, 0, sizeof(MYSQL_BIND));
    MaBind->buffer_length= 0; */
    MaBind->buffer_type=   0;
    MaBind->is_unsigned=   0;

    *LengthPtr= (unsigned long)Length;
    MaBind->buffer_type= MADB_GetMaDBTypeAndLength(CRec->ConciseType,
      &MaBind->is_unsigned, &MaBind->buffer_length);

    if (!CRec->OctetLength)
    {
      CRec->OctetLength= MaBind->buffer_length;
    }
    *Buffer= DataPtr;
  }           /* End of switch (CRec->ConsiseType) */
  /* We need it in case SQL_SUCCESS_WITH_INFO was set, we can't just return SQL_SUCCESS */
  return Stmt->Error.ReturnValue;
}
/* }}} */

/* {{{ MADB_C2SQL */
/* Main entrance function for C type to SQL type conversion*/
SQLRETURN MADB_C2SQL(MADB_Stmt* Stmt, MADB_DescRecord *CRec, MADB_DescRecord *SqlRec, SQLULEN ParamSetIdx, MYSQL_BIND *MaBind)
{
  SQLLEN *IndicatorPtr= NULL;
  SQLLEN *OctetLengthPtr= NULL;
  void   *DataPtr= NULL;
  SQLLEN  Length= 0;

  IndicatorPtr=   (SQLLEN *)GetBindOffset(Stmt->Apd, CRec, CRec->IndicatorPtr, ParamSetIdx, sizeof(SQLLEN));
  OctetLengthPtr= (SQLLEN *)GetBindOffset(Stmt->Apd, CRec, CRec->OctetLengthPtr, ParamSetIdx, sizeof(SQLLEN));

  if (PARAM_IS_DAE(OctetLengthPtr))
  {
    if (!DAE_DONE(Stmt))
    {
      return SQL_NEED_DATA;
    }
    else
    {
      MaBind->buffer_type= MADB_GetMaDBTypeAndLength(CRec->ConciseType, &MaBind->is_unsigned, &MaBind->buffer_length);
      /* I guess we can leave w/out this. Keeping it so far for safety */
      MaBind->long_data_used= '\1';
      return SQL_SUCCESS;
    }
  }    /* -- End of DAE parameter processing -- */

  if (IndicatorPtr && MADB_ProcessIndicator(Stmt, *IndicatorPtr, CRec->DefaultValue, MaBind))
  {
    return SQL_SUCCESS;
  }

  /* -- Special cases are done, i.e. not a DAE etc, general case -- */
 
  DataPtr= GetBindOffset(Stmt->Apd, CRec, CRec->DataPtr, ParamSetIdx, CRec->OctetLength);

  /* If indicator wasn't NULL_DATA, but data pointer is still NULL, we convert NULL value */
  if (!DataPtr)
  {
    return MADB_ConvertNullValue(Stmt, MaBind);
  }
  
  Length= MADB_CalculateLength(Stmt, OctetLengthPtr, CRec, DataPtr);

  RETURN_ERROR_OR_CONTINUE(MADB_ConvertC2Sql(Stmt, CRec, DataPtr, Length, SqlRec, MaBind, NULL, NULL));
  /* We need it in case SUCCESS_WITH_INFO was set */
  return Stmt->Error.ReturnValue;
}
/* }}} */

SQLRETURN MADB_C2RefCursor(MADB_Stmt* Stmt, MADB_DescRecord* IrdRec, SQLULEN ParamSetIdx, MYSQL_BIND *bind)
{
  bind->buffer_type = MYSQL_TYPE_NULL;
  return SQL_SUCCESS;
}

SQLLEN MADB_CalculateLength2Str(MADB_Stmt *Stmt, SQLLEN *OctetLengthPtr, MADB_DescRecord *CRec, void* DataPtr)
{
  /* If no OctetLengthPtr was specified, or OctetLengthPtr is SQL_NTS character
   are considered to be NULL binary data are null terminated */
  if (!OctetLengthPtr || *OctetLengthPtr == SQL_NTS)
  {
    /* Meaning of Buffer Length is not quite clear in specs. Thus we treat in the way, that does not break
        (old) testcases. i.e. we neglect its value if Length Ptr is specified */
    SQLLEN BufferLen = OctetLengthPtr ? -1 : CRec->OctetLength;
    SQLLEN tmp = 0;
    switch (CRec->ConciseType)
    {
    case SQL_C_WCHAR:
      /* CRec->OctetLength eq 0 means not 0-length buffer, but that this value is not specified. Thus -1, for SqlwcsLen
          and SafeStrlen that means buffer len is not specified */
      tmp = SqlwcsLen((SQLWCHAR *)DataPtr, BufferLen / sizeof(SQLWCHAR) - test(BufferLen == 0)) ;
      return tmp * sizeof(SQLWCHAR);
      break;
    case SQL_C_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
      return 2 * SafeStrlen((SQLCHAR *)DataPtr, BufferLen != 0 ? BufferLen : -1);
    case SQL_C_CHAR:
      return SafeStrlen((SQLCHAR *)DataPtr, BufferLen != 0 ? BufferLen : -1);
    }
  }
  else
  {
    if (IS_ORACLE_MODE(Stmt)){
      if (CRec->ConciseType == SQL_C_BINARY || CRec->ConciseType== SQL_VARBINARY || CRec->ConciseType == SQL_LONGVARBINARY){
        SQLLEN BufferLen = *OctetLengthPtr>0 ? *OctetLengthPtr : CRec->OctetLength;
        return 2 * BufferLen;
      }
      if ((CRec->ConciseType == SQL_C_CHAR || CRec->ConciseType == SQL_C_WCHAR) && CRec->OctetLength > 0){
        return min(CRec->OctetLength, *OctetLengthPtr);
      } else {
        return *OctetLengthPtr;
      }
    } else {
      return *OctetLengthPtr;
    }
  }
  return CRec->OctetLength;
}

int utf32toutf8(unsigned int i, unsigned char *c)
{
  int len = 0, x;

  if (i < 0x80)
  {
    *c = (unsigned char)(i & 0x7f);
    return 1;
  }
  else if (i < 0x800)
  {
    *c++ = (3 << 6) | (i >> 6);
    len = 2;
  }
  else if (i < 0x10000)
  {
    *c++ = (7 << 5) | (i >> 12);
    len = 3;
  }
  else if (i < 0x10ffff)
  {
    *c++ = (0xf << 4) | (i >> 18);
    len = 4;
  }
  x = len;
  if (x)
    while (--x)
    {
      *c++ = (1 << 7) | ((i >> (6 * (x - 1))) & 0x3f);
    }

  return len;
}
int utf16toutf32(unsigned short *i, unsigned int *u)
{
  if (*i >= 0xd800 && *i <= 0xdbff)
  {
    *u = 0x10000 | ((*i++ & 0x3ff) << 10);
    if (*i < 0xdc00 || *i > 0xdfff) /* invalid */
      return 0;
    *u |= *i & 0x3ff;
    return 2;
  }
  else
  {
    *u = *i;
    return 1;
  }
}

SQLCHAR *sqlwchar_as_utf8_ext(const SQLWCHAR *str, long *len, SQLCHAR *buff, uint buff_max, int *utf8mb4_used)
{
  const SQLWCHAR *str_end;
  unsigned char *u8 = buff;
  int utf8len, dummy;
  long i;

  if (!str || *len <= 0)
  {
    *len = 0;
    return buff;
  }

  if (utf8mb4_used == NULL)
  {
    utf8mb4_used = &dummy;
  }

  if (!u8)
  {
    *len = -1;
    return NULL;
  }

  str_end = str + *len;

  if (sizeof(SQLWCHAR) == 4)
  {
    for (i = 0; str < str_end; )
    {
      i += (utf8len = utf32toutf8((unsigned int)*str++, u8 + i));
      /*
        utf8mb4 is a superset of utf8, only supplemental characters
        which require four bytes differs in storage characteristics (length)
        between utf8 and utf8mb4.
      */
      if (utf8len == 4)
      {
        *utf8mb4_used = 1;
      }
    }
  }
  else
  {
    for (i = 0; str < str_end; )
    {
      unsigned int u32;
      int consumed = utf16toutf32((unsigned short *)str, &u32);
      if (!consumed)
      {
        break;
      }
      str += consumed;
      i += (utf8len = utf32toutf8(u32, u8 + i));
      /*
        utf8mb4 is a superset of utf8, only supplemental characters
        which require four bytes differs in storage characteristics (length)
        between utf8 and utf8mb4.
      */
      if (utf8len == 4)
      {
        *utf8mb4_used = 1;
      }
    }
  }
  *len = i;
  return u8;
}

SQLRETURN ConvertTimeStr2Str(MADB_Stmt *Stmt, SQLSMALLINT ctype, MADB_DescRecord *sqlCRec, char *dataPtr, long *length, char *buff, uint buff_max)
{
  SQLRETURN ret = SQL_SUCCESS;
  MYSQL_TIME tm;
  BOOL isTime;

  if (sqlCRec->ConciseType == SQL_C_TIME || sqlCRec->ConciseType == SQL_TIME || sqlCRec->ConciseType == SQL_TYPE_TIME)
  {
    if (!SQL_SUCCEEDED(ret = MADB_Str2Ts(dataPtr, *length, &tm, FALSE, &Stmt->Error, &isTime)))
    {
      return ret;
    }
    if (tm.hour > 23 || tm.minute > 59 || tm.second > 59)
    {
      return MADB_SetError(&Stmt->Error, MADB_ERR_22008, "Not a valid time value supplied", 0);
    }
    //oracle mode add date for time
    time_t sec_time;
    struct tm * cur_tm;
    sec_time = time(NULL);
    cur_tm = localtime(&sec_time);
    *length = sprintf(buff, "TIMESTAMP'%04d-%02d-%02d %02d:%02d:%02d'",
      1900 + cur_tm->tm_year, cur_tm->tm_mon + 1, cur_tm->tm_mday, tm.hour, tm.minute, tm.second);
  }
  else if (sqlCRec->ConciseType == SQL_C_DATE || sqlCRec->ConciseType == SQL_DATE || sqlCRec->ConciseType == SQL_TYPE_DATE)
  {
    if (!SQL_SUCCEEDED(ret = MADB_Str2Ts(dataPtr, *length, &tm, FALSE, &Stmt->Error, &isTime)))
    {
      return ret;
    }
    if (tm.month>12 || tm.month < 1 || tm.day > 31 || tm.day < 1){
      return MADB_SetError(&Stmt->Error, MADB_ERR_22008, "Not a valid date value supplied", 0);
    }
    //oracle mode del time for date
    *length = sprintf(buff, "DATE'%04d-%02d-%02d'", tm.year, tm.month, tm.day);
  }
  else if (sqlCRec->ConciseType == SQL_C_TIMESTAMP || sqlCRec->ConciseType == SQL_TIMESTAMP || sqlCRec->ConciseType == SQL_TYPE_TIMESTAMP)
  {
    SQL_TIMESTAMP_STRUCT Ts;
    BOOL isTime;

    /* Enforcing constraints on date/time values */
    if (!SQL_SUCCEEDED(ret = MADB_Str2Ts(dataPtr, *length, &tm, FALSE, &Stmt->Error, &isTime)))
    {
      return ret;
    }
    MADB_CopyMadbTimeToOdbcTs(&tm, &Ts);
    if (!SQL_SUCCEEDED(ret = MADB_TsConversionIsPossible(&Ts, sqlCRec->ConciseType, &Stmt->Error, MADB_ERR_22018, isTime)))
    {
      return ret;
    }
    //oracle mode del time for date
    *length = sprintf(buff, "TIMESTAMP'%s'", dataPtr);
  }
  return ret;
}

SQLRETURN MADB_ConvertType2Str(MADB_Stmt *Stmt, SQLSMALLINT ctype, MADB_DescRecord *sqlCRec, char *dataPtr, long *length, char *buff, uint buff_max)
{
  SQLRETURN ret = SQL_SUCCESS;
  switch (ctype)
  {
  case SQL_C_BINARY:
  {
    if (IS_ORACLE_MODE(Stmt)){
      int i = 0;
      long len = *length / 2;
      buff[0] = '\'';
      for (i = 0; i < len; i++)
        sprintf(&buff[1 + 2 * i], "%02X", (unsigned char)dataPtr[i]);
      buff[2 * len + 1] = '\'';
      *length = 2 * len + 2;
    } else {
      buff[0] = '\'';
      memcpy(buff + 1, dataPtr, *length);
      buff[*length + 1] = '\'';
      *length = *length + 2;
    }
    break;
  }
  case SQL_C_CHAR:
  {
    if (IS_ORACLE_MODE(Stmt)){
      if (sqlCRec->ConciseType == SQL_C_TIME || sqlCRec->ConciseType == SQL_TIME || sqlCRec->ConciseType == SQL_TYPE_TIME ||
        sqlCRec->ConciseType == SQL_C_DATE || sqlCRec->ConciseType == SQL_DATE || sqlCRec->ConciseType == SQL_TYPE_DATE ||
        sqlCRec->ConciseType == SQL_C_TIMESTAMP || sqlCRec->ConciseType == SQL_TIMESTAMP || sqlCRec->ConciseType == SQL_TYPE_TIMESTAMP) {
        char buf[128] = { 0 };
        if (!SQL_SUCCEEDED(ret = ConvertTimeStr2Str(Stmt, ctype, sqlCRec, dataPtr, length, buf, 128))){
          return ret;
        }
        memcpy(buff, buf, *length);
      } else {
        int i = 0, j = 0;
        buff[j++] = '\'';
        for (i = 0; i < *length; i++) {
          if (dataPtr[i] == '\'') {
            buff[j++] = dataPtr[i];
            buff[j++] = dataPtr[i];
          } else {
            buff[j++] = dataPtr[i];
          }
        }
        buff[j++] = '\'';
        *length = j;
      }
    } else {
      int i = 0, j = 0;
      buff[j++] = '\'';
      for (i = 0; i < *length; i++) {
        if (dataPtr[i] == '\'') {
          buff[j++] = dataPtr[i];
          buff[j++] = dataPtr[i];
        } else {
          buff[j++] = dataPtr[i];
        }
      }
      buff[j++] = '\'';
      *length = j;
    }
    break;
  }
  case SQL_C_WCHAR:
  {
#define MAX_BYTES_PER_UTF8_CP 4 /* max 4 bytes per utf8 codepoint */
    /* Convert SQL_C_WCHAR (utf-16 or utf-32) to utf-8. */
    int has_utf8_maxlen4 = 0;
    uint pbuf_max = *length * MAX_BYTES_PER_UTF8_CP;
    char *pbuf = NULL, *rst = NULL;

    /* length is in bytes, we want chars */
    *length = *length / sizeof(SQLWCHAR);

    pbuf = MADB_ALLOC(pbuf_max);
    if (pbuf==NULL){
      return MADB_SetError(&Stmt->Error, MADB_ERR_HY001, NULL, 0);
    }
    rst = (char*)sqlwchar_as_utf8_ext((SQLWCHAR*)dataPtr, length, (SQLCHAR*)pbuf, pbuf_max, &has_utf8_maxlen4);
    if (rst==NULL || buff_max < *length +2){
      MADB_FREE(pbuf);
      return MADB_SetError(&Stmt->Error, MADB_ERR_HY000, "MADB_ConvertType2Str fail.", 0);
    }

    {
      int i = 0, j = 0;
      buff[j++] = '\'';
      for (i = 0; i < *length; i++) {
        if (pbuf[i] == '\'') {
          buff[j++] = pbuf[i];
          buff[j++] = pbuf[i];
        } else {
          buff[j++] = pbuf[i];
        }
      }
      buff[j++] = '\'';
      *length = j;
    }

    MADB_FREE(pbuf);
    break;
  }
  case SQL_C_BIT:
  case SQL_C_TINYINT:
  case SQL_C_STINYINT:
    *length = Myll2str((long) * ((signed char *)dataPtr), buff, -10) - buff;
    break;
  case SQL_C_UTINYINT:
    *length = Myll2str((long) * ((unsigned char *)dataPtr), buff, 10) - buff;
    break;
  case SQL_C_SHORT:
  case SQL_C_SSHORT:
    *length = Myll2str((long) * ((short int *)dataPtr), buff, -10) - buff;
    break;
  case SQL_C_USHORT:
    *length = Myll2str((long) * ((unsigned short int *)dataPtr), buff, 10) - buff;
    break;
  case SQL_C_LONG:
  case SQL_C_SLONG:
    *length = Myll2str(*((SQLINTEGER*)dataPtr), buff, -10) - buff;
    break;
  case SQL_C_ULONG:
    *length = Myll2str(*((SQLUINTEGER*)dataPtr), buff, 10) - buff;
    break;
  case SQL_C_SBIGINT:
    *length = Myll2str(*((longlong*)dataPtr), buff, -10) - buff;
    break;
  case SQL_C_UBIGINT:
    *length = Myll2str(*((ulonglong*)dataPtr), buff, 10) - buff;
    break;
  case SQL_C_FLOAT:
    if (sqlCRec->ConciseType != SQL_NUMERIC && sqlCRec->ConciseType != SQL_DECIMAL)
    {
      if (IS_ORACLE_MODE(Stmt)){
        float tmp = *((float*)dataPtr);
        ma_gcvt2(tmp, 0, (int)buff_max, buff, NULL);
        //sprintf(buff, "%.17f", *((float*)dataPtr));
      } else {
        sprintf(buff, "%.17e", *((float*)dataPtr));
      }
    }
    else
    {
      /* We should perpare this data for string comparison */
      if (IS_ORACLE_MODE(Stmt)) {
        float tmp = *((float*)dataPtr);
        ma_gcvt2(tmp, 0, (int)buff_max, buff, NULL);
        //sprintf(buff, "%.15f", *((float*)dataPtr));
      } else {
        sprintf(buff, "%.15e", *((float*)dataPtr));
      }
    }
    *length = strlen(buff);
    break;
  case SQL_C_DOUBLE:
    if (sqlCRec->ConciseType != SQL_NUMERIC && sqlCRec->ConciseType != SQL_DECIMAL)
    {
      if (IS_ORACLE_MODE(Stmt)) {
        double tmp = *((double*)dataPtr);
        ma_gcvt2(tmp, 1, (int)buff_max, buff, NULL);
        //sprintf(buff, "%.17lf", *((double*)dataPtr));
      } else {
        sprintf(buff, "%.17e", *((double*)dataPtr));
      }
    }
    else
    {
      /* We should perpare this data for string comparison */
      if (IS_ORACLE_MODE(Stmt)) {
        double tmp = *((double*)dataPtr);
        ma_gcvt2(tmp, 1, (int)buff_max, buff, NULL);
        //sprintf(buff, "%.15lf", *((double*)dataPtr));
      } else {
        sprintf(buff, "%.15e", *((double*)dataPtr));
      }
    }
    *length = strlen(buff);
    break;
  case SQL_C_DATE:
  case SQL_C_TYPE_DATE:
  {
    char buf[128] = { 0 };
    DATE_STRUCT *date = (DATE_STRUCT*)dataPtr;
    if (!date->year && (date->month == date->day == 1))
    {
      *length = sprintf(buf, "0000-00-00");
    }
    else
    {
      *length = sprintf(buf, "%04d-%02d-%02d", date->year, date->month, date->day);
    }
    memset(buff, 0, buff_max);
    if (IS_ORACLE_MODE(Stmt)) {
      sprintf(buff, "DATE'%s'", buf);
    } else {
      sprintf(buff, "'%s'", buf);
    }
    *length = strlen(buff);
    break;
  }
  case SQL_C_TIME:
  case SQL_C_TYPE_TIME:
  {
    TIME_STRUCT *times = (TIME_STRUCT*)dataPtr;
    if (times->hour > 23)
    {
      return MADB_SetError(&Stmt->Error, MADB_ERR_22008, "Not a valid time value supplied", 0);
    }
    if (IS_ORACLE_MODE(Stmt)){
      time_t sec_time;
      struct tm * cur_tm;
      sec_time = time(NULL);
      cur_tm = localtime(&sec_time);

      *length = sprintf(buff, "TIMESTAMP'%04d-%02d-%02d %02d:%02d:%02d'", 
        1900 + cur_tm->tm_year, cur_tm->tm_mon + 1, cur_tm->tm_mday, times->hour, times->minute, times->second);
    } else {
      *length = sprintf(buff, "'%02d:%02d:%02d'", times->hour, times->minute, times->second);
    }
    break;
  }
  case SQL_C_TIMESTAMP:
  case SQL_C_TYPE_TIMESTAMP:
  {
    TIMESTAMP_STRUCT *time = (TIMESTAMP_STRUCT*)dataPtr;
    char buf[128] = { 0 };
    if (IS_ORACLE_MODE(Stmt)){
      if (time->year == 0 || time->month == 0 || time->day == 0) {
        return MADB_SetError(&Stmt->Error, MADB_ERR_22007, NULL, 0);
      }
    }
    
    if (!time->year && (time->month == time->day == 1))
    {
      *length = sprintf(buf, "0000-00-00 %02d:%02d:%02d", time->hour, time->minute, time->second);
    }
    else
    {
      *length = sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d", time->year, time->month, time->day, time->hour, time->minute, time->second);
    }
    if (time->fraction)
    {
      char *tmp_buf = buf + *length;
      /* Start cleaning from the end */
      int tmp_pos = 9;
      sprintf(tmp_buf, ".%09d", time->fraction);

      /*
        ODBC specification defines nanoseconds granularity for
        the fractional part of seconds. MySQL only supports
        microseconds for TIMESTAMP, TIME and DATETIME.

        We are trying to remove the trailing zeros because this
        does not really modify the data, but often helps to substitute
        9 digits with only 6.
      */
      while (tmp_pos && tmp_buf[tmp_pos] == '0')
      {
        tmp_buf[tmp_pos--] = 0;
      }
      *length += tmp_pos + 1;
    }
    memset(buff, 0, buff_max);
    if (IS_ORACLE_MODE(Stmt)){
      sprintf(buff, "TIMESTAMP'%s'", buf);
    } else {
      sprintf(buff, "'%s'", buf);
    }
    *length = strlen(buff);
    break;
  }
  case SQL_C_NUMERIC:
  {
    int ErrorCode = 0;
    SQL_NUMERIC_STRUCT *p = (SQL_NUMERIC_STRUCT *)dataPtr;
    p->scale = (SQLSCHAR)sqlCRec->Scale;
    p->precision = (SQLSCHAR)sqlCRec->Precision;
    *length = MADB_ConvertNumericToChar(p, buff, &ErrorCode);
    if (ErrorCode)
      return MADB_SetError(&Stmt->Error, ErrorCode, NULL, 0);
    break;
  }
  case SQL_C_INTERVAL_HOUR_TO_MINUTE:
  case SQL_C_INTERVAL_HOUR_TO_SECOND:
  {
    SQL_INTERVAL_STRUCT *interval = (SQL_INTERVAL_STRUCT*)dataPtr;
    if (ctype == SQL_C_INTERVAL_HOUR_TO_MINUTE)
    {
      *length = sprintf(buff, "'%d:%02d:00'", interval->intval.day_second.hour,interval->intval.day_second.minute);
    }
    else
    {
      *length = sprintf(buff, "'%d:%02d:%02d'", interval->intval.day_second.hour, 
        interval->intval.day_second.minute, interval->intval.day_second.second);
    }
    break;
  }
  /* If we are missing processing of some valid C type. Probably means a bug elsewhere */
  default:
    return MADB_SetError(&Stmt->Error, MADB_ERR_07006, "Conversion is not supported", 0);
  }
  return SQL_SUCCESS;
}
