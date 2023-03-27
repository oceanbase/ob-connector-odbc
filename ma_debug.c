/************************************************************************************
   Copyright (C) 2013, 2015 MariaDB Corporation AB
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
#include <wchar.h>

#define MAX_FILE_NAME_LEN 256
static const long long int max_log_file_sz = 104857600; // 100MB

extern char LogFile[];
extern char LogEnable[];

my_bool ma_debug_enable(void *Dbc)
{
  static int isLogEnable = -1;
  my_bool rst = FALSE;
#define MA_DEBUG_FLAG 4

  rst = ((Dbc) && (((MADB_Dbc*)(Dbc))->Options & MA_DEBUG_FLAG)) ? TRUE : FALSE;
  if (isLogEnable == -1) {
    if (strncasecmp(LogEnable, "true", 4) == 0) {
      isLogEnable = 1;
    } else if (strncasecmp(LogEnable, "false", 5) == 0) {
      isLogEnable = 0;
    } else {
      isLogEnable = 2;
    }
  }
  if (isLogEnable == 2) {
    return rst;
  } else {
    return isLogEnable;
  }
}

void get_time(char *time_str, size_t time_str_size, const char *fmt)
{
  size_t len;
  time_t ti = time(NULL);
  struct tm* local_time = localtime(&ti);

  strftime(time_str, time_str_size, fmt, local_time);
  //add us to time_str
  /*len = strlen(time_str);
  if (time_str_size > 20 && time_str_size >= len + 7) {
    char *tmp = time_str + len;
    snprintf(tmp, (time_str_size - len), ".%03ld", local_time->tm_sec);
  }*/
}

void* get_open_file(char* current_file_name) {
  FILE* fp = NULL;
  int repeat_num = 0;
  struct stat log_file_stat;

  while (NULL == fp) {
    fp = fopen(current_file_name, "a+");
    if (NULL == fp) {
      fprintf(stderr, "open log file %s failed\n", current_file_name);
      return NULL;
    }

    if (0 == repeat_num
      && 0 == stat(current_file_name, &log_file_stat)
      && log_file_stat.st_size >= max_log_file_sz) {
      char new_file_name[MAX_FILE_NAME_LEN];
      char time_str[MAX_FILE_NAME_LEN] = { 0 };
      get_time(time_str, MAX_FILE_NAME_LEN, "%Y%m%d%H%M%S");
      snprintf(new_file_name, MAX_FILE_NAME_LEN, "%s.%s", current_file_name, time_str);

      fclose(fp);
      fp = NULL;
      if (rename(current_file_name, new_file_name)) {
        fprintf(stderr, "rename log file %s,%s failed\n", current_file_name, new_file_name);
        repeat_num++;
      }
    }
  }
  return fp;
}

void ma_debug_print(my_bool ident, char *format, ...)
{
  FILE *fp = get_open_file(LogFile);
  if (fp) {
    va_list va;
    va_start(va, format);
    if (ident)
      fprintf(fp, "\t");
    vfprintf(fp, format, va);
    fprintf(fp, "\n");
    va_end(va);
    fclose(fp);
  }
}

void ma_debug_printw(wchar_t *format, ...)
{
  FILE *fp= get_open_file(LogFile);
  if (fp) {
    va_list va;
    va_start(va, format);
    fwprintf(fp, format, va);
    fwprintf(fp, L"\n");
    va_end(va);
    fclose(fp);
  }
}

void ma_debug_printv(char *format, va_list args)
{
  FILE *fp= get_open_file(LogFile);
  if (fp) {
    vfprintf(fp, format, args);
    fclose(fp);
  }
}


void ma_debug_print_error(MADB_Error *err)
{
 /*TODO: Make it without #ifdefs */
#ifdef _WIN32
  SYSTEMTIME st;

  GetSystemTime(&st);
  ma_debug_print(1, "%d-%02d-%02d %02d:%02d:%02d [%s](%u)%s", st.wYear, st.wMonth, st.wDay, st.wHour, st.wMinute, st.wSecond, err->SqlState, err->NativeError, err->SqlErrorMsg);
#else
  time_t t = time(NULL);\
  struct tm st = *gmtime(&t);\
  ma_debug_print(1, "%d-%02d-%02d %02d:%02d:%02d [%s](%u)%s", st.tm_year + 1900, st.tm_mon + 1, st.tm_mday, st.tm_hour, st.tm_min, st.tm_sec, err->SqlState, err->NativeError, err->SqlErrorMsg);
#endif
}


void ma_print_value(SQLSMALLINT OdbcType, SQLPOINTER Value, SQLLEN octets)
{
  if (Value == 0)
  {
    ma_debug_print(1, "NULL ptr");
  }
  if (octets <= 0)
  {
    octets= 1;
  }
  switch (OdbcType)
  {
    case SQL_C_BIT:
    case SQL_C_TINYINT:
    case SQL_C_STINYINT:
    case SQL_C_UTINYINT:
      ma_debug_print(1, "%d", 0 + *((char*)Value));
      break;
    case SQL_C_SHORT:
    case SQL_C_SSHORT:
    case SQL_C_USHORT:
      ma_debug_print(1, "%d", 0 + *((short int*)Value));
      break;
    case SQL_C_LONG:
    case SQL_C_SLONG:
    case SQL_C_ULONG:
      ma_debug_print(1, "%d", 0 + *((int*)Value));
      break;
    case SQL_C_UBIGINT:
    case SQL_C_SBIGINT:
      ma_debug_print(1, "%ll", 0 + *((long long*)Value));
      break;
    case SQL_C_DOUBLE:
      ma_debug_print(1, "%f", 0.0 + *((SQLDOUBLE*)Value));
      break;
    case SQL_C_FLOAT:
      ma_debug_print(1, "%f", 0.0 + *((SQLFLOAT*)Value));
      break;
    case SQL_C_NUMERIC:
      ma_debug_print(1, "%s", "[numeric struct]");
      break;
    case SQL_C_TYPE_TIME:
    case SQL_C_TIME:
      ma_debug_print(1, "%02d:02d:02d", ((SQL_TIME_STRUCT*)Value)->hour, ((SQL_TIME_STRUCT*)Value)->minute, ((SQL_TIME_STRUCT*)Value)->second);
      break;
    case SQL_C_TYPE_DATE:
    case SQL_C_DATE:
      ma_debug_print(1, "%4d-02d-02d", ((SQL_DATE_STRUCT*)Value)->year, ((SQL_DATE_STRUCT*)Value)->month, ((SQL_DATE_STRUCT*)Value)->day);
      break;
    case SQL_C_TYPE_TIMESTAMP:
    case SQL_C_TIMESTAMP:
      ma_debug_print(1, "%4d-02d-02d %02d:02d:02d", ((SQL_TIMESTAMP_STRUCT*)Value)->year, ((SQL_TIMESTAMP_STRUCT*)Value)->month,
        ((SQL_TIMESTAMP_STRUCT*)Value)->day, ((SQL_TIMESTAMP_STRUCT*)Value)->hour, ((SQL_TIMESTAMP_STRUCT*)Value)->minute, ((SQL_TIMESTAMP_STRUCT*)Value)->second);
      break;
    case SQL_C_CHAR:
      ma_debug_print(1, "%*s%s", MIN(10, octets), (char*)Value, octets > 10 ? "..." : "");
      break;
    default:
      ma_debug_print(1, "%*X%s", MIN(10, octets), (char*)Value, octets > 10 ? "..." : "");
      break;
  }
}

/* #ifdef __APPLE__
void TravisTrace(char *format, va_list args)
{
  BOOL Travis= FALSE;
  Travis= getenv("TRAVIS") != NULL;

  if (Travis != FALSE)
  {
    printf("#");
    vprintf(format, args);
  }

}
#endif */
