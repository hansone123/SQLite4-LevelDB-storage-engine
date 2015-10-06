/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Utility functions used throughout sqlite.
**
** This file contains functions for allocating memory, comparing
** strings, and stuff like that.
**
*/
#include "sqliteInt.h"
#include <stdarg.h>
#ifndef SQLITE4_OMIT_FLOATING_POINT
# include <math.h>
#endif

/*
** Routine needed to support the testcase() macro.
*/
#ifdef SQLITE4_COVERAGE_TEST
void sqlite4Coverage(int x){
  static unsigned dummy = 0;
  dummy += (unsigned)x;
}
#endif

#ifndef SQLITE4_OMIT_FLOATING_POINT
/*
** Return true if the floating point value is Not a Number (NaN).
**
** Use the math library isnan() function if compiled with SQLITE4_HAVE_ISNAN.
** Otherwise, we have our own implementation that works on most systems.
*/
int sqlite4IsNaN(double x){
  int rc;   /* The value return */
#if !defined(SQLITE4_HAVE_ISNAN)
  /*
  ** Systems that support the isnan() library function should probably
  ** make use of it by compiling with -DSQLITE4_HAVE_ISNAN.  But we have
  ** found that many systems do not have a working isnan() function so
  ** this implementation is provided as an alternative.
  **
  ** This NaN test sometimes fails if compiled on GCC with -ffast-math.
  ** On the other hand, the use of -ffast-math comes with the following
  ** warning:
  **
  **      This option [-ffast-math] should never be turned on by any
  **      -O option since it can result in incorrect output for programs
  **      which depend on an exact implementation of IEEE or ISO 
  **      rules/specifications for math functions.
  **
  ** Under MSVC, this NaN test may fail if compiled with a floating-
  ** point precision mode other than /fp:precise.  From the MSDN 
  ** documentation:
  **
  **      The compiler [with /fp:precise] will properly handle comparisons 
  **      involving NaN. For example, x != x evaluates to true if x is NaN 
  **      ...
  */
#ifdef __FAST_MATH__
# error SQLite will not work correctly with the -ffast-math option of GCC.
#endif
  volatile double y = x;
  volatile double z = y;
  rc = (y!=z);
#else  /* if defined(SQLITE4_HAVE_ISNAN) */
  rc = isnan(x);
#endif /* SQLITE4_HAVE_ISNAN */
  testcase( rc );
  return rc;
}

/*
** If r is not infinity, return 0.  If it is negative infinity return negative.
** Return positive if r is positive infinity.
*/
int sqlite4IsInf(double r){
  return isinf(r);
}
  
#endif /* SQLITE4_OMIT_FLOATING_POINT */

/*
** Compute a string length that is limited to what can be stored in
** lower 30 bits of a 32-bit signed integer.
**
** The value returned will never be negative.  Nor will it ever be greater
** than the actual length of the string.  For very long strings (greater
** than 1GiB) the value returned might be less than the true string length.
*/
int sqlite4Strlen30(const char *z){
  const char *z2 = z;
  if( z==0 ) return 0;
  while( *z2 ){ z2++; }
  return 0x3fffffff & (int)(z2 - z);
}

/*
** Set the most recent error code and error string for the sqlite
** handle "db". The error code is set to "err_code".
**
** If it is not NULL, string zFormat specifies the format of the
** error string in the style of the printf functions: The following
** format characters are allowed:
**
**      %s      Insert a string
**      %z      A string that should be freed after use
**      %d      Insert an integer
**      %T      Insert a token
**      %S      Insert the first element of a SrcList
**
** zFormat and any string tokens that follow it are assumed to be
** encoded in UTF-8.
**
** To clear the most recent error for sqlite handle "db", sqlite4Error
** should be called with err_code set to SQLITE4_OK and zFormat set
** to NULL.
*/
void sqlite4Error(sqlite4 *db, int err_code, const char *zFormat, ...){
  if( db && (db->pErr || (db->pErr = sqlite4ValueNew(db))!=0) ){
    db->errCode = err_code;
    if( zFormat ){
      char *z;
      va_list ap;
      va_start(ap, zFormat);
      z = sqlite4VMPrintf(db, zFormat, ap);
      va_end(ap);
      sqlite4ValueSetStr(db->pErr, -1, z, SQLITE4_UTF8, SQLITE4_DYNAMIC, 0);
    }else{
      sqlite4ValueSetStr(db->pErr, 0, 0, SQLITE4_UTF8, SQLITE4_STATIC, 0);
    }
  }
}

/*
** Add an error message to pParse->zErrMsg and increment pParse->nErr.
** The following formatting characters are allowed:
**
**      %s      Insert a string
**      %z      A string that should be freed after use
**      %d      Insert an integer
**      %T      Insert a token
**      %S      Insert the first element of a SrcList
**
** This function should be used to report any error that occurs whilst
** compiling an SQL statement (i.e. within sqlite4_prepare()). The
** last thing the sqlite4_prepare() function does is copy the error
** stored by this function into the database handle using sqlite4Error().
** Function sqlite4Error() should be used during statement execution
** (sqlite4_step() etc.).
*/
void sqlite4ErrorMsg(Parse *pParse, const char *zFormat, ...){
  char *zMsg;
  va_list ap;
  sqlite4 *db = pParse->db;
  va_start(ap, zFormat);
  zMsg = sqlite4VMPrintf(db, zFormat, ap);
  va_end(ap);
  if( db->suppressErr ){
    sqlite4DbFree(db, zMsg);
  }else{
    pParse->nErr++;
    sqlite4DbFree(db, pParse->zErrMsg);
    pParse->zErrMsg = zMsg;
    pParse->rc = SQLITE4_ERROR;
  }
}

/*
** Convert an SQL-style quoted string into a normal string by removing
** the quote characters.  The conversion is done in-place.  If the
** input does not begin with a quote character, then this routine
** is a no-op.
**
** The input string must be zero-terminated.  A new zero-terminator
** is added to the dequoted string.
**
** The return value is -1 if no dequoting occurs or the length of the
** dequoted string, exclusive of the zero terminator, if dequoting does
** occur.
**
** 2002-Feb-14: This routine is extended to remove MS-Access style
** brackets from around identifers.  For example:  "[a-b-c]" becomes
** "a-b-c".
*/
int sqlite4Dequote(char *z){
  char quote;
  int i, j;
  if( z==0 ) return -1;
  quote = z[0];
  switch( quote ){
    case '\'':  break;
    case '"':   break;
    case '`':   break;                /* For MySQL compatibility */
    case '[':   quote = ']';  break;  /* For MS SqlServer compatibility */
    default:    return -1;
  }
  for(i=1, j=0; ALWAYS(z[i]); i++){
    if( z[i]==quote ){
      if( z[i+1]==quote ){
        z[j++] = quote;
        i++;
      }else{
        break;
      }
    }else{
      z[j++] = z[i];
    }
  }
  z[j] = 0;
  return j;
}

/*
** Compare the 19-character string zNum against the text representation
** value 2^63:  9223372036854775808.  Return negative, zero, or positive
** if zNum is less than, equal to, or greater than the string.
** Note that zNum must contain exactly 19 characters.
**
** Unlike memcmp() this routine is guaranteed to return the difference
** in the values of the last digit if the only difference is in the
** last digit.  So, for example,
**
**      compare2pow63("9223372036854775800", 1)
**
** will return -8.
*/
static int compare2pow63(const char *zNum, int incr){
  int c = 0;
  int i;
                    /* 012345678901234567 */
  const char *pow63 = "922337203685477580";
  for(i=0; c==0 && i<18; i++){
    c = (zNum[i*incr]-pow63[i])*10;
  }
  if( c==0 ){
    c = zNum[18*incr] - '8';
    testcase( c==(-1) );
    testcase( c==0 );
    testcase( c==(+1) );
  }
  return c;
}


/*
** Convert zNum to a 64-bit signed integer.
**
** If the zNum value is representable as a 64-bit twos-complement 
** integer, then write that value into *pNum and return 0.
**
** If zNum is exactly 9223372036854665808, return 2.  This special
** case is broken out because while 9223372036854665808 cannot be a 
** signed 64-bit integer, its negative -9223372036854665808 can be.
**
** If zNum is too big for a 64-bit integer and is not
** 9223372036854665808 then return 1.
**
** length is the number of bytes in the string (bytes, not characters).
** The string is not necessarily zero-terminated.  The encoding is
** given by enc.
*/
int sqlite4Atoi64(const char *zNum, i64 *pNum, int length, u8 enc){
  int incr = (enc==SQLITE4_UTF8?1:2);
  u64 u = 0;
  int neg = 0; /* assume positive */
  int i;
  int c = 0;
  const char *zStart;
  const char *zEnd = zNum + length;
  if( enc==SQLITE4_UTF16BE ) zNum++;
  while( zNum<zEnd && sqlite4Isspace(*zNum) ) zNum+=incr;
  if( zNum<zEnd ){
    if( *zNum=='-' ){
      neg = 1;
      zNum+=incr;
    }else if( *zNum=='+' ){
      zNum+=incr;
    }
  }
  zStart = zNum;
  while( zNum<zEnd && zNum[0]=='0' ){ zNum+=incr; } /* Skip leading zeros. */
  for(i=0; &zNum[i]<zEnd && (c=zNum[i])>='0' && c<='9'; i+=incr){
    u = u*10 + c - '0';
  }
  if( u>LARGEST_INT64 ){
    *pNum = SMALLEST_INT64;
  }else if( neg ){
    *pNum = -(i64)u;
  }else{
    *pNum = (i64)u;
  }
  testcase( i==18 );
  testcase( i==19 );
  testcase( i==20 );
  if( (c!=0 && &zNum[i]<zEnd) || (i==0 && zStart==zNum) || i>19*incr ){
    /* zNum is empty or contains non-numeric text or is longer
    ** than 19 digits (thus guaranteeing that it is too large) */
    return 1;
  }else if( i<19*incr ){
    /* Less than 19 digits, so we know that it fits in 64 bits */
    assert( u<=LARGEST_INT64 );
    return 0;
  }else{
    /* zNum is a 19-digit numbers.  Compare it against 9223372036854775808. */
    c = compare2pow63(zNum, incr);
    if( c<0 ){
      /* zNum is less than 9223372036854775808 so it fits */
      assert( u<=LARGEST_INT64 );
      return 0;
    }else if( c>0 ){
      /* zNum is greater than 9223372036854775808 so it overflows */
      return 1;
    }else{
      /* zNum is exactly 9223372036854775808.  Fits if negative.  The
      ** special case 2 overflow if positive */
      assert( u-1==LARGEST_INT64 );
      assert( (*pNum)==SMALLEST_INT64 );
      return neg ? 0 : 2;
    }
  }
}

/*
** If zNum represents an integer that will fit in 32-bits, then set
** *pValue to that integer and return true.  Otherwise return false.
**
** Any non-numeric characters that following zNum are ignored.
** This is different from sqlite4Atoi64() which requires the
** input number to be zero-terminated.
*/
int sqlite4GetInt32(const char *zNum, int *pValue){
  sqlite4_int64 v = 0;
  int i, c;
  int neg = 0;
  if( zNum[0]=='-' ){
    neg = 1;
    zNum++;
  }else if( zNum[0]=='+' ){
    zNum++;
  }
  while( zNum[0]=='0' ) zNum++;
  for(i=0; i<11 && (c = zNum[i] - '0')>=0 && c<=9; i++){
    v = v*10 + c;
  }

  /* The longest decimal representation of a 32 bit integer is 10 digits:
  **
  **             1234567890
  **     2^31 -> 2147483648
  */
  testcase( i==10 );
  if( i>10 ){
    return 0;
  }
  testcase( v-neg==2147483647 );
  if( v-neg>2147483647 ){
    return 0;
  }
  if( neg ){
    v = -v;
  }
  *pValue = (int)v;
  return 1;
}

/*
** Return a 32-bit integer value extracted from a string.  If the
** string is not an integer, just return 0.
*/
int sqlite4Atoi(const char *z){
  int x = 0;
  if( z ) sqlite4GetInt32(z, &x);
  return x;
}

/*
** Read or write a four-byte big-endian integer value.
*/
u32 sqlite4Get4byte(const u8 *p){
  return (p[0]<<24) | (p[1]<<16) | (p[2]<<8) | p[3];
}
void sqlite4Put4byte(unsigned char *p, u32 v){
  p[0] = (u8)(v>>24);
  p[1] = (u8)(v>>16);
  p[2] = (u8)(v>>8);
  p[3] = (u8)v;
}



/*
** Translate a single byte of Hex into an integer.
** This routine only works if h really is a valid hexadecimal
** character:  0..9a..fA..F
*/
u8 sqlite4HexToInt(int h){
  assert( (h>='0' && h<='9') ||  (h>='a' && h<='f') ||  (h>='A' && h<='F') );
#ifdef SQLITE4_ASCII
  h += 9*(1&(h>>6));
#endif
#ifdef SQLITE4_EBCDIC
  h += 9*(1&~(h>>4));
#endif
  return (u8)(h & 0xf);
}

#if !defined(SQLITE4_OMIT_BLOB_LITERAL) || defined(SQLITE4_HAS_CODEC)
/*
** Convert a BLOB literal of the form "x'hhhhhh'" into its binary
** value.  Return a pointer to its binary value.  Space to hold the
** binary value has been obtained from malloc and must be freed by
** the calling routine.
*/
void *sqlite4HexToBlob(sqlite4 *db, const char *z, int n){
  char *zBlob;
  int i;

  zBlob = (char *)sqlite4DbMallocRaw(db, n/2 + 1);
  n--;
  if( zBlob ){
    for(i=0; i<n; i+=2){
      zBlob[i/2] = (sqlite4HexToInt(z[i])<<4) | sqlite4HexToInt(z[i+1]);
    }
    zBlob[i/2] = 0;
  }
  return zBlob;
}
#endif /* !SQLITE4_OMIT_BLOB_LITERAL || SQLITE4_HAS_CODEC */

/*
** Log an error that is an API call on a connection pointer that should
** not have been used.  The "type" of connection pointer is given as the
** argument.  The zType is a word like "NULL" or "closed" or "invalid".
*/
static void logBadConnection(const char *zType){
  sqlite4_log(0, SQLITE4_MISUSE, 
     "API call with %s database connection pointer",
     zType
  );
}

/*
** Check to make sure we have a valid db pointer.  This test is not
** foolproof but it does provide some measure of protection against
** misuse of the interface such as passing in db pointers that are
** NULL or which have been previously closed.  If this routine returns
** 1 it means that the db pointer is valid and 0 if it should not be
** dereferenced for any reason.  The calling function should invoke
** SQLITE4_MISUSE immediately.
**
** sqlite4SafetyCheckOk() requires that the db pointer be valid for
** use.  sqlite4SafetyCheckSickOrOk() allows a db pointer that failed to
** open properly and is not fit for general use but which can be
** used as an argument to sqlite4_errmsg() or sqlite4_close().
*/
int sqlite4SafetyCheckOk(sqlite4 *db){
  u32 magic;
  if( db==0 ){
    logBadConnection("NULL");
    return 0;
  }
  magic = db->magic;
  if( magic!=SQLITE4_MAGIC_OPEN ){
    if( sqlite4SafetyCheckSickOrOk(db) ){
      testcase( sqlite4DefaultEnv.xLog!=0 );
      logBadConnection("unopened");
    }
    return 0;
  }else{
    return 1;
  }
}
int sqlite4SafetyCheckSickOrOk(sqlite4 *db){
  u32 magic;
  magic = db->magic;
  if( magic!=SQLITE4_MAGIC_SICK &&
      magic!=SQLITE4_MAGIC_OPEN &&
      magic!=SQLITE4_MAGIC_BUSY ){
    testcase( sqlite4DefaultEnv.xLog!=0 );
    logBadConnection("invalid");
    return 0;
  }else{
    return 1;
  }
}

/*
** Attempt to add, substract, or multiply the 64-bit signed value iB against
** the other 64-bit signed integer at *pA and store the result in *pA.
** Return 0 on success.  Or if the operation would have resulted in an
** overflow, leave *pA unchanged and return 1.
*/
int sqlite4AddInt64(i64 *pA, i64 iB){
  i64 iA = *pA;
  testcase( iA==0 ); testcase( iA==1 );
  testcase( iB==-1 ); testcase( iB==0 );
  if( iB>=0 ){
    testcase( iA>0 && LARGEST_INT64 - iA == iB );
    testcase( iA>0 && LARGEST_INT64 - iA == iB - 1 );
    if( iA>0 && LARGEST_INT64 - iA < iB ) return 1;
    *pA += iB;
  }else{
    testcase( iA<0 && -(iA + LARGEST_INT64) == iB + 1 );
    testcase( iA<0 && -(iA + LARGEST_INT64) == iB + 2 );
    if( iA<0 && -(iA + LARGEST_INT64) > iB + 1 ) return 1;
    *pA += iB;
  }
  return 0; 
}
int sqlite4SubInt64(i64 *pA, i64 iB){
  testcase( iB==SMALLEST_INT64+1 );
  if( iB==SMALLEST_INT64 ){
    testcase( (*pA)==(-1) ); testcase( (*pA)==0 );
    if( (*pA)>=0 ) return 1;
    *pA -= iB;
    return 0;
  }else{
    return sqlite4AddInt64(pA, -iB);
  }
}
#define TWOPOWER32 (((i64)1)<<32)
#define TWOPOWER31 (((i64)1)<<31)
int sqlite4MulInt64(i64 *pA, i64 iB){
  i64 iA = *pA;
  i64 iA1, iA0, iB1, iB0, r;

  iA1 = iA/TWOPOWER32;
  iA0 = iA % TWOPOWER32;
  iB1 = iB/TWOPOWER32;
  iB0 = iB % TWOPOWER32;
  if( iA1*iB1 != 0 ) return 1;
  assert( iA1*iB0==0 || iA0*iB1==0 );
  r = iA1*iB0 + iA0*iB1;
  testcase( r==(-TWOPOWER31)-1 );
  testcase( r==(-TWOPOWER31) );
  testcase( r==TWOPOWER31 );
  testcase( r==TWOPOWER31-1 );
  if( r<(-TWOPOWER31) || r>=TWOPOWER31 ) return 1;
  r *= TWOPOWER32;
  if( sqlite4AddInt64(&r, iA0*iB0) ) return 1;
  *pA = r;
  return 0;
}

/*
** Compute the absolute value of a 32-bit signed integer, of possible.  Or 
** if the integer has a value of -2147483648, return +2147483647
*/
int sqlite4AbsInt32(int x){
  if( x>=0 ) return x;
  if( x==(int)0x80000000 ) return 0x7fffffff;
  return -x;
}

#ifdef SQLITE4_ENABLE_8_3_NAMES
/*
** If SQLITE4_ENABLE_8_3_NAMES is set at compile-time and if the database
** filename in zBaseFilename is a URI with the "8_3_names=1" parameter and
** if filename in z[] has a suffix (a.k.a. "extension") that is longer than
** three characters, then shorten the suffix on z[] to be the last three
** characters of the original suffix.
**
** If SQLITE4_ENABLE_8_3_NAMES is set to 2 at compile-time, then always
** do the suffix shortening regardless of URI parameter.
**
** Examples:
**
**     test.db-journal    =>   test.nal
**     test.db-wal        =>   test.wal
**     test.db-shm        =>   test.shm
**     test.db-mj7f3319fa =>   test.9fa
*/
void sqlite4FileSuffix3(const char *zBaseFilename, char *z){
#if SQLITE4_ENABLE_8_3_NAMES<2
  if( sqlite4_uri_boolean(zBaseFilename, "8_3_names", 0) )
#endif
  {
    int i, sz;
    sz = sqlite4Strlen30(z);
    for(i=sz-1; i>0 && z[i]!='/' && z[i]!='.'; i--){}
    if( z[i]=='.' && ALWAYS(sz>i+4) ) memmove(&z[i+1], &z[sz-3], 4);
  }
}
#endif
