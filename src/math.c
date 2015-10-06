/*
** 2011 November 21
**
** The authors renounce all claim of copyright to this code and dedicate
** this code to the public domain.  In place of legal notice, here is
** a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** Routines for doing math operations on sqlite4_num objects.
*/
#include "sqliteInt.h"

#define SQLITE4_MX_EXP   999    /* Maximum exponent */
#define SQLITE4_NAN_EXP 2000    /* Exponent to use for NaN */

/*
** 1/10th the maximum value of an unsigned 64-bit integer
*/
#define TENTH_MAX (LARGEST_UINT64/10)

/*
** Adjust the significand and exponent of pA and pB so that the
** exponent is the same.
*/
static void adjustExponent(sqlite4_num *pA, sqlite4_num *pB){
  if( pA->e<pB->e ){
    sqlite4_num *t = pA;
    pA = pB;
    pB = t;
  }
  if( pB->m==0 ){
    pB->e = pA->e;
    return;
  }
  if( pA->m==0 ){
    pA->e = pB->e;
    return;
  }
  if( pA->e > pB->e+40 ){
    pB->approx = 1;
    pB->e = pA->e;
    pB->m = 0;
    return;
  }
  while( pA->e>pB->e && pB->m%10==0  ){
    pB->m /= 10;
    pB->e++;
  }
  while( pA->e>pB->e && pA->m<=TENTH_MAX ){
    pA->m *= 10;
    pA->e--;
  }
  while( pA->e>pB->e ){
    pB->m /= 10;
    pB->e++;
    pB->approx = 1;
  }
}

/*
** Add two numbers and return the result.
*/
sqlite4_num sqlite4_num_add(sqlite4_num A, sqlite4_num B){
  sqlite4_uint64 r;
  if( A.sign!=B.sign ){
    if( A.sign ){
      A.sign = 0;
      return sqlite4_num_sub(B,A);
    }else{
      B.sign = 0;
      return sqlite4_num_sub(A,B);
    }
  }
  if( A.e>SQLITE4_MX_EXP ){
    if( B.e>SQLITE4_MX_EXP && B.m==0 ) return B;
    return A;
  }
  if( B.e>SQLITE4_MX_EXP ){
    return B;
  }
  adjustExponent(&A, &B);
  r = A.m+B.m;
  A.approx |= B.approx;
  if( r>=A.m ){
    A.m = r;
  }else{
    if( A.approx==0 && (A.m%10)!=0 ) A.approx = 1;
    A.m /= 10;
    A.e++;
    if( A.e>SQLITE4_MX_EXP ) return A;
    if( A.approx==0 && (B.m%10)!=0 ) A.approx = 1;
    A.m += B.m/10;
  }
  return A;
}

/*
** Subtract the second number from the first and return the result.
*/
sqlite4_num sqlite4_num_sub(sqlite4_num A, sqlite4_num B){
  if( A.sign!=B.sign ){
    B.sign = A.sign;
    return sqlite4_num_add(A,B);
  }
  if( A.e>SQLITE4_MX_EXP || B.e>SQLITE4_MX_EXP ){
    A.e = SQLITE4_NAN_EXP;
    A.m = 0;
    return A;
  }
  adjustExponent(&A, &B);
  if( B.m > A.m ){
    sqlite4_num t = A;
    A = B;
    B = t;
    A.sign = 1-A.sign;
  }
  A.m -= B.m;
  A.approx |= B.approx;
  return A;
}

/*
** Return true if multiplying x and y will cause 64-bit unsigned overflow.
*/
static int multWillOverflow(sqlite4_uint64 x, sqlite4_uint64 y){
  sqlite4_uint64 xHi, xLo, yHi, yLo;
  xHi = x>>32;
  yHi = y>>32;
  if( xHi*yHi ) return 1;
  xLo = x & 0xffffffff;
  yLo = y & 0xffffffff;
  if( (xHi*yLo + yHi*xLo + (xLo*yLo>>32))>0xffffffff ) return 1;
  return 0;
}

/*
** Multiply two numbers and return the result.
*/
sqlite4_num sqlite4_num_mul(sqlite4_num A, sqlite4_num B){
  sqlite4_num r;

  if( A.e>SQLITE4_MX_EXP || B.e>SQLITE4_MX_EXP ){
    r.sign = A.sign ^ B.sign;
    r.m = (A.m && B.m) ? 1 : 0;
    r.e = SQLITE4_MX_EXP+1;
    r.approx = 0;
    return r;
  }
  if( A.m==0 ) return A;
  if( B.m==0 ) return B;
  while( A.m%10==0 ){ A.m /= 10; A.e++; }
  while( B.m%10==0 ){ B.m /= 10; B.e++; }
  while( A.m%5==0 && B.m%2==0 ){ A.m /= 5; A.e++; B.m /= 2; }
  while( B.m%5==0 && A.m%2==0 ){ B.m /= 5; B.e++; A.m /= 2; }
  r.sign = A.sign ^ B.sign;
  r.approx = A.approx | B.approx;
  while( multWillOverflow(A.m, B.m) ){
    r.approx = 1;
    if( A.m>B.m ){
      A.m /= 10;
      A.e++;
    }else{
      B.m /= 10;
      B.e++;
    }
  }
  r.m = A.m*B.m;
  r.e = A.e + B.e;
  return r;
}

/*
** Divide two numbers and return the result.
*/
sqlite4_num sqlite4_num_div(sqlite4_num A, sqlite4_num B){
  sqlite4_num r;
  if( A.e>SQLITE4_MX_EXP ){
    A.m = 0;
    return A;
  }
  if( B.e>SQLITE4_MX_EXP ){
    if( B.m!=0 ){
      r.m = 0;
      r.e = 0;
      r.sign = 0;
      r.approx = 1;
      return r;
    }
    return B;
  }
  if( B.m==0 ){
    r.sign = A.sign ^ B.sign;
    r.e = SQLITE4_NAN_EXP;
    r.m = 0;
    r.approx = 1;
    return r;
  }
  if( A.m==0 ){
    return A;
  }
  while( A.m<TENTH_MAX ){
    A.m *= 10;
    A.e--;
  }
  while( B.m%10==0 ){
    B.m /= 10;
    B.e++;
  }
  r.sign = A.sign ^ B.sign;
  r.approx = A.approx | B.approx;
  if( r.approx==0 && A.m%B.m!=0 ) r.approx = 1;
  r.m = A.m/B.m;
  r.e = A.e - B.e;
  return r;
}

/*
** Test if A is infinite.
*/
int sqlite4_num_isinf(sqlite4_num A){
  return A.e>SQLITE4_MX_EXP && A.m!=0;
}

/*
** Test if A is NaN.
*/
int sqlite4_num_isnan(sqlite4_num A){
  return A.e>SQLITE4_MX_EXP && A.m==0; 
}

/*
** Compare numbers A and B.  Return:
**
**    1     if A<B
**    2     if A==B
**    3     if A>B
**    0     the values are not comparible.
**
** NaN values are always incompariable.  Also +inf returns 0 when 
** compared with +inf and -inf returns 0 when compared with -inf.
*/
int sqlite4_num_compare(sqlite4_num A, sqlite4_num B){
  if( A.e>SQLITE4_MX_EXP ){
    if( A.m==0 ) return 0;
    if( B.e>SQLITE4_MX_EXP ){
      if( B.m==0 ) return 0;
      if( B.sign==A.sign ) return 0;
    }
    return A.sign ? 1 : 3;
  }
  if( B.e>SQLITE4_MX_EXP ){
    if( B.m==0 ) return 0;
    return B.sign ? 3 : 1;
  }
  if( A.sign!=B.sign ){
    if ( A.m==0 && B.m==0 ) return 2;
    return A.sign ? 1 : 3;
  }
  adjustExponent(&A, &B);
  if( A.sign ){
    sqlite4_num t = A;
    A = B;
    B = t;
  }
  if( A.e!=B.e ){
    return A.e<B.e ? 1 : 3;
  }
  if( A.m!=B.m ){
    return A.m<B.m ? 1 : 3;
  }
  return 2;
}

/*
** Round the value so that it has at most N digits to the right of the
** decimal point.
*/
sqlite4_num sqlite4_num_round(sqlite4_num x, int N){
  if( N<0 ) N = 0;
  if( x.e >= -N ) return x;
  if( x.e < -(N+30) ){
    memset(&x, 0, sizeof(x));
    return x;
  }
  while( x.e < -(N+1) ){
    x.m /= 10;
    x.e++;
  }
  x.m = (x.m+5)/10;
  x.e++;
  return x;
}

/*
** Convert text into a number and return that number.
**
** When converting from UTF16, this routine only looks at the
** least significant byte of each character.  It is assumed that
** the most significant byte of every character in the string
** is 0.  If that assumption is violated, then this routine can
** yield an anomalous result.  If the most significant byte of
** the final character is beyond the nIn examined bytes, then 
** it is treated as 0.
**
** Conversion stops at the first \000 character.  At most nIn bytes
** of zIn are examined.  Or if nIn is negative, up to a billion bytes
** are scanned, which we assume is more than will be found in any valid
** numeric string.
**
** If the value does not contain a decimal point or exponent, and is
** within the range of a signed 64-bit integer, it is guaranteed that
** the exponent of the returned value is zero.
*/
sqlite4_num sqlite4_num_from_text(
  const char *zIn,                /* Pointer to text to parse */
  int nIn,                        /* Size of zIn in bytes or (-ve) */
  unsigned flags,                 /* Conversion flags */
  int *pbReal                     /* OUT: True if text looks like a real */
){
  /* Return this value (NaN) if a parse error occurs. */
  static const sqlite4_num error_value = {0, 0, SQLITE4_MX_EXP+1, 0};

  static const i64 L10 = (LARGEST_INT64 / 10);
  int aMaxFinal[2] = {7, 8};
  static int one = 1;             /* Used to test machine endianness */
  int bRnd = 1;                   /* If mantissa overflows, round it */
  int bReal = 0;                  /* If text looks like a real */
  int seenRadix = 0;              /* True after decimal point has been parsed */
  int seenDigit = 0;              /* True after first non-zero digit parsed */
  int incr = 1;                   /* 1 for utf-8, 2 for utf-16 */
  sqlite4_num r;                  /* Value to return */
  char c;
  int i;

  assert( L10==922337203685477580 );
  
  memset(&r, 0, sizeof(r));
  if( nIn<0 ) nIn = 1000000000;
  c = flags & 0xf;
  if( c==0 || c==SQLITE4_UTF8 ){
    incr = 1;
  }else{
    if( c==SQLITE4_UTF16 ){ c = (3 - *(char*)&one); }
    assert( c==SQLITE4_UTF16LE || c==SQLITE4_UTF16BE );
    incr = 2;
    if( c==SQLITE4_UTF16BE ){
      zIn += 1;
      nIn -= 1;
    }
  }
  
  /* If the IGNORE_WHITESPACE flag is set, ignore any leading whitespace. */
  i = 0;
  if( flags & SQLITE4_IGNORE_WHITESPACE ){
    while( i<nIn && sqlite4Isspace(zIn[i]) ) i+=incr;
  }
  if( nIn<=i ) return error_value;

  /* Check for a leading '+' or '-' symbol. */
  if( zIn[i]=='-' ){
    r.sign = 1;
    i += incr;
  }else if( zIn[i]=='+' ){
    i += incr;
  }else if( flags & SQLITE4_NEGATIVE ){
    r.sign = 1;
  }
  if( nIn<=i ) return error_value;

  /* Check for the string "inf". This is a special case. */
  if( (flags & SQLITE4_INTEGER_ONLY)==0 
   && (nIn-i)>=incr*3
   && ((c=zIn[i])=='i' || c=='I')
   && ((c=zIn[i+incr])=='n' || c=='N')
   && ((c=zIn[i+incr*2])=='f' || c=='F')
  ){
    r.e = SQLITE4_MX_EXP+1;
    r.m = 1;
    bReal = 1;
    i += incr*3;
    goto finished;
  }

  for( ; i<nIn && (c = zIn[i])!=0; i+=incr){
    if( c>='0' && c<='9' ){
      int iDigit = (c - '0');

      if( iDigit==0 && seenDigit==0 ){
        /* Handle leading zeroes. If they occur to the right of the decimal
        ** point they can just be ignored. Otherwise, decrease the exponent
        ** by one.  */
        if( seenRadix ) r.e--;
        continue;
      }

      seenDigit = 1;
      if( r.e>0 || r.m>L10 || (r.m==L10 && iDigit>aMaxFinal[r.sign]) ){
        /* Mantissa overflow. */
        if( seenRadix==0 ) r.e++;
        if( iDigit!=0 ){ r.approx = 1; }
        if( bRnd ){
          if( iDigit>5 && r.m<((u64)LARGEST_INT64 + r.sign)) r.m++;
          bRnd = 0;
        }
        bReal = 1;
      }else{
        if( seenRadix ) r.e -= 1;
        r.m = (r.m*10) + iDigit;
      }

    }else{
      if( flags & SQLITE4_INTEGER_ONLY ) goto finished;

      if( c=='.' ){
        /* Permit only a single radix in each number */
        if( seenRadix ) goto finished;
        seenRadix = 1;
        bReal = 1;
      }else if( c=='e' || c=='E' ){
        int f = (flags & (SQLITE4_PREFIX_ONLY|SQLITE4_IGNORE_WHITESPACE));
        sqlite4_num exp;
        if( incr==2 ) f |= SQLITE4_UTF16LE; 
        if( (i+incr)>=nIn ) goto finished;
        i += incr;
        exp = sqlite4_num_from_text(&zIn[i], nIn-i, f, 0);
        if( sqlite4_num_isnan(exp) ) goto finished;
        if( exp.e || exp.m>999 ) goto finished;
        bReal = 1;
        r.e += (int)(exp.m) * (exp.sign ? -1 : 1);
        i = nIn;
        break;
      }else{
        goto finished;
      }
    }
  }

finished:

  /* Check for a parse error. If one has occurred, set the return value
  ** to NaN.  */
  if( (flags & SQLITE4_PREFIX_ONLY)==0 && i<nIn && zIn[i] ){
    if( flags & SQLITE4_IGNORE_WHITESPACE ){
      while( i<nIn && sqlite4Isspace(zIn[i]) ) i += incr;
    }
    if( i<nIn && zIn[i] ){
      r.e = SQLITE4_MX_EXP+1;
      r.m = 0;
    }
  }


  if( pbReal ) *pbReal = bReal;
  return r;
}

/*
** Convert an sqlite4_int64 to a number and return that number.
*/
sqlite4_num sqlite4_num_from_int64(sqlite4_int64 n){
  sqlite4_num r;
  r.approx = 0;
  r.e = 0;
  r.sign = n < 0;
  if( n>=0 ){
    r.m = n;
  }else if( n!=SMALLEST_INT64 ){
    r.m = -n;
  }else{
    r.m = 1+(u64)LARGEST_INT64;
  }
  return r;
}

/*
** Return an sqlite4_num containing a value as close as possible to the
** double value passed as the only argument.
**
** TODO: This is an inefficient placeholder implementation only.
*/
sqlite4_num sqlite4_num_from_double(double d){
  const double large = (double)LARGEST_UINT64;
  const double large10 = (double)TENTH_MAX;
  sqlite4_num x = {0, 0, 0, 0};

  /* TODO: How should this be set? */
  x.approx = 1;

  if( d<0.0 ){
    x.sign = 1;
    d = d*-1.0;
  }

  while( d>large || (d>1.0 && d==(i64)d) ){
    d = d / 10.0;
    x.e++;
  }

  while( d<large10 && d!=(double)((i64)d) ){
    d = d * 10.0;
    x.e--;
  }
  x.m = (u64)d;

  return x;
}

/*
** Convert the number passed as the first argument to a signed 32-bit
** integer and return the value. If the second argument is not NULL,
** then set the value that it points to 1 if data was lost as part
** of the conversion, or 0 otherwise.
**
** Values round towards 0. If the number is outside the range that a
** signed 32-bit integer can represent, it is clamped to be inside
** that range.
*/
int sqlite4_num_to_int32(sqlite4_num num, int *pbLossy){
  sqlite4_int64 iVal; 
  iVal = sqlite4_num_to_int64(num, pbLossy);
  if( iVal<SMALLEST_INT32 ){
    if( pbLossy ) *pbLossy = 1;
    return SMALLEST_INT32;
  }else if( iVal>LARGEST_INT32 ){
    if( pbLossy ) *pbLossy = 1;
    return LARGEST_INT32;
  }else{
    return (int)iVal;
  }
}

int sqlite4_num_to_double(sqlite4_num num, double *pr){
  double rRet;
  int i;
  rRet = num.m;
  if( num.sign ) rRet = rRet*-1;
  for(i=0; i<num.e; i++){
    rRet = rRet * 10.0;
  }
  for(i=num.e; i<0; i++){
    rRet = rRet / 10.0;
  }
  *pr = rRet;
  return SQLITE4_OK;
}

/*
** Convert the number passed as the first argument to a signed 64-bit
** integer and return the value. If the second argument is not NULL,
** then set the value that it points to 1 if data was lost as part
** of the conversion, or 0 otherwise.
**
** Values round towards 0. If the number is outside the range that a
** signed 64-bit integer can represent, it is clamped to be inside
** that range.
*/
sqlite4_int64 sqlite4_num_to_int64(sqlite4_num num, int *pbLossy){
  static const i64 L10 = (LARGEST_INT64 / 10);
  u64 iRet;
  int i;
  iRet = num.m;

  if( pbLossy ) *pbLossy = num.approx;
  for(i=0; i<num.e; i++){
    if( iRet>L10 ) goto overflow;
    iRet = iRet * 10;
  }
  for(i=num.e; i<0; i++){
    if( pbLossy && (iRet % 10) ) *pbLossy = 1;
    iRet = iRet / 10;
  }

  if( num.sign ){
    if( iRet>(u64)LARGEST_INT64+1 ) goto overflow;
    return -(i64)iRet;
  }else{
    if( iRet>(u64)LARGEST_INT64 ) goto overflow; 
    return (i64)iRet;
  }

overflow:
  if( pbLossy ) *pbLossy = 1;
  return num.sign ? -LARGEST_INT64-1 : LARGEST_INT64;
}


/*
** Convert an integer into text in the buffer supplied. The
** text is zero-terminated and right-justified in the buffer.
** A pointer to the first character of text is returned.
**
** The buffer needs to be at least 21 bytes in length.
*/
static char *renderInt(sqlite4_uint64 v, char *zBuf, int nBuf){
  int i = nBuf-1;;
  zBuf[i--] = 0;
  do{
    zBuf[i--] = (v%10) + '0';
    v /= 10;
  }while( v>0 );
  return zBuf+(i+1);
}

/*
** Remove trailing zeros from a string.
*/
static void removeTrailingZeros(char *z, int *pN){
  int i = *pN;
  while( i>0 && z[i-1]=='0' ) i--;
  z[i] = 0;
  *pN = i;
}

/*
** Convert a number into text.  Store the result in zOut[].  The
** zOut buffer must be at laest 30 characters in length.  The output
** will be zero-terminated.
*/
int sqlite4_num_to_text(sqlite4_num x, char *zOut, int bReal){
  char zBuf[24];
  char *zNum;
  int n;
  static const char zeros[] = "0000000000000000000000000";

  char *z = zOut;
  
  if( x.sign && x.m>0 ){
    /* Add initial "-" for negative non-zero values */
    z[0] = '-';
    z++;
  }
  if( x.e>SQLITE4_MX_EXP ){
    /* Handle NaN and infinite values */
    if( x.m==0 ){
      memcpy(z, "NaN", 4);
    }else{
      memcpy(z, "inf", 4);
    }
    return (z - zOut)+3;
  }
  if( x.m==0 ){
    if( bReal ){
      memcpy(z, "0.0", 4);
    }else{
      memcpy(z, "0", 2);
    }
    return 1+(z-zOut);
  }
  zNum = renderInt(x.m, zBuf, sizeof(zBuf));
  n = &zBuf[sizeof(zBuf)-1] - zNum;
  if( x.e>=0 && x.e+n<=25 ){
    /* Integer values with up to 25 digits */
    memcpy(z, zNum, n+1);
    z += n;
    if( x.e>0 ){
      memcpy(z, zeros, x.e);
      z += x.e;
      z[0] = 0;
    }
    if( bReal ){
      memcpy(z, ".0", 3);
      z += 2;
    }
    return (z - zOut);
  }
  if( x.e<0 && n+x.e > 0 ){
    /* Fractional values where the decimal point occurs within the
    ** significant digits.  ex:  12.345 */
    int m = n+x.e;
    memcpy(z, zNum, m);
    z += m;
    zNum += m;
    n -= m;
    removeTrailingZeros(zNum, &n);
    if( n>0 ){
      z[0] = '.';
      z++;
      memcpy(z, zNum, n);
      z += n;
      z[0] = 0;
    }else{
      if( bReal ){
        memcpy(z, ".0", 3);
        z += 2;
      }else{
        z[0] = 0;
      }
    }
    return (z - zOut);
  }
  if( x.e<0 && x.e >= -n-5 ){
    /* Values less than 1 and with no more than 5 subsequent zeros prior
    ** to the first significant digit.  Ex:  0.0000012345 */
    int j = -(n + x.e);
    memcpy(z, "0.", 2);
    z += 2;
    if( j>0 ){
      memcpy(z, zeros, j);
      z += j;
    }
    removeTrailingZeros(zNum, &n);
    memcpy(z, zNum, n);
    z += n;
    z[0] = 0;
    return (z - zOut);
  }
  /* Exponential notation from here to the end.  ex:  1.234e-15 */
  z[0] = zNum[0];
  z++;
  if( n>1 ){
    int nOrig = n;
    removeTrailingZeros(zNum, &n);
    x.e += nOrig - n;
  }
  if( n!=1 ){
    /* Two or or more significant digits.  ex: 1.23e17 */
    *z++ = '.';
    memcpy(z, zNum+1, n-1);
    z += n-1;
    x.e += n-1;
  }
  *z++ = 'e';
  if( x.e<0 ){
    *z++ = '-';
    x.e = -x.e;
  }else{
    *z++ = '+';
  }
  zNum = renderInt(x.e&0x7fff, zBuf, sizeof(zBuf));
  while( (z[0] = zNum[0])!=0 ){ z++; zNum++; }
  return (z-zOut);
}
