/*
** 2012 January 24
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains code for encoding and decoding values and keys for
** insertion and reading from the key/value storage engine.
*/
#include "sqliteInt.h"
#include "vdbeInt.h"

/*
** The decoder object.
**
** An instance of this object is used to extract individual columns (numbers,
** strings, blobs, or NULLs) from a row of a table or index.  Usually the
** content is extract from the value side of the key/value pair, though
** sometimes information might be taken from the key as well.
**
** When the VDBE needs to extract multiple columns from the same row, it will
** try to reuse a single decoder object.  The decoder, therefore, should attempt
** to cache any intermediate results that might be useful on later invocations.
*/
struct RowDecoder {
  sqlite4 *db;                /* The database connection */
  VdbeCursor *pCur;           /* The cursor for being decoded */
  KVCursor *pKVCur;           /* Alternative KVCursor if pCur is NULL */
  const KVByteArray *a;       /* Content to be decoded */
  const KVByteArray *aKey;    /* Key content */
  KVSize n;                   /* Bytes of content in a[] */
  KVSize nKey;                /* Bytes of key content */
  int mxCol;                  /* Maximum number of columns */
};

/*
** Create an object that can be used to decode fields of the data encoding.
**
** The aIn[] value must remain stable for the life of the decoder.
*/
int sqlite4VdbeDecoderCreate(
  sqlite4 *db,                /* The database connection */
  VdbeCursor *pCur,           /* The cursor associated with this decoder */
  KVCursor *pKVCur,           /* Alternative KVCursor */
  int mxCol,                  /* Maximum number of columns to ever decode */
  RowDecoder **ppOut          /* Return the answer here */
){
  RowDecoder *p;

  assert( pCur==0 || pKVCur==0 );
  assert( pCur!=0 || pKVCur!=0 );
  p = sqlite4DbMallocZero(db, sizeof(*p));
  *ppOut = p;
  if( p==0 ) return SQLITE4_NOMEM;
  p->db = db;
  p->pCur = pCur;
  p->pKVCur = pKVCur;
  p->mxCol = mxCol;
  return SQLITE4_OK;
}

/*
** Destroy a decoder object previously created
** using sqlite4VdbeCreateDecoder().
*/
int sqlite4VdbeDecoderDestroy(RowDecoder *p){
  if( p ){
    sqlite4DbFree(p->db, p);
  }
  return SQLITE4_OK;
}

/*
** Make sure the p->a and p->n fields are valid and current.
*/
static int decoderFetchData(RowDecoder *p){
  VdbeCursor *pCur = p->pCur;
  int rc;
  if( pCur==0 ){
    rc = sqlite4KVCursorData(p->pKVCur, 0, -1, &p->a, &p->n);
    return rc;
  }
  if( pCur->rowChnged ){
    p->a = 0;
    p->aKey = 0;
    pCur->rowChnged = 0;
  }
  if( p->a ) return SQLITE4_OK;
  rc = sqlite4VdbeCursorMoveto(pCur);
  if( rc ) return rc;
  if( pCur->nullRow ){
    p->a = 0;
    p->n = 0;
    return SQLITE4_OK;
  }
  assert( pCur->pKVCur!=0 );
  return sqlite4KVCursorData(pCur->pKVCur, 0, -1, &p->a, &p->n);
}

/*
** Make sure the p->aKey and p->nKey fields are valid and current.
*/
static int decoderFetchKey(RowDecoder *p){
  VdbeCursor *pCur = p->pCur;
  int rc;
  if( pCur==0 ){
    rc = sqlite4KVCursorKey(p->pKVCur, &p->aKey, &p->nKey);
    return rc;
  }
  assert( p->a!=0 );
  if( p->aKey ) return SQLITE4_OK;
  assert( pCur->pKVCur!=0 );
  return sqlite4KVCursorKey(pCur->pKVCur, &p->aKey, &p->nKey);
}

/*
** Decode a blob from a key.  The blob-key is in a[0] through a[n-1].
** xorMask is either 0x00 for ascending order or 0xff for descending.
** Store the blob in pOut.
*/
static int decoderMemSetFromBlob(
  const KVByteArray *a, KVSize n, /* The blob as a key */
  unsigned int xorMask,           /* 0x00 (ascending) or 0xff (descending) */
  Mem *pOut                       /* Write the blob here */
){
  int rc;
  unsigned int m = 0;
  int i, j, k;

  sqlite4VdbeMemSetStr(pOut, "", 0, 0, 0, 0);
  rc = sqlite4VdbeMemGrow(pOut, n, 0);
  if( rc==SQLITE4_OK ){
    i = 0;
    j = 0;
    k = 0;
    while( i<n ){
      m = (m<<7) | ((a[i++] ^ xorMask)&0x7f);
      j += 7;
      if( j>=8 ){
        pOut->z[k++] = (m>>(j-8))&0xff;
        j -= 8;
      }
    }
    if( j>0 ){
      pOut->z[k] = m<<(7-j);
    }
    pOut->n = k;
  }
  return rc;
}

/*
** Decode a numeric key encoding.  Return the number of bytes in the
** encoding on success.  On an error, return 0.
*/
int sqlite4VdbeDecodeNumericKey(
  const KVByteArray *aKey,       /* Input encoding */
  KVSize nKey,                   /* Number of bytes in aKey[] */
  sqlite4_num *pVal              /* Write the result here */
){
  unsigned int i, y;
  unsigned int xorMask = 0;
  short e;
  sqlite4_uint64 m;
  sqlite4_int64 eBig;
  KVByteArray aInvertedKey[4];

  pVal->approx = 0;
  pVal->sign = 0;
  if( nKey<1 ) return 0;
  switch( aKey[0] ){
    case 0x06:   /* NaN ascending */
    case 0xf9:   /* NaN descending */
      pVal->m = 0;
      pVal->e = 1000;
      return 1;

    case 0x07:   /* -inf ascending */
    case 0xf8:   /* -inf descending */
      pVal->m = 1;
      pVal->sign = 1;
      pVal->e = 1000;
      return 1;

    case 0x15:   /* zero ascending */
    case 0xea:   /* zero descending */
      pVal->m = 0;
      pVal->e = 0;
      return 1;

    case 0x23:   /* +inf ascending */
    case 0xdc:   /* +inf descending */
      pVal->m = 1;
      pVal->e = 1000;
      return 1;

    case 0x09:
    case 0x0a:
    case 0x0b:
    case 0x0c:
    case 0x0d:
    case 0x0e:
    case 0x0f:
    case 0x10:
    case 0x11:
    case 0x12:
    case 0x13:   /* -medium ascending */
      pVal->sign = 1;
      xorMask = 0xff;
      e = 0x13 - aKey[0];
      i = 1;
      break;

    case 0xf6:
    case 0xf5:
    case 0xf4:
    case 0xf3:
    case 0xf2:
    case 0xf1:
    case 0xf0:
    case 0xef:
    case 0xee:
    case 0xed:
    case 0xec:   /* -medium descending */
      pVal->sign = 1;
      e = aKey[0] - 0xec;
      i = 1;
      break;

    case 0x17:
    case 0x18:
    case 0x19:
    case 0x1a:
    case 0x1b:
    case 0x1c:
    case 0x1d:
    case 0x1e:
    case 0x1f:
    case 0x20:
    case 0x21:   /* +medium ascending */
      e = aKey[0] - 0x17;
      i = 1;
      break;

    case 0xe8:
    case 0xe7:
    case 0xe6:
    case 0xe5:
    case 0xe4:
    case 0xe3:
    case 0xe2:
    case 0xe1:
    case 0xe0:
    case 0xdf:
    case 0xde:   /* +medium descending */
      e = 0xe8 - aKey[0];
      xorMask = 0xff;
      i = 1;
      break;

    case 0x14:   /* -small ascending */
    case 0xe9:   /* +small descending */
      i = 1 + sqlite4GetVarint64(aKey+1, 2, &eBig);
      e = (short)-eBig;
      xorMask = 0xff;
      break;

    case 0x16:   /* +small ascending */
    case 0xeb:   /* -small descending */
      aInvertedKey[0] = aKey[1] ^ 0xff;
      aInvertedKey[1] = aKey[2] ^ 0xff;
      i = 1 + sqlite4GetVarint64(aInvertedKey, 2, &eBig);
      e = (short)-eBig;
      break;

    case 0x08:   /* -large ascending */
    case 0xdd:   /* +large descending */
      aInvertedKey[0] = aKey[1] ^ 0xff;
      aInvertedKey[1] = aKey[2] ^ 0xff;
      i = 1 + sqlite4GetVarint64(aInvertedKey, 2, &eBig);
      e = (short)eBig;
      xorMask = 0xff;
      break;

    case 0x22:   /* +large ascending */
    case 0xf7:   /* -large descending */
      i = 1 + sqlite4GetVarint64(aKey+1, 2, &eBig);
      e = (short)eBig;
      break;

    default:
      return 0;
  }
  m = 0;
  do{
    y = aKey[i++] ^ xorMask;
    m = m*100 + y/2;
    e--;
  }while( y & 1 );
  if( m==0 ) return 0;

  pVal->m = m;
  pVal->e = 2*e;
  return i;
}

/*
** This is a private method for the RowDecoder object.
**
** Attempt to extract a single column value from the key of the current
** key/value pair.  If beginning of the value is iOfst bytes from the beginning
** of the key.  If affReal is true, then force numeric values to be floating
** point.  Write the result in pOut.  Or return non-zero if there is an
** error.
*/
static int decoderFromKey(
  RowDecoder *p,           /* The current key/value pair */
  int affReal,             /* True to coerce numbers to floating point */
  sqlite4_int64 iOfst,     /* Offset of value in the key */
  Mem *pOut                /* Write the results here */
){
  int rc;
  KVSize i;
  KVSize n;
  const KVByteArray *a;
  char *z;

  if( iOfst<0 ){
    return SQLITE4_CORRUPT_BKPT;
  }
  rc = decoderFetchKey(p);
  if( rc ) return rc;
  if( iOfst>=p->nKey ){
    return SQLITE4_CORRUPT_BKPT;
  }
  a = p->aKey;
  n = p->nKey;
  switch( a[iOfst++] ){
    case 0x05: case 0xFA:       /* NULL */
    case 0x06: case 0xF9: {     /* NaN */
      sqlite4VdbeMemSetNull(pOut);
      break;
    }

    case 0x24: {                /* Text (ascending index) */
      for(i=iOfst; i<n && a[i]!=0; i++){}
      rc = sqlite4VdbeMemSetStr(pOut, &a[iOfst], i-iOfst,
                                SQLITE4_UTF8, SQLITE4_TRANSIENT, 0);
      break;
    }
    case 0xDB: {                /* Text (descending index) */
      for(i=iOfst; i<n && a[i]!=0xFF; i++){}
      rc = sqlite4VdbeMemSetStr(pOut, &a[iOfst+1], n = i - iOfst,
                                SQLITE4_UTF8, SQLITE4_TRANSIENT, 0);
      if( rc==SQLITE4_OK ){
        z = pOut->z;
        for(i=n-1; i>=0; i--) *(z++) ^= 0xFF;
      }
      break;
    }

    case 0x25: {                /* Blob (ascending index) */
      for(i=iOfst; i<n && a[i]!=0; i++){}
      rc = decoderMemSetFromBlob(&a[iOfst], i-iOfst, 0x00, pOut);
      break;
    }
    case 0xDA: {                /* Blob (descending index) */
      for(i=iOfst; i<n && a[i]!=0xFF; i++){}
      rc = decoderMemSetFromBlob(&a[iOfst], i-iOfst, 0xFF, pOut);
      break;
    }

    case 0x26: {                /* Blob-final (ascending) */
      rc = sqlite4VdbeMemSetStr(pOut, &a[iOfst], n-iOfst, 0,
                                SQLITE4_TRANSIENT, 0);
      break;
    }
    case 0xD9: {                /* Blob-final (descending) */
      rc = sqlite4VdbeMemSetStr(pOut, &a[iOfst], n-iOfst,
                                SQLITE4_UTF8, SQLITE4_TRANSIENT, 0);
      if( rc==SQLITE4_OK ){
        z = pOut->z;
        for(i=n-iOfst; i>0; i--) *(z++) ^= 0xFF;
      }
      break;
    }
    default: {
      sqlite4_num v;
      i = sqlite4VdbeDecodeNumericKey(a+iOfst-1, n-iOfst+1, &v);
      if( i==0 ){
        rc = SQLITE4_CORRUPT_BKPT;
      }else{
        sqlite4VdbeMemSetNum(pOut, v, affReal ? MEM_Real : MEM_Int);
        rc = SQLITE4_OK;
      }
      break;
    }
  };
  return rc;
}

/*
** Decode a single column from a key/value pair taken from the storage
** engine.  The key/value pair to be decoded is the one that the VdbeCursor
** or KVCursor is currently pointing to.
**
** iVal is the column index of the value.  0 is the first column of the
** value.  If N is the number of columns in the value and iVal>=N then
** the result is pDefault.  Write the result into pOut.  Return SQLITE4_OK
** on success or an appropriate error code on failure.
**
** The key is referenced only if the iVal-th column in the value is either
** the 22 or 23 header code which indicates that the value is stored in the
** key instead.
*/
int sqlite4VdbeDecoderGetColumn(
  RowDecoder *p,             /* The decoder for the whole string */
  int iVal,                    /* Index of the value to decode.  First is 0 */
  Mem *pDefault,               /* The default value.  Often NULL */
  Mem *pOut                    /* Write the result here */
){
  u32 size;                    /* Size of a field */
  sqlite4_uint64 ofst;         /* Offset to the payload */
  sqlite4_uint64 type;         /* Datatype */
  sqlite4_uint64 subtype;      /* Subtype for a typed blob */
  int cclass;                  /* class of content */
  int n;                       /* Offset into the header */
  int i;                       /* Loop counter */
  int sz;                      /* Size of a varint */
  int endHdr;                  /* First byte past header */
  int rc;                      /* Return code */

  sqlite4VdbeMemSetNull(pOut);
  assert( iVal<=p->mxCol );
  rc = decoderFetchData(p);
  if( rc ) return rc;
  if( p->a==0 ) return SQLITE4_OK;
  n = sqlite4GetVarint64(p->a, p->n, &ofst);
  if( n==0 ) return SQLITE4_CORRUPT;
  ofst += n;
  endHdr = ofst;
  if( endHdr>p->n ) return SQLITE4_CORRUPT;
  for(i=0; i<=iVal && n<endHdr; i++){
    sz = sqlite4GetVarint64(p->a+n, p->n-n, &type);
    if( sz==0 ) return SQLITE4_CORRUPT;
    n += sz;
    if( type>=22 ){  /* STRING, BLOB, KEY, and TYPED */
      cclass = (type-22)%4;
      if( cclass==2 ){
        size = 0;  /* KEY */
      }else{
        size = (type-22)/4;
        if( cclass==3 ){  /* The TYPED header code */
          sz = sqlite4GetVarint64(p->a+n, p->n-n, &subtype);
          if( sz==0 ) return SQLITE4_CORRUPT;
          n += sz;
        }
      }
    }else if( type<=2 ){  /* NULL, ZERO, and ONE */
      size = 0;
    }else if( type<=10 ){ /* INT */
      size = type - 2;
    }else{
      assert( type>=11 && type<=21 );  /* NUM */
      size = type - 9;
    }
    if( i<iVal ){
      ofst += size;
    }else if( type==0 ){
      /* no-op */
    }else if( type<=2 ){
      sqlite4VdbeMemSetInt64(pOut, type-1);
    }else if( type<=10 ){
      int iByte;
      sqlite4_int64 v = ((char*)p->a)[ofst];
      for(iByte=1; iByte<size; iByte++){
        v = v*256 + p->a[ofst+iByte];
      }
      sqlite4VdbeMemSetInt64(pOut, v);
    }else if( type<=21 ){
      sqlite4_num num = {0, 0, 0, 0};
      sqlite4_uint64 x;
      int e;

      n = sqlite4GetVarint64(p->a+ofst, p->n-ofst, &x);
      e = (int)x;
      n += sqlite4GetVarint64(p->a+ofst+n, p->n-(ofst+n), &x);
      if( n!=size ) return SQLITE4_CORRUPT;

      num.m = x;
      num.e = (e >> 2);
      if( e & 0x02 ) num.e = -1 * num.e;
      if( e & 0x01 ) num.sign = 1;
      pOut->u.num = num;
      MemSetTypeFlag(pOut, MEM_Real);
    }else if( cclass==0 ){
      if( size==0 ){
        sqlite4VdbeMemSetStr(pOut, "", 0, SQLITE4_UTF8, SQLITE4_TRANSIENT, 0);
      }else if( p->a[ofst]>0x02 ){
        sqlite4VdbeMemSetStr(pOut, (char*)(p->a+ofst), size, 
                             SQLITE4_UTF8, SQLITE4_TRANSIENT, 0);
      }else{
        static const u8 enc[] = {SQLITE4_UTF8,SQLITE4_UTF16LE,SQLITE4_UTF16BE };
        sqlite4VdbeMemSetStr(pOut, (char*)(p->a+ofst+1), size-1, 
                             enc[p->a[ofst]], SQLITE4_TRANSIENT, 0);
      }
    }else if( cclass==2 ){
      unsigned int k = (type - 24)/4;
      return decoderFromKey(p, (k&1)!=0, k/2, pOut);
    }else{
      sqlite4VdbeMemSetStr(pOut, (char*)(p->a+ofst), size, 0,
                           SQLITE4_TRANSIENT, 0);
      pOut->enc = ENC(p->db);
    }
  }
  testcase( i==iVal );
  testcase( i==iVal+1 );
  if( i<=iVal ){
    if( pDefault ){
      sqlite4VdbeMemShallowCopy(pOut, pDefault, MEM_Static);
    }else{
      sqlite4VdbeMemSetNull(pOut);
    }
  }
  return SQLITE4_OK; 
}

/*
** Return the number of bytes needed to represent a 64-bit signed integer.
*/
static int significantBytes(sqlite4_int64 v){
  sqlite4_int64 x;
  int n = 1;
  if( v<0 ){
    x = -128;
    while( v<x && n<8 ){ n++; x *= 256; }
  }else{
    x = 127;
    while( v>x && n<8 ){ n++; x *= 256; }
  }
  return n;
}

/*
** Encode nIn values from array aIn[] using the data encoding. If argument
** aPermute[] is NULL, then the nIn elements are elements 0, 1 ... (nIn-1)
** of the array. Otherwise, aPermute[0], aPermute[1] ... aPermute[nIn-1].
**
** Assume that affinity has already been applied to all elements of the
** input array aIn[].
**
** Space to hold the record is obtained from sqlite4DbMalloc() and should
** be freed by the caller using sqlite4DbFree() to avoid a memory leak.
*/
int sqlite4VdbeEncodeData(
  sqlite4 *db,                /* The database connection */
  Mem *aIn,                   /* Array of values to encode */
  int *aPermute,              /* Permutation or NULL (see above) */
  int nIn,                    /* Number of entries in aIn[] */
  u8 **pzOut,                 /* The output data record */
  int *pnOut                  /* Bytes of content in pzOut */
){
  int i, j;
  int rc = SQLITE4_OK;
  int nHdr;
  int n;
  u8 *aOut = 0;               /* The result */
  int nOut;                   /* Bytes of aOut used */
  int nPayload = 0;           /* Payload space required */
  int encoding = ENC(db);     /* Text encoding */
  struct dencAux {            /* For each input value of aIn[] */
    int n;                       /* Size of encoding at this position */
    u8 z[12];                    /* Encoding for number at this position */
  } *aAux;

  aAux = sqlite4StackAllocZero(db, sizeof(*aAux)*nIn);
  if( aAux==0 ) return SQLITE4_NOMEM;
  aOut = sqlite4DbMallocZero(db, (nIn+1)*9);
  if( aOut==0 ){
    rc = SQLITE4_NOMEM;
    goto vdbeEncodeData_error;
  }
  nOut = 9;
  for(i=0; i<nIn; i++){
    Mem *pIn = &aIn[ aPermute ? aPermute[i] : i ];
    int flags = pIn->flags;
    if( flags & MEM_Null ){
      aOut[nOut++] = 0;
    }else if( flags & MEM_Int ){
      i64 i1;
      i1 = sqlite4_num_to_int64(pIn->u.num, 0);
      n = significantBytes(i1);
      aOut[nOut++] = n+2;
      nPayload += n;
      aAux[i].n = n;
    }else if( flags & MEM_Real ){
      sqlite4_num *p = &pIn->u.num;
      int e;
      assert( p->sign==0 || p->sign==1 );
      if( p->e<0 ){
        e = (p->e*-4) + 2 + p->sign;
      }else{
        e = (p->e*4) + p->sign;
      }
      n = sqlite4PutVarint64(aAux[i].z, (sqlite4_uint64)e);
      n += sqlite4PutVarint64(aAux[i].z+n, p->m);
      aAux[i].n = n;
      aOut[nOut++] = n+9;
      nPayload += n;
    }else if( flags & MEM_Str ){
      n = pIn->n;
      if( n && (encoding!=SQLITE4_UTF8 || pIn->z[0]<3) ) n++;
      nPayload += n;
      nOut += sqlite4PutVarint64(aOut+nOut, 22+4*(sqlite4_int64)n);
    }else{
      n = pIn->n;
      assert( flags & MEM_Blob );
      nPayload += n;
      nOut += sqlite4PutVarint64(aOut+nOut, 23+4*(sqlite4_int64)n);
    }
  }
  nHdr = nOut - 9;
  n = sqlite4PutVarint64(aOut, nHdr);
  for(i=n, j=9; j<nOut; j++) aOut[i++] = aOut[j];
  nOut = i;
  aOut = sqlite4DbReallocOrFree(db, aOut, nOut + nPayload);
  if( aOut==0 ){ rc = SQLITE4_NOMEM; goto vdbeEncodeData_error; }
  for(i=0; i<nIn; i++){
    Mem *pIn = &aIn[ aPermute ? aPermute[i] : i ];
    int flags = pIn->flags;
    if( flags & MEM_Null ){
      /* No content */
    }else if( flags & MEM_Int ){
      sqlite4_int64 v;
      v = sqlite4_num_to_int64(pIn->u.num, 0);
      n = aAux[i].n;
      aOut[nOut+(--n)] = v & 0xff;
      while( n ){
        v >>= 8;
        aOut[nOut+(--n)] = v & 0xff;
      }
      nOut += aAux[i].n;
    }else if( flags & MEM_Real ){
      memcpy(aOut+nOut, aAux[i].z, aAux[i].n);
      nOut += aAux[i].n;
    }else if( flags & MEM_Str ){
      n = pIn->n;
      if( n ){
        if( encoding==SQLITE4_UTF16LE ) aOut[nOut++] = 1;
        else if( encoding==SQLITE4_UTF16BE ) aOut[nOut++] = 2;
        else if( pIn->z[0]<3 ) aOut[nOut++] = 0;
        memcpy(aOut+nOut, pIn->z, n);
        nOut += n;
      }
    }else{
      assert( flags & MEM_Blob );
      memcpy(aOut+nOut, pIn->z, pIn->n);
      nOut += pIn->n;
    }
  }

  *pzOut = aOut;
  *pnOut = nOut;
  sqlite4StackFree(db, aAux);
  return SQLITE4_OK;

vdbeEncodeData_error:
  sqlite4StackFree(db, aAux);
  sqlite4DbFree(db, aOut);
  return rc;
}

/*
** An output buffer for sqlite4VdbeEncodeKey
*/
typedef struct KeyEncoder KeyEncoder;
struct KeyEncoder {
  sqlite4 *db;   /* Database connection */
  u8 *aOut;      /* Output buffer */
  int nOut;      /* Slots of aOut[] used */
  int nAlloc;    /* Slots of aOut[] allocated */
};

/*
** Enlarge a memory allocation, if necessary
*/
static int enlargeEncoderAllocation(KeyEncoder *p, int needed){
  assert( p->nOut<=p->nAlloc );
  if( p->nOut+needed>p->nAlloc ){
    u8 *aNew;
    p->nAlloc = p->nAlloc + needed + 10;
    aNew = sqlite4DbRealloc(p->db, p->aOut, p->nAlloc);
    if( aNew==0 ){
      sqlite4DbFree(p->db, p->aOut);
      memset(p, 0, sizeof(*p));
      return SQLITE4_NOMEM;
    }
    p->aOut = aNew;
    p->nAlloc = sqlite4DbMallocSize(p->db, p->aOut);
  }
  return SQLITE4_OK;
}

/*
** Write value v as a varint into buffer p. If parameter bInvert
** is non-zero, write the ones-complement of each byte instead of
** the usual value.
*/
static void putVarint64(KeyEncoder *p, sqlite4_uint64 v, int bInvert){
  unsigned char *z = &p->aOut[p->nOut];
  int n = sqlite4PutVarint64(z, v);
  if( bInvert ){
    int i;
    for(i=0; i<n; i++) z[i] = ~z[i];
  }
  p->nOut += n;
}

/*
** Write value num into buffer p using the key encoding.
*/
static void encodeNumericKey(KeyEncoder *p, sqlite4_num num){
  if( num.m==0 ){
    if( sqlite4_num_isnan(num) ){
      p->aOut[p->nOut++] = 0x06;  /* NaN */
    }else{
      p->aOut[p->nOut++] = 0x15;  /* Numeric zero */
    }
  }else if( sqlite4_num_isinf(num) ){
    p->aOut[p->nOut++] = num.sign ? 0x07 : 0x23;  /* Neg and Pos infinity */
  }else{
    int e;
    u64 m;
    int iDigit = 0;
    u8 aDigit[12];

    while( (num.m % 10)==0 ){
      num.e++;
      num.m = num.m / 10;
    }
    m = num.m;
    e = num.e;

    if( num.e % 2 ){
      aDigit[0] = 10 * (m % 10);
      m = m / 10;
      e--;
      iDigit = 1;
    }else{
      iDigit = 0;
    }

    while( m ){
      aDigit[iDigit++] = (m % 100);
      m = m / 100;
    }
    e = (iDigit + (e/2));

    if( e>=11 ){                /* Large value */
      if( num.sign==0 ){
        p->aOut[p->nOut++] = 0x22;
        putVarint64(p, e, 0);
      }else{
        p->aOut[p->nOut++] = 0x08;
        putVarint64(p, e, 1);
      }
    }
    else if( e>=0 ){            /* Medium value */
      if( num.sign==0 ){
        p->aOut[p->nOut++] = 0x17+e;
      }else{
        p->aOut[p->nOut++] = 0x13-e;
      }
    }
    else{                       /* Small value */
      if( num.sign==0 ){
        p->aOut[p->nOut++] = 0x16;
        putVarint64(p, -1*e, 1);
      }else{
        p->aOut[p->nOut++] = 0x14;
        putVarint64(p, -1*e, 0);
      }
    }

    /* Write M to the output. */
    while( (iDigit--)>0 ){
      u8 d = aDigit[iDigit]*2;
      if( iDigit!=0 ) d |= 0x01;
      if( num.sign ) d = ~d;
      p->aOut[p->nOut++] = d;
    }
  }
}

/*
** Encode a single integer using the key encoding.  The caller must 
** ensure that sufficient space exits in a[] (at least 12 bytes).  
** The return value is the number of bytes of a[] used.  
*/
int sqlite4VdbeEncodeIntKey(u8 *a, sqlite4_int64 v){
  KeyEncoder s;
  sqlite4_num num;

  num = sqlite4_num_from_int64(v);
  memset(&s, 0, sizeof(s));
  s.aOut = a;
  encodeNumericKey(&s, num);
  return s.nOut;
}

/*
** Encode a single column of the key
*/
static int encodeOneKeyValue(
  KeyEncoder *p,    /* Key encoder context */
  Mem *pMem,        /* Value to be encoded */
  u8 sortOrder,     /* Sort order for this value */
  u8 isLastValue,   /* True if this is the last value in the key */
  CollSeq *pColl    /* Collating sequence for the value */
){
  int flags = pMem->flags;
  int i;
  int n;
  int iStart = p->nOut;
  if( flags & MEM_Null ){
    if( enlargeEncoderAllocation(p, 1) ) return SQLITE4_NOMEM;
    p->aOut[p->nOut++] = 0x05;   /* NULL */
  }else
  if( flags & (MEM_Real|MEM_Int) ){
    if( enlargeEncoderAllocation(p, 16) ) return SQLITE4_NOMEM;
    encodeNumericKey(p, pMem->u.num);
  }else if( flags & MEM_Str ){
    int enc;                      /* Initial encoding of pMem */

    assert( pMem->enc==SQLITE4_UTF8 
         || pMem->enc==SQLITE4_UTF16LE
         || pMem->enc==SQLITE4_UTF16BE
    );
    assert( pMem->db );
    enc = pMem->enc;

    /* Write the encoded key to the output buffer. */
    if( enlargeEncoderAllocation(p, pMem->n*4 + 2) ) return SQLITE4_NOMEM;
    p->aOut[p->nOut++] = 0x24;   /* Text */
    if( pColl==0 || pColl->xMkKey==0 ){
      const char *z = (const char *)sqlite4ValueText(pMem, SQLITE4_UTF8);
      if( z ){
        const char *zCsr = z;
        const char *zEnd = &z[pMem->n];
        while( *zCsr && zCsr<zEnd ) zCsr++;
        memcpy(p->aOut+p->nOut, z, (zCsr-z));
        p->nOut += (zCsr-z);
      }
    }else{
      int rc;                     /* xMkKey() return code */
      int nReq;                   /* Space required by xMkKey() */
      int nSpc;                   /* Space available */

      nSpc = p->nAlloc-p->nOut;
      rc = pColl->xMkKey(pColl->pUser, pMem, nSpc, p->aOut+p->nOut, &nReq);
      if( rc!=SQLITE4_OK ) return rc;
      if( nReq+1>nSpc ){
        if( enlargeEncoderAllocation(p, nReq+1) ) return SQLITE4_NOMEM;
        rc = pColl->xMkKey(pColl->pUser, pMem, nReq, p->aOut+p->nOut, &nReq);
      }
      p->nOut += nReq;
    }
    p->aOut[p->nOut++] = 0x00;

    /* If the operations above changed the encoding of pMem, change it back.
    ** This call is a no-op if pMem was not modified by the code above.  */
    sqlite4VdbeChangeEncoding(pMem, enc);

  }else if( isLastValue ){
    /* A BLOB value that is the right-most value of a key */
    assert( flags & MEM_Blob );
    if( enlargeEncoderAllocation(p, pMem->n+1) ) return SQLITE4_NOMEM;
    p->aOut[p->nOut++] = 0x26;
    memcpy(p->aOut+p->nOut, pMem->z, pMem->n);
    p->nOut += pMem->n;
  }else{
    /* A BLOB value that is followed by other values */
    const unsigned char *a;
    unsigned char s, t;
    assert( flags & MEM_Blob );
    n = pMem->n;
    a = (u8*)pMem->z;
    s = 1;
    t = 0;
    if( enlargeEncoderAllocation(p, (n*8+6)/7 + 2) ) return SQLITE4_NOMEM;
    p->aOut[p->nOut++] = 0x25;   /* Blob */
    for(i=0; i<n; i++){
      unsigned char x = a[i];
      p->aOut[p->nOut++] = 0x80 | t | (x>>s);
      if( s<7 ){
        t = x<<(7-s);
        s++;
      }else{
        p->aOut[p->nOut++] = 0x80 | x;
        s = 1;
        t = 0;
      }
    }
    if( s>1 ) p->aOut[p->nOut++] = 0x80 | t;
    p->aOut[p->nOut++] = 0x00;
  }
  if( sortOrder==SQLITE4_SO_DESC ){
    for(i=iStart; i<p->nOut; i++) p->aOut[i] ^= 0xff;
  }
  assert( p->nOut<=p->nAlloc );
  return SQLITE4_OK;
}

/*
** Variables aKey/nKey contain an encoded index key. This function returns
** the length (in bytes) of the key with all but the first nField fields
** removed.
*/
int sqlite4VdbeShortKey(
  const u8 *aKey,                 /* Buffer containing encoded key */
  int nKey,                       /* Size of buffer aKey[] in bytes */
  int nField,                     /* Number of fields */
  int *pnOut                      /* Number of complete fields read (or NULL) */
){
  u8 *p = (u8*)aKey;
  u8 *pEnd = (u8*)&aKey[nKey];
  u64 dummy;
  int i;

  /* Skip over the "root page" number at the start of the key */
  p += sqlite4GetVarint64(p, pEnd-p, &dummy);

  for(i=0; i<nField && p<pEnd; i++){
    u8 c = *(p++);
    switch( c ){

      case 0x05: case 0xFA:       /* NULL */
      case 0x06: case 0xF9:       /* NaN */
      case 0x07: case 0xF8:       /* -ve infinity */
      case 0x15: case 0xEA:       /* zero */
      case 0x23: case 0xDC:       /* +ve infinity */
        break;

      case 0x24:                  /* Text (ascending index) */
      case 0x25:                  /* Blob (ascending index) */
        while( *(p++) );
        break;

      case 0xDB:                  /* Text (descending index) */
      case 0xDA:                  /* Blob (descending index) */
        while( (0xFF!=*(p++)) );
        break;

      case 0x26:                  /* Blob-final (ascending) */
      case 0xD9:                  /* Blob-final (descending) */
        p = pEnd;
        break;

      case 0x22: case 0xDD:       /* Large positive number */
      case 0x14: case 0xEB:       /* Small negative number */
      case 0x16: case 0xE9:       /* Small positive number */
      case 0x08: case 0xF7: {     /* Large negative number */
        u8 d;                     /* Value of byte following "c" */

        /* For large positive and small negative integer keys skip over
        ** a varint here. For small positive integers and larger negative 
        ** integers, skip over the ones-complement varint.  */
        if( c==0x16 || c==0x08 || c==0xDD || c==0xEB ){
          d = ~(*(p++));
        }else{
          d = *(p++);
        }
        if( d>240 ){
          p++;
          if( d>248 ) p += (d - 248);
        }

        /* Fall through */
      }

      default:                    /* Medium sized number */
        if( c<0x15 || (c>0xDC && c<0xEA) ){
          while( !((*p++) & 0x01) );
        }else{
          while( ((*p++) & 0x01) );
        }
        break;
    }

    if( p>pEnd ) break;
  }
  if( pnOut ) *pnOut = i;

  return (p - aKey);
}

/*
** Generate a database key from one or more data values.
**
** Space to hold the key is obtained from sqlite4DbMalloc() and should
** be freed by the caller using sqlite4DbFree() to avoid a memory leak.
*/
int sqlite4VdbeEncodeKey(
  sqlite4 *db,                 /* The database connection */
  Mem *aIn,                    /* Values to be encoded */
  int nIn,                     /* Number of entries in aIn[] */
  int iTabno,                  /* The table this key applies to, or negative */
  KeyInfo *pKeyInfo,           /* Collating sequence and sort-order info */
  u8 **paOut,                  /* Write the resulting key here */
  int *pnOut,                  /* Number of bytes in the key */
  int nExtra                   /* extra bytes of space appended to the key */
){
  int i;
  int rc = SQLITE4_OK;
  KeyEncoder x;
  u8 *so;
  CollSeq **aColl;

  assert( pKeyInfo );
  assert( nIn<=pKeyInfo->nField );

  x.db = db;
  x.aOut = 0;
  x.nOut = 0;
  x.nAlloc = 0;
  *paOut = 0;
  *pnOut = 0;

  if( enlargeEncoderAllocation(&x, (nIn+1)*10) ) return SQLITE4_NOMEM;
  if( iTabno>=0 ){
    x.nOut = sqlite4PutVarint64(x.aOut, iTabno);
  }
  aColl = pKeyInfo->aColl;
  so = pKeyInfo->aSortOrder;
  for(i=0; i<nIn && rc==SQLITE4_OK; i++){
    rc = encodeOneKeyValue(&x, aIn+i, so ? so[i] : SQLITE4_SO_ASC,
                           i==pKeyInfo->nField-1, aColl[i]);
  }

  if( rc==SQLITE4_OK && nExtra ){ rc = enlargeEncoderAllocation(&x, nExtra); }
  if( rc ){
    sqlite4DbFree(db, x.aOut);
  }else{
    *paOut = x.aOut;
    *pnOut = x.nOut;
  }
  return rc;
}
