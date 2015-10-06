/*
** 2004 May 26
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
** This file contains code use to manipulate "Mem" structure.  A "Mem"
** stores a single value in the VDBE.  Mem is an opaque structure visible
** only within the VDBE.  Interface routines refer to a Mem using the
** name sqlite_value
*/
#include "sqliteInt.h"
#include "vdbeInt.h"

/*
** If pMem is an object with a valid string representation, this routine
** ensures the internal encoding for the string representation is
** 'desiredEnc', one of SQLITE4_UTF8, SQLITE4_UTF16LE or SQLITE4_UTF16BE.
**
** If pMem is not a string object, or the encoding of the string
** representation is already stored using the requested encoding, then this
** routine is a no-op.
**
** SQLITE4_OK is returned if the conversion is successful (or not required).
** SQLITE4_NOMEM may be returned if a malloc() fails during conversion
** between formats.
*/
int sqlite4VdbeChangeEncoding(Mem *pMem, int desiredEnc){
  int rc;
  assert( (pMem->flags&MEM_RowSet)==0 );
  assert( desiredEnc==SQLITE4_UTF8 || desiredEnc==SQLITE4_UTF16LE
           || desiredEnc==SQLITE4_UTF16BE );
  if( !(pMem->flags&MEM_Str) || pMem->enc==desiredEnc ){
    return SQLITE4_OK;
  }
  assert( pMem->db==0 || sqlite4_mutex_held(pMem->db->mutex) );
#ifdef SQLITE4_OMIT_UTF16
  return SQLITE4_ERROR;
#else

  /* MemTranslate() may return SQLITE4_OK or SQLITE4_NOMEM. If NOMEM is returned,
  ** then the encoding of the value may not have changed.
  */
  rc = sqlite4VdbeMemTranslate(pMem, (u8)desiredEnc);
  assert(rc==SQLITE4_OK    || rc==SQLITE4_NOMEM);
  assert(rc==SQLITE4_OK    || pMem->enc!=desiredEnc);
  assert(rc==SQLITE4_NOMEM || pMem->enc==desiredEnc);
  return rc;
#endif
}

/*
** Make sure pMem->z points to a writable allocation of at least 
** n bytes.
**
** If the memory cell currently contains string or blob data
** and the third argument passed to this function is true, the 
** current content of the cell is preserved. Otherwise, it may
** be discarded.  
**
** This function sets the MEM_Dyn flag and clears any xDel callback.
** It also clears MEM_Ephem and MEM_Static. If the preserve flag is 
** not set, Mem.n is zeroed.
*/
int sqlite4VdbeMemGrow(Mem *pMem, int n, int preserve){
  assert( 1 >=
    ((pMem->zMalloc && pMem->zMalloc==pMem->z) ? 1 : 0) +
    (((pMem->flags&MEM_Dyn)&&pMem->xDel) ? 1 : 0) + 
    ((pMem->flags&MEM_Ephem) ? 1 : 0) + 
    ((pMem->flags&MEM_Static) ? 1 : 0)
  );
  assert( (pMem->flags&MEM_RowSet)==0 );

  if( n<32 ) n = 32;
  if( sqlite4DbMallocSize(pMem->db, pMem->zMalloc)<n ){
    if( preserve && pMem->z==pMem->zMalloc ){
      pMem->z = pMem->zMalloc = sqlite4DbReallocOrFree(pMem->db, pMem->z, n);
      preserve = 0;
    }else{
      sqlite4DbFree(pMem->db, pMem->zMalloc);
      pMem->zMalloc = sqlite4DbMallocRaw(pMem->db, n);
    }
  }

  if( pMem->z && preserve && pMem->zMalloc && pMem->z!=pMem->zMalloc ){
    memcpy(pMem->zMalloc, pMem->z, pMem->n);
  }
  if( pMem->flags&MEM_Dyn && pMem->xDel ){
    assert( pMem->xDel!=SQLITE4_DYNAMIC );
    pMem->xDel(pMem->pDelArg, (void *)(pMem->z));
  }

  pMem->z = pMem->zMalloc;
  if( pMem->z==0 ){
    pMem->flags = MEM_Null;
  }else{
    pMem->flags &= ~(MEM_Ephem|MEM_Static);
  }
  pMem->xDel = 0;
  return (pMem->z ? SQLITE4_OK : SQLITE4_NOMEM);
}

/*
** Make the given Mem object MEM_Dyn.  In other words, make it so
** that any TEXT or BLOB content is stored in memory obtained from
** malloc().  In this way, we know that the memory is safe to be
** overwritten or altered.
**
** Return SQLITE4_OK on success or SQLITE4_NOMEM if malloc fails.
*/
int sqlite4VdbeMemMakeWriteable(Mem *pMem){
  int f;
  assert( pMem->db==0 || sqlite4_mutex_held(pMem->db->mutex) );
  assert( (pMem->flags&MEM_RowSet)==0 );
  f = pMem->flags;
  if( (f&(MEM_Str|MEM_Blob)) && pMem->z!=pMem->zMalloc ){
    if( sqlite4VdbeMemGrow(pMem, pMem->n + 2, 1) ){
      return SQLITE4_NOMEM;
    }
    pMem->z[pMem->n] = 0;
    pMem->z[pMem->n+1] = 0;
    pMem->flags |= MEM_Term;
#ifdef SQLITE4_DEBUG
    pMem->pScopyFrom = 0;
#endif
  }

  return SQLITE4_OK;
}


/*
** Make sure the given Mem is \u0000 terminated.
*/
int sqlite4VdbeMemNulTerminate(Mem *pMem){
  assert( pMem->db==0 || sqlite4_mutex_held(pMem->db->mutex) );
  if( (pMem->flags & MEM_Term)!=0 || (pMem->flags & MEM_Str)==0 ){
    return SQLITE4_OK;   /* Nothing to do */
  }
  if( sqlite4VdbeMemGrow(pMem, pMem->n+2, 1) ){
    return SQLITE4_NOMEM;
  }
  pMem->z[pMem->n] = 0;
  pMem->z[pMem->n+1] = 0;
  pMem->flags |= MEM_Term;
  return SQLITE4_OK;
}

/*
** Add MEM_Str to the set of representations for the given Mem.  Numbers
** are converted using sqlite4_snprintf().  Converting a BLOB to a string
** is a no-op.
**
** Existing representations MEM_Int and MEM_Real are *not* invalidated.
**
** A MEM_Null value will never be passed to this function. This function is
** used for converting values to text for returning to the user (i.e. via
** sqlite4_value_text()), or for ensuring that values to be used as btree
** keys are strings. In the former case a NULL pointer is returned the
** user and the later is an internal programming error.
*/
int sqlite4VdbeMemStringify(Mem *pMem, int enc){
  int rc = SQLITE4_OK;
  int fg = pMem->flags;
  const int nByte = 32;

  assert( pMem->db==0 || sqlite4_mutex_held(pMem->db->mutex) );
  assert( !(fg&(MEM_Str|MEM_Blob)) );
  assert( fg&(MEM_Int|MEM_Real) );
  assert( (pMem->flags&MEM_RowSet)==0 );
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );

  if( sqlite4VdbeMemGrow(pMem, nByte, 0) ){
    return SQLITE4_NOMEM;
  }

  /* For a Real or Integer, use sqlite4_mprintf() to produce the UTF-8
  ** string representation of the value. Then, if the required encoding
  ** is UTF-16le or UTF-16be do a translation.
  ** 
  ** FIX ME: It would be better if sqlite4_snprintf() could do UTF-16.
  */
  sqlite4_num_to_text(pMem->u.num, pMem->z, (pMem->flags & MEM_Int)==0);

  pMem->n = sqlite4Strlen30(pMem->z);
  pMem->enc = SQLITE4_UTF8;
  pMem->flags |= MEM_Str|MEM_Term;
  sqlite4VdbeChangeEncoding(pMem, enc);
  return rc;
}

/*
** Memory cell pMem contains the context of an aggregate function.
** This routine calls the finalize method for that function.  The
** result of the aggregate is stored back into pMem.
**
** Return SQLITE4_ERROR if the finalizer reports an error.  SQLITE4_OK
** otherwise.
*/
int sqlite4VdbeMemFinalize(Mem *pMem, FuncDef *pFunc){
  int rc = SQLITE4_OK;
  if( ALWAYS(pFunc && pFunc->xFinalize) ){
    sqlite4_context ctx;
    assert( (pMem->flags & MEM_Null)!=0 || pFunc==pMem->u.pDef );
    assert( pMem->db==0 || sqlite4_mutex_held(pMem->db->mutex) );
    memset(&ctx, 0, sizeof(ctx));
    ctx.s.flags = MEM_Null;
    ctx.s.db = pMem->db;
    ctx.pMem = pMem;
    ctx.pFunc = pFunc;
    pFunc->xFinalize(&ctx); /* IMP: R-24505-23230 */
    assert( 0==(pMem->flags&MEM_Dyn) && !pMem->xDel );
    sqlite4DbFree(pMem->db, pMem->zMalloc);
    memcpy(pMem, &ctx.s, sizeof(ctx.s));
    rc = ctx.isError;
  }
  return rc;
}

/*
** If the memory cell contains a string value that must be freed by
** invoking an external callback, free it now. Calling this function
** does not free any Mem.zMalloc buffer.
*/
void sqlite4VdbeMemReleaseExternal(Mem *p){
  assert( p->db==0 || sqlite4_mutex_held(p->db->mutex) );
  if( p->flags&MEM_Agg ){
    sqlite4VdbeMemFinalize(p, p->u.pDef);
    assert( (p->flags & MEM_Agg)==0 );
    sqlite4VdbeMemRelease(p);
  }else if( p->flags&MEM_Dyn && p->xDel ){
    assert( (p->flags&MEM_RowSet)==0 );
    assert( p->xDel!=SQLITE4_DYNAMIC );
    p->xDel(p->pDelArg, (void *)p->z);
    p->xDel = 0;
  }else if( p->flags&MEM_RowSet ){
    sqlite4RowSetClear(p->u.pRowSet);
  }else if( p->flags&MEM_Frame ){
    sqlite4VdbeMemSetNull(p);
  }
}

/*
** Release any memory held by the Mem. This may leave the Mem in an
** inconsistent state, for example with (Mem.z==0) and
** (Mem.type==SQLITE4_TEXT).
*/
void sqlite4VdbeMemRelease(Mem *p){
  VdbeMemRelease(p);
  sqlite4DbFree(p->db, p->zMalloc);
  p->z = 0;
  p->zMalloc = 0;
  p->xDel = 0;
}

/*
** Return some kind of integer value which is the best we can do
** at representing the value that *pMem describes as an integer.
** If pMem is an integer, then the value is exact.  If pMem is
** a floating-point then the value returned is the integer part.
** If pMem is a string or blob, then we make an attempt to convert
** it into a integer and return that.  If pMem represents an
** an SQL-NULL value, return 0.
**
** If pMem represents a string value, its encoding might be changed.
*/
i64 sqlite4VdbeIntValue(Mem *pMem){
  assert( pMem->db==0 || sqlite4_mutex_held(pMem->db->mutex) );
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );
  return sqlite4_num_to_int64(sqlite4VdbeNumValue(pMem), 0);
}

/*
** Return the best representation of pMem that we can get into a
** double.  If pMem is already a double or an integer, return its
** value.  If it is a string or blob, try to convert it to a double.
** If it is a NULL, return 0.0.
*/
double sqlite4VdbeRealValue(Mem *pMem){
  double rVal = 0.0;
  assert( pMem->db==0 || sqlite4_mutex_held(pMem->db->mutex) );
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );
  sqlite4_num_to_double(sqlite4VdbeNumValue(pMem), &rVal);
  return rVal;
}

/*
** Extract and return a numeric value from memory cell pMem. This call
** does not modify the contents or flags of *pMem in any way.
*/
sqlite4_num sqlite4VdbeNumValue(Mem *pMem){
  if( pMem->flags & (MEM_Real|MEM_Int) ){
    return pMem->u.num;
  }else if( pMem->flags & (MEM_Str|MEM_Blob) ){
    int flags = SQLITE4_PREFIX_ONLY | SQLITE4_IGNORE_WHITESPACE | pMem->enc;
    return sqlite4_num_from_text(pMem->z, pMem->n, flags, 0);
  }else{
    sqlite4_num zero = {0,0,0,0};
    return zero;
  }
}

/*
** The MEM structure is already a MEM_Real.  Try to also make it a
** MEM_Int if we can.
*/
void sqlite4VdbeIntegerAffinity(Mem *pMem){
  i64 i;
  int bLossy;

  assert( pMem->flags & MEM_Real );
  assert( (pMem->flags & MEM_RowSet)==0 );
  assert( pMem->db==0 || sqlite4_mutex_held(pMem->db->mutex) );
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );

  i = sqlite4_num_to_int64(pMem->u.num, &bLossy);
  if( bLossy==0 ){
    MemSetTypeFlag(pMem, MEM_Int);
    pMem->u.num = sqlite4_num_from_int64(i);
  }
}

/*
** Convert pMem to type integer.  Invalidate any prior representations.
*/
int sqlite4VdbeMemIntegerify(Mem *pMem){
  assert( pMem->db==0 || sqlite4_mutex_held(pMem->db->mutex) );
  assert( (pMem->flags & MEM_RowSet)==0 );
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );

  if( (pMem->flags & MEM_Int)==0 ){
    if( pMem->flags & (MEM_Real|MEM_Null) ){
      pMem->u.num = sqlite4_num_from_int64(sqlite4VdbeIntValue(pMem));
    }else{
      unsigned int flags = pMem->enc |
          SQLITE4_INTEGER_ONLY|SQLITE4_PREFIX_ONLY|SQLITE4_IGNORE_WHITESPACE;
      pMem->u.num = sqlite4_num_from_text(pMem->z, pMem->n, flags, 0);
    }
    MemSetTypeFlag(pMem, MEM_Int);
  }
  return SQLITE4_OK;
}

/*
** Convert pMem so that it has types MEM_Real or MEM_Int or both.
** Invalidate any prior representations.
**
** Every effort is made to force the conversion, even if the input
** is a string that does not look completely like a number.  Convert
** as much of the string as we can and ignore the rest.
*/
int sqlite4VdbeMemNumerify(Mem *pMem){
  if( (pMem->flags & (MEM_Int|MEM_Real|MEM_Null))==0 ){
    int bReal = 0;
    int flags = (pMem->enc | SQLITE4_PREFIX_ONLY | SQLITE4_IGNORE_WHITESPACE);

    assert( (pMem->flags & (MEM_Blob|MEM_Str))!=0 );
    assert( pMem->db==0 || sqlite4_mutex_held(pMem->db->mutex) );
    pMem->u.num = sqlite4_num_from_text(pMem->z, pMem->n, flags, 0);
    sqlite4_num_to_int64(pMem->u.num, &bReal);
    MemSetTypeFlag(pMem, (bReal ? MEM_Real : MEM_Int));
  }
  assert( (pMem->flags & (MEM_Int|MEM_Real|MEM_Null))!=0 );
  pMem->flags &= ~(MEM_Str|MEM_Blob);
  return SQLITE4_OK;
}

/*
** Delete any previous value and set the value stored in *pMem to NULL.
*/
void sqlite4VdbeMemSetNull(Mem *pMem){
  if( pMem->flags & MEM_Frame ){
    VdbeFrame *pFrame = pMem->u.pFrame;
    pFrame->pParent = pFrame->v->pDelFrame;
    pFrame->v->pDelFrame = pFrame;
  }else if( pMem->flags & MEM_RowSet ){
    sqlite4RowSetClear(pMem->u.pRowSet);
  }
  MemSetTypeFlag(pMem, MEM_Null);
  pMem->type = SQLITE4_NULL;
}

/*
** Delete any previous value and set the value stored in *pMem to val,
** manifest type INTEGER.
*/
void sqlite4VdbeMemSetInt64(Mem *pMem, i64 val){
  sqlite4VdbeMemRelease(pMem);
  pMem->u.num = sqlite4_num_from_int64(val);
  pMem->flags = MEM_Int;
  pMem->type = SQLITE4_INTEGER;
}

void sqlite4VdbeMemSetNum(Mem *pMem, sqlite4_num val, int flag){
  assert( flag==MEM_Int || flag==MEM_Real );
  sqlite4VdbeMemRelease(pMem);
  pMem->u.num = val;
  pMem->flags = flag;
  sqlite4VdbeMemStoreType(pMem);
}

#ifndef SQLITE4_OMIT_FLOATING_POINT
/*
** Delete any previous value and set the value stored in *pMem to val,
** manifest type REAL.
*/
void sqlite4VdbeMemSetDouble(Mem *pMem, double val){
  if( sqlite4IsNaN(val) ){
    sqlite4VdbeMemSetNull(pMem);
  }else{
    sqlite4VdbeMemRelease(pMem);
    pMem->u.num = sqlite4_num_from_double(val);
    pMem->flags = MEM_Real;
    pMem->type = SQLITE4_FLOAT;
  }
}
#endif

/*
** Delete any previous value and set the value of pMem to be an
** empty RowSet object.
*/
void sqlite4VdbeMemSetRowSet(Mem *pMem){
  sqlite4 *db = pMem->db;
  assert( db!=0 );
  assert( (pMem->flags & MEM_RowSet)==0 );
  sqlite4VdbeMemRelease(pMem);
  sqlite4VdbeMemGrow(pMem, 64, 0);
  if( db->mallocFailed ){
    pMem->flags = MEM_Null;
  }else{
    int nAlloc = sqlite4DbMallocSize(db, pMem->zMalloc);
    pMem->u.pRowSet = sqlite4RowSetInit(db, pMem->zMalloc, nAlloc);
    pMem->flags = MEM_RowSet;
  }
}

/*
** Return true if the Mem object contains a TEXT or BLOB that is
** too large - whose size exceeds SQLITE4_MAX_LENGTH.
*/
int sqlite4VdbeMemTooBig(Mem *p){
  assert( p->db!=0 );
  if( p->flags & (MEM_Str|MEM_Blob) ){
    int n = p->n;
    return n>p->db->aLimit[SQLITE4_LIMIT_LENGTH];
  }
  return 0; 
}

#ifdef SQLITE4_DEBUG
/*
** This routine prepares a memory cell for modication by breaking
** its link to a shallow copy and by marking any current shallow
** copies of this cell as invalid.
**
** This is used for testing and debugging only - to make sure shallow
** copies are not misused.
*/
void sqlite4VdbeMemAboutToChange(Vdbe *pVdbe, Mem *pMem){
  int i;
  Mem *pX;
  for(i=1, pX=&pVdbe->aMem[1]; i<=pVdbe->nMem; i++, pX++){
    if( pX->pScopyFrom==pMem ){
      pX->flags |= MEM_Invalid;
      pX->pScopyFrom = 0;
    }
  }
  pMem->pScopyFrom = 0;
}
#endif /* SQLITE4_DEBUG */

/*
** Size of struct Mem not including the Mem.zMalloc member.
*/
#define MEMCELLSIZE (size_t)(&(((Mem *)0)->zMalloc))

/*
** Make an shallow copy of pFrom into pTo.  Prior contents of
** pTo are freed.  The pFrom->z field is not duplicated.  If
** pFrom->z is used, then pTo->z points to the same thing as pFrom->z
** and flags gets srcType (either MEM_Ephem or MEM_Static).
*/
void sqlite4VdbeMemShallowCopy(Mem *pTo, const Mem *pFrom, int srcType){
  assert( (pFrom->flags & MEM_RowSet)==0 );
  VdbeMemRelease(pTo);
  memcpy(pTo, pFrom, MEMCELLSIZE);
  pTo->xDel = 0;
  if( (pFrom->flags&MEM_Static)==0 ){
    pTo->flags &= ~(MEM_Dyn|MEM_Static|MEM_Ephem);
    assert( srcType==MEM_Ephem || srcType==MEM_Static );
    pTo->flags |= srcType;
  }
}

/*
** Make a full copy of pFrom into pTo.  Prior contents of pTo are
** freed before the copy is made.
*/
int sqlite4VdbeMemCopy(Mem *pTo, const Mem *pFrom){
  int rc = SQLITE4_OK;

  assert( (pFrom->flags & MEM_RowSet)==0 );
  VdbeMemRelease(pTo);
  memcpy(pTo, pFrom, MEMCELLSIZE);
  pTo->flags &= ~MEM_Dyn;

  if( pTo->flags&(MEM_Str|MEM_Blob) ){
    if( 0==(pFrom->flags&MEM_Static) ){
      pTo->flags |= MEM_Ephem;
      rc = sqlite4VdbeMemMakeWriteable(pTo);
    }
  }

  return rc;
}

/*
** Transfer the contents of pFrom to pTo. Any existing value in pTo is
** freed. If pFrom contains ephemeral data, a copy is made.
**
** pFrom contains an SQL NULL when this routine returns.
*/
void sqlite4VdbeMemMove(Mem *pTo, Mem *pFrom){
  assert( pFrom->db==0 || sqlite4_mutex_held(pFrom->db->mutex) );
  assert( pTo->db==0 || sqlite4_mutex_held(pTo->db->mutex) );
  assert( pFrom->db==0 || pTo->db==0 || pFrom->db==pTo->db );

  sqlite4VdbeMemRelease(pTo);
  memcpy(pTo, pFrom, sizeof(Mem));
  pFrom->flags = MEM_Null;
  pFrom->xDel = 0;
  pFrom->zMalloc = 0;
}

/*
** Change the value of a Mem to be a string or a BLOB.
**
** The memory management strategy depends on the value of the xDel
** parameter. If the value passed is SQLITE4_TRANSIENT, then the 
** string is copied into a (possibly existing) buffer managed by the 
** Mem structure. Otherwise, any existing buffer is freed and the
** pointer copied.
**
** If the string is too large (if it exceeds the SQLITE4_LIMIT_LENGTH
** size limit) then no memory allocation occurs.  If the string can be
** stored without allocating memory, then it is.  If a memory allocation
** is required to store the string, then value of pMem is unchanged.  In
** either case, SQLITE4_TOOBIG is returned.
*/
int sqlite4VdbeMemSetStr(
  Mem *pMem,                /* Memory cell to set to string value */
  const char *z,            /* String pointer */
  int n,                    /* Bytes in string, or negative */
  u8 enc,                   /* Encoding of z.  0 for BLOBs */
  void (*xDel)(void*,void*),/* Destructor function */
  void *pDelArg             /* First argument to xDel() */
){
  int nByte = n;      /* New value for pMem->n */
  int iLimit;         /* Maximum allowed string or blob size */
  u16 flags = 0;      /* New value for pMem->flags */

  assert( pMem->db==0 || sqlite4_mutex_held(pMem->db->mutex) );
  assert( (pMem->flags & MEM_RowSet)==0 );

  /* If z is a NULL pointer, set pMem to contain an SQL NULL. */
  if( !z ){
    sqlite4VdbeMemSetNull(pMem);
    return SQLITE4_OK;
  }

  if( pMem->db ){
    iLimit = pMem->db->aLimit[SQLITE4_LIMIT_LENGTH];
  }else{
    iLimit = SQLITE4_MAX_LENGTH;
  }
  flags = (enc==0?MEM_Blob:MEM_Str);
  if( nByte<0 ){
    assert( enc!=0 );
    if( enc==SQLITE4_UTF8 ){
      for(nByte=0; nByte<=iLimit && z[nByte]; nByte++){}
    }else{
      for(nByte=0; nByte<=iLimit && (z[nByte] | z[nByte+1]); nByte+=2){}
    }
    flags |= MEM_Term;
  }

  /* The following block sets the new values of Mem.z and Mem.xDel. It
  ** also sets a flag in local variable "flags" to indicate the memory
  ** management (one of MEM_Dyn or MEM_Static).
  */
  if( xDel==SQLITE4_TRANSIENT ){
    int nAlloc = nByte;
    if( flags&MEM_Term ){
      nAlloc += (enc==SQLITE4_UTF8?1:2);
    }
    if( nByte>iLimit ){
      return SQLITE4_TOOBIG;
    }
    if( sqlite4VdbeMemGrow(pMem, nAlloc, 0) ){
      return SQLITE4_NOMEM;
    }
    memcpy(pMem->z, z, nAlloc);
  }else if( xDel==SQLITE4_DYNAMIC ){
    sqlite4VdbeMemRelease(pMem);
    pMem->zMalloc = pMem->z = (char *)z;
    pMem->xDel = 0;
  }else{
    sqlite4VdbeMemRelease(pMem);
    pMem->z = (char *)z;
    pMem->xDel = xDel;
    pMem->pDelArg = pDelArg;
    flags |= ((xDel==SQLITE4_STATIC)?MEM_Static:MEM_Dyn);
  }

  pMem->n = nByte;
  pMem->flags = flags;
  pMem->enc = (enc==0 ? SQLITE4_UTF8 : enc);
  pMem->type = (enc==0 ? SQLITE4_BLOB : SQLITE4_TEXT);

#ifndef SQLITE4_OMIT_UTF16
  if( pMem->enc!=SQLITE4_UTF8 && sqlite4VdbeMemHandleBom(pMem) ){
    return SQLITE4_NOMEM;
  }
#endif

  if( nByte>iLimit ){
    return SQLITE4_TOOBIG;
  }

  return SQLITE4_OK;
}

/*
** Compare the values contained by the two memory cells, returning
** negative, zero or positive if pMem1 is less than, equal to, or greater
** than pMem2. Sorting order is NULL's first, followed by numbers (integers
** and reals) sorted numerically, followed by text ordered by the collating
** sequence pColl and finally blob's ordered by memcmp().
**
** Two NULL values are considered equal by this function.
*/
int sqlite4MemCompare(
  Mem *pMem1, 
  Mem *pMem2, 
  const CollSeq *pColl,
  int *pRes                       /* OUT: Result of comparison operation */
){
  int rc = SQLITE4_OK;
  int f1, f2;
  int combined_flags;

  f1 = pMem1->flags;
  f2 = pMem2->flags;
  combined_flags = f1|f2;
  assert( (combined_flags & MEM_RowSet)==0 );
 
  /* If one value is NULL, it is less than the other. If both values
  ** are NULL, return 0.
  */
  if( combined_flags&MEM_Null ){
    *pRes = (f2&MEM_Null) - (f1&MEM_Null);
    return SQLITE4_OK;
  }

  /* If one value is a number and the other is not, the number is less.
  ** If both are numbers, compare as reals if one is a real, or as integers
  ** if both values are integers.
  */
  if( combined_flags&(MEM_Int|MEM_Real) ){
    if( !(f1&(MEM_Int|MEM_Real)) ){
      *pRes = 1;
    }else if( !(f2&(MEM_Int|MEM_Real)) ){
      *pRes = -1;
    }else{
      *pRes = (sqlite4_num_compare(pMem1->u.num, pMem2->u.num) - 2);
    }
    return SQLITE4_OK;
  }

  /* If one value is a string and the other is a blob, the string is less.
  ** If both are strings, compare using the collating functions.
  */
  if( combined_flags&MEM_Str ){

    if( (f1 & f2 & MEM_Str)==0 ){
      /* This branch is taken if one of the values is not a string. So, if
      ** f1 is a string, then f2 must be a blob. Return -1. Otherwise,
      ** if f2 is a string and f1 is a blob, return +1.  */
      *pRes = (f1 & MEM_Str) ? -1 : +1;
      return SQLITE4_OK;
    }

    assert( pMem1->enc==pMem2->enc );
    assert( pMem1->enc==SQLITE4_UTF8 || 
            pMem1->enc==SQLITE4_UTF16LE || pMem1->enc==SQLITE4_UTF16BE );

    /* The collation sequence must be defined at this point, even if
    ** the user deletes the collation sequence after the vdbe program is
    ** compiled (this was not always the case).
    */
    assert( !pColl || pColl->xCmp );

    if( pColl ){
      int enc = pMem1->enc;
      void *pUser = pColl->pUser;
      rc = pColl->xCmp(pUser, pMem1, pMem2, pRes);
      sqlite4VdbeChangeEncoding(pMem1, enc);
      sqlite4VdbeChangeEncoding(pMem2, enc);
      return rc;
    }

    /* If a NULL pointer was passed as the collate function, fall through
    ** to the blob case and use memcmp().  */
  }
 
  /* Both values must be blobs.  Compare using memcmp().  */
  *pRes = memcmp(pMem1->z, pMem2->z, (pMem1->n>pMem2->n)?pMem2->n:pMem1->n);
  if( *pRes==0 ){
    *pRes = pMem1->n - pMem2->n;
  }
  return rc;
}

/* This function is only available internally, it is not part of the
** external API. It works in a similar way to sqlite4_value_text(),
** except the data returned is in the encoding specified by the second
** parameter, which must be one of SQLITE4_UTF16BE, SQLITE4_UTF16LE or
** SQLITE4_UTF8.
*/
const void *sqlite4ValueText(sqlite4_value* pVal, u8 enc){
  if( !pVal ) return 0;

  assert( pVal->db==0 || sqlite4_mutex_held(pVal->db->mutex) );
  assert( (pVal->flags & MEM_RowSet)==0 );

  if( pVal->flags&MEM_Null ){
    return 0;
  }
  assert( (MEM_Blob>>3) == MEM_Str );
  pVal->flags |= (pVal->flags & MEM_Blob)>>3;
  if( pVal->flags&MEM_Str ){
    sqlite4VdbeChangeEncoding(pVal, enc);
    sqlite4VdbeMemNulTerminate(pVal); /* IMP: R-31275-44060 */
  }else{
    assert( (pVal->flags&MEM_Blob)==0 );
    sqlite4VdbeMemStringify(pVal, enc);
    assert( 0==(1&SQLITE4_PTR_TO_INT(pVal->z)) );
  }
  assert(pVal->enc==enc || pVal->db==0 || pVal->db->mallocFailed );
  if( pVal->enc==enc ){
    return pVal->z;
  }else{
    return 0;
  }
}

/*
** Create a new sqlite4_value object.
*/
sqlite4_value *sqlite4ValueNew(sqlite4 *db){
  Mem *p = sqlite4DbMallocZero(db, sizeof(*p));
  if( p ){
    p->flags = MEM_Null;
    p->type = SQLITE4_NULL;
    p->db = db;
  }
  return p;
}

/*
** Create a new sqlite4_value object, containing the value of pExpr.
**
** This only works for very simple expressions that consist of one constant
** token (i.e. "5", "5.1", "'a string'"). If the expression can
** be converted directly into a value, then the value is allocated and
** a pointer written to *ppVal. The caller is responsible for deallocating
** the value by passing it to sqlite4ValueFree() later on. If the expression
** cannot be converted to a value, then *ppVal is set to NULL.
*/
int sqlite4ValueFromExpr(
  sqlite4 *db,              /* The database connection */
  Expr *pExpr,              /* The expression to evaluate */
  u8 enc,                   /* Encoding to use */
  u8 affinity,              /* Affinity to use */
  sqlite4_value **ppVal     /* Write the new value here */
){
  int op;
  char *zVal = 0;
  sqlite4_value *pVal = 0;
  int negInt = 1;
  const char *zNeg = "";

  if( !pExpr ){
    *ppVal = 0;
    return SQLITE4_OK;
  }
  op = pExpr->op;

  /* op can only be TK_REGISTER if we have compiled with SQLITE4_ENABLE_STAT3.
  ** The ifdef here is to enable us to achieve 100% branch test coverage even
  ** when SQLITE4_ENABLE_STAT3 is omitted.
  */
#ifdef SQLITE4_ENABLE_STAT3
  if( op==TK_REGISTER ) op = pExpr->op2;
#else
  if( NEVER(op==TK_REGISTER) ) op = pExpr->op2;
#endif

  /* Handle negative integers in a single step.  This is needed in the
  ** case when the value is -9223372036854775808.
  */
  if( op==TK_UMINUS
   && (pExpr->pLeft->op==TK_INTEGER || pExpr->pLeft->op==TK_FLOAT) ){
    pExpr = pExpr->pLeft;
    op = pExpr->op;
    negInt = -1;
    zNeg = "-";
  }

  if( op==TK_STRING || op==TK_FLOAT || op==TK_INTEGER ){
    pVal = sqlite4ValueNew(db);
    if( pVal==0 ) goto no_mem;
    if( ExprHasProperty(pExpr, EP_IntValue) ){
      sqlite4VdbeMemSetInt64(pVal, (i64)pExpr->u.iValue*negInt);
    }else{
      zVal = sqlite4MPrintf(db, "%s%s", zNeg, pExpr->u.zToken);
      if( zVal==0 ) goto no_mem;
      sqlite4ValueSetStr(pVal, -1, zVal, SQLITE4_UTF8, SQLITE4_DYNAMIC, 0);
      if( op==TK_FLOAT ) pVal->type = SQLITE4_FLOAT;
    }
    if( (op==TK_INTEGER || op==TK_FLOAT ) && affinity==SQLITE4_AFF_NONE ){
      sqlite4ValueApplyAffinity(pVal, SQLITE4_AFF_NUMERIC, SQLITE4_UTF8);
    }else{
      sqlite4ValueApplyAffinity(pVal, affinity, SQLITE4_UTF8);
    }
    if( pVal->flags & (MEM_Int|MEM_Real) ) pVal->flags &= ~MEM_Str;
    if( enc!=SQLITE4_UTF8 ){
      sqlite4VdbeChangeEncoding(pVal, enc);
    }
  }else if( op==TK_UMINUS ) {
    /* This branch happens for multiple negative signs.  Ex: -(-5) */
    if( SQLITE4_OK==sqlite4ValueFromExpr(db,pExpr->pLeft,enc,affinity,&pVal) ){
      sqlite4VdbeMemNumerify(pVal);
      pVal->u.num = sqlite4_num_mul(pVal->u.num, sqlite4_num_from_int64(-1));
      sqlite4ValueApplyAffinity(pVal, affinity, enc);
    }
  }else if( op==TK_NULL ){
    pVal = sqlite4ValueNew(db);
    if( pVal==0 ) goto no_mem;
  }
#ifndef SQLITE4_OMIT_BLOB_LITERAL
  else if( op==TK_BLOB ){
    int nVal;
    assert( pExpr->u.zToken[0]=='x' || pExpr->u.zToken[0]=='X' );
    assert( pExpr->u.zToken[1]=='\'' );
    pVal = sqlite4ValueNew(db);
    if( !pVal ) goto no_mem;
    zVal = &pExpr->u.zToken[2];
    nVal = sqlite4Strlen30(zVal)-1;
    assert( zVal[nVal]=='\'' );
    sqlite4VdbeMemSetStr(pVal, sqlite4HexToBlob(db, zVal, nVal), nVal/2,
                         0, SQLITE4_DYNAMIC, 0);
  }
#endif

  if( pVal ){
    sqlite4VdbeMemStoreType(pVal);
  }
  *ppVal = pVal;
  return SQLITE4_OK;

no_mem:
  db->mallocFailed = 1;
  sqlite4DbFree(db, zVal);
  sqlite4ValueFree(pVal);
  *ppVal = 0;
  return SQLITE4_NOMEM;
}

/*
** Change the string value of an sqlite4_value object
*/
void sqlite4ValueSetStr(
  sqlite4_value *v,          /* Value to be set */
  int n,                     /* Length of string z */
  const void *z,             /* Text of the new string */
  u8 enc,                    /* Encoding to use */
  void (*xDel)(void*,void*), /* Destructor for the string */
  void *pDelArg              /* First argument to xDel() */
){
  if( v ) sqlite4VdbeMemSetStr((Mem *)v, z, n, enc, xDel, pDelArg);
}

/*
** Free an sqlite4_value object
*/
void sqlite4ValueFree(sqlite4_value *v){
  if( !v ) return;
  sqlite4VdbeMemRelease((Mem *)v);
  sqlite4DbFree(((Mem*)v)->db, v);
}

/*
** Return the number of bytes in the sqlite4_value object assuming
** that it uses the encoding "enc"
*/
int sqlite4ValueBytes(sqlite4_value *pVal, u8 enc){
  Mem *p = (Mem*)pVal;
  if( (p->flags & MEM_Blob)!=0 || sqlite4ValueText(pVal, enc) ){
    return p->n;
  }
  return 0;
}
