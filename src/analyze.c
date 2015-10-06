/*
** 2005 July 8
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code associated with the ANALYZE command.
**
** The ANALYZE command gather statistics about the content of tables
** and indices.  These statistics are made available to the query planner
** to help it make better decisions about how to perform queries.
**
** The following system tables are or have been supported:
**
**    CREATE TABLE sqlite_stat1(tbl, idx, stat);
**    CREATE TABLE sqlite_stat3(tbl, idx, nEq, nLt, nDLt, sample);
**
** The sqlite_stat3 table is only created if SQLITE4_ENABLE_STAT3 is
** defined.
**
** Format of sqlite_stat1:
**
** There is normally one row per index, with the index identified by the
** name in the idx column.  The tbl column is the name of the table to
** which the index belongs.  In each such row, the stat column will be
** a string consisting of a list of integers.  The first integer in this
** list is the number of rows in the index and in the table.  The second
** integer is the average number of rows in the index that have the same
** value in the first column of the index.  The third integer is the average
** number of rows in the index that have the same value for the first two
** columns.  The N-th integer (for N>1) is the average number of rows in 
** the index which have the same value for the first N-1 columns.  For
** a K-column index, there will be K+1 integers in the stat column.  If
** the index is unique, then the last integer will be 1.
**
** The list of integers in the stat column can optionally be followed
** by the keyword "unordered".  The "unordered" keyword, if it is present,
** must be separated from the last integer by a single space.  If the
** "unordered" keyword is present, then the query planner assumes that
** the index is unordered and will not use the index for a range query.
** 
** If the sqlite_stat1.idx column is NULL, then the sqlite_stat1.stat
** column contains a single integer which is the (estimated) number of
** rows in the table identified by sqlite_stat1.tbl.
**
** Format for sqlite_stat3:
**
** The sqlite_stat3 table may contain multiple entries for each index.
** The idx column names the index and the tbl column contains the name
** of the indexed table. If the idx and tbl columns are the same, then the 
** sample is of the PRIMARY KEY index. The sample column is a value taken 
** from the left-most column of the index encoded using the key-encoding. 
** The nEq column is the approximate number of entires in the index whose 
** left-most column exactly matches the sample. nLt is the approximate 
** number of entires whose left-most column is less than the sample. The 
** nDLt column is the approximate number of distinct left-most entries in 
** the index that are less than the sample.
**
** Future versions of SQLite might change to store a string containing
** multiple integers values in the nDLt column of sqlite_stat3.  The first
** integer will be the number of prior index entries that are distinct in
** the left-most column.  The second integer will be the number of prior index
** entries that are distinct in the first two columns.  The third integer
** will be the number of prior index entries that are distinct in the first
** three columns.  And so forth.  With that extension, the nDLt field is
** similar in function to the sqlite_stat1.stat field.
**
** There can be an arbitrary number of sqlite_stat3 entries per index.
** The ANALYZE command will typically generate sqlite_stat3 tables
** that contain between 10 and 40 samples which are distributed across
** the key space, though not uniformly, and which include samples with
** largest possible nEq values.
*/
#ifndef SQLITE4_OMIT_ANALYZE
#include "sqliteInt.h"

/*
** This routine generates code that opens the sqlite_stat1 table for
** writing with cursor iStatCur. If the library was built with the
** SQLITE4_ENABLE_STAT3 macro defined, then the sqlite_stat3 table is
** opened for writing using cursor (iStatCur+1)
**
** If the sqlite_stat1 tables does not previously exist, it is created.
** Similarly, if the sqlite_stat3 table does not exist and the library
** is compiled with SQLITE4_ENABLE_STAT3 defined, it is created. 
**
** Argument zWhere may be a pointer to a buffer containing a table name,
** or it may be a NULL pointer. If it is not NULL, then all entries in
** the sqlite_stat1 and (if applicable) sqlite_stat3 tables associated
** with the named table are deleted. If zWhere==0, then code is generated
** to delete all stat table entries.
*/
static void openStatTable(
  Parse *pParse,          /* Parsing context */
  int iDb,                /* The database we are looking in */
  int iStatCur,           /* Open the sqlite_stat1 table on this cursor */
  const char *zWhere,     /* Delete entries for this table or index */
  const char *zWhereType  /* Either "tbl" or "idx" */
){
  static const struct {
    const char *zName;
    const char *zCols;
  } aTable[] = {
    { "sqlite_stat1", "tbl,idx,stat" },
#ifdef SQLITE4_ENABLE_STAT3
    { "sqlite_stat3", "tbl,idx,neq,nlt,ndlt,sample" },
#endif
  };

  int aRoot[] = {0, 0};
  u8 aCreateTbl[] = {0, 0};

  int i;
  sqlite4 *db = pParse->db;
  Db *pDb;
  Vdbe *v = sqlite4GetVdbe(pParse);
  if( v==0 ) return;
  assert( sqlite4VdbeDb(v)==db );
  pDb = &db->aDb[iDb];

  /* Create new statistic tables if they do not exist, or clear them
  ** if they do already exist.
  */
  for(i=0; i<ArraySize(aTable); i++){
    const char *zTab = aTable[i].zName;
    Table *pStat;
    if( (pStat = sqlite4FindTable(db, zTab, pDb->zName))==0 ){
      /* The sqlite_stat[12] table does not exist. Create it. Note that a 
      ** side-effect of the CREATE TABLE statement is to leave the rootpage 
      ** of the new table in register pParse->regRoot. This is important 
      ** because the OpenWrite opcode below will be needing it. */
      pParse->pPKRoot = &aRoot[i];
      sqlite4NestedParse(pParse,
          "CREATE TABLE %Q.%s(%s)", pDb->zName, zTab, aTable[i].zCols
      );
      assert( pParse->nErr>0 || aRoot[i]>0 );
      pParse->pPKRoot = 0;
      aCreateTbl[i] = 1;
    }else{
      /* The table already exists. If zWhere is not NULL, delete all entries 
      ** associated with the table zWhere. If zWhere is NULL, delete the
      ** entire contents of the table. */
      Index *pPK = sqlite4FindPrimaryKey(pStat, 0);
      aRoot[i] = pPK->tnum;
      assert( aRoot[i]>0 );
      if( zWhere ){
        sqlite4NestedParse(pParse,
           "DELETE FROM %Q.%s WHERE %s=%Q", pDb->zName, zTab, zWhereType, zWhere
        );
      }else{
        /* The sqlite_stat[13] table already exists.  Delete all rows. */
        sqlite4VdbeAddOp2(v, OP_Clear, aRoot[i], iDb);
      }
    }
  }

  /* Open the sqlite_stat[13] tables for writing. */
  for(i=0; i<ArraySize(aTable); i++){
    sqlite4VdbeAddOp3(v, OP_OpenWrite, iStatCur+i, aRoot[i], iDb);
    sqlite4VdbeChangeP4(v, -1, (char *)3, P4_INT32);
    sqlite4VdbeChangeP5(v, aCreateTbl[i]);
  }
}

/*
** Recommended number of samples for sqlite_stat3
*/
#ifndef SQLITE4_STAT3_SAMPLES
# define SQLITE4_STAT3_SAMPLES 24
#endif

/*
** Three SQL functions - stat3_init(), stat3_push(), and stat3_get() -
** share an instance of the following structure to hold their state
** information.
*/
typedef struct Stat3Accum Stat3Accum;
struct Stat3Accum {
  tRowcnt nRow;             /* Number of rows in the entire table */
  tRowcnt nPSample;         /* How often to do a periodic sample */
  int iMin;                 /* Index of entry with minimum nEq and hash */
  int mxSample;             /* Maximum number of samples to accumulate */
  int nSample;              /* Current number of samples */
  u32 iPrn;                 /* Pseudo-random number used for sampling */
  struct Stat3Sample {
    void *pKey;                /* Index key for this sample */
    int nKey;                  /* Bytes of pKey in use */
    int nAlloc;                /* Bytes of space allocated at pKey */
    tRowcnt nEq;               /* sqlite_stat3.nEq */
    tRowcnt nLt;               /* sqlite_stat3.nLt */
    tRowcnt nDLt;              /* sqlite_stat3.nDLt */
    u8 isPSample;              /* True if a periodic sample */
    u32 iHash;                 /* Tiebreaker value (pseudo-random) */
  } *a;                     /* An array of samples */
};

#ifdef SQLITE4_ENABLE_STAT3

/*
** Delete a Stat3Accum object.
*/
static void delStat3Accum(void *pCtx, void *pDel){
  sqlite4 *db = (sqlite4*)pCtx;
  Stat3Accum *p = (Stat3Accum*)pDel;
  int i;

  for(i=0; i<p->nSample; i++){
    sqlite4DbFree(db, p->a[i].pKey);
  }
  sqlite4DbFree(db, p);
}

/*
** Implementation of the stat3_init(C,S) SQL function.  The two parameters
** are the number of rows in the table or index (C) and the number of samples
** to accumulate (S).
**
** This routine allocates the Stat3Accum object.
**
** The return value is the Stat3Accum object (P).
*/
static void stat3Init(
  sqlite4_context *context,
  int argc,
  sqlite4_value **argv
){
  sqlite4 *db = sqlite4_context_db_handle(context);
  Stat3Accum *p;
  tRowcnt nRow;
  int mxSample;
  int n;

  UNUSED_PARAMETER(argc);
  nRow = (tRowcnt)sqlite4_value_int64(argv[0]);
  mxSample = sqlite4_value_int(argv[1]);
  n = sizeof(*p) + sizeof(p->a[0])*mxSample;
  p = (Stat3Accum *)sqlite4DbMallocZero(db, n);
  if( p==0 ){
    sqlite4_result_error_nomem(context);
    return;
  }
  p->a = (struct Stat3Sample*)&p[1];
  p->nRow = nRow;
  p->mxSample = mxSample;
  p->nPSample = p->nRow/(mxSample/3+1) + 1;
  sqlite4_randomness(sqlite4_db_env(db), sizeof(p->iPrn), &p->iPrn);
  sqlite4_result_blob(context, p, sizeof(p), delStat3Accum, (void*)db);
}
static const FuncDef stat3InitFuncdef = {
  2,                /* nArg */
  0,                /* flags */
  0,                /* pUserData */
  0,                /* pNext */
  stat3Init,        /* xFunc */
  0,                /* xStep */
  0,                /* xFinalize */
  "stat3_init",     /* zName */
  0,                /* pHash */
  0                 /* pDestructor */
};


/*
** Implementation of the stat3_push(nEq,nLt,nDLt,idxkey,P) SQL function.  The
** arguments describe a single key instance.  This routine makes the 
** decision about whether or not to retain this key for the sqlite_stat3
** table.
**
** The return value is NULL.
*/
static void stat3Push(
  sqlite4_context *context,
  int argc,
  sqlite4_value **argv
){
  Stat3Accum *p = (Stat3Accum*)sqlite4_value_blob(argv[4], 0);
  tRowcnt nEq = sqlite4_value_int64(argv[0]);
  tRowcnt nLt = sqlite4_value_int64(argv[1]);
  tRowcnt nDLt = sqlite4_value_int64(argv[2]);
  const void *pKey;
  int nKey;
  u8 isPSample = 0;
  u8 doInsert = 0;
  int iMin = p->iMin;
  struct Stat3Sample *pSample;
  int i;
  u32 h;

  UNUSED_PARAMETER(context);
  UNUSED_PARAMETER(argc);
  if( nEq==0 ) return;
  h = p->iPrn = p->iPrn*1103515245 + 12345;
  if( (nLt/p->nPSample)!=((nEq+nLt)/p->nPSample) ){
    doInsert = isPSample = 1;
  }else if( p->nSample<p->mxSample ){
    doInsert = 1;
  }else{
    if( nEq>p->a[iMin].nEq || (nEq==p->a[iMin].nEq && h>p->a[iMin].iHash) ){
      doInsert = 1;
    }
  }
  if( !doInsert ) return;
  if( p->nSample==p->mxSample ){
    void *pKey = p->a[iMin].pKey;
    int nAlloc = p->a[iMin].nAlloc;
    assert( p->nSample - iMin - 1 >= 0 );
    memmove(&p->a[iMin], &p->a[iMin+1], sizeof(p->a[0])*(p->nSample-iMin-1));
    pSample = &p->a[p->nSample-1];
    memset(pSample, 0, sizeof(struct Stat3Sample));
    pSample->pKey = pKey;
    pSample->nAlloc = nAlloc;
  }else{
    pSample = &p->a[p->nSample++];
  }

  pKey = sqlite4_value_blob(argv[3], &nKey);
  if( nKey>pSample->nAlloc ){
    sqlite4 *db = sqlite4_context_db_handle(context);
    int nReq = nKey*4;
    pSample->pKey = sqlite4DbReallocOrFree(db, pSample->pKey, nReq);
    if( pSample->pKey==0 ) return;
    pSample->nAlloc = nReq;
  }
  memcpy(pSample->pKey, pKey, nKey);
  pSample->nKey = nKey;
  pSample->nEq = nEq;
  pSample->nLt = nLt;
  pSample->nDLt = nDLt;
  pSample->iHash = h;
  pSample->isPSample = isPSample;

  /* Find the new minimum */
  if( p->nSample==p->mxSample ){
    pSample = p->a;
    i = 0;
    while( pSample->isPSample ){
      i++;
      pSample++;
      assert( i<p->nSample );
    }
    nEq = pSample->nEq;
    h = pSample->iHash;
    iMin = i;
    for(i++, pSample++; i<p->nSample; i++, pSample++){
      if( pSample->isPSample ) continue;
      if( pSample->nEq<nEq
       || (pSample->nEq==nEq && pSample->iHash<h)
      ){
        iMin = i;
        nEq = pSample->nEq;
        h = pSample->iHash;
      }
    }
    p->iMin = iMin;
  }
}
static const FuncDef stat3PushFuncdef = {
  5,                /* nArg */
  0,                /* flags */
  0,                /* pUserData */
  0,                /* pNext */
  stat3Push,        /* xFunc */
  0,                /* xStep */
  0,                /* xFinalize */
  "stat3_push",     /* zName */
  0,                /* pHash */
  0                 /* pDestructor */
};

/*
** Implementation of the stat3_get(P,N,...) SQL function.  This routine is
** used to query the results.  Content is returned for the Nth sqlite_stat3
** row where N is between 0 and S-1 and S is the number of samples.  The
** value returned depends on the number of arguments.
**
**    CREATE TABLE sqlite_stat3(tbl, idx, nEq, nLt, nDLt, sample);

**   argc==2    result:  nEq
**   argc==3    result:  nLt
**   argc==4    result:  nDLt
**   argc==5    result:  sample
*/
static void stat3Get(
  sqlite4_context *context,
  int argc,
  sqlite4_value **argv
){
  int n = sqlite4_value_int(argv[1]);
  Stat3Accum *p = (Stat3Accum*)sqlite4_value_blob(argv[0], 0);

  assert( p!=0 );
  if( p->nSample<=n ) return;
  switch( argc ){
    case 2: sqlite4_result_int64(context, p->a[n].nEq);    break;
    case 3: sqlite4_result_int64(context, p->a[n].nLt);    break;
    case 4: sqlite4_result_int64(context, p->a[n].nDLt);   break;
    default: {
      assert( argc==5 );
      sqlite4_result_blob(
          context, p->a[n].pKey, p->a[n].nKey, SQLITE4_TRANSIENT, 0
      );
      break;
    }
  }
}
static const FuncDef stat3GetFuncdef = {
  -1,               /* nArg */
  0,                /* flags */
  0,                /* pUserData */
  0,                /* pNext */
  stat3Get,         /* xFunc */
  0,                /* xStep */
  0,                /* xFinalize */
  "stat3_get",      /* zName */
  0,                /* pHash */
  0                 /* pDestructor */
};
#endif /* SQLITE4_ENABLE_STAT3 */

/*
** Generate code to do an analysis of all indices associated with
** a single table.
*/
static void analyzeOneTable(
  Parse *pParse,   /* Parser context */
  Table *pTab,     /* Table whose indices are to be analyzed */
  Index *pOnlyIdx, /* If not NULL, only analyze this one index */
  int iStatCur,    /* Index of VdbeCursor that writes the sqlite_stat1 table */
  int iMem         /* Available memory locations begin here */
){
  sqlite4 *db = pParse->db;    /* Database handle */
  Index *pIdx;                 /* An index to being analyzed */
  int iIdxCur;                 /* Cursor open on index being analyzed */
  Vdbe *v;                     /* The virtual machine being built up */
  int i;                       /* Loop counter */
  int topOfLoop;               /* The top of the loop */
  int endOfLoop;               /* The end of the loop */
  int jZeroRows = -1;          /* Jump from here if number of rows is zero */
  int iDb;                     /* Index of database containing pTab */
  int regTabname = iMem++;     /* Register containing table name */
  int regIdxname = iMem++;     /* Register containing index name */
  int regStat1 = iMem++;       /* The stat column of sqlite_stat1 */
#ifdef SQLITE4_ENABLE_STAT3
  int regNumEq = regStat1;     /* Number of instances.  Same as regStat1 */
  int regNumLt = iMem++;       /* Number of keys less than regSample */
  int regNumDLt = iMem++;      /* Number of distinct keys less than regSample */
  int regSample = iMem++;      /* The next sample value */
  int regAccum = iMem++;       /* Register to hold Stat3Accum object */
  int regLoop = iMem++;        /* Loop counter */
  int regCount = iMem++;       /* Number of rows in the table or index */
  int regTemp1 = iMem++;       /* Intermediate register */
  int regTemp2 = iMem++;       /* Intermediate register */
  int regNewSample = iMem++;
  int once = 1;                /* One-time initialization */
  int iTabCur = pParse->nTab++; /* Table cursor */
  int addrEq;
#endif
  int regRec = iMem++;         /* Register holding completed record */
  int regTemp = iMem++;        /* Temporary use register */
  int regNewRowid = iMem++;    /* Rowid for the inserted record */

  v = sqlite4GetVdbe(pParse);
  if( v==0 || NEVER(pTab==0) ){
    return;
  }
  if( pTab->pIndex==0 ){
    /* Do not gather statistics on views or virtual tables */
    return;
  }
  if( sqlite4_strnicmp(pTab->zName, "sqlite_", 7)==0 ){
    /* Do not gather statistics on system tables */
    return;
  }
  iDb = sqlite4SchemaToIndex(db, pTab->pSchema);
  assert( iDb>=0 );
#ifndef SQLITE4_OMIT_AUTHORIZATION
  if( sqlite4AuthCheck(pParse, SQLITE4_ANALYZE, pTab->zName, 0,
      db->aDb[iDb].zName ) ){
    return;
  }
#endif

  iIdxCur = pParse->nTab++;
  sqlite4VdbeAddOp4(v, OP_String8, 0, regTabname, 0, pTab->zName, 0);
  for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
    int nCol;
    KeyInfo *pKey;
    int regCnt;                  /* Total number of rows in table. */
    int regPrev;                 /* Previous index key read from database */
    int aregCard;                /* Cardinality array registers */
#ifdef SQLITE4_ENABLE_STAT3
    int addrAddimm;              /* Address at top of stat3 output loop */
    int addrIsnull;              /* Another address within the stat3 loop */
#endif

    if( pOnlyIdx && pOnlyIdx!=pIdx ) continue;
    VdbeNoopComment((v, "Begin analysis of %s", pIdx->zName));
    nCol = pIdx->nColumn;
    pKey = sqlite4IndexKeyinfo(pParse, pIdx);
    if( iMem+1+(nCol*2)>pParse->nMem ){
      pParse->nMem = iMem+1+(nCol*2);
    }

    /* Open a cursor to the index to be analyzed. */
    assert( iDb==sqlite4SchemaToIndex(db, pIdx->pSchema) );
    sqlite4VdbeAddOp4(v, OP_OpenRead, iIdxCur, pIdx->tnum, iDb,
        (char *)pKey, P4_KEYINFO_HANDOFF);
    VdbeComment((v, "%s", pIdx->zName));

    /* Populate the register containing the index name. */
    sqlite4VdbeAddOp4(v, OP_String8, 0, regIdxname, 0, pIdx->zName, 0);

#ifdef SQLITE4_ENABLE_STAT3
    if( once ){
      once = 0;
      sqlite4OpenTable(pParse, iTabCur, iDb, pTab, OP_OpenRead);
    }

    sqlite4VdbeAddOp2(v, OP_Integer, 0, regNumEq);
    sqlite4VdbeAddOp2(v, OP_Integer, 0, regNumLt);
    sqlite4VdbeAddOp2(v, OP_Integer, 0, regNumDLt);

    assert( regAccum==regSample+1 );
    sqlite4VdbeAddOp3(v, OP_Null, 0, regSample, regAccum);
    assert( regTemp1==regCount+1 );
    sqlite4VdbeAddOp2(v, OP_Count, iIdxCur, regCount);
    sqlite4VdbeAddOp2(v, OP_Integer, SQLITE4_STAT3_SAMPLES, regTemp1);
    sqlite4VdbeAddOp4(v, OP_Function, 1, regCount, regAccum,
                      (char*)&stat3InitFuncdef, P4_FUNCDEF);
    sqlite4VdbeChangeP5(v, 2);
#endif /* SQLITE4_ENABLE_STAT3 */

    /* The block of memory cells initialized here is used as follows.
    **
    **    iMem:                
    **        The total number of rows in the table.
    **
    **    iMem+1:
    **        Previous record read from index.
    **
    **    iMem+1+1 .. iMem+1+nCol: 
    **        Number of distinct entries in index considering the 
    **        left-most N columns only, where N is between 1 and nCol, 
    **        inclusive.
    */
    regCnt = iMem;
    regPrev = iMem+1;
    aregCard = iMem+2;

    sqlite4VdbeAddOp2(v, OP_Integer, 0, regCnt);
    sqlite4VdbeAddOp2(v, OP_Null, 0, regPrev);
#ifdef SQLITE4_ENABLE_STAT3
    sqlite4VdbeAddOp2(v, OP_Null, 0, regSample);
#endif
    for(i=0; i<nCol; i++){
      sqlite4VdbeAddOp2(v, OP_Integer, 1, aregCard+i);
    }

    /* Start the analysis loop. This loop runs through all the entries in
    ** the index b-tree.  */
    endOfLoop = sqlite4VdbeMakeLabel(v);
    sqlite4VdbeAddOp2(v, OP_Rewind, iIdxCur, endOfLoop);
    topOfLoop = sqlite4VdbeCurrentAddr(v);
    sqlite4VdbeAddOp2(v, OP_AddImm, regCnt, 1);  /* Increment row counter */
    sqlite4VdbeAddOp4Int(v, OP_AnalyzeKey, iIdxCur, regPrev, aregCard, nCol);

#ifdef SQLITE4_ENABLE_STAT3
    sqlite4VdbeAddOp2(v, OP_RowKey, iIdxCur, regNewSample);
    sqlite4VdbeChangeP5(v, 1);
    addrEq = sqlite4VdbeAddOp3(v, OP_Eq, regNewSample, 0, regSample);
    addrIsnull = sqlite4VdbeAddOp2(v, OP_IsNull, regSample, 0);

    assert( regNumEq==regNumLt-1  && regNumEq==regNumDLt-2
         && regNumEq==regSample-3 && regNumEq==regAccum-4
    );
    sqlite4VdbeAddOp4(v, OP_Function, 1, regNumEq, regTemp2, 
        (char*)&stat3PushFuncdef, P4_FUNCDEF
    );
    sqlite4VdbeChangeP5(v, 5);
    sqlite4VdbeAddOp3(v, OP_Add, regNumEq, regNumLt, regNumLt);
    sqlite4VdbeAddOp2(v, OP_AddImm, regNumDLt, 1);

    sqlite4VdbeJumpHere(v, addrIsnull);
    sqlite4VdbeAddOp2(v, OP_Integer, 0, regNumEq);
    sqlite4VdbeAddOp2(v, OP_Copy, regNewSample, regSample);
    sqlite4VdbeJumpHere(v, addrEq);
    sqlite4VdbeAddOp2(v, OP_AddImm, regNumEq, 1);
#endif

    /* Always jump here after updating the iMem+1...iMem+1+nCol counters */
    sqlite4VdbeResolveLabel(v, endOfLoop);

    sqlite4VdbeAddOp2(v, OP_Next, iIdxCur, topOfLoop);
    sqlite4VdbeAddOp1(v, OP_Close, iIdxCur);

#ifdef SQLITE4_ENABLE_STAT3
    /* Push the last record (if any) to the accumulator. */
    sqlite4VdbeAddOp4(v, OP_Function, 1, regNumEq, regTemp2,
                      (char*)&stat3PushFuncdef, P4_FUNCDEF);
    sqlite4VdbeChangeP5(v, 5);

    /* This block codes a loop that iterates through all entries stored
    ** by the accumulator (the Stat3Accum object). 
    */
    sqlite4VdbeAddOp2(v, OP_Integer, -1, regLoop);
    addrAddimm = sqlite4VdbeAddOp2(v, OP_AddImm, regLoop, 1);
    for(i=0; i<4; i++){
      sqlite4VdbeAddOp3(v, OP_Function, 1, regAccum, regNumEq+i);
      sqlite4VdbeChangeP4(v, -1, (char*)&stat3GetFuncdef, P4_FUNCDEF);
      sqlite4VdbeChangeP5(v, i+2);
    }
    addrIsnull = sqlite4VdbeAddOp1(v, OP_IsNull, regNumEq);
    sqlite4VdbeAddOp4(v, OP_MakeRecord, regTabname, 6, regRec, "bbbbbb", 0);
    sqlite4VdbeAddOp2(v, OP_NewRowid, iStatCur+1, regNewRowid);
    sqlite4VdbeAddOp3(v, OP_Insert, iStatCur+1, regRec, regNewRowid);
    sqlite4VdbeAddOp2(v, OP_Goto, 0, addrAddimm);
    sqlite4VdbeJumpHere(v, addrIsnull);
#endif

    /* Store the results in sqlite_stat1.
    **
    ** The result is a single row of the sqlite_stat1 table.  The first
    ** two columns are the names of the table and index.  The third column
    ** is a string composed of a list of integer statistics about the
    ** index.  The first integer in the list is the total number of entries
    ** in the index.  There is one additional integer in the list for each
    ** column of the table.  This additional integer is a guess of how many
    ** rows of the table the index will select.  If D is the count of distinct
    ** values and K is the total number of rows, then the integer is computed
    ** as:
    **
    **        I = (K+D-1)/D
    **
    ** If K==0 then no entry is made into the sqlite_stat1 table.  
    ** If K>0 then it is always the case the D>0 so division by zero
    ** is never possible.
    */
    sqlite4VdbeAddOp2(v, OP_SCopy, iMem, regStat1);
    if( jZeroRows<0 ){
      jZeroRows = sqlite4VdbeAddOp1(v, OP_IfNot, iMem);
    }
    for(i=0; i<nCol; i++){
      sqlite4VdbeAddOp4(v, OP_String8, 0, regTemp, 0, " ", 0);
      sqlite4VdbeAddOp3(v, OP_Concat, regTemp, regStat1, regStat1);
      sqlite4VdbeAddOp3(v, OP_Add, iMem, aregCard+i, regTemp);
      sqlite4VdbeAddOp2(v, OP_AddImm, regTemp, -1);
      sqlite4VdbeAddOp3(v, OP_Divide, aregCard+i, regTemp, regTemp);
      sqlite4VdbeAddOp1(v, OP_ToInt, regTemp);
      sqlite4VdbeAddOp3(v, OP_Concat, regTemp, regStat1, regStat1);
    }
    sqlite4VdbeAddOp4(v, OP_MakeRecord, regTabname, 3, regRec, "aaa", 0);
    sqlite4VdbeAddOp2(v, OP_NewRowid, iStatCur, regNewRowid);
    sqlite4VdbeAddOp3(v, OP_Insert, iStatCur, regRec, regNewRowid);
  }

  sqlite4VdbeJumpHere(v, jZeroRows);
  jZeroRows = sqlite4VdbeAddOp0(v, OP_Goto);
  sqlite4VdbeAddOp2(v, OP_Null, 0, regIdxname);
  sqlite4VdbeAddOp4(v, OP_MakeRecord, regTabname, 3, regRec, "aaa", 0);
  sqlite4VdbeAddOp2(v, OP_NewRowid, iStatCur, regNewRowid);
  sqlite4VdbeAddOp3(v, OP_Insert, iStatCur, regRec, regNewRowid);
  if( pParse->nMem<regRec ) pParse->nMem = regRec;
  sqlite4VdbeJumpHere(v, jZeroRows);
}

/*
** Generate code that will cause the most recent index analysis to
** be loaded into internal hash tables where is can be used.
*/
static void loadAnalysis(Parse *pParse, int iDb){
  Vdbe *v = sqlite4GetVdbe(pParse);
  if( v ){
    sqlite4VdbeAddOp1(v, OP_LoadAnalysis, iDb);
  }
}

/*
** Generate code that will do an analysis of an entire database
*/
static void analyzeDatabase(Parse *pParse, int iDb){
  sqlite4 *db = pParse->db;
  Schema *pSchema = db->aDb[iDb].pSchema;    /* Schema of database iDb */
  HashElem *k;
  int iStatCur;
  int iMem;

  sqlite4BeginWriteOperation(pParse, 0, iDb);
  iStatCur = pParse->nTab;
  pParse->nTab += 3;
  openStatTable(pParse, iDb, iStatCur, 0, 0);
  iMem = pParse->nMem+1;
  for(k=sqliteHashFirst(&pSchema->tblHash); k; k=sqliteHashNext(k)){
    Table *pTab = (Table*)sqliteHashData(k);
    analyzeOneTable(pParse, pTab, 0, iStatCur, iMem);
  }
  loadAnalysis(pParse, iDb);
}

/*
** Generate code that will do an analysis of a single table in
** a database.  If pOnlyIdx is not NULL then it is a single index
** in pTab that should be analyzed.
*/
static void analyzeTable(Parse *pParse, Table *pTab, Index *pOnlyIdx){
  int iDb;
  int iStatCur;

  assert( pTab!=0 );
  iDb = sqlite4SchemaToIndex(pParse->db, pTab->pSchema);
  sqlite4BeginWriteOperation(pParse, 0, iDb);
  iStatCur = pParse->nTab;
  pParse->nTab += 3;
  if( pOnlyIdx ){
    openStatTable(pParse, iDb, iStatCur, pOnlyIdx->zName, "idx");
  }else{
    openStatTable(pParse, iDb, iStatCur, pTab->zName, "tbl");
  }
  analyzeOneTable(pParse, pTab, pOnlyIdx, iStatCur, pParse->nMem+1);
  loadAnalysis(pParse, iDb);
}

/*
** Generate code for the ANALYZE command.  The parser calls this routine
** when it recognizes an ANALYZE command.
**
**        ANALYZE                            -- 1
**        ANALYZE  <database>                -- 2
**        ANALYZE  ?<database>.?<tablename>  -- 3
**
** Form 1 causes all indices in all attached databases to be analyzed.
** Form 2 analyzes all indices the single database named.
** Form 3 analyzes all indices associated with the named table.
*/
void sqlite4Analyze(Parse *pParse, Token *pName1, Token *pName2){
  sqlite4 *db = pParse->db;
  int iDb;
  int i;
  char *z, *zDb;
  Table *pTab;
  Index *pIdx;
  Token *pTableName;

  /* Read the database schema. If an error occurs, leave an error message
  ** and code in pParse and return NULL. */
  if( SQLITE4_OK!=sqlite4ReadSchema(pParse) ){
    return;
  }

  assert( pName2!=0 || pName1==0 );
  if( pName1==0 ){
    /* Form 1:  Analyze everything */
    for(i=0; i<db->nDb; i++){
      if( i==1 ) continue;  /* Do not analyze the TEMP database */
      analyzeDatabase(pParse, i);
    }
  }else if( pName2->n==0 ){
    /* Form 2:  Analyze the database or table named */
    iDb = sqlite4FindDb(db, pName1);
    if( iDb>=0 ){
      analyzeDatabase(pParse, iDb);
    }else{
      z = sqlite4NameFromToken(db, pName1);
      if( z ){
        if( (pTab = sqlite4LocateTable(pParse, 0, z, 0))!=0 ){
          analyzeTable(pParse, pTab, 0);
        }else if( (pIdx = sqlite4FindIndex(db, z, 0))!=0 ){
          analyzeTable(pParse, pIdx->pTable, pIdx);
        }
        sqlite4DbFree(db, z);
      }
    }
  }else{
    /* Form 3: Analyze the fully qualified table name */
    iDb = sqlite4TwoPartName(pParse, pName1, pName2, &pTableName);
    if( iDb>=0 ){
      zDb = db->aDb[iDb].zName;
      z = sqlite4NameFromToken(db, pTableName);
      if( z ){
        if( (pTab = sqlite4LocateTable(pParse, 0, z, zDb))!=0 ){
          analyzeTable(pParse, pTab, 0);
        }else if( (pIdx = sqlite4FindIndex(db, z, zDb))!=0 ){
          analyzeTable(pParse, pIdx->pTable, pIdx);
        }
        sqlite4DbFree(db, z);
      }
    }   
  }
}

/*
** Used to pass information from the analyzer reader through to the
** callback routine.
*/
typedef struct analysisInfo analysisInfo;
struct analysisInfo {
  sqlite4 *db;
  const char *zDatabase;
};

/*
** This callback is invoked once for each index when reading the
** sqlite_stat1 table.  
**
**     argv[0] = name of the table
**     argv[1] = name of the index (might be NULL)
**     argv[2] = results of analysis - one integer for each column
**
** Entries for which argv[1]==NULL simply record the number of rows in
** the table.
*/
static int analysisLoader(
  void *pData,                    /* Pointer to analysisInfo structure */
  int nVal,                       /* Size of apVal[] array */
  sqlite4_value **apVal,          /* Values for current row */
  const char **NotUsed            /* Column names (not used by this function) */
){
  analysisInfo *pInfo = (analysisInfo*)pData;
  Index *pIndex;
  Table *pTable;
  int i, c, n;
  tRowcnt v;

  const char *zTab = sqlite4_value_text(apVal[0], 0);
  const char *zIdx = sqlite4_value_text(apVal[1], 0);
  const char *z = sqlite4_value_text(apVal[2], 0);

  assert( nVal==3 );
  UNUSED_PARAMETER2(NotUsed, nVal);

  if( zTab==0 || zIdx==0 || z==0 ) return 0;

  pTable = sqlite4FindTable(pInfo->db, zTab, pInfo->zDatabase);
  if( pTable==0 ){
    return 0;
  }
  pIndex = sqlite4FindIndex(pInfo->db, zIdx, pInfo->zDatabase);

  n = pIndex ? pIndex->nColumn : 0;
  for(i=0; *z && i<=n; i++){
    v = 0;
    while( (c=z[0])>='0' && c<='9' ){
      v = v*10 + c - '0';
      z++;
    }
    if( i==0 ) pTable->nRowEst = v;
    if( pIndex==0 ) break;
    pIndex->aiRowEst[i] = v;
    if( *z==' ' ) z++;
#if 0
    if( strcmp(z, "unordered")==0 ){
      pIndex->bUnordered = 1;
      break;
    }
#endif
  }
  return 0;
}

/*
** If the Index.aSample variable is not NULL, delete the aSample[] array
** and its contents.
*/
void sqlite4DeleteIndexSamples(sqlite4 *db, Index *pIdx){
#ifdef SQLITE4_ENABLE_STAT3
  sqlite4DbFree(db, pIdx->aSample);
  if( db && db->pnBytesFreed==0 ){
    pIdx->nSample = 0;
    pIdx->aSample = 0;
  }
#else
  UNUSED_PARAMETER(db);
  UNUSED_PARAMETER(pIdx);
#endif
}

#ifdef SQLITE4_ENABLE_STAT3
/*
** Load content from the sqlite_stat3 table into the Index.aSample[]
** arrays of all indices.
*/
static int loadStat3(sqlite4 *db, const char *zDb){
  int rc;                       /* Result codes from subroutines */
  sqlite4_stmt *pStmt = 0;      /* An SQL statement being run */
  char *zSql;                   /* Text of the SQL statement */
  Index *pPrevIdx = 0;          /* Previous index in the loop */
  int idx = 0;                  /* slot in pIdx->aSample[] for next sample */
  IndexSample *pSample;         /* A slot in pIdx->aSample[] */
  u8 *pSpace;                   /* Space for copy of all samples */

  assert( db->lookaside.bEnabled==0 );
  if( !sqlite4FindTable(db, "sqlite_stat3", zDb) ){
    return SQLITE4_OK;
  }

  zSql = sqlite4MPrintf(db, 
      "SELECT idx, count(*), sum(length(sample)) FROM %Q.sqlite_stat3"
      " GROUP BY idx", zDb);
  if( !zSql ){
    return SQLITE4_NOMEM;
  }
  rc = sqlite4_prepare(db, zSql, -1, &pStmt, 0);
  sqlite4DbFree(db, zSql);
  if( rc ) return rc;

  while( sqlite4_step(pStmt)==SQLITE4_ROW ){
    char *zIndex;   /* Index name */
    Index *pIdx;    /* Pointer to the index object */
    int nSample;    /* Number of samples */
    int nSpace;     /* Bytes of space required for all samples */
    int nAlloc;     /* Bytes of space to allocate */

    zIndex = (char *)sqlite4_column_text(pStmt, 0, 0);
    if( zIndex==0 ) continue;
    nSample = sqlite4_column_int(pStmt, 1);
    nSpace = sqlite4_column_int(pStmt, 2);
    pIdx = sqlite4FindIndex(db, zIndex, zDb);
    if( pIdx==0 ) continue;
    assert( pIdx->nSample==0 );
    nAlloc = nSample*sizeof(IndexSample) + nSpace;
    pIdx->nSample = nSample;
    pIdx->aSample = (IndexSample*)sqlite4DbMallocZero(db, nAlloc);
    pIdx->avgEq = pIdx->aiRowEst[1];
    if( pIdx->aSample==0 ){
      db->mallocFailed = 1;
      sqlite4_finalize(pStmt);
      return SQLITE4_NOMEM;
    }
  }
  rc = sqlite4_finalize(pStmt);
  if( rc ) return rc;

  zSql = sqlite4MPrintf(db, 
      "SELECT idx,neq,nlt,ndlt,sample FROM %Q.sqlite_stat3", zDb);
  if( !zSql ){
    return SQLITE4_NOMEM;
  }
  rc = sqlite4_prepare(db, zSql, -1, &pStmt, 0);
  sqlite4DbFree(db, zSql);
  if( rc ) return rc;

  while( sqlite4_step(pStmt)==SQLITE4_ROW ){
    char *zIndex;   /* Index name */
    Index *pIdx;    /* Pointer to the index object */
    int i;          /* Loop counter */
    tRowcnt sumEq;  /* Sum of the nEq values */
    const u8 *aVal;
    int nVal;

    zIndex = (char *)sqlite4_column_text(pStmt, 0, 0);
    if( zIndex==0 ) continue;
    pIdx = sqlite4FindIndex(db, zIndex, zDb);
    if( pIdx==0 ) continue;
    if( pIdx==pPrevIdx ){
      idx++;
    }else{
      pPrevIdx = pIdx;
      idx = 0;
      pSpace = (u8*)&pIdx->aSample[pIdx->nSample];
    }
    assert( idx<pIdx->nSample );
    pSample = &pIdx->aSample[idx];
    pSample->nEq = (tRowcnt)sqlite4_column_int64(pStmt, 1);
    pSample->nLt = (tRowcnt)sqlite4_column_int64(pStmt, 2);
    pSample->nDLt = (tRowcnt)sqlite4_column_int64(pStmt, 3);
    if( idx==pIdx->nSample-1 ){
      if( pSample->nDLt>0 ){
        for(i=0, sumEq=0; i<=idx-1; i++) sumEq += pIdx->aSample[i].nEq;
        pIdx->avgEq = (pSample->nLt - sumEq)/pSample->nDLt;
      }
      if( pIdx->avgEq<=0 ) pIdx->avgEq = 1;
    }

    aVal = sqlite4_column_blob(pStmt, 4, &nVal);
    pSample->aVal = pSpace;
    pSample->nVal = nVal;
    memcpy(pSample->aVal, aVal, nVal);
    pSpace += nVal;
  }
  return sqlite4_finalize(pStmt);
}
#endif /* SQLITE4_ENABLE_STAT3 */

/*
** Load the content of the sqlite_stat1 and sqlite_stat3 tables. The
** contents of sqlite_stat1 are used to populate the Index.aiRowEst[]
** arrays. The contents of sqlite_stat3 are used to populate the
** Index.aSample[] arrays.
**
** If the sqlite_stat1 table is not present in the database, SQLITE4_ERROR
** is returned. In this case, even if SQLITE4_ENABLE_STAT3 was defined 
** during compilation and the sqlite_stat3 table is present, no data is 
** read from it.
**
** If SQLITE4_ENABLE_STAT3 was defined during compilation and the 
** sqlite_stat3 table is not present in the database, SQLITE4_ERROR is
** returned. However, in this case, data is read from the sqlite_stat1
** table (if it is present) before returning.
**
** If an OOM error occurs, this function always sets db->mallocFailed.
** This means if the caller does not care about other errors, the return
** code may be ignored.
*/
int sqlite4AnalysisLoad(sqlite4 *db, int iDb){
  analysisInfo sInfo;
  HashElem *i;
  char *zSql;
  int rc;

  assert( iDb>=0 && iDb<db->nDb );
  assert( db->aDb[iDb].pKV!=0 );

  /* Clear any prior statistics */
  for(i=sqliteHashFirst(&db->aDb[iDb].pSchema->idxHash);i;i=sqliteHashNext(i)){
    Index *pIdx = sqliteHashData(i);
    sqlite4DefaultRowEst(pIdx);
#ifdef SQLITE4_ENABLE_STAT3
    sqlite4DeleteIndexSamples(db, pIdx);
    pIdx->aSample = 0;
#endif
  }

  /* Check to make sure the sqlite_stat1 table exists */
  sInfo.db = db;
  sInfo.zDatabase = db->aDb[iDb].zName;
  if( sqlite4FindTable(db, "sqlite_stat1", sInfo.zDatabase)==0 ){
    return SQLITE4_ERROR;
  }

  /* Load new statistics out of the sqlite_stat1 table */
  zSql = sqlite4MPrintf(db, 
      "SELECT tbl,idx,stat FROM %Q.sqlite_stat1", sInfo.zDatabase);
  if( zSql==0 ){
    rc = SQLITE4_NOMEM;
  }else{
    rc = sqlite4_exec(db, zSql, analysisLoader, &sInfo);
    sqlite4DbFree(db, zSql);
  }


  /* Load the statistics from the sqlite_stat3 table. */
#ifdef SQLITE4_ENABLE_STAT3
  if( rc==SQLITE4_OK ){
    int lookasideEnabled = db->lookaside.bEnabled;
    db->lookaside.bEnabled = 0;
    rc = loadStat3(db, sInfo.zDatabase);
    db->lookaside.bEnabled = lookasideEnabled;
  }
#endif

  if( rc==SQLITE4_NOMEM ){
    db->mallocFailed = 1;
  }
  return rc;
}


#endif /* SQLITE4_OMIT_ANALYZE */
