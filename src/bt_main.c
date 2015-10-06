/*
** 2013 September 14
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
*/

#include "btInt.h"
#include <string.h>
#include <assert.h>
#include <stddef.h>

#define BT_MAX_DEPTH 32           /* Maximum possible depth of tree */
#define BT_MAX_DIRECT_OVERFLOW 8  /* Maximum direct overflow pages per cell */

/* Maximum size of a key-prefix stored on an internal node. Parts of the
** code in this file assume that this value can be encoded as a single
** byte SQLite4 varint.  */
#define BT_MAX_INTERNAL_KEY 200   /* Maximum bytes of key on internal node */

/*
** Values that make up the single byte flags field at the start of
** b-tree pages. 
*/
#define BT_PGFLAGS_INTERNAL  0x01  /* True for non-leaf nodes */
#define BT_PGFLAGS_METATREE  0x02  /* True for a meta-tree page */
#define BT_PGFLAGS_SCHEDULE  0x04  /* True for a schedule-tree page */
#define BT_PGFLAGS_LARGEKEYS 0x08  /* True if keys larger than 200 bytes */

/*
** Maximum depth of fast-insert sub-trees.
*/
#define MAX_SUBTREE_DEPTH 8

/* #define BT_STDERR_DEBUG 1 */

typedef struct BtCursor BtCursor;
typedef struct FiCursor FiCursor;
typedef struct FiSubCursor FiSubCursor;

struct bt_db {
  sqlite4_env *pEnv;              /* SQLite environment */
  sqlite4_mm *pMM;                /* Memory allocator for pEnv */
  BtPager *pPager;                /* Underlying page-based database */
  bt_cursor *pAllCsr;             /* List of all open cursors */
  int nMinMerge;
  int nScheduleAlloc;
  int bFastInsertOp;              /* Set by CONTROL_FAST_INSERT_OP */

  BtCursor *pFreeCsr;
};

/*
** Overflow buffer is valid if nKey!=0.
*/
typedef struct BtOvfl BtOvfl;
struct BtOvfl {
  int nKey;
  int nVal;
  sqlite4_buffer buf;
};

/*
** Values that make up the bt_cursor.flags mask.
**
** CSR_NEXT_OK, CSR_PREV_OK:
**   These are only used by fast-insert cursors. The CSR_NEXT_OK flag is
**   set if xNext() may be safely called on the cursor. CSR_PREV_OK is
**   true if xPrev() is Ok.
**
** CSR_VISIT_DEL:
**   If this flag is set, do not skip over delete keys that occur in the
**   merged cursor output. This is used by checkpoint merges.
*/
#define CSR_TYPE_BT   0x0001
#define CSR_TYPE_FAST 0x0002
#define CSR_NEXT_OK   0x0004
#define CSR_PREV_OK   0x0008
#define CSR_VISIT_DEL 0x0010

#define IsBtCsr(pCsr) (((pCsr)->flags & CSR_TYPE_BT)!=0)

/* 
** Base class for both cursor types (BtCursor and FiCursor).
*/
struct bt_cursor {
  int flags;                      /* Cursor flags */
  void *pExtra;                   /* Extra allocated space */
  bt_db *pDb;                     /* Database this cursor belongs to */
  bt_cursor *pNextCsr;            /* Next cursor opened by same db handle */
};

/*
** Database b-tree cursor handle.
*/
struct BtCursor {
  bt_cursor base;                 /* Base cursor class */

  u32 iRoot;                      /* Root page of b-tree this cursor queries */
  int nPg;                        /* Number of valid entries in apPage[] */
  BtOvfl ovfl;                    /* Overflow cache (see above) */

  int bRequireReseek;             /* True if a btCsrReseek() is required */
  int bSkipNext;                  /* True if next CsrNext() is a no-op */
  int bSkipPrev;                  /* True if next CsrPrev() is a no-op */

  BtCursor *pNextFree;            /* Next in list of free BtCursor structures */

  int aiCell[BT_MAX_DEPTH];       /* Current cell of each apPage[] entry */
  BtPage *apPage[BT_MAX_DEPTH];   /* All pages from root to current leaf */
};

/*
** Database f-tree cursor handle.
*/
struct FiSubCursor {
  u8 aPrefix[8];                  /* Meta-tree key prefix for this age/level */
  BtCursor mcsr;                  /* Cursor opened on meta table (scans only) */
  BtCursor csr;                   /* Cursor opened on current sub-tree */
};
struct FiCursor {
  bt_cursor base;                 /* Base cursor class */
  int iBt;                        /* Current sub-tree (or -1 for EOF) */
  int nBt;                        /* Number of entries in aBt[] array */
  FiSubCursor *aSub;              /* Array of sub-tree cursors */
};

/*
** TODO: Rearrange things so these are not required!
*/
static int fiLoadSummary(bt_db *, BtCursor *, const u8 **, int *);
static void btReadSummary(const u8 *, int, u16 *, u16 *, u16 *);
static int btCsrData(BtCursor *, int, int, const void **, int *);
static int btReadSchedule(bt_db *, u8 *, BtSchedule *);
static int btCsrEnd(BtCursor *pCsr, int bLast);
static int btCsrStep(BtCursor *pCsr, int bNext);
static int btCsrKey(BtCursor *pCsr, const void **ppK, int *pnK);
void sqlite4BtDebugFastTree(bt_db *db, int iCall);


/* 
** Meta-table summary key.
*/
static const u8 aSummaryKey[] = {0xFF, 0xFF, 0xFF, 0xFF};

#ifndef btErrorBkpt
int btErrorBkpt(int rc){
  static int error_cnt = 0;
  error_cnt++;
  return rc;
}
#endif

#if !defined(NDEBUG) 
static void btCheckPageRefs(bt_db *pDb){
  int nActual = 0;                /* Outstanding refs according to pager */
  int nExpect = 0;                /* According to the set of open cursors */
  bt_cursor *pCsr;                /* Iterator variable */

  for(pCsr=pDb->pAllCsr; pCsr; pCsr=pCsr->pNextCsr){
    if( IsBtCsr(pCsr) ){
      BtCursor *p = (BtCursor*)pCsr;
      if( p->nPg>0 ) nExpect += p->nPg;
    }else{
      FiCursor *p = (FiCursor*)pCsr;
      int i;
      for(i=0; i<p->nBt; i++){
        if( p->aSub[i].mcsr.nPg>0 ) nExpect += p->aSub[i].mcsr.nPg;
        if( p->aSub[i].csr.nPg>0 ) nExpect += p->aSub[i].csr.nPg;
      }
    }
  }
  nActual = sqlite4BtPagerRefcount(pDb->pPager);
  assert( nActual==nExpect );
}
#else
# define btCheckPageRefs(x) 
#endif

/*
** Interpret the first 4 bytes of the buffer indicated by the first 
** parameter as a 32-bit unsigned big-endian integer.
*/
u32 sqlite4BtGetU32(const u8 *a){
  return ((u32)a[0] << 24) + ((u32)a[1] << 16) + ((u32)a[2] << 8) + ((u32)a[3]);
}
#define btGetU32(x) sqlite4BtGetU32(x)

/*
** Interpret the first 2 bytes of the buffer indicated by the first 
** parameter as a 16-bit unsigned big-endian integer.
*/
static u16 btGetU16(const u8 *a){
  return ((u32)a[0] << 8) + (u32)a[1];
}

/*
** Write the value passed as the second argument to the buffer passed
** as the first. Formatted as an unsigned 16-bit big-endian integer.
*/
static void btPutU16(u8 *a, u16 i){
  a[0] = (u8)((i>>8) & 0xFF);
  a[1] = (u8)((i>>0) & 0xFF);
}

/*
** Write the value passed as the second argument to the buffer passed
** as the first. Formatted as an unsigned 32-bit big-endian integer.
*/
void sqlite4BtPutU32(u8 *a, u32 i){
  a[0] = (u8)((i>>24) & 0xFF);
  a[1] = (u8)((i>>16) & 0xFF);
  a[2] = (u8)((i>>8) & 0xFF);
  a[3] = (u8)((i>>0) & 0xFF);
}
#define btPutU32(x,y) sqlite4BtPutU32(x,y)

struct FakePage { u8 *aData; };
#define btPageData(pPg) (((struct FakePage*)(pPg))->aData)

/*
** Allocate a new database handle.
*/
int sqlite4BtNew(sqlite4_env *pEnv, int nExtra, bt_db **ppDb){
  static const int MIN_MERGE = 2;
  static const int SCHEDULE_ALLOC = 4;

  bt_db *db = 0;                  /* New database object */
  BtPager *pPager = 0;            /* Pager object for this database */
  int nReq;                       /* Bytes of space required for bt_db object */
  int rc;                         /* Return code */

  nReq = sizeof(bt_db);
  rc = sqlite4BtPagerNew(pEnv, nExtra + nReq, &pPager);
  if( rc==SQLITE4_OK ){
    db = (bt_db*)sqlite4BtPagerExtra(pPager);
    db->pPager = pPager;
    db->pEnv = pEnv;

    db->nMinMerge = MIN_MERGE;
    db->nScheduleAlloc = SCHEDULE_ALLOC;
  }

  *ppDb = db;
  return rc;
}

/*
** Close an existing database handle. Once this function has been 
** called, the handle may not be used for any purpose.
*/
int sqlite4BtClose(bt_db *db){
  if( db ){
    BtCursor *pCsr;
    BtCursor *pNext;
    for(pCsr=db->pFreeCsr; pCsr; pCsr=pNext){
      pNext = pCsr->pNextFree;
      sqlite4_free(db->pEnv, pCsr);
    }
    sqlite4BtPagerClose(db->pPager);
  }
  return SQLITE4_OK;
}

/*
** Return a pointer to the nExtra bytes of space allocated along with 
** the database handle. 
*/
void *sqlite4BtExtra(bt_db *db){
  return (void*)&db[1];
}

int sqlite4BtOpen(bt_db *db, const char *zFilename){
  int rc;
  sqlite4_env_config(db->pEnv, SQLITE4_ENVCONFIG_GETMM, &db->pMM);
  rc = sqlite4BtPagerOpen(db->pPager, zFilename);
  return rc;
}

int sqlite4BtBegin(bt_db *db, int iLevel){
  int rc;
  rc = sqlite4BtPagerBegin(db->pPager, iLevel);
  return rc;
}

int sqlite4BtCommit(bt_db *db, int iLevel){
  int rc;
  rc = sqlite4BtPagerCommit(db->pPager, iLevel);
  return rc;
}

int sqlite4BtRevert(bt_db *db, int iLevel){
  int rc;
  rc = sqlite4BtPagerRevert(db->pPager, iLevel);
  return rc;
}

int sqlite4BtRollback(bt_db *db, int iLevel){
  int rc;
  rc = sqlite4BtPagerRollback(db->pPager, iLevel);
  return rc;
}

int sqlite4BtTransactionLevel(bt_db *db){
  return sqlite4BtPagerTransactionLevel(db->pPager);
}

static void btCsrSetup(bt_db *db, u32 iRoot, BtCursor *pCsr){
  memset(pCsr, 0, offsetof(BtCursor, aiCell));
  pCsr->base.flags = CSR_TYPE_BT;
  pCsr->base.pExtra = (void*)&pCsr[1];
  pCsr->base.pDb = db;
  pCsr->iRoot = iRoot;
  pCsr->ovfl.buf.pMM = db->pMM;
}

int sqlite4BtCsrOpen(bt_db *db, int nExtra, bt_cursor **ppCsr){
  int rc = SQLITE4_OK;            /* Return Code */
  bt_cursor *pRet = 0;

  assert( sqlite4BtPagerTransactionLevel(db->pPager)>0 );

  if( db->bFastInsertOp ){
    int nByte = sizeof(FiCursor) + nExtra;
    FiCursor *pCsr;

    pCsr = (FiCursor*)sqlite4_malloc(db->pEnv, nByte);
    if( pCsr==0 ){
      rc = btErrorBkpt(SQLITE4_NOMEM);
    }else{
      memset(pCsr, 0, nByte);
      pCsr->base.flags = CSR_TYPE_FAST;
      pCsr->base.pExtra = (void*)&pCsr[1];
      pCsr->base.pDb = db;
      pRet = (bt_cursor*)pCsr;
    }

  }else{
    BtCursor *pCsr;                /* New cursor object */
    u32 iRoot = sqlite4BtPagerDbhdr(db->pPager)->iRoot;
    
    if( db->pFreeCsr ){
      pCsr = db->pFreeCsr;
      db->pFreeCsr = pCsr->pNextFree;
    }else{
      int nByte = sizeof(BtCursor) + nExtra;
      pCsr = (BtCursor*)sqlite4_malloc(db->pEnv, nByte);
      if( pCsr==0 ){
        rc = btErrorBkpt(SQLITE4_NOMEM);
        goto csr_open_out;
      }
    }

    btCsrSetup(db, iRoot, pCsr);
    pRet = (bt_cursor*)pCsr;
  }

  assert( (pRet==0)==(rc!=SQLITE4_OK) );
  if( rc==SQLITE4_OK ){
    pRet->pNextCsr = db->pAllCsr;
    db->pAllCsr = pRet;
  }

 csr_open_out:
  *ppCsr = pRet;
  btCheckPageRefs(db);
  db->bFastInsertOp = 0;
  return rc;
}

static void btCsrReleaseAll(BtCursor *pCsr){
  int i;
  for(i=0; i<pCsr->nPg; i++){
    sqlite4BtPageRelease(pCsr->apPage[i]);
  }
  pCsr->nPg = 0;
}


static void btCsrReset(BtCursor *pCsr, int bFreeBuffer){
  btCsrReleaseAll(pCsr);
  if( bFreeBuffer ){
    sqlite4_buffer_clear(&pCsr->ovfl.buf);
  }
  pCsr->bSkipNext = 0;
  pCsr->bSkipPrev = 0;
  pCsr->bRequireReseek = 0;
  pCsr->ovfl.nKey = 0;
}

static void fiCsrReset(FiCursor *pCsr){
  int i;
  bt_db *db = pCsr->base.pDb;
  for(i=0; i<pCsr->nBt; i++){
    btCsrReset(&pCsr->aSub[i].csr, 1);
    btCsrReset(&pCsr->aSub[i].mcsr, 1);
  }
  sqlite4_free(db->pEnv, pCsr->aSub);
  pCsr->aSub = 0;
  pCsr->nBt = 0;
  pCsr->iBt = -1;
}

int sqlite4BtCsrClose(bt_cursor *pCsr){
  if( pCsr ){
    bt_cursor **pp;
    bt_db *pDb = pCsr->pDb;

    btCheckPageRefs(pDb);

    /* Remove this cursor from the all-cursors list. */
    for(pp=&pDb->pAllCsr; *pp!=pCsr; pp=&(*pp)->pNextCsr);
    *pp = pCsr->pNextCsr;

    if( IsBtCsr(pCsr) ){
      /* A regular b-tree cursor */
      BtCursor *p = (BtCursor*)pCsr;
      btCsrReset(p, 1);
      p->pNextFree = pDb->pFreeCsr;
      pDb->pFreeCsr = p;
    }else{
      /* A fast-insert-tree cursor */
      fiCsrReset((FiCursor*)pCsr);
      sqlite4_free(pDb->pEnv, pCsr);
    }
    btCheckPageRefs(pDb);
  }
  return SQLITE4_OK;
}

void *sqlite4BtCsrExtra(bt_cursor *pCsr){
  return pCsr->pExtra;
}

/*
** Set pCsr->apPage[pCsr->nPg] to a reference to database page pgno.
*/
static int btCsrDescend(BtCursor *pCsr, u32 pgno, BtPage **ppPg){
  int rc;
  if( pCsr->nPg>=BT_MAX_DEPTH ){
    rc = btErrorBkpt(SQLITE4_CORRUPT);
  }else{
    assert( pCsr->nPg>=0 );
    rc = sqlite4BtPageGet(pCsr->base.pDb->pPager, pgno, ppPg);
    assert( ((*ppPg)==0)==(rc!=SQLITE4_OK) );
    pCsr->apPage[pCsr->nPg] = *ppPg;
    pCsr->nPg++;
  }
  return rc;
}

/*
** Move the cursor from the current page to the parent. Return 
** SQLITE4_NOTFOUND if the cursor already points to the root page,
** or SQLITE4_OK otherwise.
*/
static int btCsrAscend(BtCursor *pCsr, int nLvl){
  int i;
  for(i=0; i<nLvl && ( pCsr->nPg>0 ); i++){
    pCsr->nPg--;
    sqlite4BtPageRelease(pCsr->apPage[pCsr->nPg]);
    pCsr->apPage[pCsr->nPg] = 0;
  }
  return (pCsr->nPg==0 ? SQLITE4_NOTFOUND : SQLITE4_OK);
}

/**************************************************************************
** The functions in this section are used to extract data from buffers
** containing formatted b-tree pages. They do not entirely encapsulate all
** page format details, but go some way to doing so.
*/

static int btCellCount(const u8 *aData, int nData){
  return (int)btGetU16(&aData[nData-2]);
}

static int btFreeSpace(const u8 *aData, int nData){
  return (int)btGetU16(&aData[nData-4]);
}

static int btFreeOffset(const u8 *aData, int nData){
  return (int)btGetU16(&aData[nData-6]);
}

static int btFreeContiguous(const u8 *aData, int nData){
  int nCell = btCellCount(aData, nData);
  return nData - btFreeOffset(aData, nData) - (3+nCell)*2;
}

static u8 btFlags(const u8 *aData){
  return aData[0];
}

static u8 *btCellFind(u8 *aData, int nData, int iCell){
  int iOff = btGetU16(&aData[nData - 6 - iCell*2 - 2]);
  return &aData[iOff];
}

/*
** Return a pointer to the big-endian u16 field that contains the 
** pointer to cell iCell.
*/
static u8* btCellPtrFind(u8 *aData, int nData, int iCell){
  return &aData[nData - 6 - iCell*2 - 2];
}

/*
** Parameters aData and nData describe a buffer containing an internal
** b-tree node. The page number of the iCell'th leftmost child page
** is returned.
*/
static u32 btChildPgno(u8 *aData, int nData, int iCell){
  u32 pgno;                       /* Return value */
  int nCell = btCellCount(aData, nData);

  if( iCell>=nCell ){
    pgno = btGetU32(&aData[1]);
  }else{
    int nKey;
    u8 *pCell = btCellFind(aData, nData, iCell);
    pCell += sqlite4BtVarintGet32(pCell, &nKey);
    pCell += nKey;
    pgno = btGetU32(pCell);
  }

  return pgno;
}

/*
**************************************************************************/

void sqlite4BtBufAppendf(sqlite4_buffer *pBuf, const char *zFormat, ...){
  char *zAppend;
  va_list ap;

  va_start(ap, zFormat);
  zAppend = sqlite4_vmprintf(0, zFormat, ap);
  va_end(ap);

  sqlite4_buffer_append(pBuf, zAppend, strlen(zAppend));
  sqlite4_free(0, zAppend);
}

#include <ctype.h>

void btBufferAppendBlob(
  sqlite4_buffer *pBuf, 
  int bAscii, 
  u8 *pBlob, int nBlob
){
  int j;
  for(j=0; j<nBlob; j++){
    if( bAscii ){
      sqlite4BtBufAppendf(pBuf, "%c", isalnum((int)pBlob[j]) ? pBlob[j] : '.');
    }else{
      sqlite4BtBufAppendf(pBuf, "%02X", (int)pBlob[j]);
    }
  }
}

/*
** Append a human-readable interpretation of the b-tree page in aData/nData
** to buffer pBuf.
*/
static void btPageToAscii(
  u32 pgno,                       /* Page number */
  int bAscii,                     /* True to print keys and values as ASCII */
  BtPager *pPager,                /* Pager object (or NULL) */
  u8 *aData, int nData,           /* Buffer containing page data */
  sqlite4_buffer *pBuf            /* Output buffer */
){
  BtDbHdr *pHdr = 0;
  int i;
  int nCell = (int)btCellCount(aData, nData);
  u8 flags = btFlags(aData);      /* Page flags */

  sqlite4BtBufAppendf(pBuf, "Page %d: ", pgno);

  if( pPager ) pHdr = sqlite4BtPagerDbhdr(pPager);
  if( pHdr && pgno==pHdr->iSRoot ){
    int i;
    BtSchedule s;
    sqlite4BtBufAppendf(pBuf, "(schedule page) ");
    btReadSchedule(0, aData, &s);

    sqlite4BtBufAppendf(pBuf, "  eBusy=(%s)\n",
        s.eBusy==BT_SCHEDULE_EMPTY ? "empty" :
        s.eBusy==BT_SCHEDULE_BUSY ? "busy" :
        s.eBusy==BT_SCHEDULE_DONE ? "done" : "!ErroR"
    );
    sqlite4BtBufAppendf(pBuf, "  iAge=%d\n", (int)s.iAge);
    sqlite4BtBufAppendf(pBuf, "  iMinLevel=%d\n", (int)s.iMinLevel);
    sqlite4BtBufAppendf(pBuf, "  iMaxLevel=%d\n", (int)s.iMaxLevel);
    sqlite4BtBufAppendf(pBuf, "  iOutLevel=%d\n", (int)s.iOutLevel);
    sqlite4BtBufAppendf(pBuf, "  aBlock=(");
    for(i=0; s.aBlock[i] && i<array_size(s.aBlock); i++){
      sqlite4BtBufAppendf(pBuf, "%s%d", i==0 ? "" : " ", (int)s.aBlock[i]);
    }
    sqlite4BtBufAppendf(pBuf, ")\n");

    sqlite4BtBufAppendf(pBuf, "  iNextPg=%d\n", (int)s.iNextPg);
    sqlite4BtBufAppendf(pBuf, "  iNextCell=%d\n", (int)s.iNextCell);
    sqlite4BtBufAppendf(pBuf, "  iFreeList=%d\n", (int)s.iFreeList);

    sqlite4BtBufAppendf(pBuf, "  aRoot=(");
    for(i=0; s.aBlock[i] && i<array_size(s.aBlock); i++){
      sqlite4BtBufAppendf(pBuf, "%s%d", i==0 ? "" : " ", (int)s.aRoot[i]);
    }
    sqlite4BtBufAppendf(pBuf, ")\n");

  }else{
    sqlite4BtBufAppendf(pBuf, "nCell=%d ", nCell);
    sqlite4BtBufAppendf(pBuf, "iFree=%d ", (int)btFreeOffset(aData, nData));
    sqlite4BtBufAppendf(pBuf, "flags=%d ", (int)btFlags(aData));
    if( btFlags(aData) & BT_PGFLAGS_INTERNAL ){
      sqlite4BtBufAppendf(pBuf, "rchild=%d ", (int)btGetU32(&aData[1]));
    }
    sqlite4BtBufAppendf(pBuf, "cell-offsets=(");
    for(i=0; i<nCell; i++){
      u8 *ptr = btCellPtrFind(aData, nData, i);
      sqlite4BtBufAppendf(pBuf, "%s%d", i==0?"":" ", (int)btGetU16(ptr));
    }
    sqlite4BtBufAppendf(pBuf, ")\n");

    for(i=0; i<nCell; i++){
      u8 *pCell;          /* Cell i */
      int nKey;           /* Number of bytes of key to output */
      u8 *pKey;           /* Buffer containing key. */
      int nVal;           /* Number of bytes of value to output */
      int nDummy;         /* Unused */
      u8 *pVal = 0;       /* Buffer containing value. */
      char celltype = 'A';

      pCell = btCellFind(aData, nData, i);
      pCell += sqlite4BtVarintGet32(pCell, &nKey);
      if( flags & BT_PGFLAGS_INTERNAL ){
        celltype = 'I';
      }else{
        if( nKey==0 ){
          celltype = 'C';
          pCell += sqlite4BtVarintGet32(pCell, &nKey);
        }else if( pCell[nKey]==0 ){
          celltype = 'B';
        }
      }
      sqlite4BtBufAppendf(pBuf, "  Cell %d: [%c] ", i, celltype);

      pKey = pCell;
      pCell += nKey;
      btBufferAppendBlob(pBuf, bAscii, pKey, nKey);
      sqlite4BtBufAppendf(pBuf, "  ");

      switch( celltype ){
        case 'I':
          sqlite4BtBufAppendf(pBuf, "child=%d ", (int)btGetU32(pCell));
          break;

        case 'A':
          pCell += sqlite4BtVarintGet32(pCell, &nVal);
          if( nVal>=2 ){
            nVal -= 2;
            pVal = pCell;
          }else{
            sqlite4BtBufAppendf(pBuf, "delete-key");
          }
          break;

        case 'B':
          assert( pCell[0]==0x00 );
          pCell++;
          pCell += sqlite4BtVarintGet32(pCell, &nVal);
          pVal = pCell;
          pCell += nVal;
          pCell += sqlite4BtVarintGet32(pCell, &nDummy);
          break;

        case 'C':
          pVal = 0;
          break;
      }

      if( pVal ){
        btBufferAppendBlob(pBuf, bAscii, pVal, nVal);
        if( flags & BT_PGFLAGS_METATREE ){
          /* Interpret the meta-tree entry */
          if( nKey==sizeof(aSummaryKey) && 0==memcmp(pKey, aSummaryKey, nKey) ){
            u16 iMin, nLvl, iMerge;
            int j;
            sqlite4BtBufAppendf(pBuf, "  [summary:");
            for(j=0; j<(nVal/6); j++){
              btReadSummary(pVal, j, &iMin, &nLvl, &iMerge);
              sqlite4BtBufAppendf(pBuf, " %d/%d/%d", 
                  (int)iMin, (int)nLvl, (int)iMerge
              );
            }
            sqlite4BtBufAppendf(pBuf, "]");
            
          }else{
            int nPgPerBlk = (pHdr->blksz / pHdr->pgsz);
            u32 iAge = btGetU32(&pKey[0]);
            u32 iLevel = ~btGetU32(&pKey[4]);
            u32 iRoot = btGetU32(pVal);
            sqlite4BtBufAppendf(pBuf, "  [age=%d level=%d root=%d]", 
                (int)iAge, (int)iLevel, (int)iRoot
            );
            sqlite4BtBufAppendf(pBuf, "  (blk=%d)", 1 + (iRoot / nPgPerBlk));
          }
        }
      }

      if( celltype=='B' ){
        int j;
        u8 ctrl = *pCell++;
        int nDirect = (ctrl & 0x0F);
        int nDepth = ((ctrl>>4) & 0x0F);
        sqlite4BtBufAppendf(
            pBuf, "  [overflow: direct=%d depth=%d]", nDirect, nDepth);
        for(j=0; j<=nDirect; j++){
          sqlite4BtBufAppendf(pBuf, " %d", btGetU32(&pCell[j*4]));
        }
      }
      sqlite4BtBufAppendf(pBuf, "\n");
    }

    if( pPager && btFlags(aData) & BT_PGFLAGS_INTERNAL ){
      for(i=0; i<=btCellCount(aData, nData); i++){
        BtPage *pChild;
        u8 *aChild;
        u32 child;

        child = btChildPgno(aData, nData, i);
        sqlite4BtPageGet(pPager, child, &pChild);
        aChild = btPageData(pChild);
        btPageToAscii(child, bAscii, pPager, aChild, nData, pBuf);
        sqlite4BtPageRelease(pChild);
      }
    }
  }
  sqlite4BtBufAppendf(pBuf, "\n");
}

static int btFreelistToAscii(bt_db *db, u32 iFirst, sqlite4_buffer *pBuf){
  int rc = SQLITE4_OK;
  u32 iTrunk = iFirst;
  while( iTrunk && rc==SQLITE4_OK ){
    BtPage *pPg = 0;
    rc = sqlite4BtPageGet(db->pPager, iTrunk, &pPg);
    if( rc==SQLITE4_OK ){
      u8 *aData = btPageData(pPg);
      u32 nFree = btGetU32(aData);
      u32 iNext = btGetU32(&aData[4]);
      int i;

      sqlite4BtBufAppendf(pBuf, "iTrunk=%d ", (int)iTrunk);
      sqlite4BtBufAppendf(pBuf, "nFree=%d iNext=%d (", (int)nFree, (int)iNext);
      for(i=0; i<(int)nFree; i++){
        u32 pgnoFree = btGetU32(&aData[8 + i*sizeof(u32)]);
        sqlite4BtBufAppendf(pBuf, "%s%d", (i==0)?"": " ", (int)pgnoFree);
      }
      sqlite4BtBufAppendf(pBuf, ")\n");

      sqlite4BtPageRelease(pPg);
      iTrunk = iNext;
    }
  }
  return rc;
}

#ifndef NDEBUG
#include <stdio.h>

static void printPage(FILE *f, u32 pgno, u8 *aData, int nData){
  sqlite4_buffer buf;

  sqlite4_buffer_init(&buf, 0);
  btPageToAscii(pgno, 0, 0, aData, nData, &buf);
  sqlite4_buffer_append(&buf, "", 1);

  fprintf(f, "%s", (char*)buf.p);
  sqlite4_buffer_clear(&buf);
}

int printPgdataToStderr(u32 pgno, u8 *aData, int nData){
  printPage(stderr, pgno, aData, nData);
  return 0;
}

int printPgToStderr(BtPage *pPg){
  printPage(stderr, sqlite4BtPagePgno(pPg), btPageData(pPg), 1024);
  return 0;
}

static void btPrintMetaTree(BtPager *pPager, int bAscii, BtDbHdr *pHdr){
  u8 *aData;
  int nData;
  sqlite4_buffer buf;
  BtPage *pPg = 0;

  sqlite4BtPageGet(pPager, pHdr->iMRoot, &pPg);
  aData = btPageData(pPg);
  nData = pHdr->pgsz;
  sqlite4_buffer_init(&buf, 0);
  btPageToAscii(pHdr->iMRoot, bAscii, pPager, aData, nData, &buf);
  sqlite4_buffer_append(&buf, "", 1);

  fprintf(stderr, "%s", (char*)buf.p);
  sqlite4_buffer_clear(&buf);
  sqlite4BtPageRelease(pPg);
}


static void btDumpCsr(sqlite4_buffer *pBuf, BtCursor *pCsr){
  assert( pCsr->nPg>=0 );
  if( pCsr->nPg==0 ){
    sqlite4BtBufAppendf(pBuf, "EOF");
  }else{
    int rc;
    const void *pKey = 0;
    int nKey = 0;

    rc = btCsrKey(pCsr, &pKey, &nKey);
    assert( rc==SQLITE4_OK );
    btBufferAppendBlob(pBuf, 0, (u8*)pKey, nKey);
  }
}

static void fiDumpCsr(FiCursor *pCsr){
  int iBt;

  sqlite4_buffer buf;
  sqlite4_buffer_init(&buf, 0);
  for(iBt=0; iBt<pCsr->nBt; iBt++){
    FiSubCursor *pSub = &pCsr->aSub[iBt];

    sqlite4BtBufAppendf(&buf, "%d prefix: ", iBt);
    btBufferAppendBlob(&buf, 0, pSub->aPrefix, sizeof(pSub->aPrefix));

    sqlite4BtBufAppendf(&buf, "\n%d mcsr  : ", iBt);
    btDumpCsr(&buf, &pSub->mcsr);

    sqlite4BtBufAppendf(&buf, "\n%d csr   : ", iBt);
    btDumpCsr(&buf, &pSub->csr);
    sqlite4BtBufAppendf(&buf, "\n");
  }

  sqlite4_buffer_append(&buf, "", 1);
  fprintf(stderr, "%s", (char*)buf.p);
  sqlite4_buffer_clear(&buf);
}
#endif

#ifndef NDEBUG
/*
** This function is really just a big assert() statement. It contributes 
** nothing to the operation of the library.
**
** The assert() fails if the summary-record is not consistent with the
** actual contents of the meta-tree.
*/
static void assert_summary_ok(bt_db *db, int crc){
  BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
  BtCursor csr;                   /* Cursor used to load summary record */
  BtCursor mcsr;                  /* Cursor used to scan meta-tree */
  const u8 *aSum; int nSum;       /* Current summary record */
  int i;
  int rc;

  struct AgeData {
    int iMinLevel;                /* Smallest level of this age seen */
    int iMaxLevel;                /* Largest level of this age seen */
  } aData[32];

  if( crc!=SQLITE4_OK ) return;

  for(i=0; i<array_size(aData); i++){
    aData[i].iMinLevel = -1;
    aData[i].iMaxLevel = -1;
  }

  rc = fiLoadSummary(db, &csr, &aSum, &nSum);
  assert( rc==SQLITE4_OK );

  btCsrSetup(db, pHdr->iMRoot, &mcsr);
  for(rc=btCsrEnd(&mcsr, 0); rc==SQLITE4_OK; rc=btCsrStep(&mcsr, 1)){
    const u8 *pKey = 0; int nKey = 0;
    u32 iAge;
    u32 iLevel;
    rc = btCsrKey(&mcsr, (const void **)&pKey, &nKey);
    assert( rc==SQLITE4_OK );

    if( nKey==array_size(aSummaryKey) && memcmp(aSummaryKey, pKey, nKey)==0 ){
      break;
    }
    assert( nKey>=8 );

    iAge = btGetU32(&pKey[0]);
    iLevel = ~btGetU32(&pKey[4]);

    if( aData[iAge].iMinLevel<0 || aData[iAge].iMinLevel>iLevel ){
      aData[iAge].iMinLevel = (int)iLevel;
    }
    if( aData[iAge].iMaxLevel<0 || aData[iAge].iMaxLevel<iLevel ){
      aData[iAge].iMaxLevel = (int)iLevel;
    }
  }
  assert( rc==SQLITE4_OK );

  for(i=0; i<array_size(aData); i++){
    u16 iMin = 0; 
    u16 nLevel = 0;
    u16 iMerge = 0;
    if( i<(nSum/6) ){
      btReadSummary(aSum, i, &iMin, &nLevel, &iMerge);
    }
    if( aData[i].iMinLevel>=0 ){
      int nLevelExpect = aData[i].iMaxLevel - aData[i].iMinLevel + 1;
      assert( (int)iMin==aData[i].iMinLevel 
           || (iMerge!=0 && (int)iMin<aData[i].iMinLevel)
      );
      assert( (int)nLevel==nLevelExpect
           || (iMerge!=0 && nLevel>nLevelExpect)
      );
    }else{
      assert( iMin==0 && nLevel==0 );
    }
  }

  btCsrReset(&csr, 1);
  btCsrReset(&mcsr, 1);
}

static void assert_ficursor_ok(FiCursor *p, int crc){
  int iBt;

  if( crc!=SQLITE4_OK && crc!=SQLITE4_NOTFOUND && crc!=SQLITE4_INEXACT ) return;
  if( (p->base.flags & (CSR_NEXT_OK|CSR_PREV_OK))==0 ) return;

  for(iBt=0; iBt<p->nBt; iBt++){
    FiSubCursor *pSub = &p->aSub[iBt];

    if( pSub->mcsr.nPg>0 ){
      const void *pKey = 0; 
      int nKey = 0;
      assert( pSub->csr.nPg>0 );

      btCsrKey(&pSub->mcsr, &pKey, &nKey);
      assert( nKey>=8 && 0==memcmp(pSub->aPrefix, pKey, 8) );
    }else{
      assert( pSub->csr.nPg==0 );
    }
  }
}

#else
# define assert_summary_ok(x, rc) 
# define assert_ficursor_ok(p, rc)
#endif


/*
** This function compares the key passed via parameters pK and nK to the
** key that cursor pCsr currently points to.
**
** If the cursor key is C, and the user key K, then this function sets:
**
**     *piRes = (C - K).
**
** In other words, *piRes is +ve, zero or -ve if C is respectively larger, 
** equal to or smaller than K.
*/
static int btCellKeyCompare(
  BtCursor *pCsr,                 /* Cursor handle */
  int bLeaf,                      /* True if cursor currently points to leaf */
  const void *aPrefix,
  const void *pK, int nK,         /* Key to compare against cursor key */
  int *piRes                      /* OUT: Result of comparison */
){
  const void *pCsrKey;
  int nCsrKey;
  int nCmp;
  int nAscend = 0;
  int rc = SQLITE4_OK;
  int res;

  if( bLeaf ){
    rc = btCsrKey(pCsr, &pCsrKey, &nCsrKey);
  }else{
    const int pgsz = sqlite4BtPagerPagesize(pCsr->base.pDb->pPager);

    u8 *aData = btPageData(pCsr->apPage[pCsr->nPg-1]);
    u8 *pCell = btCellFind(aData, pgsz, pCsr->aiCell[pCsr->nPg-1]);

    pCsrKey = pCell + sqlite4BtVarintGet32(pCell, &nCsrKey);
    if( nCsrKey==0 ){
      int iCell = pCsr->aiCell[pCsr->nPg-1]+1;
      while( 1 ){
        BtPage *pPg;
        u8 *aData = btPageData(pCsr->apPage[pCsr->nPg-1]);
        u32 pgno = btChildPgno(aData, pgsz, iCell);
        nAscend++;
        rc = btCsrDescend(pCsr, pgno, &pPg);
        if( rc!=SQLITE4_OK ) break;
        aData = btPageData(pPg);
        pCsr->aiCell[pCsr->nPg-1] = 0;
        if( (btFlags(aData) & BT_PGFLAGS_INTERNAL)==0 ) break;
        iCell = 0;
      }
      rc = sqlite4BtCsrKey((bt_cursor*)pCsr, &pCsrKey, &nCsrKey);
    }
  }

  if( rc==SQLITE4_OK ){
    if( aPrefix ){
      if( nCsrKey<8 ){
        res = memcmp(pCsrKey, aPrefix, nCsrKey);
        if( res==0 ) res = -1;
      }else{
        res = memcmp(pCsrKey, aPrefix, 8);
        nCsrKey -= 8;
        pCsrKey = (void*)(((u8*)pCsrKey) + 8);
      }
      if( res ) goto keycompare_done;
    }

    nCmp = MIN(nCsrKey, nK);
    res = memcmp(pCsrKey, pK, nCmp);
    if( res==0 ){
      res = nCsrKey - nK;
    }
  }

 keycompare_done:
  btCsrAscend(pCsr, nAscend);
  *piRes = res;
  return rc;
}

/*
** Return an integer representing the result of (K1 - K2).
*/
static int btKeyCompare(
  const void *pKey1, int nKey1, 
  const void *pKey2, int nKey2
){
  int nCmp = MIN(nKey1, nKey2);
  int res;

  res = memcmp(pKey1, pKey2, nCmp);
  if( res==0 ){
    res = nKey1 - nKey2;
  }
  return res;
}


#define BT_CSRSEEK_SEEK   0
#define BT_CSRSEEK_UPDATE 1
#define BT_CSRSEEK_RESEEK 2

static int btCsrSeek(
  BtCursor *pCsr,                 /* Cursor object to seek */
  u8 *aPrefix,                    /* 8-byte key prefix, or NULL */
  const void *pK,                 /* Key to seek for */
  int nK,                         /* Size of key pK in bytes */
  int eSeek,                      /* Seek mode (a BT_SEEK_XXX constant) */
  int eCsrseek
){
  BtPager *pPager = pCsr->base.pDb->pPager;
  const int pgsz = sqlite4BtPagerPagesize(pPager);
  u32 pgno;                       /* Page number for next page to load */
  int rc;                         /* Return Code */

  assert( eSeek==BT_SEEK_EQ || eCsrseek!=BT_CSRSEEK_RESEEK );
  assert( eSeek==BT_SEEK_GE || eCsrseek!=BT_CSRSEEK_UPDATE );

  /* Reset the cursor */
  btCsrReset(pCsr, 0);

  /* Figure out the root page number */
  assert( pCsr->iRoot>1 && pCsr->nPg==0 );
  pgno = pCsr->iRoot;

  while( 1 ){
    /* Load page number pgno into the b-tree cursor. */
    BtPage *pPg;
    rc = sqlite4BtPageGet(pPager, pgno, &pPg);
    pCsr->apPage[pCsr->nPg++] = pPg;

    if( rc==SQLITE4_OK ){
      int nCell;                  /* Number of cells on this page */
      int iHi;                    /* pK/nK is <= than cell iHi */
      int iLo;                    /* pK/nK is > than cell (iLo-1) */
      int res;                    /* Result of comparison */

      u8 *aData = btPageData(pPg);
      u16 *aCellPtr = (u16*)btCellPtrFind(aData, pgsz, 0);
      int bLeaf = ((btFlags(aData) & BT_PGFLAGS_INTERNAL)==0);

      iLo = 0;
      iHi = nCell = btCellCount(aData, pgsz);

      if( btFlags(aData) & BT_PGFLAGS_LARGEKEYS ){
        while( iHi>iLo ){
          int iTst = (iHi+iLo)/2;   /* Cell to compare to pK/nK */
          u8 *pCell = &aData[btGetU16((u8*)(aCellPtr - iTst))];

          pCsr->aiCell[pCsr->nPg-1] = iTst;
          rc = btCellKeyCompare(pCsr, bLeaf, 0, pK, nK, &res);

          if( res<0 ){
            /* Cell iTst is SMALLER than pK/nK */
            iLo = iTst+1;
          }else{
            /* Cell iTst is LARGER than (or equal to) pK/nK */
            iHi = iTst;
            if( res==0 ){
              iHi += !bLeaf;
              break;
            }
          }
        }
      }else{
        while( iHi>iLo ){
          int iTst = (iHi+iLo)/2;   /* Cell to compare to pK/nK */
          u8 *pCell = &aData[btGetU16((u8*)(aCellPtr - iTst))];
          int n = *pCell;

          res = memcmp(&pCell[1], pK, MIN(nK, n));
          if( res<0 || (res==0 && (res = n - nK)<0) ){
            /* Cell iTst is SMALLER than pK/nK */
            iLo = iTst+1;
          }else{
            /* Cell iTst is LARGER than (or equal to) pK/nK */
            iHi = iTst;
            if( res==0 ){
              iHi += !bLeaf;
              break;
            }
          }
        }
      }
      if( rc!=SQLITE4_OK ) break;
      pCsr->aiCell[pCsr->nPg-1] = iHi;

      if( bLeaf==0 ){
        if( iHi==nCell ) pgno = btGetU32(&aData[1]);
        else{
          u8 *pCell = btCellFind(aData, pgsz, iHi);
          pgno = btGetU32(&pCell[1 + (int)*pCell]);
        }
        if( pCsr->nPg==BT_MAX_DEPTH ){
          rc = btErrorBkpt(SQLITE4_CORRUPT);
          break;
        }
      }else{

        if( nCell==0 ){
          rc = SQLITE4_NOTFOUND;
        }else if( res!=0 ){
          if( eSeek==BT_SEEK_EQ ){
            if( eCsrseek==BT_CSRSEEK_RESEEK ){
              rc = SQLITE4_OK;
              if( iHi==nCell ){
                assert( pCsr->aiCell[pCsr->nPg-1]>0 );
                pCsr->aiCell[pCsr->nPg-1]--;
                pCsr->bSkipPrev = 1;
              }else{
                pCsr->bSkipNext = 1;
              }
            }else{
              rc = SQLITE4_NOTFOUND;
            }
          }else{
            assert( BT_SEEK_LEFAST<0 && BT_SEEK_LE<0 );
            if( eSeek<0 ){
              rc = sqlite4BtCsrPrev((bt_cursor*)pCsr);
            }else{
              if( iHi==nCell ){
                if( eCsrseek==BT_CSRSEEK_UPDATE ){
                  rc = SQLITE4_NOTFOUND;
                }else{
                  rc = sqlite4BtCsrNext((bt_cursor*)pCsr);
                }
              }
            }
            if( rc==SQLITE4_OK ) rc = SQLITE4_INEXACT;
          }
        }

        /* The cursor now points to a leaf page. Break out of the loop. */
        break;
      }
    }
  }

  if( rc!=SQLITE4_OK && rc!=SQLITE4_INEXACT && eCsrseek!=BT_CSRSEEK_UPDATE ){
    btCsrReset(pCsr, 0);
  }
  return rc;
}

static int btCsrReseek(BtCursor *pCsr){
  int rc = SQLITE4_OK;
  if( pCsr->bRequireReseek ){
    BtOvfl o;                     /* Copy of initial overflow buffer */
    memcpy(&o, &pCsr->ovfl, sizeof(BtOvfl));

    pCsr->ovfl.buf.n = 0;
    pCsr->ovfl.buf.p = 0;
    pCsr->bSkipNext = 0;
    pCsr->bRequireReseek = 0;

    rc = btCsrSeek(pCsr, 0, o.buf.p, o.nKey, BT_SEEK_EQ, BT_CSRSEEK_RESEEK);
    assert( rc!=SQLITE4_INEXACT );
    if( pCsr->ovfl.buf.p==0 ){
      pCsr->ovfl.buf.p = o.buf.p;
    }else{
      sqlite4_buffer_clear(&o.buf);
    }
  }
  return rc;
}

/*
** This function does the work of both sqlite4BtCsrNext() (if parameter
** bNext is true) and Pref() (if bNext is false).
*/
static int btCsrStep(BtCursor *pCsr, int bNext){
  const int pgsz = sqlite4BtPagerPagesize(pCsr->base.pDb->pPager);
  int rc = SQLITE4_OK;
  int bRequireDescent = 0;

  rc = btCsrReseek(pCsr);
  if( rc==SQLITE4_OK && pCsr->nPg==0 ){
    rc = SQLITE4_NOTFOUND;
  }
  pCsr->ovfl.nKey = 0;

  if( (pCsr->bSkipNext && bNext) || (pCsr->bSkipPrev && bNext==0) ){
    pCsr->bSkipPrev = pCsr->bSkipNext = 0;
    return rc;
  }
  pCsr->bSkipPrev = pCsr->bSkipNext = 0;

  while( rc==SQLITE4_OK ){
    int iPg = pCsr->nPg-1;
    int iCell = pCsr->aiCell[iPg];

    if( bNext ){
      u8 *aData = (u8*)btPageData(pCsr->apPage[iPg]);
      int nCell = btCellCount(aData, pgsz);
      assert( bRequireDescent==0 || bRequireDescent==1 );
      if( iCell<(nCell+bRequireDescent-1) ){
        pCsr->aiCell[iPg]++;
        break;
      }
    }else{
      if( pCsr->aiCell[iPg]>0 ){
        pCsr->aiCell[iPg]--;
        break;
      }
    }

    rc = btCsrAscend(pCsr, 1);
    bRequireDescent = 1;
  }

  if( bRequireDescent && rc==SQLITE4_OK ){
    u32 pgno;                   /* Child page number */
    u8 *aData = (u8*)btPageData(pCsr->apPage[pCsr->nPg-1]);

    pgno = btChildPgno(aData, pgsz, pCsr->aiCell[pCsr->nPg-1]);

    while( 1 ){
      BtPage *pPg;
      rc = btCsrDescend(pCsr, pgno, &pPg);
      if( rc!=SQLITE4_OK ){
        break;
      }else{
        int nCell;
        aData = (u8*)btPageData(pPg);
        nCell = btCellCount(aData, pgsz);
        if( btFlags(aData) & BT_PGFLAGS_INTERNAL ){
          pCsr->aiCell[pCsr->nPg-1] = (bNext ? 0 : nCell);
          pgno = btChildPgno(aData, pgsz, pCsr->aiCell[pCsr->nPg-1]);
        }else{
          pCsr->aiCell[pCsr->nPg-1] = (bNext ? 0 : nCell-1);
          break;
        }
      }
    }
  }

  return rc;
}


/*
** This function seeks the cursor as required for either sqlite4BtCsrFirst()
** (if parameter bLast is false) or sqlite4BtCsrLast() (if bLast is true).
*/
static int btCsrEnd(BtCursor *pCsr, int bLast){
  const int pgsz = sqlite4BtPagerPagesize(pCsr->base.pDb->pPager);
  int rc = SQLITE4_OK;            /* Return Code */
  u32 pgno;                       /* Page number for next page to load */

  /* Reset the cursor */
  btCsrReset(pCsr, 0);

  /* Figure out the root page number */
  assert( pCsr->iRoot>1 && pCsr->nPg==0 );
  pgno = pCsr->iRoot;

  while( rc==SQLITE4_OK ){
    /* Load page number pgno into the b-tree */
    BtPage *pPg;
    rc = btCsrDescend(pCsr, pgno, &pPg);
    if( rc==SQLITE4_OK ){
      int nCell;                  /* Number of cells on this page */
      int nByte;
      u8 *pCell;
      u8 *aData = (u8*)btPageData(pPg);

      nCell = btCellCount(aData, pgsz);
      pCsr->aiCell[pCsr->nPg-1] = (bLast ? nCell : 0);

      /* If the cursor has descended to a leaf break out of the loop. */
      if( (aData[0] & BT_PGFLAGS_INTERNAL)==0 ){
        if( nCell==0 ){
          btCsrReset(pCsr, 0);
          rc = SQLITE4_NOTFOUND;
        }
        break;
      }
      
      /* Otherwise, set pgno to the left or rightmost child of the page
      ** just loaded, depending on whether the cursor is seeking to the
      ** start or end of the tree.  */
      if( bLast==0 ){
        pCell = btCellFind(aData, pgsz, 0);
        pCell += sqlite4BtVarintGet32(pCell, &nByte);
        pCell += nByte;
        pgno = btGetU32(pCell);
      }else{
        pgno = btGetU32(&aData[1]);
      }
    }
  }
  if( pCsr->aiCell[pCsr->nPg-1] ) pCsr->aiCell[pCsr->nPg-1]--;
  return rc;
}


static int fiCsrAllocateSubs(bt_db *db, FiCursor *pCsr, int nBt){
  int rc = SQLITE4_OK;            /* Return code */
  if( nBt>pCsr->nBt ){
    int nByte = sizeof(FiSubCursor) * nBt;
    FiSubCursor *aNew;            /* Allocated array */

    aNew = (FiSubCursor*)sqlite4_realloc(db->pEnv, pCsr->aSub, nByte);
    if( aNew ){
      memset(&aNew[pCsr->nBt], 0, sizeof(FiSubCursor)*(nBt-pCsr->nBt));
      pCsr->aSub = aNew;
      pCsr->nBt = nBt;
    }else{
      rc = btErrorBkpt(SQLITE4_NOMEM);
    }
  }

  return rc;
}

/*
** Return the page number of the first page on block iBlk.
*/
static u32 btFirstOfBlock(BtDbHdr *pHdr, u32 iBlk){
  assert( iBlk>0 );
  return (iBlk - 1) * (pHdr->blksz / pHdr->pgsz) + 1;
}

/*
** Return true if the cell that the argument cursor currently points to
** is a delete marker.
*/
static int btCsrIsDelete(BtCursor *pCsr){
  const int pgsz = sqlite4BtPagerPagesize(pCsr->base.pDb->pPager);
  int bRet;                       /* Return value */
  u8 *aData;
  u8 *pCell;
  int n;

  aData = btPageData(pCsr->apPage[pCsr->nPg-1]);
  pCell = btCellFind(aData, pgsz, pCsr->aiCell[pCsr->nPg-1]);

  pCell += sqlite4BtVarintGet32(pCell, &n);
  if( n==0 ){
    /* Type (c) cell */
    pCell += sqlite4BtVarintGet32(pCell, &n);
    pCell += n;
    pCell += sqlite4BtVarintGet32(pCell, &n);
    pCell += sqlite4BtVarintGet32(pCell, &n);
    bRet = (n==0);
  }else{
    pCell += n;
    pCell += sqlite4BtVarintGet32(pCell, &n);
    bRet = (n==1);
  }

  return bRet;
}

static int fiCsrIsDelete(FiCursor *pCsr){
  int res = 0;
  if( (pCsr->base.flags & CSR_VISIT_DEL)==0 ){
    BtCursor *p = &pCsr->aSub[pCsr->iBt].csr;
    res = btCsrIsDelete(p);
  }
  return res;
}

static int btOverflowArrayRead(
  bt_db *db,
  u8 *pOvfl,
  u8 *aOut,
  int nOut
){
  const int pgsz = sqlite4BtPagerPagesize(db->pPager);
  const int nPgPtr = pgsz / 4;
  int rc = SQLITE4_OK;
  int nDirect;                    /* Number of direct overflow pages */
  int nDepth;                     /* Depth of overflow tree */
  int iOut;                       /* Bytes of data copied so far */
  int iPg;

  nDirect = (int)(pOvfl[0] & 0x0F);
  nDepth = (int)(pOvfl[0]>>4);

  iOut = 0;

  /* Read from the direct overflow pages. And from the overflow tree, if
  ** it has a depth of zero.  */
  for(iPg=0; rc==SQLITE4_OK && iPg<(nDirect+(nDepth==0)) && iOut<nOut; iPg++){
    u32 pgno = btGetU32(&pOvfl[1+iPg*4]);
    BtPage *pPg = 0;
    rc = sqlite4BtPageGet(db->pPager, pgno, &pPg);
    if( rc==SQLITE4_OK ){
      int nCopy = MIN(nOut-iOut, pgsz);
      u8 *a = btPageData(pPg);
      memcpy(&aOut[iOut], a, nCopy);
      sqlite4BtPageRelease(pPg);
      iOut += nCopy;
    }
  }

  /* Read from the overflow tree, if it was not read by the block above. */
  if( nDepth>0 ){
    struct Heir {
      BtPage *pPg;
      int iCell;
    } apHier[8];
    int i;
    u32 pgno;
    memset(apHier, 0, sizeof(apHier));

    /* Initialize the apHier[] array. */
    pgno = btGetU32(&pOvfl[1+nDirect*4]);
    for(i=0; i<nDepth && rc==SQLITE4_OK; i++){
      u8 *a;
      rc = sqlite4BtPageGet(db->pPager, pgno, &apHier[i].pPg);
      if( rc==SQLITE4_OK ){
        a = btPageData(apHier[i].pPg);
        pgno = btGetU32(a);
      }
    }

    /* Loop runs once for each leaf page we read from. */
    while( iOut<nOut ){
      u8 *a;                      /* Data associated with some page */
      BtPage *pLeaf;              /* Leaf page */
      int nCopy;                  /* Bytes of data to read from leaf page */

      int iLvl;

      nCopy =  MIN(nOut-iOut, pgsz);
      assert( nCopy>0 );

      /* Read data from the current leaf page */
      rc = sqlite4BtPageGet(db->pPager, pgno, &pLeaf);
      if( rc!=SQLITE4_OK ) break;
      a = btPageData(pLeaf);
      memcpy(&aOut[iOut], a, nCopy);
      sqlite4BtPageRelease(pLeaf);
      iOut += nCopy;

      /* If all required data has been read, break out of the loop */
      if( iOut>=nOut ) break;

      for(iLvl=nDepth-1; iLvl>=0; iLvl--){
        if( apHier[iLvl].iCell<(nPgPtr-1) ) break;
      }
      if( iLvl<0 ) break; /* SQLITE4_CORRUPT? */
      apHier[iLvl].iCell++;

      for(; iLvl<nDepth && rc==SQLITE4_OK; iLvl++){
        a = btPageData(apHier[iLvl].pPg);
        pgno = btGetU32(&a[apHier[iLvl].iCell * 4]);
        if( iLvl<(nDepth-1) ){
          apHier[iLvl+1].iCell = 0;
          sqlite4BtPageRelease(apHier[iLvl+1].pPg);
          apHier[iLvl+1].pPg = 0;
          rc = sqlite4BtPageGet(db->pPager, pgno, &apHier[iLvl+1].pPg);
        }
      }
    }

    for(i=0; i<nDepth && rc==SQLITE4_OK; i++){
      sqlite4BtPageRelease(apHier[i].pPg);
    }
  }

  return rc;
}



/*
** Buffer the key and value belonging to the current cursor position
** in pCsr->ovfl.
*/
static int btCsrBuffer(BtCursor *pCsr, int bVal){
  int rc = SQLITE4_OK;            /* Return code */
  if( pCsr->ovfl.nKey<=0 ){
    const int pgsz = sqlite4BtPagerPagesize(pCsr->base.pDb->pPager);
    u8 *aData;                      /* Page data */
    u8 *pCell;                      /* Pointer to cell within aData[] */
    int nReq;                       /* Total required space */
    u8 *aOut;                       /* Output buffer */
    u8 *pKLocal = 0;                /* Pointer to local part of key */
    u8 *pVLocal = 0;                /* Pointer to local part of value, if any */
    int nKLocal = 0;                /* Bytes of key on page */
    int nVLocal = 0;                /* Bytes of value on page */
    int nKOvfl = 0;                 /* Bytes of key on overflow pages */
    int nVOvfl = 0;                 /* Bytes of value on overflow pages */

    aData = (u8*)btPageData(pCsr->apPage[pCsr->nPg-1]);
    pCell = btCellFind(aData, pgsz, pCsr->aiCell[pCsr->nPg-1]);
    pCell += sqlite4BtVarintGet32(pCell, &nKLocal);
    if( nKLocal==0 ){
      /* Type (c) leaf cell. */
      pCell += sqlite4BtVarintGet32(pCell, &nKLocal);
      pKLocal = pCell;
      pCell += nKLocal;
      pCell += sqlite4BtVarintGet32(pCell, &nKOvfl);
      pCell += sqlite4BtVarintGet32(pCell, &nVOvfl);
      if( nVOvfl>0 ) nVOvfl -= 1;

    }else{
      pKLocal = pCell;
      pCell += nKLocal;
      pCell += sqlite4BtVarintGet32(pCell, &nVLocal);
      if( nVLocal==0 ){
        /* Type (b) */
        pCell += sqlite4BtVarintGet32(pCell, &nVLocal);
        pVLocal = pCell;
        pCell += nVLocal;
        pCell += sqlite4BtVarintGet32(pCell, &nVOvfl);
      }else{
        /* Type (a) */
        pVLocal = pCell;
        nVLocal -= 2;
      }
    }

    /* A delete-key */
    if( nVLocal<0 ) nVLocal = 0;

    pCsr->ovfl.nKey = nKLocal + nKOvfl;
    pCsr->ovfl.nVal = nVLocal + nVOvfl;

    nReq = pCsr->ovfl.nKey + pCsr->ovfl.nVal;
    assert( nReq>0 );
    rc = sqlite4_buffer_resize(&pCsr->ovfl.buf, nReq);
    if( rc!=SQLITE4_OK ) return rc;

    /* Copy in local data */
    aOut = (u8*)pCsr->ovfl.buf.p;
    memcpy(aOut, pKLocal, nKLocal);
    memcpy(&aOut[nKLocal], pVLocal, nVLocal);

    /* Load in overflow data */
    if( nKOvfl || nVOvfl ){
      rc = btOverflowArrayRead(
          pCsr->base.pDb, pCell, &aOut[nKLocal + nVLocal], nKOvfl + nVOvfl
          );
    }
  }

  return rc;
}


static int btCsrKey(BtCursor *pCsr, const void **ppK, int *pnK){
  int rc = SQLITE4_OK;

  if( pCsr->bRequireReseek ){
    *ppK = (const void*)pCsr->ovfl.buf.p;
    *pnK = pCsr->ovfl.nKey;
  }else{
    const int pgsz = sqlite4BtPagerPagesize(pCsr->base.pDb->pPager);
    u8 *aData;
    u8 *pCell;
    int nK;
    int iCell = pCsr->aiCell[pCsr->nPg-1];

    aData = btPageData(pCsr->apPage[pCsr->nPg-1]);
    assert( btCellCount(aData, pgsz)>iCell );
    pCell = btCellFind(aData, pgsz, iCell);
    pCell += sqlite4BtVarintGet32(pCell, &nK);

    if( nK==0 ){
      /* type (c) leaf cell */
      rc = btCsrBuffer(pCsr, 0);
      if( rc==SQLITE4_OK ){
        *ppK = pCsr->ovfl.buf.p;
        *pnK = pCsr->ovfl.nKey;
      }
    }else{
      *ppK = pCell;
      *pnK = nK;
    }
  }

  return rc;
}


static int fiCsrSetCurrent(FiCursor *pCsr){
  const void *pMin = 0;
  int nMin = 0;
  int iMin = -1;
  int i;
  int rc = SQLITE4_OK;
  int mul;

  assert( pCsr->base.flags & (CSR_NEXT_OK | CSR_PREV_OK) );
  mul = ((pCsr->base.flags & CSR_NEXT_OK) ? 1 : -1);

  for(i=0; i<pCsr->nBt && rc==SQLITE4_OK; i++){
    BtCursor *pSub = &pCsr->aSub[i].csr;
    if( pSub->nPg>0 ){
      int nKey;
      const void *pKey;

      rc = btCsrKey(pSub, &pKey, &nKey);
      if( rc==SQLITE4_OK 
          && (iMin<0 || (mul * btKeyCompare(pKey, nKey, pMin, nMin))<0) 
        ){
        pMin = pKey;
        nMin = nKey;
        iMin = i;
      }
    }
  }

  if( iMin<0 && rc==SQLITE4_OK ) rc = SQLITE4_NOTFOUND;
  pCsr->iBt = iMin;
  return rc;
}

static int fiSubCsrCheckPrefix(FiSubCursor *pSub){
  const int nPrefix = sizeof(pSub->aPrefix);
  const void *pK = 0;
  int nK;
  int rc;

  rc = btCsrKey(&pSub->mcsr, &pK, &nK);
  if( rc==SQLITE4_OK && (nK<nPrefix || memcmp(pK, pSub->aPrefix, nPrefix)) ){
    rc = SQLITE4_NOTFOUND;
    btCsrReset(&pSub->mcsr, 0);
  }
  return rc;
}

/*
** Return SQLITE4_OK if the cursor is successfully stepped, or 
** SQLITE4_NOTFOUND if an EOF is encountered.
**
** If an error occurs (e.g. an IO error or OOM condition), return the
** relevant error code.
*/
static int fiSubCsrStep( 
  FiCursor *pCsr,                 /* Parent cursor */
  FiSubCursor *pSub,              /* Sub-cursor to advance */
  int bNext                       /* True for xNext(), false for xPrev() */
){
  int rc;

  rc = btCsrStep(&pSub->csr, bNext);
  if( rc==SQLITE4_NOTFOUND ){
    const void *pV;
    int nV;

#ifndef NDEBUG
    const void *pTmp; int nTmp;
    rc = btCsrKey(&pSub->mcsr, &pTmp, &nTmp);
    assert( rc==SQLITE4_OK && memcmp(pTmp, pSub->aPrefix, 8)==0 );
#endif

    rc = btCsrStep(&pSub->mcsr, bNext);
    if( rc==SQLITE4_OK ){
      rc = btCsrKey(&pSub->mcsr, &pV, &nV);
    }
    if( rc==SQLITE4_OK ){
      rc = fiSubCsrCheckPrefix(pSub);
    }
    if( rc==SQLITE4_OK ){
      rc = btCsrData(&pSub->mcsr, 0, 4, &pV, &nV);
    }
    if( rc==SQLITE4_OK ){
      pSub->csr.iRoot = sqlite4BtGetU32((const u8*)pV);
      rc = btCsrEnd(&pSub->csr, !bNext);
    }
  }
  
  if( bNext==0 ){
    /* If this is an xPrev() operation, check that the cursor has not moved
    ** into a part of the sub-tree that has been gobbled up by an ongoing
    ** merge.  */
    const void *pMin; int nMin;
    const void *pKey; int nKey;

    if( rc==SQLITE4_OK ) rc = btCsrKey(&pSub->mcsr, &pMin, &nMin);
    if( rc==SQLITE4_OK ) rc = btCsrKey(&pSub->csr, &pKey, &nKey);

    if( rc==SQLITE4_OK && btKeyCompare((u8*)pMin+8, nMin-8, pKey, nKey)>0 ){
      rc = SQLITE4_NOTFOUND;
      btCsrReset(&pSub->mcsr, 0);
      btCsrReset(&pSub->csr, 0);
    }
  }

  return rc;
}

/*
** Advance the cursor. The direction (xPrev or xNext) is implied by the
** cursor itself - as fast-insert cursors may only be advanced in one
** direction.
*/
static int fiCsrStep(FiCursor *pCsr){
  int rc = SQLITE4_OK;
  int bNext = (0!=(pCsr->base.flags & CSR_NEXT_OK));
  const void *pKey; int nKey;     /* Current key that cursor points to */
  int i;

#ifndef NDEBUG
  sqlite4_buffer buf;
  sqlite4_buffer_init(&buf, 0);
  sqlite4BtCsrKey(&pCsr->base, &pKey, &nKey);
  sqlite4_buffer_set(&buf, pKey, nKey);
#endif

  assert_ficursor_ok(pCsr, rc);
  assert( pCsr->base.flags & (CSR_NEXT_OK | CSR_PREV_OK) );
  assert( pCsr->iBt>=0 );

  do{
    /* Load the current key in to pKey/nKey. Then advance all sub-cursors 
    ** that share a key with the current sub-cursor. */
    rc = sqlite4BtCsrKey(&pCsr->base, &pKey, &nKey);
    for(i=0; rc==SQLITE4_OK && i<pCsr->nBt; i++){
      FiSubCursor *pSub = &pCsr->aSub[i];
      if( i!=pCsr->iBt && pSub->csr.nPg>0 ){
        const void *p; int n;       /* Key that this sub-cursor points to */
        rc = btCsrKey(&pSub->csr, &p, &n);
        if( rc==SQLITE4_OK && btKeyCompare(p, n, pKey, nKey)==0 ){
          rc = fiSubCsrStep(pCsr, pSub, bNext);
          if( rc==SQLITE4_NOTFOUND ){
            assert( pSub->csr.nPg==0 );
            rc = SQLITE4_OK;
          }
        }
      }
    }

    /* Advance the current sub-cursor */
    if( rc==SQLITE4_OK ){
      rc = fiSubCsrStep(pCsr, &pCsr->aSub[pCsr->iBt], bNext);
      if( rc==SQLITE4_NOTFOUND ){
        assert( pCsr->aSub[pCsr->iBt].csr.nPg==0 );
        rc = SQLITE4_OK;
      }
    }

    /* Figure out a new current bt cursor */
    if( rc==SQLITE4_OK ){
      rc = fiCsrSetCurrent(pCsr);
    }
  }while( rc==SQLITE4_OK && fiCsrIsDelete(pCsr) );

#ifndef NDEBUG
  if( rc==SQLITE4_OK ){
    sqlite4BtCsrKey(&pCsr->base, &pKey, &nKey);
    assert( btKeyCompare(buf.p, buf.n, pKey, nKey) * (bNext?1:-1) < 0 );
  }
  sqlite4_buffer_clear(&buf);
#endif
  assert_ficursor_ok(pCsr, rc);

  return rc;
}

typedef struct FiLevelIter FiLevelIter;
struct FiLevelIter {
  /* Used internally */
  BtCursor csr;                   /* Cursor used to read summary blob */
  const u8 *aSum;                 /* Summary blob */
  int nSum;                       /* Size of summary blob in bytes */

  /* Output values */
  int nSub;                       /* Total number of expected levels */
  int iAge;                       /* Current age */
  int iLvl;                       /* Current level */
  int iSub;                       /* Current sub-cursor */
};

static int fiLevelIterNext(FiLevelIter *p){
  u16 iMin, nLevel;

  p->iSub++;
  p->iLvl--;
  btReadSummary(p->aSum, p->iAge, &iMin, &nLevel, 0);
  while( p->iLvl<(int)iMin ){
    p->iAge++;
    if( p->iAge>=(p->nSum)/6 ) return 1;
    btReadSummary(p->aSum, p->iAge, &iMin, &nLevel, 0);
    p->iLvl = (int)iMin + (int)nLevel - 1;
  }

  assert( p->iSub<p->nSub );
  return 0;
}

static int fiLevelIterInit(bt_db *db, FiLevelIter *p){
  int rc;                         /* Return code */

  memset(p, 0, sizeof(FiLevelIter));
  rc = fiLoadSummary(db, &p->csr, &p->aSum, &p->nSum);
  if( rc==SQLITE4_OK ){
    int iAge;
    for(iAge=0; iAge<(p->nSum/6); iAge++){
      u16 iMin, nLevel;
      btReadSummary(p->aSum, iAge, &iMin, &nLevel, 0);
      p->nSub += nLevel;
      if( iAge==0 ){
        p->iLvl = ((int)iMin + nLevel);
        p->iSub = -1;
      }
    }
  }

  return rc;
}

static void fiLevelIterCleanup(FiLevelIter *p){
  btCsrReset(&p->csr, 1);
}

/*
** Format values iAge and iLvl into an 8 byte prefix as used in the
** meta-tree.
*/
static void fiFormatPrefix(u8 *aPrefix, u32 iAge, u32 iLvl){
  btPutU32(&aPrefix[0], iAge);
  btPutU32(&aPrefix[4], ~(u32)iLvl);
}

/*
** Seek a fast-insert cursor.
*/
static int fiCsrSeek(FiCursor *pCsr, const void *pK, int nK, int eSeek){
  int rc = SQLITE4_NOTFOUND;      /* Return code */
  bt_db *db = pCsr->base.pDb;     /* Database handle */
  BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);

  assert( eSeek==BT_SEEK_LE || eSeek==BT_SEEK_EQ || eSeek==BT_SEEK_GE );
  assert( (pCsr->base.flags & CSR_VISIT_DEL)==0 || eSeek==BT_SEEK_GE );
  fiCsrReset(pCsr);

  if( pHdr->iMRoot ){
    u8 *pKey;
    FiLevelIter iter;

    /* Initialize the iterator used to skip through database levels */
    rc = fiLevelIterInit(db, &iter);
    if( rc!=SQLITE4_OK ) return rc;
    pKey = sqlite4_malloc(db->pEnv, nK+8);
    if( pKey==0 ) return SQLITE4_NOMEM;

    if( eSeek==BT_SEEK_EQ ){
      FiSubCursor *pSub;
      BtCursor *pM;

      pCsr->base.flags &= ~(CSR_NEXT_OK | CSR_PREV_OK);

      /* A BT_SEEK_EQ is a special case. There is no need to set up a cursor
      ** that can be advanced (in either direction) in this case. All that
      ** is required is to search each level in order for the requested key 
      ** (or a corresponding delete marker). Once a match is found, there
      ** is no need to search any further. As a result, only a single
      ** sub-cursor is required.  */
      rc = fiCsrAllocateSubs(db, pCsr, 1);
      pSub = pCsr->aSub;
      pM = &pSub->mcsr;

      btCsrSetup(db, pHdr->iMRoot, pM);
      while( 0==fiLevelIterNext(&iter) ){

        fiFormatPrefix(pSub->aPrefix, iter.iAge, iter.iLvl);
        memcpy(pKey, pSub->aPrefix, sizeof(pSub->aPrefix));
        rc = btCsrSeek(pM, 0, pKey, nK+8, BT_SEEK_LE, BT_CSRSEEK_SEEK);

        if( rc==SQLITE4_INEXACT ){
          rc = fiSubCsrCheckPrefix(pSub);
        }

        if( rc==SQLITE4_NOTFOUND ){
          /* All keys in this level are greater than pK/nK. */
          /* no-op */
        }else if( rc==SQLITE4_OK || rc==SQLITE4_INEXACT ){
          const void *pV;
          int nV;
          u32 iRoot;
          sqlite4BtCsrData(&pM->base, 0, 4, &pV, &nV);
          iRoot = sqlite4BtGetU32((const u8*)pV);
          btCsrReset(&pSub->csr, 1);
          btCsrSetup(db, iRoot, &pSub->csr);

          rc = btCsrSeek(&pSub->csr, 0, pK, nK, BT_SEEK_EQ, BT_CSRSEEK_SEEK);
          assert( rc!=SQLITE4_INEXACT );
          if( rc!=SQLITE4_NOTFOUND ){
            /* A hit on the requested key or an error has occurred. Either
            ** way, break out of the loop. If this is a hit, set iBt to
            ** zero so that the BtCsrKey() and BtCsrData() routines know
            ** to return data from the first (only) sub-cursor. */
            assert( pCsr->iBt<0 );
            if( rc==SQLITE4_OK ){
              if( 0==btCsrIsDelete(&pSub->csr) ){
                pCsr->iBt = 0;
              }else{
                rc = SQLITE4_NOTFOUND;
              }
            }
            break;
          }
        }
      }
    }else{
      int bMatch = 0;           /* Found an exact match */
      int bHit = 0;             /* Found at least one entry */

      pCsr->base.flags |= (eSeek==BT_SEEK_GE ? CSR_NEXT_OK : CSR_PREV_OK);

      /* Allocate required sub-cursors. */
      if( rc==SQLITE4_OK ){
        rc = fiCsrAllocateSubs(db, pCsr, iter.nSub);
      }

      /* This loop runs once for each sub-cursor */
      while( rc==SQLITE4_OK && 0==fiLevelIterNext(&iter) ){
        FiSubCursor *pSub = &pCsr->aSub[iter.iSub];
        BtCursor *pM = &pSub->mcsr;
        btCsrSetup(db, pHdr->iMRoot, pM);

        fiFormatPrefix(pSub->aPrefix, iter.iAge, iter.iLvl);
        memcpy(pKey, pSub->aPrefix, sizeof(pSub->aPrefix));

        rc = btCsrSeek(pM, 0, pKey, nK+8, BT_SEEK_LE, BT_CSRSEEK_SEEK);
        if( rc==SQLITE4_INEXACT ) rc = fiSubCsrCheckPrefix(pSub);
        if( rc==SQLITE4_NOTFOUND && eSeek==BT_SEEK_GE ){
          rc = btCsrSeek(pM, 0, pSub->aPrefix, sizeof(pSub->aPrefix), 
              BT_SEEK_GE, BT_CSRSEEK_SEEK
          );
          if( rc==SQLITE4_INEXACT ) rc = fiSubCsrCheckPrefix(pSub);
        }

        if( rc==SQLITE4_NOTFOUND ){
          /* No keys to visit in this level */
          assert( pSub->mcsr.nPg==0 );
          assert( pSub->csr.nPg==0 );
          rc = SQLITE4_OK;
        }else if( rc==SQLITE4_OK || rc==SQLITE4_INEXACT ){
          const void *pV; int nV;
          const void *pSeek = pK; 
          int nSeek = nK;

          u32 iRoot;
          sqlite4BtCsrData(&pM->base, 0, 4, &pV, &nV);
          iRoot = sqlite4BtGetU32((const u8*)pV);
          btCsrReset(&pSub->csr, 1);
          btCsrSetup(db, iRoot, &pSub->csr);

          if( eSeek==BT_SEEK_GE ){
            const void *pMin; int nMin;
            rc = btCsrKey(pM, &pMin, &nMin);
            if( rc!=SQLITE4_OK ) break;
            nMin -= 8;
            pMin = (const void*)((const u8*)pMin + 8);
            if( btKeyCompare(pSeek, nSeek, pMin, nMin)<0 ){
              pSeek = pMin;
              nSeek = nMin;
            }
          }

          rc = btCsrSeek(&pSub->csr, 0, pSeek, nSeek, eSeek, BT_CSRSEEK_SEEK);
          if( rc==SQLITE4_NOTFOUND ){
            rc = fiSubCsrStep(pCsr, pSub, (eSeek==BT_SEEK_GE ? 1 : 0));
          }else{
            if( rc==SQLITE4_OK ) bMatch = 1;
            if( rc==SQLITE4_INEXACT ) bHit = 1;
          }

          if( rc==SQLITE4_INEXACT || rc==SQLITE4_NOTFOUND ) rc = SQLITE4_OK;
        }else{
          /* An error */
        }
      }
      assert( rc!=SQLITE4_OK || iter.iSub==iter.nSub );

      if( rc==SQLITE4_OK ){
        rc = fiCsrSetCurrent(pCsr);
        if( rc==SQLITE4_OK ){
          if( fiCsrIsDelete(pCsr) ){
            rc = fiCsrStep(pCsr);
            if( rc==SQLITE4_OK ) rc = SQLITE4_INEXACT;
          }else if( bMatch==0 ){
            rc = (bHit ? SQLITE4_INEXACT : SQLITE4_NOTFOUND);
          }
        }
      }
    }

    sqlite4_free(db->pEnv, pKey);
    fiLevelIterCleanup(&iter);
  }

  return rc;
}

static int fiCsrEnd(FiCursor *pCsr, int bLast){
  bt_db *db = pCsr->base.pDb;
  BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
  FiLevelIter iter;         /* Used to iterate through all f-tree levels */
  int rc;                   /* Return code */

  assert( (pCsr->base.flags & CSR_VISIT_DEL)==0 );

  rc = fiLevelIterInit(db, &iter);
  if( rc==SQLITE4_OK ){
    rc = fiCsrAllocateSubs(db, pCsr, iter.nSub);
  }

  while( rc==SQLITE4_OK && 0==fiLevelIterNext(&iter) ){
    FiSubCursor *pSub = &pCsr->aSub[iter.iSub];
    const int n = (int)sizeof(pSub->aPrefix);

    btPutU32(pSub->aPrefix, (u32)iter.iAge);
    btPutU32(&pSub->aPrefix[4], ~(u32)iter.iLvl);

    btCsrSetup(db, pHdr->iMRoot, &pSub->mcsr);
    if( bLast==0 ){
      rc = btCsrSeek(&pSub->mcsr, 0, pSub->aPrefix, n, BT_SEEK_GE, 0);
    }else{
      u8 aPrefix[8];
      btPutU32(aPrefix, (u32)iter.iAge + (iter.iLvl==0 ? 1 : 0));
      btPutU32(&aPrefix[4], ~((u32)iter.iLvl - (u32)1));
      rc = btCsrSeek(&pSub->mcsr, 0, aPrefix, n, BT_SEEK_LE, 0);
      if( rc==SQLITE4_OK ){
        rc = btCsrStep(&pSub->mcsr, 0);
      }
    }
    if( rc==SQLITE4_INEXACT ) rc = SQLITE4_OK;
    if( rc==SQLITE4_OK ) rc = fiSubCsrCheckPrefix(pSub);

    if( rc==SQLITE4_OK ){
      const void *pV;
      int nV;
      int iRoot;
      btCsrData(&pSub->mcsr, 0, 4, &pV, &nV);
      iRoot = sqlite4BtGetU32((const u8*)pV);
      btCsrReset(&pSub->csr, 1);
      btCsrSetup(db, iRoot, &pSub->csr);
      if( bLast ){
        rc = btCsrEnd(&pSub->csr, 1);
      }else{
        const void *pK; int nK;
        rc = btCsrKey(&pSub->mcsr, &pK, &nK);
        if( rc==SQLITE4_OK ){
          rc = btCsrSeek(&pSub->csr, 0, ((u8*)pK)+8, nK-8, BT_SEEK_GE, 0);
          if( rc==SQLITE4_INEXACT ) rc = SQLITE4_OK;
          if( rc==SQLITE4_NOTFOUND ) rc = btErrorBkpt(SQLITE4_CORRUPT);
        }
      }
    }else if( rc==SQLITE4_NOTFOUND ){
      btCsrReset(&pSub->mcsr, 0);
      rc = SQLITE4_OK;
    }
  }
  fiLevelIterCleanup(&iter);

  if( rc==SQLITE4_OK ){
    pCsr->base.flags &= ~(CSR_NEXT_OK | CSR_PREV_OK);
    pCsr->base.flags |= (bLast ? CSR_PREV_OK : CSR_NEXT_OK);
    rc = fiCsrSetCurrent(pCsr);
    if( rc==SQLITE4_OK && btCsrIsDelete(&pCsr->aSub[pCsr->iBt].csr) ){
      rc = fiCsrStep(pCsr);
    }
  }

  return rc;
}

int sqlite4BtCsrSeek(
  bt_cursor *pBase, 
  const void *pK,                 /* Key to seek for */
  int nK,                         /* Size of key pK in bytes */
  int eSeek                       /* Seek mode (a BT_SEEK_XXX constant) */
){
  int rc;
  btCheckPageRefs(pBase->pDb);
  if( IsBtCsr(pBase) ){
    BtCursor *pCsr = (BtCursor*)pBase;
    rc = btCsrSeek(pCsr, 0, pK, nK, eSeek, BT_CSRSEEK_SEEK);
  }else{
    FiCursor *pCsr = (FiCursor*)pBase;
    rc = fiCsrSeek(pCsr, pK, nK, eSeek);
    assert_ficursor_ok(pCsr, rc);
  }
  btCheckPageRefs(pBase->pDb);
  return rc;
}

/*
** Position cursor pCsr to point to the smallest key in the database.
*/
int sqlite4BtCsrFirst(bt_cursor *pBase){
  int rc;
  if( IsBtCsr(pBase) ){
    rc = btCsrEnd((BtCursor*)pBase, 0);
  }else{
    rc = fiCsrEnd((FiCursor*)pBase, 0);
    assert_ficursor_ok((FiCursor*)pBase, rc);
  }
  return rc;
}

/*
** Position cursor pCsr to point to the largest key in the database.
*/
int sqlite4BtCsrLast(bt_cursor *pBase){
  int rc;
  if( IsBtCsr(pBase) ){
    rc = btCsrEnd((BtCursor*)pBase, 1);
  }else{
    rc = fiCsrEnd((FiCursor*)pBase, 1);
    assert_ficursor_ok((FiCursor*)pBase, rc);
  }
  return rc;
}


/*
** Advance to the next entry in the tree.
*/
int sqlite4BtCsrNext(bt_cursor *pBase){
  int rc;
  if( IsBtCsr(pBase) ){
    rc = btCsrStep((BtCursor*)pBase, 1);
  }else{
    rc = fiCsrStep((FiCursor*)pBase);
  }
  return rc;
}

/*
** Retreat to the previous entry in the tree.
*/
int sqlite4BtCsrPrev(bt_cursor *pBase){
  int rc;
  if( IsBtCsr(pBase) ){
    rc = btCsrStep((BtCursor*)pBase, 0);
  }else{
    rc = fiCsrStep((FiCursor*)pBase);
  }
  return rc;
}

/*
** Helper function for btOverflowDelete(). 
**
** TODO: This uses recursion. Which is almost certainly not a problem 
** here, but makes some people nervous, so should probably be changed.
*/
static int btOverflowTrimtree(
  const int pgsz, 
  BtPager *pPager, 
  u32 pgno, 
  int nDepth
){
  int rc = SQLITE4_OK;

  assert( nDepth<=8 );
  if( nDepth>0 ){
    const int nPgPtr = pgsz / 4;
    BtPage *pPg;
    u8 *aData;
    int i;

    rc = sqlite4BtPageGet(pPager, pgno, &pPg);
    if( rc!=SQLITE4_OK ) return rc;
    aData = btPageData(pPg);

    for(i=0; rc==SQLITE4_OK && i<nPgPtr; i++){
      u32 child = btGetU32(&aData[i*4]);
      if( child==0 ) break;
      rc = btOverflowTrimtree(pgsz, pPager, child, nDepth-1);
    }

    sqlite4BtPageRelease(pPg);
  }
  
  sqlite4BtPageTrimPgno(pPager, pgno);
  return rc;
}

/*
** Cursor pCsr currently points to a leaf page cell. If the leaf page
** cell contains an overflow array, all overflow pages are trimmed here.
**
** SQLITE4_OK is returned if no error occurs, or an SQLite4 error code
** otherwise.
*/
static int btOverflowDelete(BtCursor *pCsr){
  BtPager *pPager = pCsr->base.pDb->pPager;
  const int pgsz = sqlite4BtPagerPagesize(pPager);
  u8 *aData;
  u8 *pCell;
  u8 *pOvfl = 0;
  int iCell = pCsr->aiCell[pCsr->nPg-1];
  int n;
  int rc = SQLITE4_OK;
  
  aData = (u8*)btPageData(pCsr->apPage[pCsr->nPg-1]);
  assert( btCellCount(aData, pgsz)>iCell );
  pCell = btCellFind(aData, pgsz, iCell);
  pCell += sqlite4BtVarintGet32(pCell, &n);

  if( n==0 ){
    /* Type (c) cell */
    pCell += sqlite4BtVarintGet32(pCell, &n);
    pCell += n;
    pCell += sqlite4BtVarintGet32(pCell, &n);
    pCell += sqlite4BtVarintGet32(pCell, &n);
    pOvfl = pCell;
  }else{
    pCell += n;
    pCell += sqlite4BtVarintGet32(pCell, &n);
    if( n==0 ){
      /* Type (b) cell */
      pCell += sqlite4BtVarintGet32(pCell, &n);
      pCell += n;
      pCell += sqlite4BtVarintGet32(pCell, &n);
      pOvfl = pCell;
    }
  }

  if( pOvfl ){
    int i;
    int nDirect = (int)(pOvfl[0] & 0x0F);
    int nDepth = (int)(pOvfl[0]>>4);

    /* Trim the "direct" pages. */
    for(i=0; rc==SQLITE4_OK && i<(nDirect + (nDepth==0)); i++){
      u32 pgno = btGetU32(&pOvfl[1 + i*4]);
      rc = sqlite4BtPageTrimPgno(pPager, pgno);
    }

    /* Now trim the pages that make up the overflow tree. */
    if( nDepth>0 ){
      u32 rootpgno = btGetU32(&pOvfl[1 + nDirect*4]);
      rc = btOverflowTrimtree(pgsz, pPager, rootpgno, nDepth);
    }
  }

  return rc;
}

static int btCsrData(
  BtCursor *pCsr,                 /* Cursor handle */
  int iOffset,                    /* Offset of requested data */
  int nByte,                      /* Bytes requested (or -ve for all avail.) */
  const void **ppV,               /* OUT: Pointer to data buffer */
  int *pnV                        /* OUT: Size of data buffer in bytes */
){
  const int pgsz = sqlite4BtPagerPagesize(pCsr->base.pDb->pPager);
  int rc;
  u8 *aData;
  u8 *pCell;
  int nK = 0;
  int nV = 0;

  rc = btCsrReseek(pCsr);
  if( rc==SQLITE4_OK ){
    if( pCsr->bSkipNext || pCsr->bSkipPrev ){
      /* The row has been deleted out from under this cursor. So return
       ** NULL for data.  */
      *ppV = 0;
      *pnV = 0;
    }else{
      int iCell = pCsr->aiCell[pCsr->nPg-1];

      aData = (u8*)btPageData(pCsr->apPage[pCsr->nPg-1]);
      pCell = btCellFind(aData, pgsz, iCell);
      pCell += sqlite4BtVarintGet32(pCell, &nK);
      if( nK>0 ){
        pCell += nK;
        pCell += sqlite4BtVarintGet32(pCell, &nV);
      }

      if( nV==0 ){
        /* Type (b) or (c) cell */
        rc = btCsrBuffer(pCsr, 1);
        if( rc==SQLITE4_OK ){
          u8 *aBuf = (u8*)pCsr->ovfl.buf.p;
          *ppV = &aBuf[pCsr->ovfl.nKey];
          *pnV = pCsr->ovfl.nVal;
        }
      }else{
        /* Type (a) cell */
        *ppV = pCell;
        *pnV = (nV-2);
      }

#ifndef NDEBUG
      if( rc==SQLITE4_OK ){
        const void *pK; int nK;
        rc = sqlite4BtCsrKey((bt_cursor*)pCsr, &pK, &nK);
        if( rc==SQLITE4_OK ){
          BtLock *pLock = (BtLock*)pCsr->base.pDb->pPager;
          sqlite4BtDebugKV(pLock, "select", (u8*)pK, nK, (u8*)*ppV, *pnV);
        }
      }
#endif
    }
  }

  return rc;
}

int sqlite4BtCsrKey(bt_cursor *pBase, const void **ppK, int *pnK){
  int rc = SQLITE4_OK;            /* Return code */
  
  if( IsBtCsr(pBase) ){
    rc = btCsrKey((BtCursor*)pBase, ppK, pnK);
  }else{
    FiCursor *pCsr = (FiCursor*)pBase;
    assert( pCsr->iBt>=0 );
    rc = btCsrKey(&pCsr->aSub[pCsr->iBt].csr, ppK, pnK);
  }

  return rc;
}

int sqlite4BtCsrData(
  bt_cursor *pBase,               /* Cursor handle */
  int iOffset,                    /* Offset of requested data */
  int nByte,                      /* Bytes requested (or -ve for all avail.) */
  const void **ppV,               /* OUT: Pointer to data buffer */
  int *pnV                        /* OUT: Size of data buffer in bytes */
){
  int rc = SQLITE4_OK;            /* Return code */
  
  if( IsBtCsr(pBase) ){
    rc = btCsrData((BtCursor*)pBase, iOffset, nByte, ppV, pnV);
  }else{
    FiCursor *pCsr = (FiCursor*)pBase;
    assert( pCsr->iBt>=0 );
    rc = btCsrData(&pCsr->aSub[pCsr->iBt].csr, iOffset, nByte, ppV, pnV);
  }

  return rc;
}

/*
** The argument points to a buffer containing an overflow array. Return
** the size of the overflow array in bytes. 
*/
static int btOverflowArrayLen(u8 *p){
  return 1 + ((int)(p[0] & 0x0F) + 1) * 4;
}

static int btCellSize(u8 *pCell, int bLeaf){
  u8 *p = pCell;
  int nKey;

  p += sqlite4BtVarintGet32(p, &nKey);
  if( bLeaf==0 ){
    /* Internal page cell */
    p += nKey;
    p += 4;
  }else if( nKey==0 ){
    /* Type (c) cell */
    p += sqlite4BtVarintGet32(p, &nKey);
    p += nKey;
    p += sqlite4BtVarintGet32(p, &nKey);
    p += sqlite4BtVarintGet32(p, &nKey);
    p += btOverflowArrayLen(p);
  }else{
    p += nKey;
    p += sqlite4BtVarintGet32(p, &nKey);
    if( nKey==0 ){
      /* Type (b) cell */
      p += sqlite4BtVarintGet32(p, &nKey);
      p += nKey;
      p += sqlite4BtVarintGet32(p, &nKey);
      p += btOverflowArrayLen(p);
    }else if( nKey>=2 ){
      p += (nKey-2);
    }
  }

  return (p-pCell);
}

static u8 *btCellFindSize(u8 *aData, int nData, int iCell, int *pnByte){
  u8 *pCell;

  pCell = btCellFind(aData, nData, iCell);
  *pnByte = btCellSize(pCell, 0==(btFlags(aData) & BT_PGFLAGS_INTERNAL));
  return pCell;
}

/*
** Return a pointer to and the size of the cell that cursor pCsr currently
** points to.
*/
static void fiCsrCell(FiCursor *pCsr, const void **ppCell, int *pnCell){
  const int pgsz = sqlite4BtPagerPagesize(pCsr->base.pDb->pPager);
  FiSubCursor *pSub;              /* Current sub-cursor */
  u8 *aData;                      /* Current page data */
  int iCell;

  assert( pCsr->iBt>=0 );
  pSub = &pCsr->aSub[pCsr->iBt];
  aData = btPageData(pSub->csr.apPage[pSub->csr.nPg-1]);
  iCell = pSub->csr.aiCell[pSub->csr.nPg-1];

  *ppCell = btCellFindSize(aData, pgsz, iCell, pnCell);
}

/*
** Return true if the cell that the cursor currently points to contains 
** pointers to one or more overflow pages. Or false otherwise.
*/
static int btCsrOverflow(BtCursor *pCsr){
  const int pgsz = sqlite4BtPagerPagesize(pCsr->base.pDb->pPager);
  u8 *aData;                      /* Current page data */
  int nKey;                       /* First varint in cell */
  int res;                       /* First varint in cell */

  aData = btPageData(pCsr->apPage[pCsr->nPg-1]);
  aData = btCellFind(aData, pgsz, pCsr->aiCell[pCsr->nPg-1]);

  aData += sqlite4BtVarintGet32(aData, &nKey);
  res = (nKey==0 || aData[nKey]==0);
  return res;
}

/*
** Allocate a new page buffer.
*/
static int btNewBuffer(bt_db *pDb, u8 **paBuf){
  const int pgsz = sqlite4BtPagerPagesize(pDb->pPager);
  u8 *aBuf;

  *paBuf = aBuf = sqlite4_malloc(pDb->pEnv, pgsz);
  if( aBuf==0 ) return btErrorBkpt(SQLITE4_NOMEM);

#ifndef NDEBUG
  /* This stops valgrind from complaining about unitialized bytes when (if)
  ** this buffer is eventually written out to disk.  */
  memset(aBuf, 0x66, pgsz);
#endif

  return SQLITE4_OK;
}

/*
** Discard a page buffer allocated using btNewBuffer.
*/
static void btFreeBuffer(bt_db *pDb, u8 *aBuf){
  sqlite4_free(pDb->pEnv, aBuf);
}

/*
** Attach a buffer to an existing page object.
*/
static int btSetBuffer(bt_db *pDb, BtPage *pPg, u8 *aBuf){
  const int pgsz = sqlite4BtPagerPagesize(pDb->pPager);
  int rc;
  rc = sqlite4BtPageWrite(pPg);
  if( rc==SQLITE4_OK ){
    u8 *aData = btPageData(pPg);
    memcpy(aData, aBuf, pgsz);
    sqlite4_free(pDb->pEnv, aBuf);
  }
  return rc;
}

/*
** Defragment the b-tree page passed as the first argument. Return 
** SQLITE4_OK if successful, or an SQLite error code otherwise.
*/
static int btDefragmentPage(bt_db *pDb, BtPage *pPg){
  const int pgsz = sqlite4BtPagerPagesize(pDb->pPager);
  u8 *aData;                      /* Pointer to buffer of pPg */
  u8 *aTmp;                       /* Temporary buffer to assemble new page in */
  int nCell;                      /* Number of cells on page */
  int iWrite;                     /* Write next cell at this offset in aTmp[] */
  int i;                          /* Used to iterate through cells */
  int bLeaf;                      /* True if pPg is a leaf page */
  int nHdr;                       /* Bytes in header of this page */

  if( btNewBuffer(pDb, &aTmp) ) return SQLITE4_NOMEM;

  aData = btPageData(pPg);
  nCell = btCellCount(aData, pgsz);

  bLeaf = 0==(btFlags(aData) & BT_PGFLAGS_INTERNAL);
  nHdr = bLeaf ? 1 : 5;

  /* Set header bytes of new page */
  memcpy(aTmp, aData, nHdr);

  iWrite = nHdr;
  for(i=0; i<nCell; i++){
    int nByte;
    u8 *pCell;
    pCell = btCellFindSize(aData, pgsz, i, &nByte);

    btPutU16(btCellPtrFind(aTmp, pgsz, i), iWrite);
    memcpy(&aTmp[iWrite], pCell, nByte);
    iWrite += nByte;
  }

  /* Write the rest of the page footer */
  btPutU16(&aTmp[pgsz-2], nCell);
  btPutU16(&aTmp[pgsz-4], pgsz - (3+nCell)*2 - iWrite);
  btPutU16(&aTmp[pgsz-6], iWrite);

  btSetBuffer(pDb, pPg, aTmp);
  return SQLITE4_OK;
}

/*
** The following type is used to represent a single cell or cell value
** by the code that updates and rebalances the tree structure. It is
** usually manipulated using the btKV*() functions and macros.
**
** An instance of type KeyValue may represent three different types
** of cell values:
**
**   * (eType==KV_VALUE && pgno!=0): A value for an internal cell. In this case
**     pK points to a buffer nK bytes in size containing the key prefix and 
**     pgno contains the page number of the cells child page.
**
**   * (eType==KV_VALUE && pgno==0): A value for a leaf cell. The 
**     key is identified by (pK/nK) and the value by (pV/nV).
**
**   * (eType==KV_CELL): A formatted leaf cell stored in (pV/nV). This is 
**     used for cells with overflow arrays.
*/
#define KV_VALUE     0
#define KV_CELL      1
typedef struct KeyValue KeyValue;
struct KeyValue {
  int eType;
  const void *pK; int nK;
  const void *pV; int nV;
  u32 pgno;
};

/*
** Return the number of bytes consumed by a cell generated based on *pKV.
**
** If the KeyValue is not already in KV_CELL form, then assume it will
** be formatted as a type (a) cell.
*/
static int btKVCellSize(KeyValue *pKV){
  int nByte;
  assert( pKV->eType==KV_CELL || pKV->eType==KV_VALUE );
  if( pKV->eType==KV_CELL ){
    nByte = pKV->nV;
  }else{
    if( pKV->pgno ){
      nByte = sqlite4BtVarintLen32(pKV->nK) + pKV->nK + 4;
    }else{
      assert( pKV->nV>=0 || pKV->pV==0 );
      nByte = 
        sqlite4BtVarintLen32(pKV->nK) 
        + sqlite4BtVarintLen32(pKV->nV+2)
        + MAX(pKV->nV, 0) + pKV->nK;
    }
  }
  return nByte;
}

/*
** Write a cell based on *pKV to buffer aBuffer. Return the number
** of bytes written.
*/
static int btKVCellWrite(KeyValue *pKV, u8 *aBuf){
  int i = 0;
  if( pKV->eType==KV_CELL ){
    i = pKV->nV;
    memcpy(aBuf, pKV->pV, i);
  }else{
    i += sqlite4BtVarintPut32(&aBuf[i], pKV->nK);
    memcpy(&aBuf[i], pKV->pK, pKV->nK); i += pKV->nK;

    if( pKV->pgno==0 ){
      i += sqlite4BtVarintPut32(&aBuf[i], pKV->nV+2);
      if( pKV->nV>0 ){
        memcpy(&aBuf[i], pKV->pV, pKV->nV); 
        i += pKV->nV;
      }
    }else{
      btPutU32(&aBuf[i], pKV->pgno);
      i += 4;
    }
  }

  assert( i==btKVCellSize(pKV) );
  return i;
}

/*
** Return the number of bytes of leaf page space required by an 
** overflow array containing nContent bytes of content, assuming the 
** page size is pgsz.
*/
static int btOverflowArraySz(int pgsz, int nContent){
  int nPg;
  nPg = (nContent + pgsz - 1) / pgsz;
  if( nPg<=BT_MAX_DIRECT_OVERFLOW ){
    return 1 + nPg*4;
  }
  return 1 + (BT_MAX_DIRECT_OVERFLOW+1) * 4;
}

/*
** Allocate a non-overflow page.
**
** This function is a simple wrapper around sqlite4BtPageAllocate(),
** except that if the database is currenly in fast-insert mode the
** BtDbHdr.nSubPg counter is incremented.
*/
static int btAllocateNonOverflow(bt_db *db, BtPage **ppPg){
  int rc;
  if( db->bFastInsertOp ){
    BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
    u32 iPg;

    sqlite4BtPagerDbhdrDirty(db->pPager);
    iPg = pHdr->nSubPg + btFirstOfBlock(pHdr, pHdr->iSubBlock);
    pHdr->nSubPg++;
    rc = sqlite4BtPageGet(db->pPager, iPg, ppPg);
    if( rc==SQLITE4_OK ){
      rc = sqlite4BtPageWrite(*ppPg);
      if( rc!=SQLITE4_OK ){
        sqlite4BtPageRelease(*ppPg);
      }
    }
  }else{
    rc = sqlite4BtPageAllocate(db->pPager, ppPg);
  }
  return rc;
}

/*
** Trim a non-overflow page.
*/
static int btTrimNonOverflow(bt_db *db, BtPage *pPg){
  int rc;                         /* Return code */
  if( db->bFastInsertOp==0 ){
    rc = sqlite4BtPageTrim(pPg);
  }else{
    rc = sqlite4BtPageRelease(pPg);
  }
  return rc;
}

/*
** Allocate and zero an overflow page.
*/
static int btAllocateAndZero(bt_db *db, BtPage **ppPg){
  BtPage *pPg = 0;                /* Allocated page handle */
  int rc;                         /* Return code */

  rc = sqlite4BtPageAllocate(db->pPager, &pPg);
  if( rc==SQLITE4_OK ){
    const int pgsz = sqlite4BtPagerPagesize(db->pPager);
    memset(btPageData(pPg), 0, pgsz);
  }

  *ppPg = pPg;
  return rc;
}

static int btOverflowArrayPopulate(
  bt_db *db, u8 **ppOut,
  u8 *pBuf1, int nBuf1,
  u8 *pBuf2, int nBuf2
){
  const int pgsz = sqlite4BtPagerPagesize(db->pPager);
  const int nPgPtr = pgsz / 4;
  int rc = SQLITE4_OK;
  int n1 = 0;
  int n2 = 0;
  int iOvfl;
  int nDirect = 0;
  int nDepth = 0;
  int nOvfl;
  int i;
  u8 *aOut = *ppOut;

  struct Heir {
    BtPage *pPg;
    int iCell;
  } apHier[8];
  memset(apHier, 0, sizeof(apHier));

  /* Calculate the number of required overflow pages. And the depth of
  ** the overflow tree.  */
  nOvfl = (nBuf1+nBuf2+pgsz-1) / pgsz;
  nOvfl -= BT_MAX_DIRECT_OVERFLOW;
  while( nOvfl>1 ){
    nDepth++;
    nOvfl = (nOvfl+nPgPtr-1) / nPgPtr;
  }

  for(i=0; rc==SQLITE4_OK && i<nDepth; i++){
    u32 pgno;
    rc = btAllocateAndZero(db, &apHier[i].pPg);
    pgno = sqlite4BtPagePgno(apHier[i].pPg);
    if( i==0 ){
      btPutU32(&aOut[1 + BT_MAX_DIRECT_OVERFLOW*4], pgno);
    }else{
      u8 *a = btPageData(apHier[i-1].pPg);
      btPutU32(a, pgno);
      apHier[i-1].iCell++;
    }
  }

  for(iOvfl=0; rc==SQLITE4_OK && (n1<nBuf1 || n2<nBuf2); iOvfl++){
    int nCopy1, nCopy2;           /* Bytes to copy from pBuf1 and pBuf2 */
    u8 *aData;
    BtPage *pPg;
    u32 pgno;

    rc = sqlite4BtPageAllocate(db->pPager, &pPg);
    if( rc!=SQLITE4_OK ) break;
    aData = btPageData(pPg);
    pgno = sqlite4BtPagePgno(pPg);

    nCopy1 = MIN(pgsz, nBuf1 - n1);
    nCopy2 = MIN(pgsz - nCopy1, nBuf2 - n2);

    memcpy(aData, &pBuf1[n1], nCopy1); n1 += nCopy1;
    memcpy(&aData[nCopy1], &pBuf2[n2], nCopy2); n2 += nCopy2;
    rc = sqlite4BtPageRelease(pPg);
    if( rc!=SQLITE4_OK ) break;

    if( iOvfl<(BT_MAX_DIRECT_OVERFLOW+(nDepth==0)) ){
      btPutU32(&aOut[1 + iOvfl*4], pgno);
      nDirect++;
    }else{
      assert( nDepth>0 );
      for(i=nDepth-1; pgno && i>=0; i--){
        u8 *a = btPageData(apHier[i].pPg);
        if( apHier[i].iCell==nPgPtr ){
          BtPage *pNew = 0;
          rc = sqlite4BtPageRelease(apHier[i].pPg);
          if( rc==SQLITE4_OK ){
            rc = btAllocateAndZero(db, &pNew);
            if( rc==SQLITE4_OK ){
              u8 *a = btPageData(pNew);
              btPutU32(a, pgno);
              pgno = sqlite4BtPagePgno(pNew);
            }
          }

          if( rc!=SQLITE4_OK ){
            pgno = 0;
          }

          apHier[i].pPg = pNew;
          apHier[i].iCell = 1;
        }else{
          btPutU32(&a[apHier[i].iCell*4], pgno);
          apHier[i].iCell++;
          pgno = 0;
        }
      }
    }
  }

  for(i=0; i<nDepth; i++){
    int rc2 = sqlite4BtPageRelease(apHier[i].pPg);
    if( rc==SQLITE4_OK ) rc = rc2;
  }

  if( rc==SQLITE4_OK ){
    if( nDepth==0 ){
      nDirect--;
    }
    assert( nDirect>=0 );
    aOut[0] = (u8)nDirect | (u8)(nDepth<<4) ;
  }

  *ppOut = &aOut[1 + (nDirect+1)*4];
  return rc;
}

/*
** Argument pKV contains a key/value pair destined for a leaf page in
** a database with page size pgsz. Currently it is in KV_VALUE form.
** If the key/value pair is too large to fit entirely within a leaf
** page, this function allocates and writes the required overflow
** pages to the database, and converts pKV to a KV_CELL cell (that
** contains the overflow array).
*/
static int btOverflowAssign(bt_db *db, KeyValue *pKV){
  const int pgsz = sqlite4BtPagerPagesize(db->pPager);
  int nMaxSize = (pgsz - 1 - 6 - 2);
  int nReq;
  int rc = SQLITE4_OK;

  assert( pKV->pgno==0 && pKV->eType==KV_VALUE );

  /* Check if this is a type (a) cell - one that can fit entirely on a 
  ** leaf page. If so, do nothing.  */
  nReq = btKVCellSize(pKV);
  if( nReq > nMaxSize ){
    int nArraySz = btOverflowArraySz(pgsz, pKV->nK + MAX(0, pKV->nV));
    u8 *pBuf = 0;                 /* Buffer containing formatted cell */
    int nKeyOvfl;                 /* Bytes of key that overflow */
    int nValOvfl;                 /* Bytes of value that overflow */

    /* Check if the entire key can fit on a leaf page. If so, this is a
    ** type (b) page - entire key and partial value on the leaf page, 
    ** overflow pages contain the rest of the value.  
    **
    ** This expression uses sqlite4BtVarintLen32() to calculate an upper
    ** bound for the size of the varint that indicates the number of bytes
    ** of the value stored locally.  */
    nReq = 1 + sqlite4BtVarintLen32(pKV->nK) + pKV->nK 
         + 1 + sqlite4BtVarintLen32(pKV->nV) + nArraySz;
    if( nReq<nMaxSize && pKV->nV>=0 ){
      /* nSpc is initialized to the amount of space available to store:
      **
      **    * varint containing number of bytes stored locally (nLVal).
      **    * nLVal bytes of content.
      **    * varint containing number of bytes in overflow pages.
      */
      int nLVal;                  /* Bytes of value data on main page */
      int nSpc = (nMaxSize 
          - sqlite4BtVarintLen32(pKV->nK) - pKV->nK - 1 - nArraySz
      );
      nLVal = nSpc - sqlite4BtVarintLen32(pgsz) - sqlite4BtVarintLen32(pKV->nV);
      nKeyOvfl = 0;
      nValOvfl = pKV->nV - nLVal;
    }else{
      /* Type (c) cell. Both the key and value overflow. */
      int nLKey = nMaxSize 
          - 1                                    /* 0x00 byte */
          - sqlite4BtVarintLen32(pgsz)           /* nLKey */
          - sqlite4BtVarintLen32(pKV->nK)        /* nOKey */
          - sqlite4BtVarintLen32(pKV->nV+1)      /* nVal */
          - nArraySz;                            /* overflow array */

      nValOvfl = pKV->nV;
      nKeyOvfl = pKV->nK - nLKey;
    }

    /* Allocate a pager buffer to store the KV_CELL buffer. Using a pager
    ** buffer is convenient here as (a) it is roughly the right size and
    ** (b) can probably be allocated/deallocated faster than a regular
    ** heap allocation.  */
    rc = btNewBuffer(db, &pBuf);

    if( rc==SQLITE4_OK ){
      int nLVal = (pKV->nV - nValOvfl);
      int nLKey = (pKV->nK - nKeyOvfl);
      u8 *pOut = pBuf;

      if( nKeyOvfl>0 ){
        *pOut++ = 0x00;
      }
      pOut += sqlite4BtVarintPut32(pOut, nLKey);
      memcpy(pOut, pKV->pK, nLKey);
      pOut += nLKey;
      if( nKeyOvfl==0 ){
        /* Type (b) cell */
        assert( pKV->nV>=0 );
        *pOut++ = 0x00;
        pOut += sqlite4BtVarintPut32(pOut, nLVal);
        memcpy(pOut, pKV->pV, nLVal);
        pOut += nLVal;
      }else{
        /* Type (c) cell */
        pOut += sqlite4BtVarintPut32(pOut, nKeyOvfl);
      }
      pOut += sqlite4BtVarintPut32(pOut, nValOvfl + (nKeyOvfl>0));

      rc = btOverflowArrayPopulate(db, &pOut,
          (u8*)(pKV->pK) + nLKey, nKeyOvfl,
          (u8*)(pKV->pV) + nLVal, MAX(0, nValOvfl)
      );
      if( rc==SQLITE4_OK ){
        memset(pKV, 0, sizeof(*pKV));
        pKV->pV = pBuf;
        pKV->nV = pOut - pBuf;
        pKV->eType = KV_CELL;
        pBuf = 0;
        assert( pKV->nV<=nMaxSize );
        assert( pKV->nV==btCellSize((u8*)pKV->pV, 1) );
      }
    }

    if( pBuf ){
      btFreeBuffer(db, pBuf);
    }
  }

  return rc;
}

typedef struct BalanceCtx BalanceCtx;
struct BalanceCtx {
  int pgsz;                       /* Database page size */
  int bLeaf;                      /* True if we are rebalancing leaf data */
  u8 flags;                       /* Flags byte for new sibling pages */
  BtCursor *pCsr;                 /* Cursor identifying where to insert pKV */
  int nKV;                        /* Number of KV pairs */
  KeyValue *apKV;                 /* New KV pairs being inserted */

  /* Populated by btGatherSiblings */
  int nIn;                        /* Number of sibling pages */
  BtPage *apPg[5];                /* Array of sibling pages */

  int nCell;                      /* Number of input cells */

  /* Array populated by btBalanceMeasure */
  int *anCellSz;
  
  /* Populated in btBalance() */
  int anOut[5];                   /* Cell counts for output pages */

  /* Variables used by btBalanceOutput */
  int nOut;                       /* Number of output pages */
  int iOut;                       /* Current output page */
  u8 *apOut[5];                   /* Buffers to assemble output in */
  KeyValue aPCell[5];             /* Cells to push into the parent page */
  u8 *pTmp;                       /* Space for apCell[x].pKey if required */
  int iTmp;                       /* Offset to free space within pTmp */
};

static int btGatherSiblings(BalanceCtx *p){
  BtCursor *pCsr = p->pCsr;
  bt_db * const pDb = pCsr->base.pDb; 
  const int pgsz = sqlite4BtPagerPagesize(pDb->pPager);

  int rc = SQLITE4_OK;
  int nCell;                      /* Number of cells in parent page */
  u8 *aParent;                    /* Buffer of parent page */
  int iChild;                     /* Index of child page within parent */
  int nSib;                       /* Number of siblings */
  int iSib;                       /* Index of left-most sibling page */

  int i;

  aParent = btPageData(pCsr->apPage[pCsr->nPg-2]);
  iChild = pCsr->aiCell[pCsr->nPg-2];
  nCell = btCellCount(aParent, pgsz);

  if( nCell<2 ){
    nSib = nCell+1;
  }else{
    nSib = 3;
  }

  if( iChild==0 ){
    iSib = 0;
  }else if( iChild==nCell ){
    iSib = nCell-(nSib-1);
  }else{
    iSib = iChild-1;
  }

  for(i=0; i<nSib && rc==SQLITE4_OK; i++){
    u32 pgno = btChildPgno(aParent, pgsz, iSib+i);
    rc = sqlite4BtPageGet(pDb->pPager, pgno, &p->apPg[i]);
    assert( (iSib+i)!=iChild || p->apPg[i]==pCsr->apPage[pCsr->nPg-1] );
  }
  p->nIn = nSib;

  pCsr->aiCell[pCsr->nPg-2] = iSib;
  return rc;
}

/*
** Argument pCell points to a cell on an internal node. Decode the
** cell into key-value object *pKV. An internal cell always has
** the same format:
**
**     * Number of bytes in the key (nKey) as a varint.
**     * nKey bytes of key data.
**     * A page pointer, stored as a 32-bit big-endian unsigned.
*/
static void btInternalCellToKeyValue(u8 *pCell, KeyValue *pKV){
  pKV->pK = pCell + sqlite4BtVarintGet32(pCell, &pKV->nK);
  pKV->pgno = btGetU32(&((u8*)pKV->pK)[pKV->nK]);
  pKV->pV = 0;
  pKV->nV = 0;
  pKV->eType = KV_VALUE;
}

static int btSetChildPgno(bt_db *pDb, BtPage *pPg, int iChild, u32 pgno){
  const int pgsz = sqlite4BtPagerPagesize(pDb->pPager);
  int rc;

  rc = sqlite4BtPageWrite(pPg);
  if( rc==SQLITE4_OK ){
    u8 *aData = btPageData(pPg);
    int nCell = btCellCount(aData, pgsz);
    if( iChild>=nCell ){
      btPutU32(&aData[1], pgno);
    }else{
      int nKey;
      u8 *pCell = btCellFind(aData, pgsz, iChild);
      pCell += sqlite4BtVarintGet32(pCell, &nKey);
      pCell += nKey;
      btPutU32(pCell, pgno);
    }
  }

  return rc;
}

/* Called recursively by btBalance(). todo: Fix this! */
static int btInsertAndBalance(BtCursor *, int, KeyValue *);
static int btDeleteFromPage(BtCursor *, int);
static int btBalanceIfUnderfull(BtCursor *pCsr);

static int btBalanceMeasure(
  BalanceCtx *p,                  /* Description of balance operation */
  int iCell,                      /* Cell number in this iteration */
  u8 *pCell, int nByte,           /* Binary cell */
  KeyValue *pKV                   /* Key-value cell */
){
  if( pCell ){
    p->anCellSz[iCell] = nByte;
  }else{
    p->anCellSz[iCell] = btKVCellSize(pKV);
  }
  return SQLITE4_OK;
}

static int btBalanceOutput(
  BalanceCtx *p,                  /* Description of balance operation */
  int iCell,                      /* Cell number in this iteration */
  u8 *pCell, int nByte,           /* Binary cell to copy to output */
  KeyValue *pKV                   /* Key-value cell to write to output */
){
  u8 *aOut = p->apOut[p->iOut];   /* Buffer for current output page */
  int iOff;                       /* Offset of new cell within page */
  int nCell;                      /* Number of cells already on page */

  assert( (pCell==0)!=(pKV==0) );

  if( p->bLeaf==0 && iCell==p->anOut[p->iOut] ){
    /* This cell is destined for the parent page of the siblings being
    ** rebalanced. So instead of writing it to a page buffer it is copied
    ** into the BalanceCtx.aPCell[] array. 
    **
    ** When this cell is eventually written to the parent, the accompanying 
    ** page pointer will be the page number of sibling page p->iOut. This
    ** value will be filled in later. 
    **
    ** The pointer that is currently part of the cell is used as the 
    ** right-child pointer of page p->iOut. This value is written now. */
    int nKey;
    u8 *pKey;
    u8 *pCopy;
    u32 pgno;
    KeyValue *pPKey = &p->aPCell[p->iOut];

    if( pCell ){
      pKey = pCell + sqlite4BtVarintGet32(pCell, &nKey);
      pgno = btGetU32(&pKey[nKey]);
    }else{
      assert( pKV->eType==KV_VALUE );
      pKey = (u8*)pKV->pK;
      nKey = pKV->nK;
      pgno = pKV->pgno;
    }

    pCopy = &p->pTmp[p->iTmp];
    p->iTmp += nKey;
    memcpy(pCopy, pKey, nKey);
    pPKey->pK = pCopy;
    pPKey->nK = nKey;

    btPutU32(&aOut[1], pgno);
    p->iOut++;
  }else{

    /* Write the new cell into the output page. */
    iOff = btFreeOffset(aOut, p->pgsz);
    if( iOff==0 ) iOff = (p->bLeaf ? 1 : 5);
    nCell = btCellCount(aOut, p->pgsz);
    btPutU16(btCellPtrFind(aOut, p->pgsz, nCell), iOff);
    if( pCell ){
      memcpy(&aOut[iOff], pCell, nByte);
      iOff += nByte;
    }else{
      iOff += btKVCellWrite(pKV, &aOut[iOff]);
    }
    btPutU16(&aOut[p->pgsz-2], nCell+1);
    btPutU16(&aOut[p->pgsz-6], iOff);

    if( (iCell+1)==p->anOut[p->iOut] ){
      /* That was the last cell for this page. Fill in the rest of the 
      ** output page footer and the flags byte at the start of the page.  */
      int nFree;                    /* Free space remaining on output page */
      nFree = p->pgsz - iOff - (6 + 2*(nCell+1));
      aOut[0] = p->flags;
      btPutU16(&aOut[p->pgsz-4], nFree);

      /* If the siblings are leaf pages, increment BalanceCtx.iOut here.
      ** for internal nodes, it will be incremented by the next call to
      ** this function, after a divider cell is pushed into the parent 
      ** node.  */
      p->iOut += p->bLeaf;
    }
  }

  return SQLITE4_OK;
}

static int btBalanceVisitCells(
  BalanceCtx *p,
  int (*xVisit)(BalanceCtx*, int, u8*, int, KeyValue*)
){
  const int pgsz = sqlite4BtPagerPagesize(p->pCsr->base.pDb->pPager);
  int rc = SQLITE4_OK;            /* Return code */
  int iPg;                        /* Current page in apPg[] */
  int iCall = 0;
  int i;                          /* Used to iterate through KV pairs */

  BtPage *pIns = p->pCsr->apPage[p->pCsr->nPg-1];
  int iIns = p->pCsr->aiCell[p->pCsr->nPg-1];

  /* Check that page pIns is actually a member of the ctx.apPg[] array. */
#ifndef NDEBUG
  for(i=0; p->apPg[i]!=pIns; i++) assert( i<p->nIn );
#endif

  for(iPg=0; iPg<p->nIn && rc==SQLITE4_OK; iPg++){
    BtPage *pPg;                  /* Current page */
    u8 *aData;                    /* Page data */
    int nCell;                    /* Number of cells on page pPg */
    int iCell;                    /* Current cell in pPg */

    pPg = p->apPg[iPg];
    aData = btPageData(pPg);
    nCell = btCellCount(aData, pgsz);

    for(iCell=0; iCell<nCell && rc==SQLITE4_OK; iCell++){
      int nByte;
      u8 *pCell;

      if( pPg==pIns && iCell==iIns ){
        for(i=0; i<p->nKV; i++){
          assert( iCall<p->nCell );
          rc = xVisit(p, iCall++, 0, 0, &p->apKV[i]);
          if( rc!=SQLITE4_OK ) break;
        }
      }

      pCell = btCellFindSize(aData, pgsz, iCell, &nByte);
      rc = xVisit(p, iCall++, pCell, nByte, 0);
    }

    if( pPg==pIns && iIns==nCell ){
      for(i=0; i<p->nKV && rc==SQLITE4_OK; i++){
        assert( iCall<p->nCell );
        rc = xVisit(p, iCall++, 0, 0, &p->apKV[i]);
      }
    }

    /* If the siblings being balanced are not leaves, and the page just
    ** processed was not the right-most sibling, visit a cell from the
    ** parent page.  */
    if( p->bLeaf==0 && iPg<(p->nIn-1) && rc==SQLITE4_OK ){
      int iPar = p->pCsr->nPg-2;
      u8 *aParent = btPageData(p->pCsr->apPage[iPar]);
      u8 *pCell = btCellFind(aParent, pgsz, p->pCsr->aiCell[iPar] + iPg);
      KeyValue kv;
      btInternalCellToKeyValue(pCell, &kv);
      kv.pgno = btGetU32(&aData[1]);
      rc = xVisit(p, iCall++, 0, 0, &kv);
    }
  }

  assert( rc!=SQLITE4_OK || iCall==p->nCell );
  return rc;
}

/*
** Extract a key-prefix from page pPg, which resides in a database with
** page size pgsz. If parameter bLast is true, the key-prefix is extracted
** from the right-most cell on the page. If bLast is false, the key-prefix
** is extracted from the left-most cell.
**
** A pointer to the key-prefix is returned. Before returning, *pnByte is
** set to the size of the prefix in bytes.
*/
static u8 *btKeyPrefix(const int pgsz, BtPage *pPg, int bLast, int *pnByte){
  u8 *p;
  int n;
  u8 *aData;

  aData = btPageData(pPg);
  p = btCellFind(aData, pgsz, bLast ? btCellCount(aData, pgsz)-1 : 0);
  p += sqlite4BtVarintGet32(p, &n);
  if( n==0 ) p += sqlite4BtVarintGet32(p, &n);

  *pnByte = n;
  return p;
}

/*
** Parameters pLeft and pRight point to a pair of adjacent leaf pages in
** a database with page size pgsz. The keys in pRight are larger than those
** in pLeft. This function populates pKV->pK and pKV->nK with a separator
** key that is:
**
**   * larger than all keys on pLeft, and 
**   * smaller than or equal to all keys on pRight.
*/
static void btPrefixKey(
    const int pgsz, BtPage *pLeft, BtPage *pRight, KeyValue *pKV
){
  int nMax;
  int nMaxPrefix = BT_MAX_INTERNAL_KEY;

  u8 *aLeft; int nLeft;
  u8 *aRight; int nRight;
  int i;

  aLeft = btKeyPrefix(pgsz, pLeft, 1, &nLeft);
  aRight = btKeyPrefix(pgsz, pRight, 0, &nRight);

  nMax = MIN(nLeft, nMaxPrefix);
  for(i=0; i<nMax && aLeft[i]==aRight[i]; i++);
  if( i<nMaxPrefix ){
    pKV->pK = aRight;
    pKV->nK = i + 1;
    assert( pKV->nK<=nRight );
  }
}

int btBalance(
  BtCursor *pCsr,                 /* Cursor pointed to page to rebalance */
  int bLeaf,                      /* True if rebalancing leaf pages */
  int nKV,                        /* Number of entries in apKV[] array */
  KeyValue *apKV                  /* Extra entries to add while rebalancing */
){
  bt_db * const pDb = pCsr->base.pDb; 
  const int pgsz = sqlite4BtPagerPagesize(pDb->pPager);
  const int nSpacePerPage = (pgsz - 1 - 6 - (!bLeaf)*4);

  int iPg;                        /* Used to iterate through pages */
  int iCell;                      /* Used to iterate through cells */

  int anByteOut[5];               /* Bytes of content on each output page */
  BtPage *pPar;                   /* Parent page */
  int iSib;                       /* Index of left-most sibling */

  int rc = SQLITE4_OK;            /* Return code */

  BalanceCtx ctx;
  memset(&ctx, 0, sizeof(ctx));
  ctx.pCsr = pCsr;
  ctx.nKV = nKV;
  ctx.apKV = apKV;
  ctx.pgsz = pgsz;
  ctx.bLeaf = bLeaf;
  ctx.flags = *(u8*)btPageData(pCsr->apPage[pCsr->nPg-1]);

  memset(anByteOut, 0, sizeof(anByteOut));

  /* Gather the sibling pages from which cells will be redistributed into
  ** the ctx.apPg[] array.  */
  assert( bLeaf==0 || bLeaf==1 );
  assert( pCsr->nPg>1 );
  rc = btGatherSiblings(&ctx);
  if( rc!=SQLITE4_OK ) goto rebalance_out;
  pPar = pCsr->apPage[pCsr->nPg-2];
  iSib = pCsr->aiCell[pCsr->nPg-2];

  /* Count the number of input cells. */
  ctx.nCell = nKV;
  for(iPg=0; iPg<ctx.nIn; iPg++){
    u8 *aData = btPageData(ctx.apPg[iPg]);
    ctx.nCell += btCellCount(aData, pgsz);
  }
  if( bLeaf==0 ) ctx.nCell += (ctx.nIn-1);
  assert( ctx.nCell>0 );

  /* Allocate and populate the anCellSz[] array */
  ctx.anCellSz = (int*)sqlite4_malloc(pDb->pEnv, sizeof(int)*ctx.nCell);
  if( ctx.anCellSz==0 ){
    rc = btErrorBkpt(SQLITE4_NOMEM);
    goto rebalance_out;
  }
  rc = btBalanceVisitCells(&ctx, btBalanceMeasure);

  /* Now figure out the number of output pages required. Set ctx.nOut to 
  ** this value. */
  iCell = 0;
  for(iPg=0; iCell<ctx.nCell; iPg++){
    assert( iPg<array_size(ctx.anOut) );
    if( bLeaf==0 && iPg!=0 ){
      /* This cell will be pushed up to the parent node as a divider cell,
      ** not written to any output page.  */
      iCell++;
    }
    assert( anByteOut[iPg]==0 );
    for(/* noop */; iCell<ctx.nCell; iCell++){
      int nByte = (ctx.anCellSz[iCell] + 2);
      if( nByte+anByteOut[iPg]>nSpacePerPage ) break;
      anByteOut[iPg] += nByte;
    }
    ctx.anOut[iPg] = iCell;
  }
  ctx.nOut = iPg;
  assert( ctx.anOut[ctx.nOut-1]==ctx.nCell );

  /* The loop in the previous block populated the anOut[] array in such a
  ** way as to make the (ctx.nOut-1) leftmost pages completely full but 
  ** leave the rightmost page partially empty. Or, if bLeaf==0, perhaps
  ** even completely empty. This block attempts to redistribute cells a bit 
  ** more evenly. 
  */
  iCell = ctx.nCell;
  
  for(iPg=(ctx.nOut-2); iPg>=0; iPg--){
    int iR = iPg+1;
    while( 1 ){
      int nLeft = ctx.anCellSz[ ctx.anOut[iPg]-1 ] + 2;
      int nRight = (bLeaf ? nLeft : (ctx.anCellSz[ ctx.anOut[iPg] ] + 2));

      if( anByteOut[iPg]==nLeft || (anByteOut[iR] + nRight) > anByteOut[iPg] ){
        break;
      }
      ctx.anOut[iPg]--;
      anByteOut[iPg] -= nLeft;
      anByteOut[iR] += nRight;
    }
  }

#ifdef BT_STDERR_DEBUG
  {
    int iDbg;
    fprintf(stderr, 
        "\nbtBalance(): bLeaf=%d nIn=%d anIn[] = ", ctx.bLeaf, ctx.nIn
    );
    for(iDbg=0; iDbg<ctx.nIn; iDbg++){
      u8 *aData = btPageData(ctx.apPg[iDbg]);
      fprintf(stderr, "%d ", btCellCount(aData, pgsz));
    }
    fprintf(stderr, " ->  nOut=%d anOut[] = ", ctx.nOut);
    for(iDbg=0; iDbg<ctx.nOut; iDbg++){
      fprintf(stderr, "%d ", ctx.anOut[iDbg]);
    }
    fprintf(stderr, "\n");
    fflush(stderr);
  }
#endif

  /* Allocate buffers for the output pages. If the pages being balanced
  ** are not leaves, grab one more buffer from the pager layer to use
  ** to temporarily store a copy of the keys destined for the parent
  ** page.  */
  for(iPg=0; iPg<ctx.nOut; iPg++){
    rc = btNewBuffer(pDb, &ctx.apOut[iPg]);
    if( rc!=SQLITE4_OK ) goto rebalance_out;
    memset(ctx.apOut[iPg] + pgsz-6, 0, 6);
  }
  if( bLeaf==0 ){
    rc = btNewBuffer(pDb, &ctx.pTmp);
    if( rc!=SQLITE4_OK ) goto rebalance_out;
  }

  /* Populate the new buffers with the new page images. */
  rc = btBalanceVisitCells(&ctx, btBalanceOutput);
  if( rc!=SQLITE4_OK ) goto rebalance_out;

  if( ctx.bLeaf==0 ){
    /* Set the right-child pointer of the rightmost new sibling to a copy
    ** of the same pointer from the rightmost original sibling.  */
    u8 *aRightSibling = btPageData(ctx.apPg[ctx.nIn-1]);
    memcpy(&(ctx.apOut[ctx.nOut-1])[1], &aRightSibling[1], 4);
  }

  /* Clobber the old pages with the new buffers */
  for(iPg=0; iPg<ctx.nOut; iPg++){
    if( iPg>=ctx.nIn ){
      rc = btAllocateNonOverflow(pDb, &ctx.apPg[iPg]);
      if( rc!=SQLITE4_OK ) goto rebalance_out;
    }
    btSetBuffer(pDb, ctx.apPg[iPg], ctx.apOut[iPg]);
    ctx.apOut[iPg] = 0;
  }
  for(iPg=ctx.nOut; iPg<ctx.nIn; iPg++){
    rc = btTrimNonOverflow(pDb, ctx.apPg[iPg]);
    ctx.apPg[iPg] = 0;
    if( rc!=SQLITE4_OK ) goto rebalance_out;
  }

#ifdef BT_STDERR_DEBUG
  {
    int iDbg;
    for(iDbg=0; iDbg<ctx.nOut; iDbg++){
      u8 *aData = btPageData(ctx.apPg[iDbg]);
      printPage(stderr, sqlite4BtPagePgno(ctx.apPg[iDbg]), aData, pgsz);
    }
  }
#endif

  /* The leaves are written. Now gather the keys and page numbers to
  ** push up into the parent page. This is only required when rebalancing
  ** b-tree leaves. When internal nodes are balanced, the btBalanceOutput
  ** loop accumulates the cells destined for the parent page.  */
  for(iPg=0; iPg<(ctx.nOut-1); iPg++){
    ctx.aPCell[iPg].pgno = sqlite4BtPagePgno(ctx.apPg[iPg]);
    if( bLeaf ){
      assert( ctx.aPCell[iPg].nK==0 );
      btPrefixKey(pgsz, ctx.apPg[iPg], ctx.apPg[iPg+1], &ctx.aPCell[iPg]);
    }
  }

  rc = btSetChildPgno(
      pDb, pPar, iSib+ctx.nIn-1, sqlite4BtPagePgno(ctx.apPg[ctx.nOut-1])
  );
  if( rc==SQLITE4_OK ){
    btCsrAscend(pCsr, 1);
    rc = btDeleteFromPage(pCsr, ctx.nIn-1);
  }
  iPg = pCsr->nPg;
  if( rc==SQLITE4_OK && ctx.nOut>1 ){
    rc = btInsertAndBalance(pCsr, ctx.nOut-1, ctx.aPCell);
  }
  if( rc==SQLITE4_OK && iPg==pCsr->nPg ){
    rc = btBalanceIfUnderfull(pCsr);
  }

#ifdef BT_STDERR_DEBUG
  {
    u8 *aData = btPageData(pPar);
    printPage(stderr, sqlite4BtPagePgno(pPar), aData, pgsz);
  }
#endif

 rebalance_out:
  for(iPg=0; iPg<array_size(ctx.apPg); iPg++){
    sqlite4BtPageRelease(ctx.apPg[iPg]);
  }
  btFreeBuffer(pDb, ctx.pTmp);
  sqlite4_free(pDb->pEnv, ctx.anCellSz);
  return rc;
}

static int btExtendTree(BtCursor *pCsr){
  bt_db * const pDb = pCsr->base.pDb;
  BtDbHdr *pHdr = sqlite4BtPagerDbhdr(pDb->pPager);
  const int pgsz = pHdr->pgsz;
  int rc;                         /* Return code */
  BtPage *pNew;                   /* New (and only) child of root page */
  BtPage *pRoot = pCsr->apPage[0];

  assert( pCsr->nPg==1 );

  rc = sqlite4BtPageWrite(pRoot);
  if( rc==SQLITE4_OK ){
    rc = btAllocateNonOverflow(pDb, &pNew);
  }
  if( rc==SQLITE4_OK ){
    u8 *aRoot = btPageData(pRoot);
    u8 *aData = btPageData(pNew);

    memcpy(aData, aRoot, pgsz);
    aRoot[0] = BT_PGFLAGS_INTERNAL;
    if( pHdr->iMRoot==pCsr->iRoot ) aRoot[0] |= BT_PGFLAGS_METATREE;
    btPutU32(&aRoot[1], sqlite4BtPagePgno(pNew));
    btPutU16(&aRoot[pgsz-2], 0);
    btPutU16(&aRoot[pgsz-4], 5);
    btPutU16(&aRoot[pgsz-6], pgsz - 5 - 6);

    pCsr->nPg = 2;
    pCsr->aiCell[1] = pCsr->aiCell[0];
    pCsr->apPage[1] = pNew;
    pCsr->aiCell[0] = 0;
  }

  return rc;
}

/*
** The cursor passed as the first argument points to a leaf page into
** which the array of KV pairs specified by the second and third arguments
** would be inserted, except that there is insufficient free space on
** the page.
**
** If nKV==1, the tree is more than one level high (i.e. the root is not a
** leaf) and the new key is larger than all existing keys on the page,
** handle this in the same way as an SQLite3 "quick-balance" operation.
** Return SQLITE4_OK if successful, or an error code if an error occurs.
**
** If the quick-balance is not attempted, return SQLITE4_NOTFOUND.
*/
static int btTryAppend(BtCursor *pCsr, int nKV, KeyValue *apKV){
  int rc = SQLITE4_NOTFOUND;
  if( nKV==1 && pCsr->nPg>1 ){
    bt_db *pDb = pCsr->base.pDb;
    const int pgsz = sqlite4BtPagerPagesize(pDb->pPager);
    BtPage *pOld = pCsr->apPage[pCsr->nPg-1];
    u8 *aData = btPageData(pOld);
    int nCell = btCellCount(aData, pgsz);
    if( nCell==pCsr->aiCell[pCsr->nPg-1] ){
      KeyValue kv;
      BtPage *pNew = 0;
      rc = btAllocateNonOverflow(pDb, &pNew);
      if( rc==SQLITE4_OK ){
        aData = btPageData(pNew);
        btPutU16(&aData[pgsz-2], 0);
        aData[0] = 0x00;
        pCsr->apPage[pCsr->nPg-1] = pNew;
        pCsr->aiCell[pCsr->nPg-1] = 0;
        rc = btInsertAndBalance(pCsr, 1, apKV);
      }
      if( rc==SQLITE4_OK ){
        rc = btSetChildPgno(pDb, 
            pCsr->apPage[pCsr->nPg-2], pCsr->aiCell[pCsr->nPg-2], 
            sqlite4BtPagePgno(pNew)
        );
      }
      if( rc==SQLITE4_OK ){
        assert( pCsr->apPage[pCsr->nPg-1]==pNew );
        btPrefixKey(pgsz, pOld, pNew, &kv);
        kv.pgno = sqlite4BtPagePgno(pOld);
        kv.pV = 0;
        kv.nV = 0;
        kv.eType = 0;
        rc = btCsrAscend(pCsr, 1);
      }
      if( rc==SQLITE4_OK ){
        rc = btInsertAndBalance(pCsr, 1, &kv);
      }
      if( pNew ) sqlite4BtPageRelease(pOld);
    }
  }
  return rc;
}

/*
** The cursor currently points to a cell on a b-tree page that may or
** may not be a leaf page. This routine modifies the contents of that
** page, balancing the b-tree if necessary. The page is modified as
** follows:
**
**     * nDel entries, starting with the one the cursor points to, are
**       deleted from the page.
**
**     * nKV entries are inserted in their place.
**
** The tree balancing routine is called if this causes the page to
** become either overfull or to contain no entries at all.
*/
static int btInsertAndBalance(
  BtCursor *pCsr,                 /* Cursor identifying page to modify */
  int nKV,                        /* Number of entries in apKV */
  KeyValue *apKV                  /* New cells to insert into the page */
){
  int rc = SQLITE4_OK;
  const int pgsz = sqlite4BtPagerPagesize(pCsr->base.pDb->pPager);
  u8 *aData;                      /* Page buffer */
  int nCell;                      /* Number of cells on this page already */
  int nFree;                      /* Contiguous free space on this page */
  int nReq = 0;                   /* Space required for type (a) cells */
  int iCell;                      /* Position to insert new key */
  int iWrite;                     /* Byte offset at which to write new cell */
  int i;
  int bLeaf;                      /* True if inserting into leaf page */
  BtPage *pLeaf;

  bLeaf = (apKV[0].pgno==0);
  assert( bLeaf==0 || nKV==1 );

  /* Determine the number of bytes of space required on the current page. */
  for(i=0; i<nKV; i++){
    nReq += btKVCellSize(&apKV[i]) + 2;
  }

  iCell = pCsr->aiCell[pCsr->nPg-1];
  assert( pCsr->nPg>0 );
  pLeaf = pCsr->apPage[pCsr->nPg-1];
  aData = (u8*)btPageData(pLeaf);

  /* Set the bLeaf variable to true if inserting into a leaf page, or
  ** false otherwise. Return SQLITE4_CORRUPT if the page is a leaf but
  ** the KeyValue pairs being inserted are suitable for internal nodes,
  ** or vice-versa.  */
  assert( nKV>0 );
  if( (0==(btFlags(aData) & BT_PGFLAGS_INTERNAL))!=bLeaf ){
    return btErrorBkpt(SQLITE4_CORRUPT);
  }

  nCell = btCellCount(aData, pgsz);
  assert( iCell<=btCellCount(aData, pgsz) );

  if( nCell==0 ){
    /* If the nCell field is zero, then the rest of the header may 
    ** contain invalid values (zeroes - as it may never have been 
    ** initialized). So set our stack variables to values appropriate
    ** to an empty page explicitly here.  */
    iWrite = (bLeaf ? 1 : 5);
    nFree = pgsz - iWrite - 6;
  }else{
    if( btFreeContiguous(aData, pgsz)<nReq && btFreeSpace(aData, pgsz)>=nReq ){
      /* Special case - the new entry will not fit on the page at present
      ** but would if the page were defragmented. So defragment it before
      ** continuing.  */
      rc = btDefragmentPage(pCsr->base.pDb, pLeaf);
      aData = btPageData(pLeaf);
    }

    iWrite = btFreeOffset(aData, pgsz);
    nFree = btFreeContiguous(aData, pgsz);
  }

  if( nFree>=nReq ){
    /* The new entry will fit on the page. So in this case all there
    ** is to do is update this single page. The easy case. */
    rc = sqlite4BtPageWrite(pLeaf);
    if( rc==SQLITE4_OK ){
      aData = btPageData(pLeaf);

      /* Make space within the cell pointer array */
      if( iCell!=nCell ){
        u8 *aFrom = btCellPtrFind(aData, pgsz, nCell-1);
        u8 *aTo = btCellPtrFind(aData, pgsz, nCell-1+nKV);
        memmove(aTo, aFrom, (nCell-iCell) * 2);
      }

      for(i=0; i<nKV; i++){
        /* Write the cell pointer */
        btPutU16(btCellPtrFind(aData, pgsz, iCell+i), iWrite);
      
        /* Write the cell itself */
        iWrite += btKVCellWrite(&apKV[i], &aData[iWrite]);
      }

      /* Set the new total free space */
      if( nCell==0 ){
        btPutU16(&aData[pgsz-4], nFree - nReq);
      }else{
        btPutU16(&aData[pgsz-4], btFreeSpace(aData, pgsz) - nReq);
      }

      /* Increase cell count */
      btPutU16(&aData[pgsz-2], nCell+nKV);

      /* Set the offset to the block of empty space */
      btPutU16(&aData[pgsz-6], iWrite);
    }

  }else{
    /* The new entry will not fit on the leaf page. Entries will have
    ** to be shuffled between existing leaves and new leaves may need
    ** to be added to make space for it. */
    bt_db *db = pCsr->base.pDb;
    if( bLeaf && db->bFastInsertOp ){
      /* This operation will need to allocate further pages. The worst
      ** case scenario is (nDepth+1) pages. If fewer than that remain
      ** available in the block, return BT_BLOCKFULL. */
      BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
      int nPgPerBlk = (pHdr->blksz / pHdr->pgsz);
      if( (nPgPerBlk - pHdr->nSubPg) < pCsr->nPg+1 ){
        sqlite4BtPagerDbhdrDirty(db->pPager);
        rc = BT_BLOCKFULL;
        pHdr->iSubBlock = 0;
      }
    }
    if( rc==SQLITE4_OK && pCsr->nPg==1 ){
      rc = btExtendTree(pCsr);
    }
    if( rc==SQLITE4_OK ){
      rc = btBalance(pCsr, bLeaf, nKV, apKV);
#if 0
      if( bLeaf==0 || SQLITE4_NOTFOUND==(rc = btTryAppend(pCsr, nKV, apKV)) ){
        rc = btBalance(pCsr, bLeaf, nKV, apKV);
      }
#endif
    }
  }

  return rc;
}

static int btDeleteFromPage(BtCursor *pCsr, int nDel){
  const int pgsz = sqlite4BtPagerPagesize(pCsr->base.pDb->pPager);
  int rc = SQLITE4_OK;            /* Return code */
  BtPage *pPg;                    /* Page to delete entries from */

  pPg = pCsr->apPage[pCsr->nPg-1];
  rc = sqlite4BtPageWrite(pPg);
  if( rc==SQLITE4_OK ){
    int i;                        /* Used to iterate through cells to delete */
    u8 *aData;                    /* Page buffer */
    int nCell;                    /* Number of cells initially on this page */
    int iDel;                     /* Index of cell to delete */
    int nFreed = 0;               /* Total bytes of space freed */

    iDel = pCsr->aiCell[pCsr->nPg-1];
    aData = (u8*)btPageData(pPg);
    nCell = btCellCount(aData, pgsz);

    for(i=iDel; i<(iDel+nDel); i++){
      int nByte;
      btCellFindSize(aData, pgsz, i, &nByte);
      nFreed += nByte + 2;
    }

    if( (iDel+nDel)<nCell ){
      u8 *aTo = btCellPtrFind(aData, pgsz, nCell-1-nDel);
      u8 *aFrom = btCellPtrFind(aData, pgsz, nCell-1);
      memmove(aTo, aFrom, 2*(nCell-(iDel+nDel)));
    }

    /* Decrease cell count */
    btPutU16(&aData[pgsz-2], nCell-nDel);

    /* Increase total free space */
    btPutU16(&aData[pgsz-4], btFreeSpace(aData, pgsz) + nFreed);
  }
  
  return rc;
}

static int btBalanceIfUnderfull(BtCursor *pCsr){
  const int pgsz = sqlite4BtPagerPagesize(pCsr->base.pDb->pPager);
  int rc = SQLITE4_OK;
  int iPg = pCsr->nPg-1;
  BtPage *pPg = pCsr->apPage[iPg];
  u8 *aData = btPageData(pPg);
  int nCell = btCellCount(aData, pgsz);
  int nFree = btFreeSpace(aData, pgsz);
  int bLeaf = (0==(btFlags(aData) & BT_PGFLAGS_INTERNAL));

  if( iPg==0 ){
    /* Root page. If it contains no cells at all and is not already
    ** a leaf, shorten the tree by one here by copying the contents 
    ** of the only child into the root. */
    if( nCell==0 && bLeaf==0 ){
      BtPager *pPager = pCsr->base.pDb->pPager;
      u32 pgno = btChildPgno(aData, pgsz, 0);
      BtPage *pChild;

      rc = sqlite4BtPageWrite(pPg);
      if( rc==SQLITE4_OK ){
        rc = sqlite4BtPageGet(pPager, pgno, &pChild);
      }
      if( rc==SQLITE4_OK ){
        u8 *a = btPageData(pChild);
        memcpy(aData, a, pgsz);
        rc = btTrimNonOverflow(pCsr->base.pDb, pChild);
      }
    }
  }else if( nCell==0 || (nFree>(2*pgsz/3) && bLeaf==0) ){
    rc = btBalance(pCsr, bLeaf, 0, 0);
  }
  return rc;
}

static int btSaveAllCursor(bt_db *pDb, BtCursor *pCsr){
  int rc = SQLITE4_OK;            /* Return code */
  bt_cursor *pIter;               /* Used to iterate through cursors */

  for(pIter=pDb->pAllCsr; rc==SQLITE4_OK && pIter; pIter=pIter->pNextCsr){
    if( IsBtCsr(pIter) ){
      BtCursor *p = (BtCursor*)pIter;
      if( p->nPg>0 ){
        assert( p->bRequireReseek==0 );
        rc = btCsrBuffer(p, 0);
        if( rc==SQLITE4_OK ){
          assert( p->ovfl.buf.p );
          p->bRequireReseek = 1;
          if( p!=pCsr ) btCsrReleaseAll(p);
        }
      }
    }else{
      /* ?? */
    }
  }

  return rc;
}

static int btFastInsertRoot(bt_db *db, BtDbHdr *pHdr, u32 *piRoot);
static int btScheduleMerge(bt_db *db);

static int btReplaceEntry(
  bt_db *db,                      /* Database handle */
  u32 iRoot,                      /* Root page of b-tree to update */
  const void *pK, int nK,         /* Key to insert */
  const void *pV, int nV          /* Value to insert. (nV<0) -> delete */
){
  BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
  int rc = SQLITE4_OK;            /* Return code */
  BtCursor csr;                  /* Cursor object to seek to insert point */
  u32 iRootPg = iRoot;

  if( iRoot==0 ){
    rc = btFastInsertRoot(db, pHdr, &iRootPg);
  }
  btCsrSetup(db, iRootPg, &csr);

  /* Seek stack cursor csr to the b-tree page that key pK/nK is/would be
  ** stored on.  */
  if( rc==SQLITE4_OK ){
    rc = btCsrSeek(&csr, 0, pK, nK, BT_SEEK_GE, BT_CSRSEEK_UPDATE);
  }

  if( rc==SQLITE4_OK ){
    /* The cursor currently points to an entry with key pK/nK. This call
    ** should therefore replace that entry. So delete it and then re-seek
    ** the cursor.  */
    rc = sqlite4BtDelete(&csr.base);
    if( rc==SQLITE4_OK && (nV>=0 || iRoot==0) ){
      rc = btCsrSeek(&csr, 0, pK, nK, BT_SEEK_GE, BT_CSRSEEK_UPDATE);
      if( rc==SQLITE4_OK ) rc = btErrorBkpt(SQLITE4_CORRUPT);
    }
  }


  if( rc==SQLITE4_NOTFOUND || rc==SQLITE4_INEXACT ){
    if( nV<0 && iRoot!=0 ){
      /* This is a delete on the regular b-tree (not the fast-insert tree).
      ** Nothing more to do.  */
      rc = SQLITE4_OK;
    }else{
      KeyValue kv;
      kv.pgno = 0;
      kv.eType = KV_VALUE;
      kv.pK = pK; kv.nK = nK;
      kv.pV = pV; kv.nV = nV;

      rc = btOverflowAssign(db, &kv);
      if( rc==SQLITE4_OK ){
        do{
          /* Insert the new KV pair into the current leaf. */
          rc = btInsertAndBalance(&csr, 1, &kv);

          /* Unless this is a block-full error, break out of the loop */
          if( rc!=BT_BLOCKFULL ) break;
          assert( iRoot==0 );

          /* Try to schedule a merge operation */
          rc = btScheduleMerge(db);

          if( rc==SQLITE4_OK ){
            rc = btFastInsertRoot(db, pHdr, &iRootPg);
          }
          if( rc==SQLITE4_OK ){
            btCsrReset(&csr, 1);
            btCsrSetup(db, iRootPg, &csr);
            rc = btCsrSeek(&csr, 0, pK, nK, BT_SEEK_GE, BT_CSRSEEK_UPDATE);
          }
        }while( rc==SQLITE4_NOTFOUND || rc==SQLITE4_INEXACT );
      }

      if( kv.eType==KV_CELL ){
        sqlite4_free(db->pEnv, (void*)kv.pV);
      }
    }
  }

  btCsrReset(&csr, 1);
  return rc;
}

static int btAllocateNewRoot(bt_db *db, int flag, u32 *piNew){
  u32 iNew = 0;
  BtPage *pPg;
  int rc;

  assert( flag==BT_PGFLAGS_METATREE || flag==BT_PGFLAGS_SCHEDULE || flag==0 );
  rc = sqlite4BtPageAllocate(db->pPager, &pPg);
  if( rc==SQLITE4_OK ){
    u8 *aData = btPageData(pPg);
    aData[0] = (flag & 0xFF);
    iNew = sqlite4BtPagePgno(pPg);
    sqlite4BtPageRelease(pPg);
  }

  *piNew = iNew;
  return rc;
}

static int btDecodeMetatreeKey(
  BtCursor *pCsr,
  u32 *piAge,
  u32 *piLevel,
  u8 **paKey,
  int *pnKey
){
  u8 *aK; int nK;
  int rc = sqlite4BtCsrKey((bt_cursor*)pCsr, (const void**)&aK, &nK);
  if( rc==SQLITE4_OK ){
    *piAge = btGetU32(&aK[0]);
    *piLevel = ~btGetU32(&aK[4]);
    if( paKey ){
      *paKey = &aK[8];
      *pnKey = nK-8;
    }
  }
  return rc;
}

static void *btMalloc(bt_db *db, int nByte, int *pRc){
  void *pRet = 0;
  if( *pRc==SQLITE4_OK ){
    pRet = sqlite4_malloc(db->pEnv, nByte);
    if( !pRet ) *pRc = btErrorBkpt(SQLITE4_NOMEM);
  }
  return pRet;
}
static void btFree(bt_db *db, void *p){
  sqlite4_free(db->pEnv, p);
}

static int fiLoadSummary(
  bt_db *db, 
  BtCursor *p, 
  const u8 **paSummary, 
  int *pnSummary
){
  static const u8 aZero[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
  int rc;

  btCsrSetup(db, pHdr->iMRoot, p);
  rc = btCsrSeek(
      p, 0, aSummaryKey, sizeof(aSummaryKey), BT_SEEK_EQ, BT_CSRSEEK_SEEK
  );
  if( rc==SQLITE4_OK ){
    rc = btCsrData(p, 0, -1, (const void**)paSummary, pnSummary);
  }else if( rc==SQLITE4_NOTFOUND ){
    *paSummary = aZero;
    *pnSummary = sizeof(aZero);
    rc = SQLITE4_OK;
  }

  return rc;
}

static void btReadSummary(
  const u8 *aSum, int iAge, 
  u16 *piMinLevel,
  u16 *pnLevel,
  u16 *piMergeLevel
){
  if( piMinLevel ) *piMinLevel = btGetU16(&aSum[iAge * 6]);
  if( pnLevel ) *pnLevel = btGetU16(&aSum[iAge * 6 + 2]);
  if( piMergeLevel ) *piMergeLevel = btGetU16(&aSum[iAge * 6 + 4]);
}

static void btWriteSummary(
  u8 *aSum, int iAge,
  u16 iMinLevel,
  u16 nLevel,
  u16 iMergeLevel
){
  btPutU16(&aSum[iAge * 6], iMinLevel);
  btPutU16(&aSum[iAge * 6 + 2], nLevel);
  btPutU16(&aSum[iAge * 6 + 4], iMergeLevel);
}

/*
** Allocate a new level for a new age=0 segment. The new level is always
** one greater than the current largest age=0 level number.
*/
static int btAllocateNewLevel(
  bt_db *db, 
  BtDbHdr *pHdr, 
  u32 *piNext
){

  int rc;                         /* Return code */
  BtCursor csr;                   /* Cursor to read meta-table summary */
  int nByte;                      /* Size of buffer aByte[] */
  const u8 *aByte;                /* Summary data */
  u8 *aNew;                       /* New summary data */

  rc = fiLoadSummary(db, &csr, &aByte, &nByte);

  aNew = (u8*)btMalloc(db, nByte, &rc);
  if( rc==SQLITE4_OK ){
    u16 iMin, nLevel, iMerge;
    btReadSummary(aByte, 0, &iMin, &nLevel, &iMerge);

    memcpy(aNew, aByte, nByte);
    btPutU16(&aNew[2], nLevel+1);
    rc = btReplaceEntry(
        db, pHdr->iMRoot, aSummaryKey, sizeof(aSummaryKey), aNew, nByte
    );
    btFree(db, aNew);

    *piNext = (iMin + nLevel);
  }

  btCsrReset(&csr, 1);
  return rc;
}

static void btWriteSchedule(u8 *aData, BtSchedule *p, int *pRc){
  if( *pRc==SQLITE4_OK ){
    u32 *a = (u32*)p;
    int i;
    for(i=0; i<sizeof(BtSchedule)/sizeof(u32); i++){
      btPutU32(&aData[i*4], a[i]);
    }
  }
}
static int btReadSchedule(bt_db *db, u8 *aData, BtSchedule *p){
  u32 *a = (u32*)p;
  int i;
  for(i=0; i<sizeof(BtSchedule)/sizeof(u32); i++){
    a[i] = btGetU32(&aData[i*4]);
  }
  return SQLITE4_OK;
}

static void btWriteSchedulePage(BtPage *pPg, BtSchedule *p, int *pRc){
  if( *pRc==SQLITE4_OK ){
    int rc = sqlite4BtPageWrite(pPg);
    if( rc==SQLITE4_OK ){
      u8 *aData = btPageData(pPg);
      btWriteSchedule(aData, p, &rc);
    }
    *pRc = rc;
  }
}

static int btAllocateBlock(
  bt_db *db, 
  int nBlk,
  u32 *aiBlk
){
  return sqlite4BtBlockAllocate(db->pPager, nBlk, aiBlk);
}

/*
** This is a helper function for btScheduleMerge(). It determines the
** age and range of levels to be used as inputs by the merge (if any).
*/
static int btFindMerge(
  bt_db *db,                      /* Database handle */
  u32 *piAge,                     /* OUT: Age of input segments to merge */
  u32 *piMinLevel,                /* OUT: Minimum input level value */
  u32 *piMaxLevel,                /* OUT: Maximum input level value */
  u32 *piOutLevel                 /* OUT: Output level value */
){
  BtCursor csr;                   /* Cursor used to read summary record */
  int rc;                         /* Return code */
  const u8 *aSum;
  int nSum;

  rc = fiLoadSummary(db, &csr, &aSum, &nSum);
  if( rc==SQLITE4_OK ){
    int iAge;
    int iBestAge = -1;            /* Best age to merge levels from */
    int nBest = (db->nMinMerge-1);/* Number of levels merged at iBestAge */
    u16 iMin, nLevel, iMerge;     /* Summary of current age */

    rc = SQLITE4_NOTFOUND;
    for(iAge=0; iAge<(nSum/6); iAge++){
      btReadSummary(aSum, iAge, &iMin, &nLevel, &iMerge);
      if( iMerge ){
        int n = 1 + (iMerge-iMin);
        if( n>nBest ){
          *piMinLevel = iMin;
          *piMaxLevel = iMerge;
          *piAge = iAge;
          btReadSummary(aSum, iAge+1, &iMin, &nLevel, &iMerge);
          *piOutLevel = (iMin + nLevel - 1);
          rc = SQLITE4_OK;
        }
        break;
      }else{
        if( nLevel>nBest ){
          iBestAge = iAge;
          nBest = nLevel;
        }
      }
    }

    if( rc==SQLITE4_NOTFOUND && iBestAge>=0 ){
      u8 *aNew;
      int nByte = nSum;
      if( iBestAge+1>=(nSum/6) ) nByte += 6;

      rc = SQLITE4_OK;
      aNew = (u8*)btMalloc(db, nByte, &rc);
      if( rc==SQLITE4_OK ){

        /* Create a copy of the summary record */
        memcpy(aNew, aSum, nSum);
        if( nByte>nSum ) btWriteSummary(aNew, iBestAge+1, 0, 0, 0);

        /* Find the input age and maximum level */
        btReadSummary(aSum, iBestAge, &iMin, &nLevel, &iMerge);
        *piMinLevel = (u32)iMin;
        *piMaxLevel = (u32)(iMin + nLevel - 1);
        *piAge = iBestAge;

        /* Find the output level */
        btReadSummary(aNew, iBestAge+1, &iMin, &nLevel, &iMerge);
        *piOutLevel = iMin + nLevel;
      }
    }
  }

  btCsrReset(&csr, 1);
  return rc;
}

/*
** The connection passed as the first argument to this function currently
** has a write transaction open. The schedule object passed as the second
** is in BT_SCHEDULE_DONE state. This function updates the meta-tree to
** integrate the results of the completed merge into the main fast-insert
** tree structure.
**
** If successful, SQLITE4_OK is returned. If an error occurs, an SQLite
** error code.
*/ 
static int btIntegrateMerge(bt_db *db, BtSchedule *p){
  BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
  const int nPgPerBlk = (pHdr->blksz / pHdr->pgsz);
  int rc = SQLITE4_OK;
  BtCursor csr;                   /* Cursor for reading various sub-trees */
  BtCursor mcsr;                  /* Cursor for reading the meta-tree */
  const void *pKey = 0;           /* If not NULL, first key to leave in input */
  int nKey = 0;                   /* Size of pKey in bytes */
  const u8 *aSum; int nSum;       /* Summary value */
  sqlite4_buffer buf;             /* Buffer object used for various purposes */
  u32 iLvl;
  int iBlk;

  /* Save the value of the fast-insert flag. It will be restored before
  ** this function returns. Leaving it set here interferes with page 
  ** allocation if the meta-tree needs to be extended.  */
  const int bFastInsertOp = db->bFastInsertOp;
  db->bFastInsertOp = 0;
  
#if 0
  static int nCall = 0; nCall++;
  fprintf(stderr, "BEFORE %d\n", nCall);
  btPrintMetaTree(db->pPager, 1, pHdr);
#endif
  assert_summary_ok(db, SQLITE4_OK);

  memset(&csr, 0, sizeof(csr));
  memset(&mcsr, 0, sizeof(mcsr));
  btCsrSetup(db, pHdr->iMRoot, &mcsr);
  sqlite4_buffer_init(&buf, 0);
  
  if( p->iNextPg ){
    btCsrSetup(db, p->iNextPg, &csr);
    rc = btCsrEnd(&csr, 0);
    if( rc==SQLITE4_OK ){
      csr.aiCell[0] = p->iNextCell;
      rc = btCsrKey(&csr, &pKey, &nKey);
    }
  }

  if( p->iFreeList ){
    u32 iTrunk = p->iFreeList;
    BtCursor delcsr;
    memset(&delcsr, 0, sizeof(BtCursor));
    delcsr.nPg = 1;
    delcsr.base.pDb = db;

    while( rc==SQLITE4_OK && iTrunk!=0 ){
      BtPage *pTrunk = 0;
      rc = sqlite4BtPageGet(db->pPager, iTrunk, &pTrunk);
      if( rc==SQLITE4_OK ){
        u8 *aTData = btPageData(pTrunk);
        int nOvfl = btGetU32(aTData);
        int i;

        for(i=0; i<nOvfl; i++){
          u32 lpgno = btGetU32(&aTData[8 + i*8]);
          delcsr.aiCell[0] = (int)btGetU32(&aTData[8 + i*8 + 4]);
          rc = sqlite4BtPageGet(db->pPager, lpgno, &delcsr.apPage[0]);
          if( rc==SQLITE4_OK ){
            rc = btOverflowDelete(&delcsr);
            sqlite4BtPageRelease(delcsr.apPage[0]);
          }
        }

        iTrunk = btGetU32(&aTData[4]);
        sqlite4BtPageRelease(pTrunk);
      }
    }
  }

  /* The following loop iterates through each of the input levels. Each
  ** level is either removed from the database completely (if the merge
  ** completed) or else modified so that it contains no keys smaller
  ** than (pKey/nKey).  */ 
  for(iLvl=p->iMinLevel; iLvl<=p->iMaxLevel; iLvl++){
    u8 aPrefix[8];
    u32 iRoot = 0;

    /* Seek mcsr to the first sub-tree (smallest keys) in level iLvl. */
    btPutU32(&aPrefix[0], p->iAge);
    btPutU32(&aPrefix[4], ~iLvl);
    rc = btCsrSeek(&mcsr, 0, aPrefix, sizeof(aPrefix), BT_SEEK_GE, 0);
    if( rc==SQLITE4_INEXACT ) rc = SQLITE4_OK;

    /* Loop through the meta-tree entries that compose level iLvl, deleting 
    ** them as they are visited. If (pKey!=0), stop (and do not delete) the 
    ** first sub-tree for which all keys are larger than pKey.
    **
    ** When the loop exits, variable iRoot is left set to the root page of
    ** the last sub-tree deleted.  */
    while( rc==SQLITE4_OK ){
      const u8 *pMKey; int nMKey;
      rc = btCsrKey(&mcsr, (const void **)&pMKey, &nMKey);

      if( rc==SQLITE4_OK ){
        if( nMKey<sizeof(aPrefix) || memcmp(aPrefix, pMKey, sizeof(aPrefix)) ){
          rc = SQLITE4_NOTFOUND;
        }else{
          rc = SQLITE4_OK;
        }
      }
      if( rc==SQLITE4_OK ){
        if( pKey ){
          int res = btKeyCompare(pMKey + 8, nMKey - 8, pKey, nKey);
          if( res>0 ){
            break;
          }
        }
        if( iRoot ){
          rc = sqlite4BtBlockTrim(db->pPager, 1 + (iRoot / nPgPerBlk));
        }
      }

      if( rc==SQLITE4_OK ){
        const void *pData; int nData;
        btCsrData(&mcsr, 0, 4, &pData, &nData);
        iRoot = btGetU32(pData);
        rc = sqlite4BtDelete(&mcsr.base);
      }

      if( rc==SQLITE4_OK ){
        /* rc = btCsrStep(&mcsr, 1); */
        rc = btCsrSeek(&mcsr, 0, aPrefix, sizeof(aPrefix), BT_SEEK_GE, 0);
        if( rc==SQLITE4_INEXACT ) rc = SQLITE4_OK;
      }
    }
    if( rc==SQLITE4_NOTFOUND ) rc = SQLITE4_OK;

    if( rc==SQLITE4_OK && iRoot ){
      if( pKey ){
        int n = sizeof(aPrefix) + nKey;
        rc = sqlite4_buffer_resize(&buf, n);
        if( rc==SQLITE4_OK ){
          u8 aData[4];
          u8 *a = (u8*)buf.p;
          memcpy(a, aPrefix, sizeof(aPrefix));
          memcpy(&a[sizeof(aPrefix)], pKey, nKey);
          btPutU32(aData, iRoot);
          rc = btReplaceEntry(db, pHdr->iMRoot, a, n, aData, sizeof(aData));
        }
      }else{
        rc = sqlite4BtBlockTrim(db->pPager, 1 + (iRoot / nPgPerBlk));
      }
    }
  }

  /* Add new entries for the new output level blocks. */
  for(iBlk=0; 
      rc==SQLITE4_OK && iBlk<array_size(p->aRoot) && p->aRoot[iBlk]; 
      iBlk++
  ){
    btCsrReset(&csr, 1);
    btCsrSetup(db, p->aRoot[iBlk], &csr);
    rc = btCsrEnd(&csr, 0);
    if( rc==SQLITE4_OK ){
      rc = btCsrKey(&csr, &pKey, &nKey);
    }
    if( rc==SQLITE4_OK ){
      rc = sqlite4_buffer_resize(&buf, nKey+8);
    }
    if( rc==SQLITE4_OK ){
      u8 aData[4];
      u8 *a = (u8*)buf.p;
      btPutU32(a, p->iAge+1);
      btPutU32(&a[4], ~p->iOutLevel);
      memcpy(&a[8], pKey, nKey);
      btPutU32(aData, p->aRoot[iBlk]);
      rc = btReplaceEntry(db, pHdr->iMRoot, a, nKey+8, aData, sizeof(aData));
    }
  }

  /* Trim any unused blocks */
  while( rc==SQLITE4_OK && iBlk<array_size(p->aBlock) && p->aBlock[iBlk] ){
    rc = sqlite4BtBlockTrim(db->pPager, p->aBlock[iBlk]);
    iBlk++;
  }

  /* Update the summary record with the outcome of the merge operation.  */
  if( rc==SQLITE4_OK ){
    btCsrReset(&csr, 1);
    rc = fiLoadSummary(db, &csr, &aSum, &nSum);
  }
  if( rc==SQLITE4_OK ){
    rc = sqlite4_buffer_resize(&buf, MAX(nSum, (p->iAge+1+1)*6));
    if( rc==SQLITE4_OK ){
      u16 iMinLevel = 0;
      u16 nLevel = 0;
      u16 iMergeLevel = 0;

      memcpy(buf.p, aSum, nSum);
      if( nSum>(6*(p->iAge+1)) ){
        btReadSummary(aSum, p->iAge+1, &iMinLevel, &nLevel, &iMergeLevel);
      }
      if( (iMinLevel+nLevel)>=p->iOutLevel ){
        nLevel = p->iOutLevel - iMinLevel + 1;
        btWriteSummary((u8*)buf.p, p->iAge+1, iMinLevel, nLevel, iMergeLevel);
      }

      btReadSummary(aSum, p->iAge, &iMinLevel, &nLevel, &iMergeLevel);
      if( p->iNextPg==0 ){
        u16 nNewLevel = nLevel - (1 + p->iMaxLevel - p->iMinLevel);
        iMinLevel = (nNewLevel==0 ? 0 : p->iMaxLevel+1);
        btWriteSummary((u8*)buf.p, p->iAge, iMinLevel, nNewLevel, 0);
      }else{
        btWriteSummary((u8*)buf.p, p->iAge, iMinLevel, nLevel, p->iMaxLevel);
      }

      rc = btReplaceEntry(
          db, pHdr->iMRoot, aSummaryKey, sizeof(aSummaryKey), buf.p, buf.n
      );
    }
  }

  btCsrReset(&csr, 1);
  btCsrReset(&mcsr, 1);
  sqlite4_buffer_clear(&buf);

#if 0
  if( rc==SQLITE4_OK ){
    btPrintMetaTree(db->pPager, 1, pHdr);
    sqlite4BtDebugFastTree(db, nCall);
  }
#endif
  assert_summary_ok(db, SQLITE4_OK);
  db->bFastInsertOp = bFastInsertOp;
  return rc;
}

/*
** If possible, schedule a merge operation. 
**
** The merge operation is selected based on the following criteria:
**
**   * The more levels involved in the merge the better, and
**   * It is better to merge younger segments than older ones.
*/
static int btScheduleMerge(bt_db *db){
  BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
  BtPage *pPg = 0;                /* Schedule page */
  u8 *aData;                      /* Schedule page data */
  int rc;                         /* Return code */

  /* Details of proposed merge: */
  u32 iAge;                       /* Input age */
  u32 iMin;                       /* Minimum input level number */
  u32 iMax;                       /* Maximum input level number */
  u32 iOutLvl;                    /* Output level number */

  /* Find the schedule page. If there is no schedule page, allocate it now. */
  if( pHdr->iSRoot==0 ){
    rc = sqlite4BtPageAllocate(db->pPager, &pPg);
    if( rc==SQLITE4_OK ){
      u8 *aData = btPageData(pPg);
      memset(aData, 0, pHdr->pgsz);
      sqlite4BtPagerDbhdrDirty(db->pPager);
      pHdr->iSRoot = sqlite4BtPagePgno(pPg);
    }
  }else{
    rc = sqlite4BtPageGet(db->pPager, pHdr->iSRoot, &pPg);
  }

  /* Check if the schedule page is busy. If so, no new merge may be 
  ** scheduled. If the schedule page is not busy, call btFindMerge() to
  ** figure out which levels should be scheduled for merge.  */
  if( rc==SQLITE4_OK ){
    aData = btPageData(pPg);
    
    switch( btGetU32(aData) ){
      case BT_SCHEDULE_BUSY:
        rc = SQLITE4_NOTFOUND;
        break;

      case BT_SCHEDULE_DONE: {
        BtSchedule s;
        rc = btReadSchedule(db, aData, &s);
        if( rc==SQLITE4_OK ){
          rc = btIntegrateMerge(db, &s);
        }
        if( rc==SQLITE4_OK ){
          s.eBusy = BT_SCHEDULE_EMPTY;
          btWriteSchedulePage(pPg, &s, &rc);
        }
        break;
      }

      default: /* BT_SCHEDULE_EMPTY */
        break;
    }

    if( rc==SQLITE4_OK ){
      rc = btFindMerge(db, &iAge, &iMin, &iMax, &iOutLvl);
    }
  }

  if( rc==SQLITE4_OK ){
    BtSchedule s;
    memset(&s, 0, sizeof(BtSchedule));

    s.eBusy = BT_SCHEDULE_BUSY;
    s.iAge = iAge;
    s.iMaxLevel = iMax;
    s.iMinLevel = iMin;
    s.iOutLevel = iOutLvl;
    rc = btAllocateBlock(db, db->nScheduleAlloc, s.aBlock);

    btWriteSchedulePage(pPg, &s, &rc);
  }

  sqlite4BtPageRelease(pPg);
  if( rc==SQLITE4_NOTFOUND ) rc = SQLITE4_OK;
  return rc;
}

static int btFastInsertRoot(
  bt_db *db, 
  BtDbHdr *pHdr, 
  u32 *piRoot
){
  int rc = SQLITE4_OK;

  assert( db->bFastInsertOp );
  db->bFastInsertOp = 0;

  /* If the meta-tree has not been created, create it now. */
  if( pHdr->iMRoot==0 ){
    sqlite4BtPagerDbhdrDirty(db->pPager);
    rc = btAllocateNewRoot(db, BT_PGFLAGS_METATREE, &pHdr->iMRoot);
  }

  /* If no writable sub-tree current exists, create one */ 
  if( rc==SQLITE4_OK && pHdr->iSubBlock==0 ){
    u32 iLevel;                   /* Level number for new sub-tree */
    u32 iSubBlock;                /* New block */

    rc = btAllocateNewLevel(db, pHdr, &iLevel);
    if( rc==SQLITE4_OK ){
      rc = btAllocateBlock(db, 1, &iSubBlock);
    }

    if( rc==SQLITE4_OK ){
      u8 aKey[8];
      u8 aVal[4];
      sqlite4BtPagerDbhdrDirty(db->pPager);
      pHdr->iSubBlock = iSubBlock;
      pHdr->nSubPg = 1;           /* Root page is automatically allocated */

      /* The key for the new entry consists of the concatentation of two 
      ** 32-bit big-endian integers - the <age> and <level-no>. The age
      ** of the new segment is 0. The level number is one greater than the
      ** level number of the previous segment.  */
      btPutU32(&aKey[0], 0);
      btPutU32(&aKey[4], ~iLevel);
      btPutU32(&aVal[0], btFirstOfBlock(pHdr, iSubBlock));
      rc = btReplaceEntry(db, pHdr->iMRoot, aKey, 8, aVal, 4);
    }

    if( rc==SQLITE4_OK ){
      u32 iRoot = btFirstOfBlock(pHdr, pHdr->iSubBlock);
      BtPage *pPg = 0;

      rc = sqlite4BtPageGet(db->pPager, iRoot, &pPg);
      if( rc==SQLITE4_OK ) rc = sqlite4BtPageWrite(pPg);
      if( rc==SQLITE4_OK ){
        u8 *aData = btPageData(pPg);
        memset(&aData[pHdr->pgsz-6], 0, 6);
        aData[0] = 0;
      }
      sqlite4BtPageRelease(pPg);
    }
  }

  if( rc==SQLITE4_OK ){
    *piRoot = btFirstOfBlock(pHdr, pHdr->iSubBlock);
  }
  db->bFastInsertOp = 1;
  return rc;
}

/*
** Set up a fast-insert cursor to read the input data for a merge operation.
*/
static int fiSetupMergeCsr(
  bt_db *db,                      /* Database handle */
  BtDbHdr *pHdr,                  /* Current database header values */
  BtSchedule *p,                  /* Description of merge operation */
  FiCursor *pCsr                  /* Populate this object before returning */
){
  int iSub;                       /* Used to loop through component cursors */
  int rc;                         /* Return code */

  memset(pCsr, 0, sizeof(FiCursor));
  pCsr->base.flags = CSR_TYPE_FAST | CSR_NEXT_OK | CSR_VISIT_DEL;
  pCsr->base.pDb = db;
  rc = fiCsrAllocateSubs(db, pCsr, (p->iMaxLevel - p->iMinLevel) + 1);
  assert( rc==SQLITE4_OK || pCsr->nBt==0 );

  /* Initialize each sub-cursor */
  for(iSub=0; iSub<pCsr->nBt && rc==SQLITE4_OK; iSub++){
    u32 iLvl = p->iMaxLevel - iSub;
    FiSubCursor *pSub = &pCsr->aSub[iSub];
    BtCursor *pM = &pSub->mcsr;
    const void *pKey = 0; int nKey = 0;

    /* Seek the meta-tree cursor to the first entry (smallest keys) for the
    ** current level. If an earlier merge operation completely emptied the
    ** level, the sought entry may not exist at all.  */
    fiFormatPrefix(pSub->aPrefix, p->iAge, iLvl);
    btCsrSetup(db, pHdr->iMRoot, pM);
    rc = btCsrSeek(pM, 0, pSub->aPrefix, sizeof(pSub->aPrefix), BT_SEEK_GE, 0);

    if( rc==SQLITE4_INEXACT ){
      const int nPrefix = sizeof(pSub->aPrefix);
      rc = btCsrKey(pM, &pKey, &nKey);
      if( rc==SQLITE4_OK ){
        if( nKey<nPrefix || memcmp(pKey, pSub->aPrefix, nPrefix) ){
          /* Level is completely empty. Nothing to do for this level. */
          btCsrReset(pM, 0);
          rc = SQLITE4_NOTFOUND;
        }else{
          nKey -= nPrefix;
          pKey = (const void*)(((const u8*)pKey) + nPrefix);
        }
      }
    }

    /* Assuming the process above found a block, set up the block cursor and
    ** seek it to the smallest valid key.  */
    if( rc==SQLITE4_OK ){
      const void *pVal = 0; int nVal = 0;
      rc = btCsrData(pM, 0, 4, &pVal, &nVal);
      if( rc==SQLITE4_OK ){
        u32 iRoot = sqlite4BtGetU32((const u8*)pVal);
        btCsrSetup(db, iRoot, &pSub->csr);
        rc = btCsrSeek(&pSub->csr, 0, pKey, nKey, BT_SEEK_GE, 0);
        if( rc==SQLITE4_INEXACT ) rc = SQLITE4_OK;
        if( rc==SQLITE4_NOTFOUND ){
          rc = fiSubCsrStep(0, pSub, 1);
          if( rc==SQLITE4_NOTFOUND ) rc = SQLITE4_OK;
        }
      }
    }else if( rc==SQLITE4_NOTFOUND ){
      assert( pSub->csr.nPg==0 );
      assert( pSub->mcsr.nPg==0 );
      rc = SQLITE4_OK;
    }
  }

  if( rc==SQLITE4_OK ){
    rc = fiCsrSetCurrent(pCsr);
  }

  return rc;
}

/*
** An object of type FiWriter is used to write to a fast-insert sub-tree.
*/
typedef struct FiWriter FiWriter;
typedef struct FiWriterPg FiWriterPg;
struct FiWriterPg {
  int bAllocated;                 /* True if between Alloc() and Flush() */
  int nCell;                      /* Number of cells on page */
  int iFree;                      /* Free space offset */
  u8 *aBuf;                       /* Buffer to assemble content in */
};
struct FiWriter {
  bt_db *db;                      /* Database handle */
  BtSchedule *pSched;             /* Schedule object being implemented */
  int pgsz;                       /* Page size in bytes */
  int nPgPerBlk;                  /* Pages per block in this database */
  u32 iBlk;                       /* Block to write to */
  int nOvflPerPage;               /* Overflow pointers per page */

  int nAlloc;                     /* Pages allocated from current block */
  int nWrite;                     /* Pages written to current block */
  int nHier;                      /* Valid entries in apHier[] array */
  FiWriterPg aHier[MAX_SUBTREE_DEPTH];      /* Path from root to current leaf */

  /* Variables used to collect overflow pages freed by merge operation */
  int nOvfl;                      /* Number of pointers already in buffer */
  u8 *aTrunk;                     /* Buffer for current trunk page */
};

/*
** Write the page out to disk.
*/
static int fiWriterFlush(
  FiWriter *p, 
  FiWriterPg *pPg, 
  u32 *pPgno
){
  int rc;                         /* Return code */
  u32 pgno;                       /* New page number */

  assert( pPg->bAllocated==1 );
  assert( p->iBlk>1 );

  pPg->bAllocated = 0;
  pgno = (p->nPgPerBlk * (p->iBlk-1) + 1) + p->nWrite;
  p->nWrite++;
  assert( p->nWrite<=p->nAlloc );
  assert( p->nWrite<=p->nPgPerBlk );
  rc = sqlite4BtPagerRawWrite(p->db->pPager, pgno, pPg->aBuf);
  *pPgno = pgno;
  return rc;
}

static int fiWriterFlushAll(FiWriter *p){
  int i;
  int rc = SQLITE4_OK;
  u32 pgno = 0;

  for(i=0; rc==SQLITE4_OK && i<p->nHier; i++){
    FiWriterPg *pPg = &p->aHier[i];
    if( i!=0 ){
      btPutU32(&pPg->aBuf[1], pgno);
    }
    rc = fiWriterFlush(p, pPg, &pgno);

    btFreeBuffer(p->db, pPg->aBuf);
    pPg->aBuf = 0;
  }

  for(i=0; p->pSched->aBlock[i]!=p->iBlk; i++);
  assert( p->pSched->aRoot[i]==0 );
  p->pSched->aRoot[i] = pgno;

  return rc;
}

static void fiWriterCleanup(FiWriter *p){
}

/*
** Allocate a page buffer to use as part of writing a sub-tree. 
**
** No page number is assigned at this point.
*/
static int fiWriterAlloc(FiWriter *p, int bLeaf, FiWriterPg *pPg){
  int rc = SQLITE4_OK;

  assert( pPg->bAllocated==0 );
  assert( p->nAlloc<p->nPgPerBlk );

  pPg->bAllocated = 1;
  p->nAlloc++;
  pPg->nCell = 0;

  if( pPg->aBuf==0 ){
    rc = btNewBuffer(p->db, &pPg->aBuf);
  }
  if( rc==SQLITE4_OK ){
    if( bLeaf ){
      pPg->iFree = 1;
      pPg->aBuf[0] = 0x00;
    }else{
      pPg->iFree = 5;
      pPg->aBuf[0] = BT_PGFLAGS_INTERNAL;
    }
    btPutU16(&pPg->aBuf[p->pgsz-2], 0);
  }
  return rc;
}

static int fiWriterPush(FiWriter *p, const u8 *pKey, int nKey){
  int rc = SQLITE4_OK;
  int i;                          /* Iterator variable */
  int iIns;                       /* Index in aHier[] to insert new key at */
  int nByte;                      /* Bytes required by new cell */
  int iOff;                       /* Byte offset to write to */
  int nMaxAlloc;                  /* Maximum pages that may be allocated */
  u32 pgno;
  FiWriterPg *pPg;

  nByte = 2 + sqlite4BtVarintLen32(nKey) + nKey + 4;

  nMaxAlloc = MIN((p->nPgPerBlk - p->nAlloc), MAX_SUBTREE_DEPTH);
  for(iIns=1; iIns<p->nHier; iIns++){
    /* Check if the key will fit on page FiWriter.aHier[iIns]. If so,
    ** break out of the loop. */
    int nFree;
    pPg = &p->aHier[iIns];
    nFree = p->pgsz - (pPg->iFree + 6 + pPg->nCell*2);
    if( nFree>=nByte ) break;
  }

  if( (iIns + (iIns==p->nHier))>=nMaxAlloc ){
    rc = BT_BLOCKFULL;
  }
  if( rc==SQLITE4_OK && iIns==p->nHier ){
    p->nHier = iIns+1;
    rc = fiWriterAlloc(p, 0, &p->aHier[iIns]);
  }

  for(i=0; rc==SQLITE4_OK && i<iIns; i++){
    pPg = &p->aHier[i];
    if( i!=0 ){
      btPutU32(&pPg->aBuf[1], pgno);
    }
    rc = fiWriterFlush(p, pPg, &pgno);
    if( rc==SQLITE4_OK ){
      rc = fiWriterAlloc(p, (i==0), pPg);
    }
  }

  /* Write the b+tree cell containing (pKey/nKey) into page p->aHier[iIns]. */
  if( rc==SQLITE4_OK ){
    pPg = &p->aHier[iIns];
    iOff = pPg->iFree;
    btPutU16(btCellPtrFind(pPg->aBuf, p->pgsz, pPg->nCell), (u16)iOff);
    pPg->nCell++;
    btPutU16(&pPg->aBuf[p->pgsz-2], pPg->nCell);
    iOff += sqlite4BtVarintPut32(&pPg->aBuf[iOff], nKey);
    memcpy(&pPg->aBuf[iOff], pKey, nKey);
    iOff += nKey;
    btPutU32(&pPg->aBuf[iOff], pgno);
    iOff += 4;
    pPg->iFree = iOff;
  }

  return rc;
}


/*
** Initialize a writer object that will be used to implement the schedule
** passed as the second argument.
*/
static void fiWriterInit(
  bt_db *db, 
  BtSchedule *pSched, 
  FiWriter *p,
  int *pRc
){
  if( *pRc==SQLITE4_OK ){
    BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
    int i;

    memset(p, 0, sizeof(FiWriter));
    p->db = db;
    p->pSched = pSched;
    p->pgsz = pHdr->pgsz;
    p->nPgPerBlk = (pHdr->blksz / pHdr->pgsz);
    p->nOvflPerPage = ((pHdr->pgsz / 8) - 1);

    /* Find a block to write to */
    for(i=0; pSched->aBlock[i]; i++){
      if( pSched->aRoot[i]==0 ) break;
    }
    if( pSched->aBlock[i]==0 ){
      *pRc = BT_BLOCKFULL;
    }else{
      p->iBlk = pSched->aBlock[i];
    }
  }
}

static int fiWriterFlushOvfl(FiWriter *p, u32 *pPgno){
  u32 pgno;                       /* Page number for new overflow ptr page */
  int rc;

  /* Set the "number of entries" field */
  btPutU32(p->aTrunk, p->nOvfl);

  /* Write the page to disk */
  pgno = (p->nPgPerBlk * (p->iBlk-1) + 1) + p->nWrite;
  p->nWrite++;
  p->nAlloc++;
  rc = sqlite4BtPagerRawWrite(p->db->pPager, pgno, p->aTrunk);

  btPutU32(&p->aTrunk[4], pgno);
  if( pPgno ) *pPgno = pgno;
  p->nOvfl = 0;

  return rc;
}


static int fiWriterFreeOverflow(FiWriter *p, FiCursor *pCsr){
  const void *pKey;               /* Buffer containing current key for pCsr */
  int nKey;                       /* Size of pKey in bytes */
  int rc;
  int i;

  rc = btCsrKey(&pCsr->aSub[pCsr->iBt].csr, &pKey, &nKey);
  for(i=pCsr->iBt+1; i<pCsr->nBt && rc==SQLITE4_OK; i++){
    BtCursor *pSub = &pCsr->aSub[i].csr;
    if( pSub->nPg ){
      const void *pSKey;            /* Current key for pSub */
      int nSKey;                    /* Size of pSKey in bytes */
      rc = btCsrKey(pSub, &pSKey, &nSKey);
      if( rc==SQLITE4_OK 
       && 0==btKeyCompare(pKey, nKey, pSKey, nSKey) 
       && btCsrOverflow(pSub)
      ){
        u32 pgno = sqlite4BtPagePgno(pSub->apPage[pSub->nPg-1]);
        int iCell = pSub->aiCell[pSub->nPg-1];

        if( p->aTrunk==0 ){
          assert( p->nOvfl==0 );
          rc = btNewBuffer(p->db, &p->aTrunk);
          if( rc==SQLITE4_OK ) memset(p->aTrunk, 0, 8);
        }else if( p->nOvflPerPage==p->nOvfl ){
          rc = fiWriterFlushOvfl(p, 0);
          assert( p->nOvfl==0 );
        }
        if( rc==SQLITE4_OK ){
          assert( p->nOvfl<p->nOvflPerPage );
          btPutU32(&p->aTrunk[8 + p->nOvfl*8], pgno);
          btPutU32(&p->aTrunk[8 + p->nOvfl*8 + 4], iCell);
          p->nOvfl++;
        }
      }
    }
  }

  return rc;
}

/*
** Argument aBuf points to a buffer containing a leaf cell. This function
** returns a pointer to the key prefix embedded within the cell. Before
** returning, *pnKey is set to the size of the key prefix in bytes.
*/
static const u8 *btKeyPrefixFromCell(const u8 *aBuf, int *pnKey){
  u8 *p = (u8*)aBuf;
  int nKey;

  p += sqlite4BtVarintGet32(p, &nKey);
  if( nKey==0 ){
    p += sqlite4BtVarintGet32(p, &nKey);
  }

  *pnKey = nKey;
  return p;
}

/*
** Return the size in bytes of the shortest prefix of (pNew/nNew) that
** is greater than (pOld/nOld). Or, if that prefix would be too large
** to store on an internal b+tree node, return 
*/
static int btPrefixLength(
  int pgsz, 
  const u8 *pOld, int nOld, 
  const u8 *pNew, int nNew
){
  int nPrefix;
  int nCmp = MIN(nOld, nNew);
  for(nPrefix=0; nPrefix<nCmp && pOld[nPrefix]==pNew[nPrefix]; nPrefix++);
  if( nPrefix>=(pgsz/4) ) return 0;
  assert( nPrefix<nNew );
  return nPrefix+1;
}

static int fiWriterAdd(FiWriter *p, const void *pCell, int nCell){
  int rc = SQLITE4_OK;
  FiWriterPg *pPg;
  int nReq;                       /* Bytes of space required on leaf page */
  int nFree;                      /* Bytes of space available on leaf page */

  if( p->nHier==0 ){
    assert( p->aHier[0].nCell==0 );
    assert( p->aHier[0].iFree==0 );
    p->nHier++;
    rc = fiWriterAlloc(p, 1, &p->aHier[0]);
    assert( rc!=BT_BLOCKFULL );
  }
  pPg = &p->aHier[0];

  /* Calculate the space required for the cell. And space available on
  ** the current leaf page.  */
  nReq = nCell + 2;
  nFree = p->pgsz - pPg->iFree - (6 + 2*pPg->nCell);

  if( nReq>nFree ){
    /* The current leaf page is finished. Cell pCell/nCell will become
    ** the first cell on the next leaf page.  */
    const u8 *pOld; int nOld;     /* Prefix of last key on current leaf */
    const u8 *pNew; int nNew;     /* Prefix of new key */
    int nPrefix;

    pOld = btCellFind(pPg->aBuf, p->pgsz, btCellCount(pPg->aBuf, p->pgsz)-1);
    pOld = btKeyPrefixFromCell(pOld, &nOld);
    pNew = btKeyPrefixFromCell(pCell, &nNew);

    /* Push the shortest prefix of key (pNew/nNew) that is greater than key
    ** (pOld/nOld) up into the b+tree hierarchy.  */
    nPrefix = btPrefixLength(p->pgsz, pOld, nOld, pNew, nNew);
    rc = fiWriterPush(p, pNew, nPrefix);
  }

  /* Write the leaf cell into the page at FiWriter.aHier[0] */
  if( rc==SQLITE4_OK ){
    memcpy(&pPg->aBuf[pPg->iFree], pCell, nCell);
    btPutU16(btCellPtrFind(pPg->aBuf, p->pgsz, pPg->nCell), pPg->iFree);
    pPg->nCell++;
    btPutU16(&pPg->aBuf[p->pgsz-2], pPg->nCell);
    pPg->iFree += nCell;
  }

  return rc;
}

/*
** This is called by a checkpointer to handle a schedule page.
*/
int sqlite4BtMerge(bt_db *db, BtDbHdr *pHdr, u8 *aSched){
  BtSchedule s;                   /* Deserialized schedule object */
  int rc = SQLITE4_OK;            /* Return code */

  /* Set up the input cursor. */
  btReadSchedule(db, aSched, &s);
  if( s.eBusy==BT_SCHEDULE_BUSY ){
    FiCursor fcsr;                /* FiCursor used to read input */
    FiWriter writer;              /* FiWriter used to write output */

    rc = fiSetupMergeCsr(db, pHdr, &s, &fcsr);
    assert( rc!=SQLITE4_NOTFOUND );
    assert_ficursor_ok(&fcsr, rc);
    fiWriterInit(db, &s, &writer, &rc);

    /* The following loop runs once for each key copied from the input to
    ** the output segments. It terminates either when the input is exhausted
    ** or when all available output blocks are full.  */
    while( rc==SQLITE4_OK ){
      const void *pCell;          /* Cell to copy to output */
      int nCell;                  /* Size of cell in bytes */

      /* Read the current cell from the input and push it to the output. */
      fiCsrCell(&fcsr, &pCell, &nCell);
      rc = fiWriterAdd(&writer, pCell, nCell);
      if( rc==BT_BLOCKFULL ){
        int nOvflSaved = writer.nOvfl;
        u8 *aTrunkSaved = writer.aTrunk;
        rc = fiWriterFlushAll(&writer);
        fiWriterInit(db, &s, &writer, &rc);
        writer.nOvfl = nOvflSaved;
        writer.aTrunk = aTrunkSaved;
      }else if( rc==SQLITE4_OK ){
        rc = fiWriterFreeOverflow(&writer, &fcsr);
        if( rc==SQLITE4_OK ) rc = fiCsrStep(&fcsr);
      }
    }

    /* Assuming no error has occurred, update the serialized BtSchedule
    ** structure stored in buffer aSched[]. The caller will write this
    ** buffer to the database file as page (pHdr->iSRoot).  */
    if( rc==BT_BLOCKFULL || rc==SQLITE4_NOTFOUND ){

      if( rc==SQLITE4_NOTFOUND ){
        assert( fcsr.iBt<0 );
        s.iNextPg = 0;
        s.iNextCell = 0;
      }else{
        BtCursor *pCsr = &fcsr.aSub[fcsr.iBt].csr;
        assert( pCsr->nPg>0 );
        s.iNextPg = sqlite4BtPagePgno(pCsr->apPage[pCsr->nPg-1]);
        s.iNextCell = pCsr->aiCell[pCsr->nPg-1];
      }
      s.iFreeList = 0;
      s.eBusy = BT_SCHEDULE_DONE;

      assert( s.iFreeList==0 );
      rc = SQLITE4_OK;
      if( writer.aTrunk ){
        rc = fiWriterFlushOvfl(&writer, &s.iFreeList);
      }
      if( rc==SQLITE4_OK ){
        rc = fiWriterFlushAll(&writer);
      }
      if( rc==SQLITE4_OK ){
        btWriteSchedule(aSched, &s, &rc);
      }
    }

    fiWriterCleanup(&writer);
    fiCsrReset(&fcsr);
  }
  return rc;
}

/*
** Insert a new key/value pair or replace an existing one.
**
** This function may modify either the b-tree or fast-insert-tree, depending
** on whether or not the db->bFastInsertOp flag is set.
*/
int sqlite4BtReplace(bt_db *db, const void *pK, int nK, const void *pV, int nV){
  int rc = SQLITE4_OK;

  /* Debugging output. */
  sqlite4BtDebugKV((BtLock*)db->pPager, "replace", (u8*)pK, nK, (u8*)pV, nV);

  /* Save the position of any open cursors */
  rc = btSaveAllCursor(db, 0);
  assert( rc!=SQLITE4_NOTFOUND && rc!=SQLITE4_INEXACT );
  btCheckPageRefs(db);

  /* Call btReplaceEntry() to update either the main b-tree or the top-level
  ** sub-tree. Pass iRoot=0 to update the sub-tree, or the root page number
  ** of the b-tree to update the b-tree.  */
  if( rc==SQLITE4_OK ){
    u32 iRoot = 0;
    if( !db->bFastInsertOp ){
      BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
      iRoot = pHdr->iRoot;
    }
    if( rc==SQLITE4_OK ){
      rc = btReplaceEntry(db, iRoot, pK, nK, pV, nV);
    }
  }

  btCheckPageRefs(db);
  db->bFastInsertOp = 0;
  return rc;
}

#ifndef NDEBUG
void sqlite4BtDebugTree(bt_db *db, int iCall, u32 iRoot){
  BtPage *pPg;
  sqlite4_buffer buf;
  int pgsz;

  pgsz = sqlite4BtPagerPagesize(db->pPager);
  sqlite4_buffer_init(&buf, 0);
  sqlite4BtPageGet(db->pPager, iRoot, &pPg);
  btPageToAscii(iRoot, 1, db->pPager, btPageData(pPg), pgsz, &buf);
  fprintf(stderr, "%d TREE at %d:\n", iCall, (int)iRoot);
  fprintf(stderr, "%.*s", buf.n, (char*)buf.p);
  sqlite4_buffer_clear(&buf);
  sqlite4BtPageRelease(pPg);
}

void sqlite4BtDebugFastTree(bt_db *db, int iCall){
  BtDbHdr *pHdr;
  BtCursor mcsr;
  int rc;

  pHdr = sqlite4BtPagerDbhdr(db->pPager);
  btCsrSetup(db, pHdr->iMRoot, &mcsr);
  for(rc=btCsrEnd(&mcsr, 0); rc==SQLITE4_OK; rc=btCsrStep(&mcsr, 1)){
    u32 iSubRoot;
    const void *pK; int nK;
    rc = btCsrKey(&mcsr, &pK, &nK);
    if( rc!=SQLITE4_OK ) break;
    if( nK==sizeof(aSummaryKey) && 0==memcmp(aSummaryKey, pK, nK) ) break;
    rc = btCsrData(&mcsr, 0, 4, &pK, &nK);
    if( rc!=SQLITE4_OK ) break;

    iSubRoot = btGetU32((const u8*)pK);
    sqlite4BtDebugTree(db, iCall, iSubRoot);
  }
  btCsrReset(&mcsr, 1);
}
#endif   /* ifndef NDEBUG */


/*
** Delete the entry that the cursor currently points to.
*/
int sqlite4BtDelete(bt_cursor *pBase){
  bt_db *db = pBase->pDb;
  int rc;

  if( IsBtCsr(pBase) ){
    BtCursor *pCsr = (BtCursor*)pBase;

    rc = btCsrReseek(pCsr);
    if( rc==SQLITE4_OK ){
      rc = btSaveAllCursor(db, pCsr);
    }
    if( rc==SQLITE4_OK ){
      rc = btOverflowDelete(pCsr);
    }
    if( rc==SQLITE4_OK ){
      rc =  btDeleteFromPage(pCsr, 1);
    }
    if( rc==SQLITE4_OK ){
      rc = btBalanceIfUnderfull(pCsr);
    }

    btCsrReleaseAll(pCsr);
  }else{
    FiCursor *pCsr = (FiCursor*)pBase;
    BtCursor *pSub = &pCsr->aSub[pCsr->iBt].csr;

    void *pKey;
    int nKey;

    rc = btCsrBuffer(pSub, 0);
    pKey = pSub->ovfl.buf.p;
    nKey = pSub->ovfl.nKey;

    if( rc==SQLITE4_OK ){
      int bFastInsertOp = db->bFastInsertOp;
      db->bFastInsertOp = 1;
      rc = sqlite4BtReplace(db, pKey, nKey, 0, -1);
      db->bFastInsertOp = bFastInsertOp;
    }

  }
  return rc;
}

int sqlite4BtSetCookie(bt_db *db, unsigned int iVal){
  return sqlite4BtPagerSetCookie(db->pPager, iVal);
}

int sqlite4BtGetCookie(bt_db *db, unsigned int *piVal){
  return sqlite4BtPagerGetCookie(db->pPager, piVal);
}

static int btControlTransaction(bt_db *db, int *piCtx){
  int rc = SQLITE4_OK;
  int iTrans = sqlite4BtTransactionLevel(db);

  if( iTrans==0 ){
    rc = sqlite4BtBegin(db, 1);
  }
  *piCtx = iTrans;
  return rc;
}

static void btControlTransactionDone(bt_db *db, int iCtx){
  if( iCtx==0 ) sqlite4BtCommit(db, 0);
}

static int btCheckForPageLeaks(bt_db*, sqlite4_buffer*);

static int btControlInfo(bt_db *db, bt_info *pInfo){
  int rc = SQLITE4_OK;

  switch( pInfo->eType ){
    case BT_INFO_PAGEDUMP_ASCII:
    case BT_INFO_PAGEDUMP: {
      int iCtx;                   /* ControlTransaction() context */
      rc = btControlTransaction(db, &iCtx);
      if( rc==SQLITE4_OK ){
        BtPage *pPg = 0;
        rc = sqlite4BtPageGet(db->pPager, pInfo->pgno, &pPg);
        if( rc==SQLITE4_OK ){
          BtPager *p = db->pPager;
          int bAscii = (pInfo->eType==BT_INFO_PAGEDUMP_ASCII);
          u8 *aData;
          int nData;
          aData = btPageData(pPg);
          nData = sqlite4BtPagerPagesize(p);
          btPageToAscii(pInfo->pgno, bAscii, p, aData, nData, &pInfo->output);
          sqlite4_buffer_append(&pInfo->output, "", 1);
          sqlite4BtPageRelease(pPg);
        }
        btControlTransactionDone(db, iCtx);
      }
      break;
    }

    case BT_INFO_FILENAME: {
      const char *zFile;
      zFile = sqlite4BtPagerFilename(db->pPager, BT_PAGERFILE_DATABASE);
      rc = sqlite4_buffer_set(&pInfo->output, zFile, strlen(zFile)+1);
      break;
    }

    case BT_INFO_HDRDUMP: {
      int iCtx;                   /* ControlTransaction() context */
      rc = btControlTransaction(db, &iCtx);
      if( rc==SQLITE4_OK ){
        rc = sqlite4BtPagerHdrdump(db->pPager, &pInfo->output);
        btControlTransactionDone(db, iCtx);
      }
      break;
    }

    case BT_INFO_BLOCK_FREELIST: 
    case BT_INFO_PAGE_FREELIST: {
      int iCtx;                   /* ControlTransaction() context */
      rc = btControlTransaction(db, &iCtx);
      if( rc==SQLITE4_OK ){
        BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
        u32 iFirst = (
            pInfo->eType==BT_INFO_BLOCK_FREELIST?pHdr->iFreeBlk:pHdr->iFreePg
        );
        rc = btFreelistToAscii(db, iFirst, &pInfo->output);
        btControlTransactionDone(db, iCtx);
      }
      break;
    }

    case BT_INFO_PAGE_LEAKS: {
      int iCtx;                   /* ControlTransaction() context */
      rc = btControlTransaction(db, &iCtx);
      if( rc==SQLITE4_OK ){
        rc = btCheckForPageLeaks(db, &pInfo->output);
        btControlTransactionDone(db, iCtx);
      }
      break;
    }

    default: {
      rc = SQLITE4_ERROR;
      break;
    }
  }
  sqlite4_buffer_append(&pInfo->output, "\0", 1);
  return rc;
}

int sqlite4BtControl(bt_db *db, int op, void *pArg){
  int rc = SQLITE4_OK;

  switch( op ){
    case BT_CONTROL_INFO: {
      bt_info *pInfo = (bt_info*)pArg;
      rc = btControlInfo(db, pInfo);
      break;
    }

    case BT_CONTROL_GETVFS: {
      *((bt_env**)pArg) = sqlite4BtPagerGetEnv(db->pPager);
      break;
    }

    case BT_CONTROL_SETVFS: {
      sqlite4BtPagerSetEnv(db->pPager, (bt_env*)pArg);
      break;
    }

    case BT_CONTROL_SAFETY: {
      int *pInt = (int*)pArg;
      sqlite4BtPagerSetSafety(db->pPager, pInt);
      break;
    }

    case BT_CONTROL_AUTOCKPT: {
      int *pInt = (int*)pArg;
      sqlite4BtPagerSetAutockpt(db->pPager, pInt);
      break;
    }

    case BT_CONTROL_LOGSIZE: {
      int *pInt = (int*)pArg;
      sqlite4BtPagerLogsize(db->pPager, pInt);
      break;
    }
                             
    case BT_CONTROL_MULTIPROC: {
      int *pInt = (int*)pArg;
      sqlite4BtPagerMultiproc(db->pPager, pInt);
      break;
    }

    case BT_CONTROL_LOGSIZECB: {
      bt_logsizecb *p = (bt_logsizecb*)pArg;
      sqlite4BtPagerLogsizeCb(db->pPager, p);
      break;
    }

    case BT_CONTROL_CHECKPOINT: {
      bt_checkpoint *p = (bt_checkpoint*)pArg;
      rc = sqlite4BtPagerCheckpoint(db->pPager, p);
      break;
    }

    case BT_CONTROL_FAST_INSERT_OP: {
      db->bFastInsertOp = 1;
      break;
    }

    case BT_CONTROL_BLKSZ: {
      int *pInt = (int*)pArg;
      ((BtLock*)db->pPager)->nBlksz = *pInt;
      break;
    }

    case BT_CONTROL_PAGESZ: {
      int *pInt = (int*)pArg;
      if( sqlite4BtPagerFilename(db->pPager, BT_PAGERFILE_DATABASE) ){
        int iCtx;                   /* ControlTransaction() context */
        rc = btControlTransaction(db, &iCtx);
        if( rc==SQLITE4_OK ){
          *pInt = (int)(sqlite4BtPagerDbhdr(db->pPager)->pgsz);
          btControlTransactionDone(db, iCtx);
        }
      }else{
        BtLock *pLock = (BtLock*)db->pPager;
        int nNew = *pInt;
        if( ((nNew-1)&nNew)==0 && nNew>=512 && nNew<=32768 ){
          pLock->nPgsz = nNew;
        }
        *pInt = pLock->nPgsz;
      }
      break;
    }

  }

  return rc;
}

static void markBlockAsUsed(
  bt_db *db,
  u32 iBlk,
  u8 *aUsed
){
  if( iBlk ){
    BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
    int nPgPerBlk = (pHdr->blksz / pHdr->pgsz);
    int i;

    for(i=0; i<nPgPerBlk; i++){
      u32 iPg = (iBlk-1) * nPgPerBlk + 1 + i;
      assert( aUsed[iPg]==0 || aUsed[iPg]==1 );
      aUsed[iPg] = 1;
    }
  }
}

/*
** Iterate through the b-tree with root page iRoot. For each page used
** by the b-tree, set the corresponding entry in the aUsed[] array.
*/
static void assert_pages_used(
  bt_db *db,                      /* Database handle */
  u32 iRoot,                      /* Root page of b-tree to iterate through */
  const void *pFirst, int nFirst, /* Starting with this key (if pFirst!=0) */
  int *pRc                        /* IN/OUT: Error code */
){
  if( *pRc==SQLITE4_OK && iRoot ){
    BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
    int nPgPerBlk = (pHdr->blksz / pHdr->pgsz);
    int bMeta = (iRoot==pHdr->iMRoot);
    BtLock *pLock = (BtLock*)(db->pPager);
    u8 *aUsed = pLock->aUsed;
    int rc;
    BtCursor csr;

    btCsrSetup(db, iRoot, &csr);
    if( nFirst==0 ){
      rc = btCsrEnd(&csr, 0);
    }else{
      pLock->aUsed = 0;
      rc = btCsrSeek(&csr, 0, pFirst, nFirst, BT_SEEK_GE, BT_CSRSEEK_SEEK);
      if( rc==SQLITE4_INEXACT ) rc = SQLITE4_OK;
      pLock->aUsed = aUsed;
      csr.ovfl.nKey = 0;
    }

    while( rc==SQLITE4_OK ){
      rc = btCsrBuffer(&csr, 1);
      if( bMeta && rc==SQLITE4_OK ){
        u8 *aKey; int nKey;

        btCsrKey(&csr, (const void**)&aKey, &nKey);
        if( nKey!=sizeof(aSummaryKey) || memcmp(aKey, aSummaryKey, nKey) ){
          u8 *aVal; int nVal;
          u32 iSubRoot;
          u32 iBlk;
          int i;
          btCsrData(&csr, 0, 4, (const void**)&aVal, &nVal);
          iSubRoot = btGetU32(aVal);
          iBlk = (iSubRoot / nPgPerBlk) + 1;
          assert_pages_used(db, iSubRoot, &aKey[8], nKey-8, &rc);
          markBlockAsUsed(db, iBlk, aUsed);
        }
      }
      if( rc==SQLITE4_OK ) rc = btCsrStep(&csr, 1);
    }
  }
}

static void assert_freelist_pages_used(
  bt_db *db, 
  int bBlocklist,                 /* True to examine free-block list */
  u8 *aUsed, 
  int *pRc
){
  if( *pRc==SQLITE4_OK ){
    BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
    u32 iTrunk = (bBlocklist ? pHdr->iFreeBlk : pHdr->iFreePg);
    int rc = SQLITE4_OK;

    while( rc==SQLITE4_OK && iTrunk ){
      BtPage *pPg = 0;
      rc = sqlite4BtPageGet(db->pPager, iTrunk, &pPg);
      if( rc==SQLITE4_OK ){
        int i;
        u32 nFree;
        u8 *aData = btPageData(pPg);

        nFree = btGetU32(aData);
        for(i=0; i<nFree; i++){
          u32 pgno = btGetU32(&aData[8 + i*4]);
          if( bBlocklist ){
            markBlockAsUsed(db, pgno, aUsed);
          }else{
            aUsed[pgno]++;
          }
        }

        iTrunk = btGetU32(&aData[4]);
        sqlite4BtPageRelease(pPg);
      }
    }

    *pRc = rc;
  }
}

static void assert_schedule_page_used(bt_db *db, u8 *aUsed, int *pRc){
  BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
  if( *pRc==SQLITE4_OK && pHdr->iSRoot!=0 ){
    int nPgPerBlk = (pHdr->blksz / pHdr->pgsz);
    BtPage *pPg = 0;
    int rc;

    rc = sqlite4BtPageGet(db->pPager, pHdr->iSRoot, &pPg);
    if( rc==SQLITE4_OK ){
      BtSchedule s;
      int i;

      btReadSchedule(db, btPageData(pPg), &s);
      sqlite4BtPageRelease(pPg);

      assert( s.eBusy!=BT_SCHEDULE_BUSY || s.aRoot[0]==0 );
      if( s.eBusy!=BT_SCHEDULE_EMPTY ){
        for(i=0; rc==SQLITE4_OK && i<array_size(s.aBlock); i++){
          markBlockAsUsed(db, s.aBlock[i], aUsed);
        }
      }
    }

    *pRc = rc;
  }
}

/*
** Check that all pages in the database file are accounted for (not leaked).
** If any problems are detected, append a description of them to the buffer
** passed as the second argument.
*/
static int btCheckForPageLeaks(
  bt_db *db,                      /* Database handle */
  sqlite4_buffer *pBuf            /* Write error messages here */
){
  BtLock *pLock = (BtLock*)(db->pPager);

  static int nCall = 0;
  nCall++;

  if( pLock->aUsed==0 ){
    int rc;
    int iCtx;                     /* ControlTransaction() context */
    rc = btControlTransaction(db, &iCtx);

    if( rc==SQLITE4_OK ){
      BtDbHdr *pHdr = sqlite4BtPagerDbhdr(db->pPager);
      u8 *aUsed;
      int i;

      aUsed = btMalloc(db, pHdr->nPg, &rc);
      if( aUsed ) memset(aUsed, 0, pHdr->nPg);
      aUsed[0] = 1;                 /* Page 1 is always in use */
      pLock->aUsed = &aUsed[-1];

      /* The scheduled-merge page, if it is allocated */
      assert_schedule_page_used(db, pLock->aUsed, &rc);

      /* Walk the main b-tree */
      assert_pages_used(db, pHdr->iRoot, 0, 0, &rc);

      /* Walk the meta-tree */
      assert_pages_used(db, pHdr->iMRoot, 0, 0, &rc);

      /* Walk the free-page list */
      assert_freelist_pages_used(db, 0, pLock->aUsed, &rc);

      /* The free-block list */
      assert_freelist_pages_used(db, 1, pLock->aUsed, &rc);

      for(i=0; i<pHdr->nPg; i++){
        if( aUsed[i]!=1 ){
          sqlite4BtBufAppendf(
              pBuf, "refcount on page %d is %d\n", i+1, (int)aUsed[i]
          );
        }
      }
      btFree(db, aUsed);
      btControlTransactionDone(db, iCtx);
      pLock->aUsed = 0;
    }
  }

  return SQLITE4_OK;
}


