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
*/

#include "btInt.h"

#include <string.h>
#include <assert.h>
#include <stdio.h>


/* By default auto-checkpoint is 1000 */
#define BT_DEFAULT_AUTOCKPT 1000

#define BT_DEFAULT_SAFETY BT_SAFETY_NORMAL

#define BT_DEFAULT_MULTIPROC 1

typedef struct BtPageHash BtPageHash;

typedef struct BtSavepoint BtSavepoint;
typedef struct BtSavepage BtSavepage;

/*
** Hash table for page references currently in memory. Manipulated using
** the following functions:
**
**     btHashAdd()
**     btHashRemove()
**     btHashSearch()
**     btHashClear()
*/
struct BtPageHash {
  int nEntry;                     /* Number of entries in hash table */
  int nHash;                      /* Size of aHash[] array */
  BtPage **aHash;                 /* Hash array */
};

/*
** There is one object of this type for each open sub-transaction. Stored
** in the BtPager.aSavepoint[] array.
*/
struct BtSavepoint {
  int iLevel;                     /* Transaction level value (always >2) */
  BtSavepage *pSavepage;          /* First in linked list of saved data */
  BtDbHdr hdr;                    /* Database header at start of savepoint */
};

struct BtSavepage {
  BtPage *pPg;                    /* Pointer to page this object belongs to */
  u8 *aData;                      /* Saved data */
  BtSavepage *pNext;              /* Next saved page in the same savepoint */
  int iSavepoint;                 /* Transaction number of savepoint */
  BtSavepage *pNextSavepage;      /* Next saved page on the same BtPage */
};

/*
** See macro btPageData() in bt_main.c for why the aData variable must be
** first in this structure.
*/
struct BtPage {
  u8 *aData;                      /* Pointer to current data. MUST BE FIRST */
  BtPager *pPager;                /* Pager object that owns this page handle */
  u32 pgno;                       /* Current page number */
  int nRef;                       /* Number of references to this page */
  int flags;                      /* Mask of BTPAGE_XXX flags */
  BtPage *pNextHash;              /* Next entry with same hash key */
  BtPage *pNextDirty;             /* Next page in BtPager.pDirty list */
  BtPage *pNextLru;               /* Next page in LRU list */
  BtPage *pPrevLru;               /* Previous page in LRU list */
  BtSavepage *pSavepage;          /* List of saved page images */
};

/*
** Candidate values for BtPage.flags
*/
#define BT_PAGE_DIRTY 0x0001      /* Set for pages in BtPager.pDirty list */

/*
** Pager object.
**
** bDirtyHdr:
*/
struct BtPager {
  BtLock btl;                     /* Variables shared with bt_lock module */
  BtLog *pLog;                    /* Logging module */
  int iTransactionLevel;          /* Current transaction level (see bt.h) */
  char *zFile;                    /* Database file name */
  int nFile;                      /* Length of string zFile in bytes */
  BtPageHash hash;                /* Hash table */
  BtPage *pDirty;                 /* List of all dirty pages */
  BtPage *pLru;                   /* Head of LRU list */
  BtPage *pLruTail;               /* Tail of LRU list */
  int nPageAlloc;                 /* Number of page objects allocated */
  int nPageLimit;                 /* Maximum page objects to allocate */
  int nTotalRef;                  /* Total number of outstanding page refs */
  int bDoAutoCkpt;                /* Do auto-checkpoint after next unlock */
  BtSavepoint *aSavepoint;        /* Savepoint array */
  int nSavepoint;                 /* Number of entries in aSavepoint array */
  BtDbHdr *pHdr;                  /* Header object for current read snapshot */
  int bDirtyHdr;                  /* True if pHdr has been modified */
  void *pLogsizeCtx;              /* A copy of this is passed to xLogsize() */
  void (*xLogsize)(void*, int);   /* Log-size Callback function */
};


/**************************************************************************
** Interface to BtPageHash object.
*/

/*
** Return the hash key for page number pgno in a hash table with nHash
** buckets.
*/
static int hashkey(int nHash, u32 pgno){
  return (pgno % nHash);
}

/*
** Add page pPg to the hash table.
*/
static int btHashAdd(BtPager *p, BtPage *pPg){
  int h;

  /* If required, increase the number of buckets in the hash table. */
  if( p->hash.nEntry>=p->hash.nHash/2 ){
    int i;
    int nNew = (p->hash.nHash ? p->hash.nHash*2 : 256);
    BtPage **aNew;
    BtPage **aOld = p->hash.aHash;

    aNew = (BtPage **)sqlite4_malloc(p->btl.pEnv, nNew*sizeof(BtPage*));
    if( aNew==0 ) return btErrorBkpt(SQLITE4_NOMEM);
    memset(aNew, 0, nNew*sizeof(BtPage*));
    for(i=0; i<p->hash.nHash; i++){
      while( aOld[i] ){
        BtPage *pShift = aOld[i];
        aOld[i] = pShift->pNextHash;
        h = hashkey(nNew, pShift->pgno);
        pShift->pNextHash = aNew[h];
        aNew[h] = pShift;
      }
    }
    p->hash.aHash = aNew;
    p->hash.nHash = nNew;
    sqlite4_free(p->btl.pEnv, aOld);
  }

  /* Add the new entry to the hash table. */
  assert( pPg->pNextHash==0 );
  h = hashkey(p->hash.nHash, pPg->pgno);
  pPg->pNextHash = p->hash.aHash[h];
  p->hash.aHash[h] = pPg;
  p->hash.nEntry++;

  return SQLITE4_OK;
}

/*
** Remove page pPg from the hash table.
*/
static void btHashRemove(BtPager *p, BtPage *pPg){
  BtPage **pp;
  int h = hashkey(p->hash.nHash, pPg->pgno);
  for(pp=&p->hash.aHash[h]; *pp!=pPg; pp = &((*pp)->pNextHash));
  *pp = pPg->pNextHash;
  p->hash.nEntry--;
}

/*
** Search the hash table for a page with page number pgno. If found, return
** a pointer to the BtPage object. Otherwise, return NULL.
*/
static BtPage *btHashSearch(BtPager *p, u32 pgno){
  BtPage *pRet = 0;
  if( p->hash.nHash ){
    int h = hashkey(p->hash.nHash, pgno);
    for(pRet=p->hash.aHash[h]; pRet && pRet->pgno!=pgno; pRet=pRet->pNextHash);
  }
  return pRet;
}

/*
** Remove all entries from the hash-table. And free any allocations made
** by earlier calls to btHashAdd().
*/
static void btHashClear(BtPager *p){
  sqlite4_free(p->btl.pEnv, p->hash.aHash);
  memset(&p->hash, 0, sizeof(BtPageHash));
}

#ifndef NDEBUG
static void btHashIterate(
  BtPager *p, 
  void (*xCall)(void*, BtPage*),
  void *pCtx
){
  int i;
  for(i=0; i<p->hash.nHash; i++){
    BtPage *pPg;
    for(pPg=p->hash.aHash[i]; pPg; pPg=pPg->pNextHash){
      xCall(pCtx, pPg);
    }
  }
}
#endif
/*
** End of BtPageHash object interface.
**************************************************************************/

static void btLruAdd(BtPager *pPager, BtPage *pPg){
  assert( pPg->pPrevLru==0 );
  assert( pPg->pNextLru==0 );
  if( pPager->pLru ){
    pPager->pLruTail->pNextLru = pPg;
    pPg->pPrevLru = pPager->pLruTail;
    pPager->pLruTail = pPg;
  }else{
    pPager->pLru = pPg;
    pPager->pLruTail = pPg;
  }
}

/*
** Remove page pPg from the LRU list. If pPg is not currently part of
** the LRU list, the results are undefined.
*/
static void btLruRemove(BtPager *pPager, BtPage *pPg){
  assert( (pPg==pPager->pLru)==(pPg->pPrevLru==0) );
  assert( (pPg==pPager->pLruTail)==(pPg->pNextLru==0) );

  if( pPg->pNextLru ){
    pPg->pNextLru->pPrevLru = pPg->pPrevLru;
  }else{
    pPager->pLruTail = pPg->pPrevLru;
  }
  if( pPg->pPrevLru ){
    pPg->pPrevLru->pNextLru = pPg->pNextLru;
  }else{
    pPager->pLru = pPg->pNextLru;
  }

  pPg->pNextLru = 0;
  pPg->pPrevLru = 0;
}

/*
** Open a new pager database handle.
*/
int sqlite4BtPagerNew(sqlite4_env *pEnv, int nExtra, BtPager **pp){
  BtPager *p;
  int nByte;

  nByte = sizeof(BtPager) + nExtra;
  p = (BtPager*)sqlite4_malloc(pEnv, nByte);
  if( !p ) return btErrorBkpt(SQLITE4_NOMEM); 
  memset(p, 0, nByte);

  p->btl.pEnv = pEnv;
  p->btl.pVfs = sqlite4BtEnvDefault();
  p->btl.iSafetyLevel = BT_DEFAULT_SAFETY;
  p->btl.nAutoCkpt = BT_DEFAULT_AUTOCKPT;
  p->btl.bRequestMultiProc = BT_DEFAULT_MULTIPROC;
  p->btl.nBlksz = BT_DEFAULT_BLKSZ;
  p->btl.nPgsz = BT_DEFAULT_PGSZ;
  p->nPageLimit = BT_DEFAULT_CACHESZ;
  *pp = p;
  return SQLITE4_OK;
}

static void btFreePage(BtPager *p, BtPage *pPg){
  if( pPg ){
    sqlite4_free(p->btl.pEnv, pPg->aData);
    sqlite4_free(p->btl.pEnv, pPg);
  }
}

static void btPurgeCache(BtPager *p){
  int i;
  assert( p->iTransactionLevel==0 );
  assert( p->nTotalRef==0 );

  for(i=0; i<p->hash.nHash; i++){
    BtPage *pPg;
    BtPage *pNext;
    for(pPg=p->hash.aHash[i]; pPg; pPg=pNext){
      pNext = pPg->pNextHash;
      btFreePage(p, pPg);
    }
  }
  btHashClear(p);

  p->pLruTail = 0;
  p->pLru = 0;
}

static int btCheckpoint(BtLock *pLock){
  BtPager *p = (BtPager*)pLock;
  if( p->pLog==0 ) return SQLITE4_BUSY;
  return sqlite4BtLogCheckpoint(p->pLog, 0);
}

static int btCleanup(BtLock *pLock){
  BtPager *p = (BtPager*)pLock;
  int rc = sqlite4BtLogClose(p->pLog, 1);
  p->pLog = 0;
  return rc;
}

static int btOpenSavepoints(BtPager *p, int iLevel){
  int rc = SQLITE4_OK;            /* Return code */
  int nReq = iLevel - 2;          /* Required number of savepoints */

  if( nReq>p->nSavepoint ){
    BtSavepoint *aNew;
    int nByte = (nReq * sizeof(BtSavepoint));

    aNew = sqlite4_realloc(p->btl.pEnv, p->aSavepoint, nByte);
    if( aNew ){
      int i;
      for(i=p->nSavepoint; i<nReq; i++){
        aNew[i].pSavepage = 0;
        aNew[i].iLevel = i+3;
        memcpy(&aNew[i].hdr, p->pHdr, sizeof(BtDbHdr));
      }
      p->aSavepoint = aNew;
      p->nSavepoint = nReq;
    }else{
      rc = btErrorBkpt(SQLITE4_NOMEM);
    }
  }

  return rc;
}

#ifndef NDEBUG
static void btDebugCheckSavepagesInOrder(BtPage *pPg, int iMax){
  BtSavepage *p;
  int i = iMax;
  for(p=pPg->pSavepage; p; p=p->pNextSavepage){
    assert( p->iSavepoint<=i );
    i = p->iSavepoint-1;
  }
}
#else
# define btDebugCheckSavepagesInOrder(a,b)
#endif

/*
** If it has not already been added, add page pPg to the innermost
** savepoint.
*/
static int btAddToSavepoint(BtPager *p, BtPage *pPg){
  int rc = SQLITE4_OK;
  BtSavepage *pSavepage;
  int iLevel = p->iTransactionLevel;

  /* Assert that the linked list of BtSavepage objects is sorted in 
  ** descending order of level.  */
  btDebugCheckSavepagesInOrder(pPg, iLevel);

  if( pPg->pSavepage==0 || pPg->pSavepage->iSavepoint!=iLevel ){

    /* Allocate the new BtSavepage structure */
    pSavepage = sqlite4_malloc(p->btl.pEnv, sizeof(BtSavepage));
    if( pSavepage==0 ){
      rc = btErrorBkpt(SQLITE4_NOMEM);
    }else{
      memset(pSavepage, 0, sizeof(BtSavepage));
    }

    /* Populate the new BtSavepage structure */
    if( rc==SQLITE4_OK && (1 || (pPg->flags & BT_PAGE_DIRTY)) ){
      pSavepage->aData = (u8*)sqlite4_malloc(p->btl.pEnv, p->pHdr->pgsz);
      if( pSavepage->aData==0 ){
        sqlite4_free(p->btl.pEnv, pSavepage);
        rc = btErrorBkpt(SQLITE4_NOMEM);
      }else{
        memcpy(pSavepage->aData, pPg->aData, p->pHdr->pgsz);
      }
    }

    /* Link the new BtSavepage structure into the pPg->pSavepage list */
    if( rc==SQLITE4_OK ){
      pSavepage->pPg = pPg;
      pSavepage->iSavepoint = iLevel;

      pSavepage->pNextSavepage = pPg->pSavepage;
      pPg->pSavepage = pSavepage;

      assert( p->aSavepoint[iLevel-3].iLevel==iLevel ); 
      pSavepage->pNext = p->aSavepoint[iLevel-3].pSavepage;
      p->aSavepoint[iLevel-3].pSavepage = pSavepage;
    }
  }

  return rc;
}


/*
** Close enough savepoints (and discard any associated rollback data) to 
** cause the number remaining open to be consistent with transaction
** level iLevel. If the bRollback parameter is true, then the data is
** used to restore page states before is discarded. 
**
** If parameter iLevel is 2 (or lower) this means close all open 
** savepoints.
*/
static int btCloseSavepoints(
  BtPager *p,                     /* Pager handle */
  int iLevel,                     /* New transaction level */
  int bRollback                   /* True to rollback pages */
){
  int nReq = MAX(0, iLevel - 2);

  if( nReq<=p->nSavepoint ){
    int i;
    for(i=p->nSavepoint-1; i>=nReq; i--){
      BtSavepoint *pSavepoint = &p->aSavepoint[i];
      BtSavepage *pSavepg;
      BtSavepage *pNext;

      /* If this is a rollback operation, restore the BtDbHdr object to the
      ** state it was in at the start of this savepoint.  */
      if( bRollback ){
        memcpy(p->pHdr, &pSavepoint->hdr, sizeof(BtDbHdr));
      }

      /* Loop through each of the BtSavepage objects associated with this
      ** savepoint. Detach them from the BtPage objects and free all
      ** allocated memory.  */
      for(pSavepg=pSavepoint->pSavepage; pSavepg; pSavepg=pNext){
        BtPage *pPg = pSavepg->pPg;
        pNext = pSavepg->pNext;

        assert( pSavepg==pPg->pSavepage );
        assert( pSavepg->iSavepoint==pSavepoint->iLevel );

        /* If bRollback is set, restore the page data */
        if( bRollback ){
          memcpy(pPg->aData, pSavepg->aData, p->pHdr->pgsz);
        }else{
          int iNextSaved = (
              pSavepg->pNextSavepage ? pSavepg->pNextSavepage->iSavepoint : 2
          );
          if( iLevel>iNextSaved ){
            assert( iLevel>=3 );
            assert( p->aSavepoint[iLevel-3].iLevel==iLevel ); 
            pSavepg->pNext = p->aSavepoint[iLevel-3].pSavepage;
            pSavepg->iSavepoint = iLevel;
            p->aSavepoint[iLevel-3].pSavepage = pSavepg;
            pSavepg = 0;
          }
        }

        if( pSavepg ){
          /* Detach the BtSavepage from its BtPage object */
          pPg->pSavepage = pSavepg->pNextSavepage;

          /* Free associated memory allocations */
          assert( pSavepg->aData ); /* temp */
          sqlite4_free(p->btl.pEnv, pSavepg->aData);
          sqlite4_free(p->btl.pEnv, pSavepg);
        }
      }

      pSavepoint->pSavepage = 0;
    }

    p->nSavepoint = nReq;
  }

  return SQLITE4_OK;
}


/*
** Close a pager database handle.
*/
int sqlite4BtPagerClose(BtPager *p){
  int rc;

  if( p->btl.pFd ){
    sqlite4BtPagerRollback(p, 0);
  }

  rc = sqlite4BtLockDisconnect((BtLock*)p, btCheckpoint, btCleanup);
  p->iTransactionLevel = 0;
  btCloseSavepoints(p, 0, 0);
  btPurgeCache(p);
  sqlite4BtLogClose(p->pLog, 0);
  sqlite4_free(p->btl.pEnv, p->zFile);
  sqlite4_free(p->btl.pEnv, p->aSavepoint);
  sqlite4_free(p->btl.pEnv, p);
  return rc;
}

/*
** Return a pointer to the nExtra bytes of space allocated by PagerNew().
*/
void *sqlite4BtPagerExtra(BtPager *p){
  return (void*)&p[1];
}

/*
** Open the logging module and run recovery on the database. This is 
** called during connection by the bt_lock module.
*/
static int btRecover(BtLock *pLock){
  BtPager *p = (BtPager*)pLock;
  int rc;
  rc = sqlite4BtLogOpen(p, 1, &p->pLog);
  return rc;
}

/*
** Attach a database file to a pager object.
**
** This function may only be called once for each BtPager object. If it
** fails, the BtPager is rendered unusable (and must be closed by the
** caller using BtPagerClose()).
**
** If successful, SQLITE4_OK is returned. Otherwise, an SQLite error code.
*/
int sqlite4BtPagerOpen(BtPager *p, const char *zFilename){
  int rc;                         /* Return code */
  sqlite4_env *pEnv = p->btl.pEnv;
  bt_env *pVfs = p->btl.pVfs;

  assert( p->btl.pFd==0 && p->zFile==0 );

  rc = pVfs->xFullpath(pEnv, pVfs, zFilename, &p->zFile);
  if( rc==SQLITE4_OK ){
    p->nFile = strlen(p->zFile);
    rc = sqlite4BtLockConnect((BtLock*)p, btRecover);
    if( rc==SQLITE4_OK && p->pLog==0 ){
      rc = sqlite4BtLogOpen(p, 0, &p->pLog);
    }
  }

  if( rc!=SQLITE4_OK ){
    sqlite4BtLockDisconnect((BtLock*)p, btCheckpoint, btCleanup);
    sqlite4BtLogClose(p->pLog, 0);
    p->pLog = 0;
  }

  return rc;
}

/*
** Open a read-transaction.
*/
static int btOpenReadTransaction(BtPager *p){
  int rc;

  assert( p->iTransactionLevel==0 );
  assert( p->btl.pFd );
  assert( p->pHdr==0 );

  rc = sqlite4BtLogSnapshotOpen(p->pLog);

  if( rc==SQLITE4_OK ){
    /* If the read transaction was successfully opened, the transaction 
    ** level is now 1.  */
    p->iTransactionLevel = 1;
    p->pHdr = sqlite4BtLogDbhdr(p->pLog);
  }
  return rc;
}

static int btOpenWriteTransaction(BtPager *p){
  int rc;
  assert( p->iTransactionLevel==1 );
  assert( p->btl.pFd );

  rc = sqlite4BtLogSnapshotWrite(p->pLog);
  return rc;
}

static int btCloseReadTransaction(BtPager *p){
  int rc;
  assert( p->iTransactionLevel==0 );

  assert( p->pHdr );
  p->pHdr = 0;
  rc = sqlite4BtLogSnapshotClose(p->pLog);

  /* Purge the page cache. */
  assert( p->pDirty==0 );
  //btPurgeCache(p);

  if( rc==SQLITE4_OK && p->bDoAutoCkpt ){
    sqlite4BtLogCheckpoint(p->pLog, (p->btl.nAutoCkpt / 2));
  }
  p->bDoAutoCkpt = 0;

  return rc;
}

int btPagerDbhdrFlush(BtPager *p){
  int rc = SQLITE4_OK;
  if( p->bDirtyHdr ){
    rc = sqlite4BtLogDbhdrFlush(p->pLog);
    p->bDirtyHdr = 0;
  }
  return rc;
}

/*
** Commit the current write transaction to disk.
*/
static int btCommitTransaction(BtPager *p){
  int rc = SQLITE4_OK;
  int nLogsize;                   /* Number of frames in log after commit */
  BtPage *pPg;
  BtPage *pNext;
  assert( p->iTransactionLevel>=2 );

  rc = btPagerDbhdrFlush(p);
  btCloseSavepoints(p, 2, 0);

  for(pPg=p->pDirty; rc==SQLITE4_OK && pPg; pPg=pNext){
    int nPg;
    pNext = pPg->pNextDirty;
    nPg = ((pNext==0) ? p->pHdr->nPg : 0);
    rc = sqlite4BtLogWrite(p->pLog, pPg->pgno, pPg->aData, nPg);
    pPg->flags &= ~(BT_PAGE_DIRTY);
    pPg->pNextDirty = 0;
    if( pPg->nRef==0 ) btLruAdd(p, pPg);
  }
  p->pDirty = pPg;
  sqlite4BtLogSnapshotEndWrite(p->pLog);

  nLogsize = sqlite4BtLogSize(p->pLog);

  if( p->btl.nAutoCkpt && nLogsize>=p->btl.nAutoCkpt ){
    p->bDoAutoCkpt = 1;
  }
  if( p->xLogsize ){
    p->xLogsize(p->pLogsizeCtx, nLogsize);
  }

  return rc;
}

static int btLoadPageData(BtPager *p, BtPage *pPg){
  int rc;                         /* Return code */

  /* Try to load data from the logging module. If SQLITE4_OK is returned,
  ** data was loaded successfully. If SQLITE4_NOTFOUND, the required page
  ** is not present in the log and should be loaded from the database
  ** file. Any other error code is returned to the caller.  */
  rc = sqlite4BtLogRead(p->pLog, pPg->pgno, pPg->aData);

  /* If necessary, load data from the database file. */
  if( rc==SQLITE4_NOTFOUND ){
    i64 iOff = (i64)p->pHdr->pgsz * (i64)(pPg->pgno-1);
    rc = p->btl.pVfs->xRead(p->btl.pFd, iOff, pPg->aData, p->pHdr->pgsz);
  }

  return rc;
}

static int btAllocatePage(BtPager *p, BtPage **ppPg){
  int rc = SQLITE4_OK;            /* Return code */
  BtPage *pRet;

  if( p->hash.nEntry>=p->nPageLimit && p->pLru ){
    BtPage **pp;
    int h;

    /* Remove the page from the head of the LRU list. */
    pRet = p->pLru;
    assert( (pRet->pNextLru==0)==(pRet==p->pLruTail) );
    p->pLru = pRet->pNextLru;
    if( p->pLru==0 ){
      p->pLruTail = 0;
    }else{
      p->pLru->pPrevLru = 0;
    }

    /* Remove the page from the hash table. */
    btHashRemove(p, pRet);

    assert( pRet->pPrevLru==0 );
    assert( pRet->nRef==0 );
    assert( pRet->pSavepage==0 );
    pRet->flags = 0;
    pRet->pNextHash = 0;
    pRet->pNextDirty = 0;
    pRet->pNextLru = 0;
  }else{
    u8 *aData = (u8*)sqlite4_malloc(p->btl.pEnv, p->pHdr->pgsz);
    pRet = (BtPage*)sqlite4_malloc(p->btl.pEnv, sizeof(BtPage));

    if( pRet && aData ){
      memset(pRet, 0, sizeof(BtPage));
      pRet->aData = aData;
      pRet->pPager = p;
    }else{
      sqlite4_free(p->btl.pEnv, pRet);
      sqlite4_free(p->btl.pEnv, aData);
      rc = btErrorBkpt(SQLITE4_NOMEM);
      pRet = 0;
    }
  }

  *ppPg = pRet;
  return rc;
}

/*
** Roll back, but do not close, the current write transaction. 
*/
static int btRollbackTransaction(BtPager *p){
  int rc = SQLITE4_OK;
  BtPage *pPg;
  BtPage *pNext;

  assert( p->iTransactionLevel>=2 );
  btCloseSavepoints(p, 2, 0);

  /* Loop through all dirty pages in memory. Discard those with nRef==0.
  ** Reload data from disk for any others.  */
  for(pPg=p->pDirty; pPg; pPg=pNext){
    pNext = pPg->pNextDirty;
    pPg->flags &= ~(BT_PAGE_DIRTY);
    pPg->pNextDirty = 0;
    if( pPg->nRef==0 ){
      btHashRemove(p, pPg);
      btFreePage(p, pPg);
    }else if( rc==SQLITE4_OK && (pPg->pgno<=p->pHdr->nPg) ){
      rc = btLoadPageData(p, pPg);
    }
  }
  p->pDirty = 0;
  sqlite4BtLogReloadDbHdr(p->pLog);

  return rc;
}

/*
** Transactions. These methods are more or less the same as their 
** counterparts in bt.h.
*/
int sqlite4BtPagerBegin(BtPager *p, int iLevel){
  int rc = SQLITE4_OK;
  assert( p->btl.pFd );

  if( p->iTransactionLevel<iLevel ){
    /* Open a read transaction if one is not already open */
    if( p->iTransactionLevel==0 ){
      rc = btOpenReadTransaction(p);
    }

    /* Open a write transaction if one is required */
    if( rc==SQLITE4_OK && p->iTransactionLevel<2 && iLevel>=2 ){
      rc = btOpenWriteTransaction(p);
    }

    /* Open any required savepoints */
    if( rc==SQLITE4_OK ){
      rc = btOpenSavepoints(p, iLevel);
    }

    /* If nothing has gone wrong, update BtPager.iTransactionLevel */
    if( rc==SQLITE4_OK ){
      assert( p->iTransactionLevel>=1 && iLevel>=p->iTransactionLevel );
      p->iTransactionLevel = iLevel;
    }
  }

  return rc;
}

/*
** The sqlite4_kvstore.xCommit method.
*/
int sqlite4BtPagerCommit(BtPager *p, int iLevel){
  int rc = SQLITE4_OK;

  assert( p->btl.pFd );
  if( p->iTransactionLevel>=iLevel ){
    btCloseSavepoints(p, iLevel, 0);

    if( p->iTransactionLevel>=2 && iLevel<2 ){
      /* Commit the main write transaction. */
      rc = btCommitTransaction(p);
    }
    p->iTransactionLevel = iLevel;

    if( iLevel==0 ){
      int rc2 = btCloseReadTransaction(p);
      if( rc==SQLITE4_OK ) rc = rc2;
    }
  }
  return rc;
}

int sqlite4BtPagerRawWrite(BtPager *p, u32 pgno, u8 *aBuf){
  int pgsz = p->pHdr->pgsz;
  i64 iOff = (i64)pgsz * (i64)(pgno-1);
  return p->btl.pVfs->xWrite(p->btl.pFd, iOff, aBuf, pgsz);
}

int sqlite4BtPagerRollback(BtPager *p, int iLevel){
  int rc = SQLITE4_OK;

  assert( p->btl.pFd );
  if( p->iTransactionLevel>=iLevel ){

    /* If a write transaction is open and the requested level is 2 or
    ** lower, rollback the outermost write transaction. If the requested
    ** level is less than 2, also drop the WRITER lock.  */
    if( p->iTransactionLevel>=2 ){
      if( iLevel<=2 ){
        rc = btRollbackTransaction(p);
        if( iLevel<2 ) sqlite4BtLogSnapshotEndWrite(p->pLog);
      }else{
        rc = btCloseSavepoints(p, iLevel-1, 1);
        p->nSavepoint++;
      }
    }

    if( p->iTransactionLevel>iLevel ){
      p->iTransactionLevel = iLevel;
      if( iLevel==0 ){
        int rc2 = btCloseReadTransaction(p);
        if( rc==SQLITE4_OK ) rc = rc2;
      }
    }
  }

  return rc;
}

int sqlite4BtPagerRevert(BtPager *p, int iLevel){
  int rc;
  assert( 0 );                    /* TODO: Fix this */

  assert( p->btl.pFd );
  rc = sqlite4BtPagerRollback(p, iLevel);
  if( rc==SQLITE4_OK && iLevel>=2 && p->iTransactionLevel==iLevel ){
    /* Rollback (but do not close) transaction iLevel */
  }
  return rc;
}

/*
** Return the current transaction level.
*/
int sqlite4BtPagerTransactionLevel(BtPager *p){
  return p->iTransactionLevel;
}

/*
** Query for the database page size. Requires an open read transaction.
*/
int sqlite4BtPagerPagesize(BtPager *p){
  /* assert( p->iTransactionLevel>=1 && p->btl.pFd ); */
  return (int)p->pHdr->pgsz;
}

/* 
** Query for the root page number. Requires an open read transaction.
*/
BtDbHdr *sqlite4BtPagerDbhdr(BtPager *p){
  return p->pHdr;
}

void sqlite4BtPagerDbhdrDirty(BtPager *p){
  p->bDirtyHdr = 1;
}

void sqlite4BtPagerSetDbhdr(BtPager *p, BtDbHdr *pHdr){
  assert( p->pHdr==0 || pHdr==0 );
  p->pHdr = pHdr;
}

/*
** Request a reference to page pgno of the database.
*/
int sqlite4BtPageGet(BtPager *p, u32 pgno, BtPage **ppPg){
  int rc = SQLITE4_OK;            /* Return code */
  BtPage *pRet;                   /* Returned page handle */

  if( p->btl.aUsed ){
    p->btl.aUsed[pgno]++;
  }

  /* Search the cache for an existing page. */
  pRet = btHashSearch(p, pgno);

  /* If the page is not in the cache, load it from disk */
  if( pRet==0 ){
    rc = btAllocatePage(p, &pRet);
    if( rc==SQLITE4_OK ){
      pRet->pgno = pgno;
      if( pgno<=p->pHdr->nPg ){
        rc = btLoadPageData(p, pRet);
      }else{
        assert( p->iTransactionLevel>=2 );
        memset(pRet->aData, 0, p->pHdr->pgsz);
      }

      if( rc==SQLITE4_OK ){
        rc = btHashAdd(p, pRet);
      }

      if( rc!=SQLITE4_OK ){
        btFreePage(p, pRet);
        pRet = 0;
      }else{
        sqlite4BtDebugReadPage(&p->btl, pgno, pRet->aData, p->pHdr->pgsz);
      }
    }
  }else if( pRet->nRef==0 && (pRet->flags & BT_PAGE_DIRTY)==0 ){
    btLruRemove(p, pRet);
  }

  assert( (pRet!=0)==(rc==SQLITE4_OK) );
  if( rc==SQLITE4_OK ){
    p->nTotalRef++;
    pRet->nRef++;
  }
  *ppPg = pRet;
  return rc;
}

int sqlite4BtPageWrite(BtPage *pPg){
  int rc = SQLITE4_OK;
  BtPager *p = pPg->pPager;

  /* If there are savepoints open, add this page to the innermost savepoint */
  if( p->nSavepoint>0 ){
    rc = btAddToSavepoint(p, pPg);
  }

  if( (pPg->flags & BT_PAGE_DIRTY)==0 ){
    pPg->flags |= BT_PAGE_DIRTY;
    pPg->pNextDirty = pPg->pPager->pDirty;
    pPg->pPager->pDirty = pPg;
  }
  return rc;
}

/*
** Add page pgno to the free-page list. If argument pPg is not NULL, then
** it is a reference to page pgno.
*/
static int btFreelistAdd(
  BtPager *p,                     /* Pager object */
  int bBlock,                     /* True if pgno is actually a block number */
  u32 pgno
){
  BtDbHdr *pHdr = sqlite4BtLogDbhdr(p->pLog);
  int rc = SQLITE4_OK;
  int bDone = 0;
  u32 *piFirst = (bBlock ? &pHdr->iFreeBlk : &pHdr->iFreePg);

  /* Check if there is space on the first free-list trunk page. If so,
  ** add the new entry to it. Set variable bDone to indicate that the
  ** page has already been added to the free-list. */
  if( *piFirst ){
    BtPage *pTrunk;
    rc = sqlite4BtPageGet(p, *piFirst, &pTrunk);
    if( rc==SQLITE4_OK ){
      const int nMax = ((pHdr->pgsz - 8) / 4);
      u8 *aData = pTrunk->aData;
      int nFree = (int)sqlite4BtGetU32(aData);

      if( nFree<nMax ){
        rc = sqlite4BtPageWrite(pTrunk);
        if( rc==SQLITE4_OK ){
          sqlite4BtPutU32(&pTrunk->aData[8 + nFree*4], pgno);
          sqlite4BtPutU32(pTrunk->aData, nFree+1);
          bDone = 1;
          sqlite4BtDebugPageFree((BtLock*)p, bBlock, "free-list-leaf", pgno);
        }
      }
      sqlite4BtPageRelease(pTrunk);
    } 
  }

  /* If no error has occurred but the page number has not yet been added 
  ** to the free-list, this page becomes the first trunk in the list.  */
  if( rc==SQLITE4_OK && bDone==0 ){
    BtPage *pTrunk = 0;

    if( bBlock ){
      rc = sqlite4BtPageAllocate(p, &pTrunk);
    }else{
      rc = sqlite4BtPageGet(p, pgno, &pTrunk);
      if( rc==SQLITE4_OK ) rc = sqlite4BtPageWrite(pTrunk);
    }
    if( rc==SQLITE4_OK ){
      sqlite4BtPagerDbhdrDirty(p);
      sqlite4BtPutU32(&pTrunk->aData[0], 0);
      sqlite4BtPutU32(&pTrunk->aData[4], *piFirst);
      *piFirst = pTrunk->pgno;
      sqlite4BtDebugPageFree((BtLock*)p, 0, "free-list-trunk", pTrunk->pgno);
    }
    sqlite4BtPageRelease(pTrunk);
    if( rc==SQLITE4_OK && bBlock ){
      rc = btFreelistAdd(p, 1, pgno);
    }
  }

  return rc;
}

/*
** Attempt to allocate a page from the free-list.
*/
static int btFreelistAlloc(
  BtPager *p,                     /* Pager object */
  int bBlock,                     /* True to allocate a block (not a page) */
  u32 *pPgno                      /* OUT: Page or block number */
){
  BtDbHdr *pHdr = sqlite4BtLogDbhdr(p->pLog);
  int rc = SQLITE4_OK;
  u32 *piFirst = (bBlock ? &pHdr->iFreeBlk : &pHdr->iFreePg);
  u32 pgno = 0;

  assert( *pPgno==0 );
  while( *piFirst && pgno==0 && rc==SQLITE4_OK ){
    BtPage *pTrunk = 0;
    rc = sqlite4BtPageGet(p, *piFirst, &pTrunk);
    if( rc==SQLITE4_OK ){
      rc = sqlite4BtPageWrite(pTrunk);
    }
    if( rc==SQLITE4_OK ){
      u8 *aData = pTrunk->aData;
      u32 nFree = sqlite4BtGetU32(aData);
      if( nFree>0 ){
        pgno = sqlite4BtGetU32(&aData[8 + 4*(nFree-1)]);
        sqlite4BtPutU32(aData, nFree-1);
        sqlite4BtDebugPageAlloc((BtLock*)p, "free-list", pgno);
      }else{
        u32 iNext = sqlite4BtGetU32(&aData[4]);
        sqlite4BtPagerDbhdrDirty(p);
        
        if( bBlock ){
          rc = btFreelistAdd(p, 0, *piFirst);
        }else{
          pgno = *piFirst;
          sqlite4BtDebugPageAlloc((BtLock*)p, "free-list-trunk", pgno);
        }
        *piFirst = iNext;
      }
    }

    sqlite4BtPageRelease(pTrunk);
  }

  *pPgno = pgno;
  return rc;
}

/*
** Decrement the refcount on page pPg. Also, indicate that page pPg is
** no longer in use.
*/
int sqlite4BtPageTrim(BtPage *pPg){
  int rc;                         /* Return code */
  rc = btFreelistAdd(pPg->pPager, 0, pPg->pgno);
  sqlite4BtPageRelease(pPg);
  return rc;
}

/*
** Page number pgno is no longer in use.
*/
int sqlite4BtPageTrimPgno(BtPager *pPager, u32 pgno){
  return btFreelistAdd(pPager, 0, pgno);
}

int sqlite4BtPageRelease(BtPage *pPg){
  if( pPg ){
    BtPager *pPager = pPg->pPager;

    assert( pPg->nRef>=1 );
    pPg->nRef--;
    pPg->pPager->nTotalRef--;

    /* If the refcount is now zero and the page is not dirty, add it to
    ** the LRU list.  */
    if( pPg->nRef==0 && (pPg->flags & BT_PAGE_DIRTY)==0 ){
      btLruAdd(pPager, pPg);
    }
  }
  return SQLITE4_OK;
}

void sqlite4BtPageReference(BtPage *pPg){
  assert( pPg->nRef>=1 );
  pPg->nRef++;
  pPg->pPager->nTotalRef++;
}

/*
** Allocate a new database page and return a writable reference to it.
*/
int sqlite4BtPageAllocate(BtPager *p, BtPage **ppPg){
  BtPage *pPg = 0;
  int rc;
  u32 pgno = 0;

  /* Find the page number of the new page. There are two ways a page may
  ** be allocated - from the free-list or by appending it to the end of
  ** the database file. */ 
  rc = btFreelistAlloc(p, 0, &pgno);
  if( rc==SQLITE4_OK && pgno==0 ){
    pgno = p->pHdr->nPg+1;
    sqlite4BtDebugPageAlloc((BtLock*)p, "end-of-file", pgno);
  }

  rc = sqlite4BtPageGet(p, pgno, &pPg);
  if( rc==SQLITE4_OK ){
    rc = sqlite4BtPageWrite(pPg);
    if( rc!=SQLITE4_OK ){
      sqlite4BtPageRelease(pPg);
      pPg = 0;
    }else{
      p->pHdr->nPg = MAX(p->pHdr->nPg, pgno);
    }
  }

#ifdef BT_STDERR_DEBUG
  fprintf(stderr, "allocated page %d\n", pgno);
#endif

  *ppPg = pPg;
  return rc;
}

int sqlite4BtBlockAllocate(BtPager *p, int nBlk, u32 *aiBlk){
  int rc = SQLITE4_OK;
  BtDbHdr *pHdr = p->pHdr;
  int nPgPerBlk = (pHdr->blksz / pHdr->pgsz);
  int i;

  for(i=0; rc==SQLITE4_OK && i<nBlk; i++){
    u32 iBlk = 0;
    u32 iRoot;
    u32 iFree;

    rc = btFreelistAlloc(p, 1, &iBlk);
    if( rc==SQLITE4_OK && iBlk==0 ){

      /* Figure out the next block in the file. And its root (first) page. */
      iBlk = 1 + (pHdr->nPg + nPgPerBlk - 1) / nPgPerBlk;
      iRoot = (iBlk-1) * nPgPerBlk + 1;
      assert( iBlk>0 );

      for(iFree = pHdr->nPg+1; rc==SQLITE4_OK && iFree<iRoot; iFree++){
        rc = sqlite4BtPageTrimPgno(p, iFree);
      }
      pHdr->nPg = iBlk * nPgPerBlk;
    }

    aiBlk[i] = iBlk;
  }

  return rc;
}

/*
** Trim a block.
*/
int sqlite4BtBlockTrim(BtPager *p, u32 iBlk){
  return btFreelistAdd(p, 1, iBlk);
}

/*
** Return the current page number of the argument page reference.
*/
u32 sqlite4BtPagePgno(BtPage *pPg){
  return pPg->pgno;
}

/*
** Return a pointer to the data buffer associated with page pPg.
*/
void *sqlite4BtPageData(BtPage *pPg){
  return pPg->aData;
}

/* 
** Read the schema cookie value. Requires an open read-transaction.
*/
int sqlite4BtPagerSetCookie(BtPager *p, u32 iVal){
  assert( p->iTransactionLevel>=2 );
  return sqlite4BtLogSetCookie(p->pLog, iVal);
}

/* 
** Set the schema cookie value. Requires an open write-transaction.
*/
int sqlite4BtPagerGetCookie(BtPager *p, u32 *piVal){
  assert( p->iTransactionLevel>=1 );
  *piVal = p->pHdr->iCookie;
  return SQLITE4_OK;
}

const char *sqlite4BtPagerFilename(BtPager *p, int ePagerfile){
  const char *zTail;

  /* If the database file has not yet been opened, return a null pointer. */
  if( p->zFile==0 ) return 0;

  switch( ePagerfile ){
    case BT_PAGERFILE_DATABASE:
      zTail = "";
      break;

    case BT_PAGERFILE_LOG:
      zTail = "-wal";
      break;

    default:
      assert( ePagerfile==BT_PAGERFILE_SHM );
      zTail = "-shm";
      break;
  }
  memcpy(&p->zFile[p->nFile], zTail, strlen(zTail)+1);
  return p->zFile;
}

bt_env *sqlite4BtPagerGetEnv(BtPager *p){
  return p->btl.pVfs;
}
void sqlite4BtPagerSetEnv(BtPager *p, bt_env *pVfs){
  p->btl.pVfs = pVfs;
}

void sqlite4BtPagerSetSafety(BtPager *pPager, int *piVal){
  int iVal = *piVal;
  if( iVal>=0 && iVal<=2 ){
    pPager->btl.iSafetyLevel = iVal;
  }
  *piVal = pPager->btl.iSafetyLevel;
}

void sqlite4BtPagerSetAutockpt(BtPager *pPager, int *piVal){
  int iVal = *piVal;
  if( iVal>=0 ){
    pPager->btl.nAutoCkpt = iVal;
  }
  *piVal = pPager->btl.nAutoCkpt;
}

void sqlite4BtPagerLogsize(BtPager *pPager, int *pnFrame){
  *pnFrame = sqlite4BtLogSize(pPager->pLog);
}

void sqlite4BtPagerMultiproc(BtPager *pPager, int *piVal){
  if( *piVal==0 || *piVal==1 ){
    pPager->btl.bRequestMultiProc = *piVal;
  }
  *piVal = pPager->btl.bRequestMultiProc;
}

void sqlite4BtPagerLogsizeCb(BtPager *pPager, bt_logsizecb *p){
  pPager->xLogsize = p->xLogsize;
  pPager->pLogsizeCtx = p->pCtx;
}

int sqlite4BtPagerCheckpoint(BtPager *pPager, bt_checkpoint *pCkpt){
  int rc;
  rc = sqlite4BtLogCheckpoint(pPager->pLog, pCkpt->nFrameBuffer);
  return rc;
}

/*
** It is guaranteed that at least a read-transaction is open when
** this function is called. It appends a text representation of the
** current database header (BtDbHdr) object to the buffer passed as
** the second argument.
**
** An SQLite4 error code is returned if an error (i.e. OOM) occurs,
** or SQLITE4_OK otherwise.
*/
int sqlite4BtPagerHdrdump(BtPager *pPager, sqlite4_buffer *pBuf){
  BtDbHdr *pHdr = pPager->pHdr;
  int rc = SQLITE4_OK;

  sqlite4BtBufAppendf(pBuf, 
      "pgsz=%d blksz=%d nPg=%d"
      " iRoot=%d iMRoot=%d iSRoot=%d"
      " iSubBlock=%d nSubPg=%d"
      " iCookie=%d iFreePg=%d iFreeBlk=%d",
      pHdr->pgsz, pHdr->blksz, pHdr->nPg, 
      pHdr->iRoot, pHdr->iMRoot, pHdr->iSRoot,
      pHdr->iSubBlock, pHdr->nSubPg,
      pHdr->iCookie, pHdr->iFreePg, pHdr->iFreeBlk
  );

  return rc;
}

#ifndef NDEBUG
int sqlite4BtPagerRefcount(BtPager *p){
  return p->nTotalRef;
}
#endif

