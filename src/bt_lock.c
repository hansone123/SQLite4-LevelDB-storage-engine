/*
** 2013 October 18
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


#include "sqliteInt.h"
#include "btInt.h"

#include <string.h>
#include <assert.h>
#include <stdio.h>

#define BT_LOCK_DMS1          0   /* DMS1 */
#define BT_LOCK_DMS2_RW       1   /* DMS2/rw */
#define BT_LOCK_DMS2_RO       2   /* DMS2/ro */
#define BT_LOCK_WRITER        3   /* WRITER lock */
#define BT_LOCK_CKPTER        4   /* CHECKPOINTER lock */
#define BT_LOCK_READER_DBONLY 5   /* Reading the db file only */
#define BT_LOCK_READER0       6   /* Array of BT_NREADER locks */

#define BT_LOCK_UNLOCK     0
#define BT_LOCK_SHARED     1
#define BT_LOCK_EXCL       2

/*
** Global data. All global variables used by code in this file are grouped
** into the following structure instance.
**
** pDatabase:
**   Linked list of all Database objects allocated within this process.
**   This list may not be traversed without holding the global mutex (see
**   functions enterGlobalMutex() and leaveGlobalMutex()).
**
** iDebugId:
**   Each new connection is assigned a "debug-id". This contributes 
**   nothing to the operation of the library, but sometimes makes it 
**   easier to debug various problems.
*/
static struct BtSharedData {
  BtShared *pDatabase;            /* Linked list of all Database objects */
  int iDebugId;                   /* Next free debugging id */
} gBtShared = {0, 0};

struct BtFile {
  BtFile *pNext;
  bt_file *pFd;
};

struct BtShared {
  /* Protected by the global mutex (see btLockMutexEnter()/Leave()) */
  char *zName;                    /* Canonical path to database file */
  int nName;                      /* strlen(zName) */
  int nRef;                       /* Number of handles open on this file */
  BtShared *pNext;                /* Next BtShared structure in global list */

  /* Protected by the local mutex (pClientMutex) */
  sqlite4_mutex *pClientMutex;    /* Protects the apShmChunk[] and pConn */
  int nShmChunk;                  /* Number of entries in apShmChunk[] array */
  u8 **apShmChunk;                /* Array of "shared" memory regions */
  BtLock *pLock;                  /* List of connnections to this db */

  /* Multi-process mode stuff */
  int bMultiProc;                 /* True if running in multi-process mode */
  int bReadonly;                  /* True if Database.pFile is read-only */
  bt_file *pFile;                 /* Used for locks/shm in multi-proc mode */
  BtFile *pBtFile;                /* List of deferred closes */
};

/*
** Grab the global mutex that protects the linked list of BtShared
** objects.
*/
static void btLockMutexEnter(sqlite4_env *pEnv){
  sqlite4_mutex_enter(sqlite4_mutex_alloc(pEnv, SQLITE4_MUTEX_STATIC_KV));
}

/*
** Relinquish the mutex obtained by calling btLockMutexEnter().
*/
static void btLockMutexLeave(sqlite4_env *pEnv){
  sqlite4_mutex_leave(sqlite4_mutex_alloc(pEnv, SQLITE4_MUTEX_STATIC_KV));
}

/*
** Take the specified lock on the shared file-handle associated with
** the connection passed as the first argument.
*/
static int btLockSharedFile(BtLock *p, int iLock, int eOp){
  int rc = SQLITE4_OK;
  BtShared *pShared = p->pShared;

  /* This is a no-op in single process mode */
  assert( (pShared->bMultiProc==0)==(pShared->pFile==0) );
  if( pShared->pFile ){
    rc = p->pVfs->xLock(pShared->pFile, iLock, eOp);
  }
  return rc;
}

static int btLockLockopNonblocking(
  BtLock *p,                      /* BtLock handle */
  int iLock,                      /* Slot to lock */
  int eOp                         /* One of BT_LOCK_UNLOCK, SHARED or EXCL */
){
  const u32 mask = ((u32)1 << iLock);
  int rc = SQLITE4_OK;
  BtShared *pShared = p->pShared;

  assert( iLock>=0 && iLock<(BT_LOCK_READER0 + BT_NREADER) );
  assert( (BT_LOCK_READER0+BT_NREADER)<=32 );
  assert( eOp==BT_LOCK_UNLOCK || eOp==BT_LOCK_SHARED || eOp==BT_LOCK_EXCL );

  /* Check for a no-op. Proceed only if this is not one of those. */
  if( (eOp==BT_LOCK_UNLOCK && (mask & (p->mExclLock|p->mSharedLock))!=0)
   || (eOp==BT_LOCK_SHARED && (mask & p->mSharedLock)==0)
   || (eOp==BT_LOCK_EXCL   && (mask & p->mExclLock)==0)
  ){
    BtLock *pIter;
    int nExcl = 0;                /* Number of connections holding EXCLUSIVE */
    int nShared = 0;              /* Number of connections holding SHARED */
    sqlite4_mutex_enter(pShared->pClientMutex);

    /* Figure out the locks currently held by this process on iLock, not
    ** including any held by this connection.  */
    for(pIter=pShared->pLock; pIter; pIter=pIter->pNext){
      assert( (pIter->mExclLock & pIter->mSharedLock)==0 );
      if( pIter!=p ){
        assert( (pIter->mExclLock & p->mSharedLock)==0 );
        assert( (pIter->mSharedLock & p->mExclLock)==0 );
        if( mask & pIter->mExclLock ){
          nExcl++;
        }else if( mask & pIter->mSharedLock ){
          nShared++;
        }
      }
    }
    assert( nExcl==0 || nExcl==1 );
    assert( nExcl==0 || nShared==0 );

    switch( eOp ){
      case BT_LOCK_UNLOCK:
        if( nShared==0 ){
          btLockSharedFile(p, iLock, BT_LOCK_UNLOCK);
        }
        p->mExclLock &= ~mask;
        p->mSharedLock &= ~mask;
        break;

      case BT_LOCK_SHARED:
        if( nExcl ){
          rc = SQLITE4_BUSY;
        }else{
          if( nShared==0 ){
            rc = btLockSharedFile(p, iLock, BT_LOCK_SHARED);
          }
          /* If no error occurred, set the bit in the mask of SHARED locks
          ** held. Either way, clear the bit in the mask of EXCLUSIVE locks.
          ** The idea here is that when there is any uncertainty as to whether
          ** a lock is held, the corresponding bit is cleared.  */
          if( rc==SQLITE4_OK ) p->mSharedLock |= mask;
          p->mExclLock &= ~mask;
        }
        break;

      default:
        assert( eOp==BT_LOCK_EXCL );
        if( nExcl || nShared ){
          rc = SQLITE4_BUSY;
        }else{
          rc = btLockSharedFile(p, iLock, BT_LOCK_EXCL);
          if( rc==SQLITE4_OK ){
            p->mSharedLock &= ~mask;
            p->mExclLock |= mask;
          }
        }
        break;
    }

    sqlite4_mutex_leave(pShared->pClientMutex);
  }

  return rc;
}

static void btLockDelay(void){
  usleep(10000);
#if 0
  static int nCall = 0;
  nCall++;
  fprintf(stderr, "%d delay\n", nCall);
  fflush(stderr);
#endif
}

/*
** Attempt to obtain the lock identified by the iLock and bExcl parameters.
** If successful, return SQLITE4_OK. If the lock cannot be obtained because 
** there exists some other conflicting lock, return SQLITE4_BUSY. If some 
** other error occurs, return an SQLite4 error code.
**
** Parameter iLock must be one of BT_LOCK_WRITER, WORKER or CHECKPOINTER,
** or else a value returned by the BT_LOCK_READER macro.
*/
static int btLockLockop(
  BtLock *p,                      /* BtLock handle */
  int iLock,                      /* Slot to lock */
  int eOp,                        /* One of BT_LOCK_UNLOCK, SHARED or EXCL */
  int bBlock                      /* True for a blocking lock */
){
  int rc;
  while( 1 ){
    rc = btLockLockopNonblocking(p, iLock, eOp);
    if( rc!=SQLITE4_BUSY || bBlock==0 ) break;
    /* todo: Fix blocking locks */
    btLockDelay();
  }
  return rc;
}

static void btLockSharedDeref(
  sqlite4_env *pEnv, 
  bt_env *pVfs, 
  BtShared *pShared
){
  btLockMutexEnter(pEnv);
  pShared->nRef--;
  if( pShared->nRef==0 ){
    BtShared **ppS;
    for(ppS=&gBtShared.pDatabase; *ppS!=pShared; ppS=&(*ppS)->pNext);
    *ppS = (*ppS)->pNext;
    while( pShared->pBtFile ){
      BtFile *p = pShared->pBtFile;
      pShared->pBtFile = p->pNext;
      pVfs->xClose(p->pFd);
      sqlite4_free(pEnv, p);
    }
    sqlite4_mutex_free(pShared->pClientMutex);

    /* If they were allocated in heap space, free all "shared" memory chunks */
    if( pShared->pFile==0 ){
      int i;
      for(i=0; i<pShared->nShmChunk; i++){
        sqlite4_free(pEnv, pShared->apShmChunk[i]);
      }
    }else{
      pVfs->xClose(pShared->pFile);
    }
    sqlite4_free(pEnv, pShared->apShmChunk);
    sqlite4_free(pEnv, pShared);
  }
  btLockMutexLeave(pEnv);
}

/*
** Connect to the database as a read/write connection. If recovery
** is required (i.e. if this is the first connection to the db), invoke 
** the xRecover() method.
**
** Return SQLITE4_OK if successful, or an SQLite4 error code if an
** error occurs.
*/
int sqlite4BtLockConnect(BtLock *p, int (*xRecover)(BtLock*)){
  sqlite4_env *pEnv = p->pEnv;
  bt_env *pVfs = p->pVfs;
  int rc = SQLITE4_OK;
  const char *zName;
  int nName;
  BtShared *pShared;

  zName = sqlite4BtPagerFilename((BtPager*)p, BT_PAGERFILE_DATABASE);
  nName = strlen(zName);

  btLockMutexEnter(p->pEnv);
  p->iDebugId = gBtShared.iDebugId++;
  for(pShared=gBtShared.pDatabase; pShared; pShared=pShared->pNext){
    if( pShared->nName==nName && 0==memcmp(zName, pShared->zName, nName) ){
      break;
    }
  }

  if( pShared==0 ){
    sqlite4_mutex *pMutex;
    pShared = (BtShared*)sqlite4_malloc(pEnv, sizeof(BtShared) + nName + 1);
    pMutex = sqlite4_mutex_alloc(pEnv, SQLITE4_MUTEX_RECURSIVE);

    if( pShared==0 || pMutex==0 ){
      sqlite4_free(pEnv, pShared);
      sqlite4_mutex_free(pMutex);
      pShared = 0;
      pMutex = 0;
      rc = btErrorBkpt(SQLITE4_NOMEM);
    }else{
      memset(pShared, 0, sizeof(BtShared));
      pShared->bMultiProc = p->bRequestMultiProc;
      pShared->nName = nName;
      pShared->zName = (char *)&pShared[1];
      memcpy(pShared->zName, zName, nName+1);
      pShared->pNext = gBtShared.pDatabase;
      pShared->pClientMutex = pMutex;
      gBtShared.pDatabase = pShared;
    }
  }
  if( rc==SQLITE4_OK ){
    pShared->nRef++;
  }
  btLockMutexLeave(p->pEnv);

  /* Add this connection to the linked list at BtShared.pLock */
  if( rc==SQLITE4_OK ){
    sqlite4_mutex_enter(pShared->pClientMutex);

    /* If this is a multi-process connection and the shared file-handle
    ** has not yet been opened, open it now. Under the cover of the
    ** client-mutex.  */
    if( pShared->pFile==0 && pShared->bMultiProc ){
      int flags = BT_OPEN_SHARED;
      rc = pVfs->xOpen(pEnv, pVfs, pShared->zName, flags, &pShared->pFile);
    }

    if( rc==SQLITE4_OK ){
      if( pShared->pBtFile ){
        p->pBtFile = pShared->pBtFile;
        pShared->pBtFile = p->pBtFile->pNext;
        p->pBtFile->pNext = 0;
        p->pFd = p->pBtFile->pFd;
      }else{
        p->pBtFile = (BtFile*)sqlite4_malloc(pEnv, sizeof(BtFile));
        if( p->pBtFile ){
          int flags = BT_OPEN_DATABASE;
          p->pBtFile->pNext = 0;
          rc = pVfs->xOpen(pEnv, pVfs, pShared->zName, flags, &p->pFd);
          if( rc==SQLITE4_OK ){
            p->pBtFile->pFd = p->pFd;
          }else{
            sqlite4_free(pEnv, p->pBtFile);
            p->pBtFile = 0;
          }
        }else{
          rc = btErrorBkpt(SQLITE4_NOMEM);
        }
      }
    }

    if( rc==SQLITE4_OK ){
      p->pNext = pShared->pLock;
      pShared->pLock = p;
      p->pShared = pShared;
    }
    sqlite4_mutex_leave(pShared->pClientMutex);
    if( rc!=SQLITE4_OK ){
      btLockSharedDeref(pEnv, pVfs, pShared);
    }
  }

  if( rc==SQLITE4_OK ){
    rc = btLockLockop(p, BT_LOCK_DMS1, BT_LOCK_EXCL, 1);
    if( rc==SQLITE4_OK ){
      rc = btLockLockop(p, BT_LOCK_DMS2_RW, BT_LOCK_EXCL, 0);
      if( rc==SQLITE4_OK ){
        rc = btLockLockop(p, BT_LOCK_DMS2_RO, BT_LOCK_EXCL, 0);
      }
      if( rc==SQLITE4_OK ){
        rc = pVfs->xShmMap(p->pFd, 0, 0, 0);
      }
      if( rc==SQLITE4_OK ){
        rc = xRecover(p);
      }
      btLockLockop(p, BT_LOCK_DMS2_RO, BT_LOCK_UNLOCK, 0);
      btLockLockop(p, BT_LOCK_DMS2_RW, BT_LOCK_UNLOCK, 0);
      if( rc==SQLITE4_OK || rc==SQLITE4_BUSY ){
        rc = btLockLockop(p, BT_LOCK_DMS2_RW, BT_LOCK_SHARED, 0);
      }
      btLockLockop(p, BT_LOCK_DMS1, BT_LOCK_UNLOCK, 0);
    }
  }

  return rc;
}

/*
** Disconnect the read/write connection passed as the first argument
** from the database.
**
** If a checkpoint is required (i.e. if this is the last read/write
** connection to the database), this function invokes xCkpt() to
** request it. If the wal and shm files should be deleted from the
** file-system (i.e. if there are no read/write or read-only connections
** remaining), the xDel() callback is invoked as well.
**
** Return SQLITE4_OK if successful, or an SQLite4 error code if an
** error occurs. Even if an error occurs, the connection should be
** considered to be disconnected. The error code merely indicates
** that an error occurred while checkpointing or deleting the log file.
*/
int sqlite4BtLockDisconnect(
  BtLock *p,                      /* Locker handle */
  int (*xCkpt)(BtLock*),          /* Callback to checkpoint database */
  int (*xDel)(BtLock*)            /* Callback to delete wal+shm files */
){
  BtShared *pShared = p->pShared;
  BtLock **pp;
  int rc;                         /* Return code */
  
  if( p->pShared==0 ) return SQLITE4_OK;

  sqlite4_mutex_enter(pShared->pClientMutex);
  for(pp=&p->pShared->pLock; *pp!=p; pp=&(*pp)->pNext);
  *pp = (*pp)->pNext;
  sqlite4_mutex_leave(pShared->pClientMutex);

  rc = btLockLockop(p, BT_LOCK_DMS1, BT_LOCK_EXCL, 1);
  if( rc==SQLITE4_OK ){
    rc = btLockLockop(p, BT_LOCK_DMS2_RW, BT_LOCK_EXCL, 0);
    if( rc==SQLITE4_OK ){
      rc = xCkpt(p);
    }
    if( rc==SQLITE4_OK ){
      rc = btLockLockop(p, BT_LOCK_DMS2_RO, BT_LOCK_EXCL, 0);
    }
    if( rc==SQLITE4_OK ){
      rc = xDel(p);
      if( pShared->pFile ) p->pVfs->xShmUnmap(pShared->pFile, 1);
    }
    if( rc==SQLITE4_BUSY ) rc = SQLITE4_OK;
    btLockLockop(p, BT_LOCK_DMS2_RW, BT_LOCK_UNLOCK, 0);
    btLockLockop(p, BT_LOCK_DMS2_RO, BT_LOCK_UNLOCK, 0);
    btLockLockop(p, BT_LOCK_DMS1, BT_LOCK_UNLOCK, 0);
  }

  sqlite4_mutex_enter(pShared->pClientMutex);
  assert( p->pBtFile->pNext==0 );
  p->pBtFile->pNext = pShared->pBtFile;
  pShared->pBtFile = p->pBtFile;
  sqlite4_mutex_leave(pShared->pClientMutex);

  btLockSharedDeref(p->pEnv, p->pVfs, pShared);
  p->pShared = 0;
  return rc;
}

#ifndef NDEBUG
static void assertNoLockedSlots(BtLock *pLock){
  u32 mask = (1 << (BT_LOCK_READER0+BT_NREADER+1)) - (1 << BT_LOCK_READER0);
  assert( (pLock->mExclLock & mask)==0 );
}
#else
# define assertNoLockedSlots(x)
#endif

/* 
** Obtain a READER lock. 
**
** Argument aLog points to an array of 6 frame addresses. These are the 
** first and last frames in each of log regions A, B and C. Argument 
** aLock points to the array of read-lock slots in shared memory.
*/
int sqlite4BtLockReader(
  BtLock *pLock,                  /* Lock module handle */
  u32 *aLog,                      /* Current log file topology */
  u32 iFirst,                     /* First log frame to lock */
  BtReadSlot *aSlot               /* Array of read-lock slots (in shmem) */
){
  int rc = SQLITE4_BUSY;          /* Return code */
  int i;                          /* Loop counter */
  u32 iLast = aLog[5];            /* Last frame to lock */

  /* If page iFirst does not appear to be part of the log at all, then
  ** the entire log has been checkpointed (and iFirst is the "next" 
  ** frame to use). Handle this case in the same way as an empty 
  ** log file.  
  **
  ** It is also possible that the iFirst value (read from the shared-memory
  ** checkpoint header) is much newer than the aLog[] values (read from
  ** the snapshot header). If so, the caller will figure it out.  */
  if( (iFirst<aLog[0] || iFirst>aLog[1])
   && (iFirst<aLog[2] || iFirst>aLog[3])
   && (iFirst<aLog[4] || iFirst>aLog[5])
  ){
    iLast = 0;
  }

  if( iLast==0 ){
    rc = btLockLockop(pLock, BT_LOCK_READER_DBONLY, BT_LOCK_SHARED, 0);
  }else{
    const int nMaxRetry = 100;
    int nAttempt = 100;           /* Remaining lock attempts */

    for(nAttempt=0; rc==SQLITE4_BUSY && nAttempt<nMaxRetry; nAttempt++){

      int iIdxFirst = sqlite4BtLogFrameToIdx(aLog, iFirst);
      int iIdxLast = sqlite4BtLogFrameToIdx(aLog, iLast);

      assert( iIdxFirst>=0 && iIdxLast>=0 );

      /* Try to find a slot populated with the values required. */
      for(i=0; i<BT_NREADER; i++){
        if( aSlot[i].iFirst==iFirst && aSlot[i].iLast==iLast ){
          break;
        }
      }

      /* Or, if there is no slot with the required values - try to create one */
      if( i==BT_NREADER ){
        for(i=0; i<BT_NREADER; i++){
          rc = btLockLockop(pLock, BT_LOCK_READER0 + i, BT_LOCK_EXCL, 0);
          if( rc==SQLITE4_OK ){
            /* The EXCLUSIVE lock obtained by the successful call to
            ** btLockLockop() is released below by the call to obtain
            ** a SHARED lock on the same locking slot. */
            aSlot[i].iFirst = iFirst;
            aSlot[i].iLast = iLast;
            break;
          }else if( rc!=SQLITE4_BUSY ){
            return rc;
          }
        }
      }

      /* If no existing slot with the required values was found, and the
      ** attempt to create one failed, search for any usable slot. A 
      ** usable slot is one where both the "iFirst" and "iLast" values
      ** occur at the same point or earlier in the log than the required
      ** iFirst/iLast values, respectively.  */
      if( i==BT_NREADER ){
        for(i=0; i<BT_NREADER; i++){
          int iSlotFirst = sqlite4BtLogFrameToIdx(aLog, aSlot[i].iFirst);
          int iSlotLast = sqlite4BtLogFrameToIdx(aLog, aSlot[i].iLast);
          if( iSlotFirst<0 || iSlotLast<0 ) continue;
          if( iSlotFirst<=iIdxFirst && iSlotLast<=iIdxLast ) break;
        }
      }

      if( i<BT_NREADER ){
        rc = btLockLockop(pLock, BT_LOCK_READER0 + i, BT_LOCK_SHARED, 0);
        if( rc==SQLITE4_OK ){
          int iSF = sqlite4BtLogFrameToIdx(aLog, aSlot[i].iFirst);
          int iSL = sqlite4BtLogFrameToIdx(aLog, aSlot[i].iLast);
          if( iSF>iIdxFirst || iSL>iIdxLast || iSF<0 || iSL<0 ){
            btLockLockop(pLock, BT_LOCK_READER0 + i, BT_LOCK_UNLOCK, 0);
            rc = SQLITE4_BUSY;
          }else{
            sqlite4BtDebugReadlock(pLock, aSlot[i].iFirst, aSlot[i].iLast);
          }
        }else if( rc==SQLITE4_BUSY && nAttempt>(nMaxRetry/2) ){
          btLockDelay();
        }
      }
    }
  }

  assertNoLockedSlots(pLock);
  return rc;
}

/*
** Release the READER lock currently held by connection pLock.
*/
int sqlite4BtLockReaderUnlock(BtLock *pLock){
  int i;

  /* Release any locks held on reader slots. */
  assert( (BT_LOCK_READER_DBONLY+1)==BT_LOCK_READER0 );
  for(i=0; i<BT_NREADER+1; i++){
    btLockLockop(pLock, BT_LOCK_READER_DBONLY + i, BT_LOCK_UNLOCK, 0);
  }

  return SQLITE4_OK;
}

/*
** This function is used to determine which parts of the log and database
** files are currently in use by readers. It is called in two scenarios:
**
**   * by CHECKPOINTER clients, to determine how much of the log may
**     be safely copied into the database file. In this case parameter
**     piDblocked is non-NULL.
**
**   * by WRITER clients, to determine how much of the log is no longer
**     required by any present or future reader. This case can be identified
**     by (piDblocked==NULL).
*/
int sqlite4BtLockReaderQuery(
  BtLock *pLock,                  /* Lock handle */
  u32 *aLog,                      /* Current log topology */
  BtReadSlot *aSlot,              /* Array of BT_NREADER read slots */
  u32 *piOut,                     /* OUT: Query result */
  int *piDblocked                 /* OUT: True if READER_DB_ONLY is locked */
){
  u32 iOut = 0;
  int iIdxOut = 0;
  int bLast = (piDblocked!=0);
  int rc = SQLITE4_OK;
  int i;

  if( piDblocked ){
    rc = btLockLockop(pLock, BT_LOCK_READER_DBONLY, BT_LOCK_EXCL, 0);
    if( rc==SQLITE4_OK ){
      *piDblocked = 0;
      btLockLockop(pLock, BT_LOCK_READER_DBONLY, BT_LOCK_UNLOCK, 0);
    }else if( rc==SQLITE4_BUSY ){
      *piDblocked = 1;
    }else{
      return rc;
    }
  }

  for(i=0; i<3 && iOut==0; i++){
    int iSlot;
    for(iSlot=0; iSlot<BT_NREADER; iSlot++){
      u32 iVal = (bLast ? aSlot[iSlot].iLast : aSlot[iSlot].iFirst);
      if( iVal ){
        /* Try to zero the slot. */
        rc = btLockLockop(pLock, BT_LOCK_READER0 + iSlot, BT_LOCK_EXCL, 0);
        if( rc==SQLITE4_OK ){
          aSlot[iSlot].iFirst = 0;
          aSlot[iSlot].iLast = 0;
          btLockLockop(pLock, BT_LOCK_READER0 + iSlot, BT_LOCK_UNLOCK, 0);
        }else if( rc==SQLITE4_BUSY ){
          int iIdx = sqlite4BtLogFrameToIdx(aLog, iVal);
          if( iIdx>=0 && (iOut==0 || iIdx<iIdxOut) ){
            iIdxOut = iIdx;
            iOut = iVal;
          }
        }else{
          return rc;
        }
      }
    }
  }

  *piOut = iOut;
  return SQLITE4_OK;
}

int sqlite4BtLockShmMap(BtLock *pLock, int iChunk, int nByte, u8 **ppOut){
  int rc = SQLITE4_OK;
  BtShared *pShared = pLock->pShared;
  u8 *pOut = 0;

  assert( pShared->bReadonly==0 );

  sqlite4_mutex_enter(pShared->pClientMutex);
  if( pShared->nShmChunk<=iChunk ){
    u8 **apNew;
    int nNew = iChunk+1;
    int nByte = sizeof(u8*)*nNew;

    apNew = (u8**)sqlite4_realloc(pLock->pEnv, pShared->apShmChunk, nByte);
    if( apNew==0 ){
      rc = btErrorBkpt(SQLITE4_NOMEM);
    }else{
      memset(&apNew[pShared->nShmChunk],0,nByte-sizeof(u8*)*pShared->nShmChunk);
      pShared->nShmChunk = nNew;
      pShared->apShmChunk = apNew;
    }
  }

  if( rc==SQLITE4_OK ){

    assert( (pShared->bMultiProc==0)==(pShared->pFile==0) );
    if( pShared->pFile==0 ){
      /* Single process mode. Allocate memory from the heap. */
      if( pShared->apShmChunk[iChunk]==0 ){
        u8 *p = (u8*)sqlite4_malloc(pLock->pEnv, nByte);
        if( p ){
          memset(p, 0, nByte);
          pShared->apShmChunk[iChunk] = p;
        }else{
          rc = btErrorBkpt(SQLITE4_NOMEM);
        }
      }
    }else{
      /* Multi-process mode. Request shared memory from VFS */
      void *pShm = 0;
      rc = pLock->pVfs->xShmMap(pShared->pFile, iChunk, nByte, &pShm);
      pShared->apShmChunk[iChunk] = (u8*)pShm;
    }

    pOut = pShared->apShmChunk[iChunk];
    assert( pOut || rc!=SQLITE4_OK );
  }
  sqlite4_mutex_leave(pShared->pClientMutex);
  
  *ppOut = pOut;
  return rc;
}

/*
** Attempt to obtain the CHECKPOINTER lock. If the attempt is successful,
** return SQLITE4_OK. If the CHECKPOINTER lock cannot be obtained because
** it is held by some other connection, return SQLITE4_BUSY. 
**
** If any other error occurs, return an SQLite4 error code.
*/
int sqlite4BtLockCkpt(BtLock *pLock){
  return btLockLockop(pLock, BT_LOCK_CKPTER, BT_LOCK_EXCL, 0);
}
int sqlite4BtLockCkptUnlock(BtLock *pLock){
  return btLockLockop(pLock, BT_LOCK_CKPTER, BT_LOCK_UNLOCK, 0);
}

/*
** Attempt to obtain the WRITER lock. If the attempt is successful,
** return SQLITE4_OK. If the WRITER lock cannot be obtained because
** it is held by some other connection, return SQLITE4_BUSY. 
**
** If any other error occurs, return an SQLite4 error code.
*/
int sqlite4BtLockWriter(BtLock *pLock){
  return btLockLockop(pLock, BT_LOCK_WRITER, BT_LOCK_EXCL, 0);
}
int sqlite4BtLockWriterUnlock(BtLock *pLock){
  return btLockLockop(pLock, BT_LOCK_WRITER, BT_LOCK_UNLOCK, 0);
}

