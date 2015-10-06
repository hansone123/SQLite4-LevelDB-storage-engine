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
** A b-tree key/value storage subsystem that presents the interfadce
** defined by kv.h
*/

#include "sqliteInt.h"
#include "bt.h"

/* Forward declarations of objects */
typedef struct KVBt KVBt;
typedef struct KVBtCsr KVBtCsr;

/*
** An instance of an open connection to a bt_db store.  A subclass of KVStore.
*/
struct KVBt {
  KVStore base;                   /* Base class, must be first */
  char *zFilename;                /* File to open */
  int bOpen;                      /* See above */
  int openrc;                     /* See above */
  bt_db *pDb;                     /* Database handle */
};

/*
** An instance of an open cursor pointing into an LSM store.  A subclass
** of KVCursor.
*/
struct KVBtCsr {
  KVCursor base;                  /* Base class. Must be first */
  bt_cursor *pCsr;                /* LSM cursor handle */
};
  
/*
** Begin a transaction or subtransaction.
*/
static int btBegin(KVStore *pKVStore, int iLevel){
  KVBt *p = (KVBt *)pKVStore;
  int rc;
  if( p->openrc ) return p->openrc;
  if( p->bOpen==0 ){
    p->openrc = rc = sqlite4BtOpen(p->pDb, p->zFilename);
    if( rc!=SQLITE4_OK ){
      return rc;
    }
    p->bOpen = 1;
  }
  rc = sqlite4BtBegin(p->pDb, iLevel);
  pKVStore->iTransLevel = sqlite4BtTransactionLevel(p->pDb);
  return rc;
}

/*
** Commit a transaction or subtransaction.
*/
static int btCommitPhaseOne(KVStore *pKVStore, int iLevel){
  return SQLITE4_OK;
}
static int btCommitPhaseTwo(KVStore *pKVStore, int iLevel){
  KVBt *p = (KVBt *)pKVStore;
  int rc;
  assert( p->bOpen==1 );
  rc = sqlite4BtCommit(p->pDb, iLevel);
  pKVStore->iTransLevel = sqlite4BtTransactionLevel(p->pDb);
  return rc;
}

/*
** Rollback a transaction or subtransaction.
*/
static int btRollback(KVStore *pKVStore, int iLevel){
  KVBt *p = (KVBt *)pKVStore;
  int rc = SQLITE4_OK;
  if( p->bOpen==1 ){
    rc = sqlite4BtRollback(p->pDb, iLevel);
    pKVStore->iTransLevel = sqlite4BtTransactionLevel(p->pDb);
  }
  return rc;
}

/*
** Revert a transaction back to what it was when it started.
*/
#if 0
static int btRevert(KVStore *pKVStore, int iLevel){
  KVBt *p = (KVBt *)pKVStore;
  int rc;
  rc = sqlite4BtRevert(p->pDb, iLevel);
  pKVStore->iTransLevel = sqlite4BtTransactionLevel(p->pDb);
  return rc;
}
#endif

/*
** Implementation of the xReplace(X, aKey, nKey, aData, nData) method.
*/
static int btReplace(
  KVStore *pKVStore,
  const KVByteArray *aKey, KVSize nKey,
  const KVByteArray *aData, KVSize nData
){
  KVBt *p = (KVBt *)pKVStore;
  assert( p->bOpen==1 );
  return sqlite4BtReplace(p->pDb, aKey, nKey, aData, nData);
}

/*
** Create a new cursor object.
*/
static int btOpenCursor(KVStore *pKVStore, KVCursor **ppKVCursor){
  KVBt *p = (KVBt *)pKVStore;
  int rc = SQLITE4_OK;
  bt_cursor *pCsr;
  KVBtCsr *pBtcsr;

  assert( p->bOpen==1 );
  rc = sqlite4BtCsrOpen(p->pDb, sizeof(KVBtCsr), &pCsr);
  if( rc!=SQLITE4_OK ){
    pBtcsr = 0;
  }else{
    pBtcsr = (KVBtCsr*)sqlite4BtCsrExtra(pCsr);
    memset(pBtcsr, 0, sizeof(KVBtCsr));
    pBtcsr->base.pStore = pKVStore;
    pBtcsr->base.pStoreVfunc = pKVStore->pStoreVfunc;
    pBtcsr->pCsr = pCsr;
  }

  *ppKVCursor = (KVCursor*)pBtcsr;
  return rc;
}

/*
** Reset a cursor
*/
static int btReset(KVCursor *pKVCursor){
  return SQLITE4_OK;
}

/*
** Destroy a cursor object
*/
static int btCloseCursor(KVCursor *pKVCursor){
  KVBtCsr *pBtcsr = (KVBtCsr *)pKVCursor;
  sqlite4BtCsrClose(pBtcsr->pCsr);
  return SQLITE4_OK;
}

/*
** Move a cursor to the next non-deleted node.
*/
static int btNextEntry(KVCursor *pKVCursor){
  KVBtCsr *pBtcsr = (KVBtCsr *)pKVCursor;
  return sqlite4BtCsrNext(pBtcsr->pCsr);
}

/*
** Move a cursor to the previous non-deleted node.
*/
static int btPrevEntry(KVCursor *pKVCursor){
  KVBtCsr *pBtcsr = (KVBtCsr *)pKVCursor;
  return sqlite4BtCsrPrev(pBtcsr->pCsr);
}

/*
** Seek a cursor.
*/
static int btSeek(
  KVCursor *pKVCursor, 
  const KVByteArray *aKey,
  KVSize nKey,
  int dir
){
  KVBtCsr *pCsr = (KVBtCsr *)pKVCursor;

  assert( dir==0 || dir==1 || dir==-1 || dir==-2 );
  assert( BT_SEEK_EQ==0 && BT_SEEK_GE==1 && BT_SEEK_LE==-1 );
  assert( BT_SEEK_LEFAST==-2 );

  return sqlite4BtCsrSeek(pCsr->pCsr, (void *)aKey, nKey, dir);
}

/*
** Delete the entry that the cursor is pointing to.
**
** Though the entry is "deleted", it still continues to exist as a
** phantom.  Subsequent xNext or xPrev calls will work, as will
** calls to xKey and xData, thought the result from xKey and xData
** are undefined.
*/
static int btDelete(KVCursor *pKVCursor){
  KVBtCsr *pBtcsr = (KVBtCsr *)pKVCursor;
  return sqlite4BtDelete(pBtcsr->pCsr);
}

/*
** Return the key of the node the cursor is pointing to.
*/
static int btKey(
  KVCursor *pKVCursor,         /* The cursor whose key is desired */
  const KVByteArray **paKey,   /* Make this point to the key */
  KVSize *pN                   /* Make this point to the size of the key */
){
  KVBtCsr *pCsr = (KVBtCsr *)pKVCursor;
  return sqlite4BtCsrKey(pCsr->pCsr, (const void **)paKey, (int *)pN);
}

/*
** Return the data of the node the cursor is pointing to.
*/
static int btData(
  KVCursor *pKVCursor,         /* The cursor from which to take the data */
  KVSize ofst,                 /* Offset into the data to begin reading */
  KVSize n,                    /* Number of bytes requested */
  const KVByteArray **paData,  /* Pointer to the data written here */
  KVSize *pN                   /* Number of bytes delivered */
){
  KVBtCsr *pCsr = (KVBtCsr *)pKVCursor;
  return sqlite4BtCsrData(pCsr->pCsr, ofst, n, (const void**)paData, (int*)pN);
}

/*
** Destructor for the entire in-memory storage tree.
*/
static int btClose(KVStore *pKVStore){
  KVBt *p = (KVBt *)pKVStore;
  sqlite4_free(pKVStore->pEnv, p->zFilename);
  return sqlite4BtClose(p->pDb);
}

static int btControl(KVStore *pKVStore, int op, void *pArg){
  KVBt *p = (KVBt *)pKVStore;
  return sqlite4BtControl(p->pDb, op, pArg);
}

static int btGetMeta(KVStore *pKVStore, unsigned int *piVal){
  KVBt *p = (KVBt *)pKVStore;
  assert( p->bOpen==1 );
  return sqlite4BtGetCookie(p->pDb, piVal);
}

static int btPutMeta(KVStore *pKVStore, unsigned int iVal){
  KVBt *p = (KVBt *)pKVStore;
  assert( p->bOpen==1 );
  return sqlite4BtSetCookie(p->pDb, iVal);
}

typedef struct BtPragmaCtx BtPragmaCtx;
struct BtPragmaCtx {
  sqlite4_kvstore *pKVStore;
  int ePragma;
};

/*
** Candidate values for BtPragmaCtx.ePragma
*/
#define BTPRAGMA_PAGESZ     1
#define BTPRAGMA_CHECKPOINT 2

static void btPragmaDestroy(void *pArg){
  BtPragmaCtx *p = (BtPragmaCtx*)pArg;
  sqlite4_free(p->pKVStore->pEnv, p);
}

static void btPragma(
  sqlite4_context *pCtx, 
  int nVal,
  sqlite4_value **apVal
){
  int rc = SQLITE4_OK;            /* Return code */
  BtPragmaCtx *p = (BtPragmaCtx*)sqlite4_context_appdata(pCtx);
  bt_db *db = ((KVBt*)(p->pKVStore))->pDb;

  switch( p->ePragma ){
    case BTPRAGMA_PAGESZ: {
      int pgsz = -1;
      if( nVal>0 ){
        pgsz = sqlite4_value_int(apVal[0]);
      }
      sqlite4BtControl(db, BT_CONTROL_PAGESZ, (void*)&pgsz);
      sqlite4_result_int(pCtx, pgsz);
      break;
    }

    case BTPRAGMA_CHECKPOINT: {
      bt_checkpoint ckpt;
      ckpt.nFrameBuffer = 0;
      ckpt.nCkpt = 0;
      rc = sqlite4BtControl(db, BT_CONTROL_CHECKPOINT, (void*)&ckpt);
      if( rc!=SQLITE4_OK ){
        sqlite4_result_error_code(pCtx, rc);
      }else{
        sqlite4_result_int(pCtx, ckpt.nCkpt);
      }
      break;
    }

    default:
      assert( 0 );
  }
}


static int btGetMethod(
  sqlite4_kvstore *pKVStore, 
  const char *zMethod, 
  void **ppArg,
  void (**pxFunc)(sqlite4_context *, int, sqlite4_value **),
  void (**pxDestroy)(void*)
){
  struct PragmaMethod {
    const char *zPragma;
    int ePragma;
  } aPragma[] = {
    { "page_size", BTPRAGMA_PAGESZ },
    { "checkpoint", BTPRAGMA_CHECKPOINT },
  };
  int i;
  for(i=0; i<ArraySize(aPragma); i++){
    if( sqlite4_stricmp(aPragma[i].zPragma, zMethod)==0 ){
      BtPragmaCtx *pCtx = sqlite4_malloc(pKVStore->pEnv, sizeof(BtPragmaCtx));
      if( pCtx==0 ) return SQLITE4_NOMEM;
      pCtx->ePragma = aPragma[i].ePragma;
      pCtx->pKVStore = pKVStore;
      *ppArg = (void*)pCtx;
      *pxFunc = btPragma;
      *pxDestroy = btPragmaDestroy;
      return SQLITE4_OK;
    }
  }
  return SQLITE4_NOTFOUND;
}

int sqlite4OpenBtree(
  sqlite4_env *pEnv,              /* The environment to use */
  sqlite4_kvstore **ppKVStore,    /* OUT: New KV store returned here */
  const char *zFilename,          /* Name of database file to open */
  unsigned flags                  /* Bit flags */
){
  static const sqlite4_kv_methods bt_methods = {
    1,                            /* iVersion */
    sizeof(sqlite4_kv_methods),   /* szSelf */
    btReplace,                    /* xReplace */
    btOpenCursor,                 /* xOpenCursor */
    btSeek,                       /* xSeek */
    btNextEntry,                  /* xNext */
    btPrevEntry,                  /* xPrev */
    btDelete,                     /* xDelete */
    btKey,                        /* xKey */
    btData,                       /* xData */
    btReset,                      /* xReset */
    btCloseCursor,                /* xCloseCursor */
    btBegin,                      /* xBegin */
    btCommitPhaseOne,             /* xCommitPhaseOne */
    btCommitPhaseTwo,             /* xCommitPhaseTwo */
    btRollback,                   /* xRollback */
    0,                            /* xRevert */
    btClose,                      /* xClose */
    btControl,                    /* xControl */
    btGetMeta,                    /* xGetMeta */
    btPutMeta,                    /* xPutMeta */
    btGetMethod                   /* xGetMethod */
  };

  KVBt *pNew = 0;
  bt_db *pDb = 0;
  int rc;

  rc = sqlite4BtNew(pEnv, sizeof(KVBt), &pDb);
  if( rc==SQLITE4_OK ){
    bt_env *pBtenv = 0;
    pNew = (KVBt*)sqlite4BtExtra(pDb);
    pNew->base.pStoreVfunc = &bt_methods;
    pNew->base.pEnv = pEnv;
    pNew->pDb = pDb;
    sqlite4BtControl(pDb, BT_CONTROL_GETVFS, (void*)&pBtenv);
    rc = pBtenv->xFullpath(pEnv, pBtenv, zFilename, &pNew->zFilename);
  }

  if( rc!=SQLITE4_OK && pDb ){
    sqlite4BtClose(pDb);
    pNew = 0;
  }
  *ppKVStore = pNew;
  return rc;
}

