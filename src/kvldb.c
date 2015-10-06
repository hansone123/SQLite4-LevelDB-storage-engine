#include "sqliteInt.h"
#include "kvldb.h"


/*
** An instance of an open connection to an Ldb store.  A subclass of KVStore.
*/
struct KVLdb {
  KVStore base;                   /* Base class, must be first */
  leveldb_t *pDb;                    /* ldb database handle */
  leveldb_iterator_t *pCsr;               /* leveldb read iterator */
  leveldb_readoptions_t *roptions;        /* leveldb option for any read action*/
  leveldb_writeoptions_t *woptions;       /* leveldb option for put*/
  unsigned int iMeta;   /* Schema cookie value */
};

/*
** An instance of an open cursor pointing into an LSM store.  A subclass
** of KVCursor.
*/
struct KVLdbCsr {
  KVCursor base;                  /* Base class. Must be first */
  leveldb_iterator_t *pCsr;               /* the searchiterator */
  
};

/*
** Create a new in-memory storage engine and return a pointer to it.
*/
int sqlite4KVStoreOpenLdb(
  sqlite4_env *pEnv,          /* Run-time environment */
  KVStore **ppKVStore,        /* OUT: write the new KVStore here */
  const char *zName,          /* Name of the file to open */
  unsigned openFlags          /* Flags */)
{
    /* Virtual methods for an LSM data store */
  static const KVStoreMethods kvldbMethods = {
    1,                            /* iVersion */
    sizeof(KVStoreMethods),       /* szSelf */
    kvldbReplace,                 /* xReplace */
    kvldbOpenCursor,              /* xOpenCursor */
    kvldbSeek,                    /* xSeek */
    kvldbNextEntry,               /* xNext */
    kvldbPrevEntry,               /* xPrev */
    kvldbDelete,                  /* xDelete */
    kvldbKey,                     /* xKey */
    kvldbData,                    /* xData */
    kvldbReset,                   /* xReset */
    kvldbCloseCursor,             /* xCloseCursor */
    kvldbBegin,                   /* xBegin */
    kvldbCommitPhaseOne,          /* xCommitPhaseOne */
    kvldbCommitPhaseTwo,          /* xCommitPhaseTwo */
    kvldbRollback,                /* xRollback */
    kvldbRevert,                  /* xRevert */
    kvldbClose,                   /* xClose */
    kvldbControl,                 /* xControl */
    kvldbGetMeta,                 /* xGetMeta */
    kvldbPutMeta,                 /* xPutMeta */
    kvldbGetMethod                /* xGetMethod */
  };
  
  int rc = SQLITE4_OK;
  KVLdb *pNew;
  pNew = (KVLdb *)sqlite4_malloc(pEnv, sizeof(KVLdb));
  if( pNew==0 ){
    rc = SQLITE4_NOMEM;
  }else{
      
    memset(pNew, 0, sizeof(KVLdb));
    pNew->base.pStoreVfunc = &kvldbMethods;
    pNew->base.pEnv = pEnv;
    char *err = NULL;
    //create all option that open leveldb need
    leveldb_options_t *options;
    options = leveldb_options_create();
    leveldb_options_set_create_if_missing(options, 1);
    pNew->pDb = leveldb_open(options, zName, &err);
    //create the option for data access
    pNew->woptions = leveldb_writeoptions_create();
    pNew->roptions = leveldb_readoptions_create();
    
    //check
    if (err != NULL) {
      fprintf(stderr, "Open fail.\n");
      rc = SQLITE4_ERROR;
    }
    
    if ( rc!=SQLITE4_OK ){
        leveldb_destroy_db(options, zName, &err);
        printf("Initialize leveldb failed");
        if (err != NULL) {
          fprintf(stderr, "Destroy fail.\n");
        }

        leveldb_free(err); err = NULL;
        sqlite4_free(pEnv, pNew);
        pNew = 0;
    }
  }
  
  printf("sqlite4KVStoreOpenLdb finish\n");
  
  *ppKVStore = (KVStore*)pNew;
  return rc;
}
//
static int kvldbBegin(KVStore *pKVStore, int iLevel)
{
    int rc = SQLITE4_OK;
    printf("kvldbBegin start\n");
    printf("rc=  %d\n", rc);
    printf("kvldbBegin finish\n");
    
    return rc;
}
static int kvldbCommitPhaseOne(KVStore *pKVStore, int iLevel){
    int rc = SQLITE4_OK;
    printf("kvldbCommitPhaseOne start\n");
    printf("rc=  %d\n", rc);
    printf("kvldbCommitPhaseOne finish\n");
    return SQLITE4_OK;
}
static int kvldbCommitPhaseTwo(KVStore *pKVStore, int iLevel){
    int rc = SQLITE4_OK;
    printf("kvldbCommitPhaseTwo start\n");
    printf("rc=  %d\n", rc);
    printf("kvldbCommitPhaseTwo finish\n");
    return SQLITE4_OK;
}

/*
** Rollback a transaction or subtransaction.
**
** Revert all uncommitted changes back through the most recent xBegin or 
** xCommit with the same iLevel.  If iLevel==0 then back out all uncommited
** changes.
**
** After this routine returns successfully, the transaction level will be
** equal to iLevel.
*/
static int kvldbRollback(KVStore *pKVStore, int iLevel){
  int rc = SQLITE4_OK;
  printf("kvldbRollback start\n");
  printf("rc=  %d\n", rc);
  printf("kvldbRollback finish\n");
  return rc;
}

/*
** Revert a transaction back to what it was when it started.
*/
static int kvldbRevert(KVStore *pKVStore, int iLevel){
 int rc = SQLITE4_OK;
 printf("kvldbRevert start\n");
 printf("rc=  %d\n", rc);
 printf("kvldbRevert finish\n");
  return rc;
}


/*
** Implementation of the xReplace(X, aKey, nKey, aData, nData) method.
**
** Insert or replace the entry with the key aKey[0..nKey-1].  The data for
** the new entry is aData[0..nData-1].  Return SQLITE4_OK on success or an
** error code if the insert fails.
**
** The inputs aKey[] and aData[] are only valid until this routine
** returns.  If the storage engine needs to keep that information
** long-term, it will need to make its own copy of these values.
**
** A transaction will always be active when this routine is called.
*/
static int kvldbReplace(
  KVStore *pKVStore,
  const KVByteArray *aKey, KVSize nKey,
  const KVByteArray *aData, KVSize nData
){
    printf("kvldbReplace start\n");
    printf("key: ");
    int i=0;
    for (i=0;i<nKey;i++)
        printf("%x,", *(aKey+i));
    printf("\tval: ");
    for (i=0;i<nData;i++)
        printf("%x,", *(aData+i));
    
    printf("\n");
    int rc = SQLITE4_OK;
    KVLdb *pStore = (KVLdb*)pKVStore;
    
    /*Ldb WRITE */
    char *err = NULL;
    
    leveldb_put(pStore->pDb, pStore->woptions, (char *)aKey, nKey, (char *)aData, nData, &err);

    if (err != NULL) {
      fprintf(stderr, "Ldb write fail.\n");
      rc = SQLITE4_ERROR;
    }
    leveldb_free(err);
    
    printf("rc=  %d\n", rc);
    printf("kvldbReplace finish\n");
    
    return rc;
}

/*
** Create a new cursor object.
*/
static int kvldbOpenCursor(KVStore *pKVStore, KVCursor **ppKVCursor){
    printf("kvldbOpenCursor start\n");
    int rc = SQLITE4_OK;
    KVLdb *pStore = (KVLdb*)pKVStore;
    KVLdbCsr *pCsr;
    //allocate memory
    pCsr = (KVLdbCsr *)sqlite4_malloc(pKVStore->pEnv, sizeof(KVLdbCsr));

    if( pCsr==0 ){
      rc = SQLITE4_NOMEM;
    }else{

        memset(pCsr, 0, sizeof(KVLdbCsr));
        pCsr->pCsr = leveldb_create_iterator(pStore->pDb, pStore->roptions);
        pCsr->base.pStore = pKVStore;
        pCsr->base.pStoreVfunc = pKVStore->pStoreVfunc;
        //printf("the open cursor iter valid: %d\n",leveldb_iter_valid(pCsr->pCsr));
//        if (!leveldb_iter_valid(pCsr->pCsr)) {
//          
//        }
//        else{
//          sqlite4_free(pCsr->base.pEnv, pCsr);
//          pCsr = 0;
//      }
    }

    *ppKVCursor = (KVCursor*)pCsr;
    
    printf("rc=  %d\n", rc);
    printf("kvldbOpenCursor finish\n");
    return rc;
}

/*
** Reset a cursor
*/
static int kvldbReset(KVCursor *pKVCursor){
    
    int rc = SQLITE4_OK;
    printf("kvldbReset start\n");
    printf("rc=  %d\n", rc);
    printf("kvldbReset finish\n");
    return rc;
}

/*
** Destroy a cursor object
*/
static int kvldbCloseCursor(KVCursor *pKVCursor){
    printf("kvldbCloseCursor start\n");
    int rc = SQLITE4_OK;
    KVLdbCsr *pCsr = (KVLdbCsr *)pKVCursor;
    leveldb_iter_destroy(pCsr->pCsr);
    sqlite4_free(pCsr->base.pEnv, pCsr);
    
    printf("rc=  %d\n", rc);
    printf("kvldbCloseCursor finish\n");
    return rc;
}

/*
** Move a cursor to the next non-deleted node.
*/
static int kvldbNextEntry(KVCursor *pKVCursor){
    int rc = SQLITE4_NOTFOUND;
    printf("kvldbNextEntry start\n");
    
    KVLdbCsr *pCsr = (KVLdbCsr *)pKVCursor;
    if( leveldb_iter_valid(pCsr->pCsr)==0 ) return rc;
    leveldb_iter_next(pCsr->pCsr);
    if( leveldb_iter_valid(pCsr->pCsr) ){
      rc = SQLITE4_OK;
    }
    printf("rc=  %d\n", rc);
    printf("kvldbNextEntry finish\n");
    return rc;
}

/*
** Move a cursor to the previous non-deleted node.
*/
static int kvldbPrevEntry(KVCursor *pKVCursor){
    int rc = SQLITE4_NOTFOUND;
    
    printf("kvldbPrevEntry start\n");
    KVLdbCsr *pCsr = (KVLdbCsr *)pKVCursor;

    if( leveldb_iter_valid(pCsr->pCsr)==0 ) return rc;
    leveldb_iter_prev(pCsr->pCsr);
    if( leveldb_iter_valid(pCsr->pCsr) ){
      rc = SQLITE4_OK;
    }
    printf("rc=  %d\n", rc);
    printf("kvldbPrevEntry finish\n");
    return rc;
}

/*
** Seek a cursor.
*/
static int kvldbSeek(
  KVCursor *pKVCursor, 
  const KVByteArray *aKey,
  KVSize nKey,
  int dir
){
    printf("kvldbSeek start\n");
    int rc = SQLITE4_NOTFOUND;
    KVLdbCsr *pCsr = (KVLdbCsr *)pKVCursor;
    const char *rKey;
    size_t rKeysize;
    
    printf("the key of record that we want to seek: ");
    int i=0;
    for (i=0;i<nKey;i++)
        printf("%x,", *(aKey+i));
    
    leveldb_iter_seek(pCsr->pCsr, (const void *)aKey, nKey);
    printf("\nIs cursor valid?: %d\n", leveldb_iter_valid(pCsr->pCsr));
    
    switch (dir)
    {
        case LDB_SEEK_EQ:
            printf("LDB_SEEK_EQ:\n");
            if( leveldb_iter_valid(pCsr->pCsr))
            {    
                rKey = leveldb_iter_key(pCsr->pCsr, &rKeysize);
                printf("the key we found: ");
                for (i=0;i<rKeysize;i++)
                    printf("%x,", *(rKey+i));
                printf("\nnKey:%d\trKeysize:%d\t", nKey, rKeysize);
                printf("memcmp: %d\n", memcmp((void *)rKey, aKey, nKey>rKeysize ? nKey : rKeysize));
                if (memcmp((void *)rKey, aKey, nKey>rKeysize ? nKey : rKeysize)==0)
                {
                    rc = SQLITE4_OK;
                }
                else {
                    rc = SQLITE4_NOTFOUND;
                }
                
            }else {
                rc = SQLITE4_NOTFOUND;
            }
            break;
            
        case LDB_SEEK_LEFAST:
            printf("LDB_SEEK_LEFAST:\n");
        case LDB_SEEK_LE:
            printf("LDB_SEEK_LE\n");
            if( leveldb_iter_valid(pCsr->pCsr)) 
            {
                rKey = leveldb_iter_key(pCsr->pCsr, &rKeysize);
                printf("the key we found: ");
                for (i=0;i<rKeysize;i++)
                    printf("%x,", *(rKey+i));
                
                if (memcmp((void *)rKey, aKey, nKey>rKeysize ? nKey : rKeysize)==0)
                {
                    rc = SQLITE4_OK;
                }else {
                    leveldb_iter_prev(pCsr->pCsr);
                    rc = SQLITE4_INEXACT;
                }
            }else {
                leveldb_iter_seek_to_last(pCsr->pCsr);
                rc = SQLITE4_INEXACT;
            }
            
            break;
        case LDB_SEEK_GE:  
            printf("LDB_SEEK_GE\n");
            if( leveldb_iter_valid(pCsr->pCsr)) 
            {
                rKey = leveldb_iter_key(pCsr->pCsr, &rKeysize);
                printf("the key we found: ");
                for (i=0;i<rKeysize;i++)
                    printf("%x,", *(rKey+i));
                
                if (memcmp((void *)rKey, aKey, nKey>rKeysize ? nKey : rKeysize)==0)
                {
                    rc = SQLITE4_OK;
                }else {
                    rc = SQLITE4_INEXACT;
                }
            }else {
                rc = SQLITE4_NOTFOUND;
            }
            
            break;
    }
        
    
    printf("rc=  %d\n", rc);
    printf("kvldbSeek finish\n");
    return rc;
}

/*
** Delete the entry that the cursor is pointing to.
**
** Though the entry is "deleted", it still continues to exist as a
** phantom.  Subsequent xNext or xPrev calls will work, as will
** calls to xKey and xData, thought the result from xKey and xData
** are undefined.
*/
static int kvldbDelete(KVCursor *pKVCursor){
    printf("kvldbDelete start\n");
    int rc;
    char *err = NULL;
    const char *pKey;
    size_t nKey;
    rc = SQLITE4_OK;
    
    KVLdbCsr *pCsr = (KVLdbCsr *)pKVCursor;
    KVLdb *pStore = (KVLdb *) (pKVCursor->pStore);
    
    pKey = leveldb_iter_key(pCsr->pCsr, &nKey);
    
    printf("delete key: ");
    int i=0;
    for (i=0;i<nKey;i++)
        printf("%i,", *(pKey+i));
    printf("\n");
    
    printf("cursor valid: %i\n", leveldb_iter_valid(pCsr->pCsr));
    
    if( leveldb_iter_valid(pCsr->pCsr)){
        //
            leveldb_delete(pStore->pDb, pStore->woptions, pKey, nKey, &err);

            if (err != NULL) {
              fprintf(stderr, "Delete fail.\n");
              rc = SQLITE4_ERROR;
            }
    }
    leveldb_free(err);
    
    printf("rc=  %d\n", rc);
    printf("kvldbDelete finish\n");
    
    return SQLITE4_OK;
}

/*
** Return the key of the node the cursor is pointing to.
*/
static int kvldbKey(
  KVCursor *pKVCursor,         /* The cursor whose key is desired */
  const KVByteArray **paKey,   /* Make this point to the key */
  KVSize *pN                   /* Make this point to the size of the key */
){
    printf("kvldbKey start\n");
    int rc;
    rc = SQLITE4_OK;
    KVLdbCsr *pCsr = (KVLdbCsr *)pKVCursor;
    if( leveldb_iter_valid(pCsr->pCsr)==0 ) return SQLITE4_DONE;
   
    *paKey = (const void*)leveldb_iter_key(pCsr->pCsr, (void *)pN);
    
    printf("key: ");
    int i=0;
    for (i=0;i<*pN;i++)
        printf("%x,", *(*paKey+i));
    printf("\n");
    
    printf("rc=  %d\n", rc);
    printf("kvldbKey finish\n");
    return rc;
}

/*
** Return the data of the node the cursor is pointing to.
*/
static int kvldbData(
  KVCursor *pKVCursor,         /* The cursor from which to take the data */
  KVSize ofst,                 /* Offset into the data to begin reading */
  KVSize n,                    /* Number of bytes requested */
  const KVByteArray **paData,  /* Pointer to the data written here */
  KVSize *pNData               /* Number of bytes delivered */
){
    printf("kvldbData start\n");
    KVLdbCsr *pCsr = (KVLdbCsr *)pKVCursor;
    int rc;
    void *pData;
    size_t nData;
    rc = SQLITE4_OK;
    
    pData = leveldb_iter_value(pCsr->pCsr, &nData);
    if (pData==NULL)
        rc = SQLITE4_ERROR;
    else {
        if( n<0 ){
          *paData = pData;
          *pNData = nData;
        }else{
          int nOut = n;
          if( (ofst+n)>nData ) nOut = nData - ofst;
          if( nOut<0 ) nOut = 0;

          *paData = &((u8 *)pData)[n];
          *pNData = nOut;
        }
    }
    printf("value: ");
    int i=0;
    for (i=0;i<*pNData;i++)
        printf("%x,", *((*paData)+i));
    printf("\n");
    printf("rc=  %d\n", rc);
    printf("kvldbData finish\n");
    return rc;
}

/*
** Destructor for the entire in-memory storage tree.
*/
static int kvldbClose(KVStore *pKVStore){
    int rc = SQLITE4_OK;
    
    printf("kvldbClose start\n");
    KVLdb *p = (KVLdb *)pKVStore;

    leveldb_close(p->pDb);
    sqlite4_free(p->base.pEnv, p);
    
    printf("rc=  %d\n", rc);
    printf("kvldbClose finish\n");
    
    return SQLITE4_OK;
}

static int kvldbControl(KVStore *pKVStore, int op, void *pArg){
    int rc = SQLITE4_OK;
    printf("kvldbControl start\n");
    printf("rc=  %d\n", rc);
    printf("kvldbControl finish\n");
    return rc;
}

static int kvldbGetMeta(KVStore *pKVStore, unsigned int *piVal){
    printf("kvldbGetMeta start\n");
    int rc = SQLITE4_OK;
    KVLdb *pStore = (KVLdb*)pKVStore;
    *piVal = pStore->iMeta;
    printf("piVal: %i\n", *piVal);
    printf("rc=  %d\n", rc);
    printf("kvldbGetMeta finish\n");
    return rc;
}

static int kvldbPutMeta(KVStore *pKVStore, unsigned int iVal){
    printf("kvldbPutMeta start\n");
    int rc = SQLITE4_OK;
    KVLdb *pStore = (KVLdb*)pKVStore;
    pStore->iMeta = iVal;
    printf("%i\n", pStore->iMeta);
    printf("rc=  %d\n", rc);
    printf("kvldbPutMeta finish\n");
    return rc;
    
}

typedef struct PragmaCtx PragmaCtx;
struct PragmaCtx {
  sqlite4_kvstore *pKVStore;
  int ePragma;
};


static void kvldbPragmaDestroy(void *p){
    printf("kvldbPragmaDestroy start\n");
    sqlite4_free(0, p);
    printf("kvldbPragmaDestroy finish\n");
}

static void kvldbPragma(sqlite4_context *ctx, int nArg, sqlite4_value **apArg){
    printf("kvldbPragma start\n");
    printf("kvldbPragma finish\n");
}

static int kvldbGetMethod(
  sqlite4_kvstore *pKVStore, 
  const char *zMethod, 
  void **ppArg,
  void (**pxFunc)(sqlite4_context *, int, sqlite4_value **),
  void (**pxDestroy)(void *)
){
    
    printf("kvldbGetMethod start\n");
    int rc = SQLITE4_OK;
    printf("rc=  %d\n", rc);
    printf("kvldbGetMethod finish\n");
    return rc;
}
