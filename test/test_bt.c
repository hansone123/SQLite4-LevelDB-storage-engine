/*
** 2014 February 17
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

#include <tcl.h>
#include "bt.h"
#include "sqlite4.h"
#include "testInt.h"
#include <assert.h>
#include <string.h>

#define MIN(x,y) (((x)<(y)) ? (x) : (y))
#define MAX(x,y) (((x)<(y)) ? (y) : (x))

typedef sqlite4_int64 i64;
typedef unsigned char u8;
typedef struct BtTestEnv BtTestEnv;
typedef struct BtTestFile BtTestFile;
typedef struct BtTestSector BtTestSector;

/*
** An instance of the following object is created each invocation of the 
** [btenv] command. 
*/
struct BtTestEnv {
  bt_env base;                    /* The wrapper environment */
  sqlite4_env *pSqlEnv;           /* SQLite environment */
  bt_env *pEnv;                   /* Underlying environment object */

  /* IO error injection parameters */
  int nIoerrCnt;                  /* Error countdown */
  int bIoerrPersist;              /* True for persistent errors */
  int nIoerrInjected;             /* Number of errors injected */

  /* Crash simulation parameters */
  int nCrashCnt;                  /* Crash on nCrashCnt'th xSync() */
  int bWalCrash;                  /* Crash on xSync of WAL file */
  int bCrashed;                   /* True after crash is simulated */
  
  BtTestFile *pTestFile;          /* List of open test files */
};

#define BTTEST_SECTOR_SIZE 512
struct BtTestSector {
  int iFirstUnsynced;             /* Offset of first dirty byte within sector */
  int iLastUnsynced;              /* One more than offset of last dirty byte */
  u8 aBuf[BTTEST_SECTOR_SIZE];    /* Sector contents */
};

struct BtTestFile {
  BtTestEnv *pTestEnv;            /* Test environment */
  BtTestFile *pNext;              /* Next file belonging to pTestEnv */
  int flags;                      /* Copy of flags parameter passed to xOpen */
  bt_file *pFile;                 /* Underlying file object */

  int nSector;                    /* Size of apSector[] array */
  BtTestSector **apSector;        /* Data written but not synced */
};

/*
** If required, simulate the effects of a crash or power failure on the 
** contents of the file-system. Otherwise, flush any buffers held by this
** file object to disk.
*/
static void testFlush(BtTestFile *pTest){
  int i;
  bt_env *pEnv = pTest->pTestEnv->pEnv;

  for(i=0; i<pTest->nSector; i++){
    BtTestSector *pSector = pTest->apSector[i];
    if( pSector ){
      i64 iOff = (i64)i * BTTEST_SECTOR_SIZE + pSector->iFirstUnsynced;
      int nData = pSector->iLastUnsynced - pSector->iFirstUnsynced;
      pEnv->xWrite(
          pTest->pFile, iOff, &pSector->aBuf[pSector->iFirstUnsynced], nData
      );
    }

    pTest->apSector[i] = 0;
    ckfree(pSector);
  }

  ckfree(pTest->apSector);
  pTest->apSector = 0;
  pTest->nSector = 0;
}

static void testCrash(BtTestEnv *pTestEnv){
  BtTestFile *pFile;
  for(pFile=pTestEnv->pTestFile; pFile; pFile=pFile->pNext){
    int i;
    for(i=0; i<pFile->nSector; i++){
      int r;
      BtTestSector *pSector = pFile->apSector[i];
      if( pSector==0 ) continue;

      sqlite4_randomness(pTestEnv->pSqlEnv, sizeof(int), &r);
      switch( r%3 ){
        case 0:
          /* no-op */
          break;

        case 1:
          ckfree(pSector);
          pFile->apSector[i] = 0;
          break;

        case 2: {
          int nByte = (pSector->iLastUnsynced - pSector->iFirstUnsynced);
          sqlite4_randomness(
              pTestEnv->pSqlEnv, nByte, &pSector->aBuf[pSector->iFirstUnsynced]
          );
          break;
        }
      }
    }
  }

  pTestEnv->bCrashed = 1;
}

/*
** If required, simulate the effects of a crash or power failure on the 
** contents of the file-system. Otherwise, flush any buffers held by this
** file object to disk.
*/
static void testCrashOrFlush(BtTestFile *pTest){
  int bAll = 0;
  BtTestEnv *pTestEnv = pTest->pTestEnv;

  if( pTestEnv->nCrashCnt ){
    if( ((pTest->flags & BT_OPEN_LOG) && pTestEnv->bWalCrash)
     || ((pTest->flags & BT_OPEN_DATABASE) && pTestEnv->bWalCrash==0)
    ){
      pTestEnv->nCrashCnt--;
      if( pTestEnv->nCrashCnt==0 ){
        testCrash(pTestEnv);
        bAll = 1;
      }
    }
  }

  if( bAll ){
    BtTestFile *pFile;
    for(pFile=pTestEnv->pTestFile; pFile; pFile=pFile->pNext){
      testFlush(pFile);
    }
  }else{
    testFlush(pTest);
  }
}

/*
** Return true if an IO error should be injected. False otherwise.
**
** Also return true if the BtTestEnv is in "crashed" state. In this state
** the only objective is to prevent any further write operations to the 
** file-system. 
*/
static int testInjectIoerr(BtTestEnv *p){
  if( p->bCrashed ) return 1;
  if( (p->nIoerrCnt==0 && p->bIoerrPersist)
   || (p->nIoerrCnt>0 && (--p->nIoerrCnt)==0)
  ){
    p->nIoerrInjected++;
    return 1;
  }
  return 0;
}

/*
** The bt_env.xOpen method.
*/
static int testBtOsOpen(
  sqlite4_env *pSqlEnv,
  bt_env *pEnv,
  const char *zFile,
  int flags,
  bt_file **ppFile
){
  BtTestEnv *p = (BtTestEnv*)pEnv;
  BtTestFile *pNew = 0;
  int rc;

  if( testInjectIoerr(p) ){
    rc = SQLITE4_CANTOPEN;
  }else{
    pNew = ckalloc(sizeof(BtTestFile));
    memset(pNew, 0, sizeof(BtTestFile));
    pNew->pTestEnv = p;
    pNew->flags = flags;
    rc = p->pEnv->xOpen(pSqlEnv, p->pEnv, zFile, flags, &pNew->pFile);
    if( rc!=SQLITE4_OK ){
      ckfree(pNew);
      pNew = 0;
    }else{
      pNew->pNext = p->pTestFile;
      p->pTestFile = pNew;
    }
  }
  *ppFile = (bt_file*)pNew;
  return rc;
}

static int testBtOsSize(bt_file *pFile, i64 *pnByte){
  BtTestFile *pTestFile = (BtTestFile*)pFile;
  bt_env *pEnv = pTestFile->pTestEnv->pEnv;
  int rc;

  if( testInjectIoerr(pTestFile->pTestEnv) ){
    rc = SQLITE4_IOERR_FSTAT;
  }else{
    rc = pEnv->xSize(pTestFile->pFile, pnByte);
  }
  return rc;
}

static int testBtOsWrite(
  bt_file *pFile,                 /* File to write to */
  i64 iOff,                       /* Offset to write to */
  void *pData,                    /* Write data from this buffer */
  int nData                       /* Bytes of data to write */
){
  BtTestFile *pTest = (BtTestFile*)pFile;
  BtTestEnv *pTestEnv = pTest->pTestEnv;
  int rc;

  if( pTestEnv->nCrashCnt ){
    /* A crash simulation is running. Instead of writing to the file on
    ** disk, store the new data in the BtTestfile.apSector[] array.  */
    i64 iWrite = iOff;
    int nRem = nData;
    u8 *aRem = (u8*)pData;
    int nSector;
    int iSector;

    /* Ensure the apSector[] array is large enough */
    nSector = ((iOff+nData) + BTTEST_SECTOR_SIZE - 1) / BTTEST_SECTOR_SIZE;
    if( nSector>pTest->nSector ){
      int nByte = sizeof(BtTestSector*)*nSector;
      int nExtra = nByte - sizeof(BtTestSector*)*pTest->nSector;
      pTest->apSector = (BtTestSector**)ckrealloc(pTest->apSector, nByte);
      memset(&pTest->apSector[pTest->nSector], 0, nExtra);
      pTest->nSector = nSector;
    }

    /* Update or create the required BtTestSector objects */
    for(iSector=(iOff / BTTEST_SECTOR_SIZE); iSector<nSector; iSector++){
      BtTestSector *pSector;
      int nCopy;
      int iSectorOff;

      if( pTest->apSector[iSector]==0 ){
        int nByte = sizeof(BtTestSector);
        pTest->apSector[iSector] = (BtTestSector*)ckalloc(nByte);
        memset(pTest->apSector[iSector], 0, nByte);
      }
      pSector = pTest->apSector[iSector];

      iSectorOff = (iWrite % BTTEST_SECTOR_SIZE);
      nCopy = MIN(nRem, (BTTEST_SECTOR_SIZE - iSectorOff));
      memcpy(&pSector->aBuf[iSectorOff], aRem, nCopy);

      nRem -= nCopy; 
      aRem += nCopy;
      iWrite += nCopy;

      if( pSector->iLastUnsynced==0 ){
        pSector->iFirstUnsynced = iSectorOff;
        pSector->iLastUnsynced = nCopy;
      }else{
        pSector->iFirstUnsynced = MIN(pSector->iFirstUnsynced, iSectorOff);
        pSector->iLastUnsynced = MAX(pSector->iLastUnsynced, iSectorOff+nCopy);
      }
    }
    rc = SQLITE4_OK;
  }else if( testInjectIoerr(pTest->pTestEnv) ){
    rc = SQLITE4_IOERR_WRITE;
  }else{
    rc = pTestEnv->pEnv->xWrite(pTest->pFile, iOff, pData, nData);
  }
  return rc;
}

static int testBtOsTruncate(
  bt_file *pFile,                 /* File to write to */
  i64 nSize                       /* Size to truncate file to */
){
  BtTestFile *pTestFile = (BtTestFile*)pFile;
  bt_env *pEnv = pTestFile->pTestEnv->pEnv;
  int rc;

  if( testInjectIoerr(pTestFile->pTestEnv) ){
    rc = SQLITE4_IOERR_TRUNCATE;
  }else{
    rc = pEnv->xTruncate(pTestFile->pFile, nSize);
  }
  return rc;
}

static int testBtOsRead(
  bt_file *pFile,                 /* File to read from */
  i64 iOff,                       /* Offset to read from */
  void *pData,                    /* Read data into this buffer */
  int nData                       /* Bytes of data to read */
){
  BtTestFile *pTestFile = (BtTestFile*)pFile;
  bt_env *pEnv = pTestFile->pTestEnv->pEnv;
  int rc;

  if( testInjectIoerr(pTestFile->pTestEnv) ){
    rc = SQLITE4_IOERR_READ;
  }else{
    int iSector;
    u8 *aOut = (u8*)pData;
    int nRem = nData;
    int iOffset = (iOff % BTTEST_SECTOR_SIZE);

    rc = pEnv->xRead(pTestFile->pFile, iOff, pData, nData);

    /* Read any buffered data */
    for(iSector=(iOff / BTTEST_SECTOR_SIZE); nRem>0; iSector++){
      if( (pTestFile->nSector > iSector) && pTestFile->apSector[iSector] ){
        BtTestSector *pSector = pTestFile->apSector[iSector];
        int iStart = MAX(pSector->iFirstUnsynced, iOffset);
        int nCopy = MIN(BTTEST_SECTOR_SIZE - iStart, nRem + iOffset - iStart);

        if( nCopy>0 ){
          memcpy(&aOut[iOffset-iStart], &pSector->aBuf[iStart], nCopy);
        }
      }
      nRem -= (BTTEST_SECTOR_SIZE - iOffset);
      aOut += (BTTEST_SECTOR_SIZE - iOffset);
      iOffset = 0;
    }
  }

  return rc;
}

static int testBtOsSync(bt_file *pFile){
  BtTestFile *pTestFile = (BtTestFile*)pFile;
  bt_env *pEnv = pTestFile->pTestEnv->pEnv;
  int rc;

  /* If required, simulate the effects of a crash on the file-system */
  testCrashOrFlush(pTestFile);

  if( testInjectIoerr(pTestFile->pTestEnv) ){
    rc = SQLITE4_IOERR_FSYNC;
  }else{
    rc = pEnv->xSync(pTestFile->pFile);
  }
  return rc;
}

static int testBtOsSectorSize(bt_file *pFile){
  BtTestFile *pTestFile = (BtTestFile*)pFile;
  bt_env *pEnv = pTestFile->pTestEnv->pEnv;
  int res;

  res = pEnv->xSectorSize(pTestFile->pFile);
  return res;
}

static int testBtOsFullpath(
  sqlite4_env *pSqlEnv,
  bt_env *pEnv,
  const char *zName,
  char **pzOut
){
  BtTestEnv *p = (BtTestEnv*)pEnv;
  int rc;

  if( testInjectIoerr(p) ){
    rc = SQLITE4_IOERR;
  }else{
    rc = p->pEnv->xFullpath(pSqlEnv, p->pEnv, zName, pzOut);
  }
  return rc;
}

static int testBtOsUnlink(
  sqlite4_env *pSqlEnv, 
  bt_env *pEnv, 
  const char *zFile
){
  BtTestEnv *p = (BtTestEnv*)pEnv;
  int rc;

  if( testInjectIoerr(p) ){
    rc = SQLITE4_IOERR_DIR_FSYNC;
  }else{
    rc = p->pEnv->xUnlink(pSqlEnv, p->pEnv, zFile);
  }
  return rc;
}

int testBtOsLock(bt_file *pFile, int iLock, int eType){
  BtTestFile *pTestFile = (BtTestFile*)pFile;
  bt_env *pEnv = pTestFile->pTestEnv->pEnv;
  int rc;

  if( testInjectIoerr(pTestFile->pTestEnv) ){
    rc = SQLITE4_IOERR_LOCK;
  }else{
    rc = pEnv->xLock(pTestFile->pFile, iLock, eType);
  }
  return rc;
}

int testBtOsTestLock(bt_file *pFile, int iLock, int nLock, int eType){
  BtTestFile *pTestFile = (BtTestFile*)pFile;
  bt_env *pEnv = pTestFile->pTestEnv->pEnv;
  int rc;

  if( testInjectIoerr(pTestFile->pTestEnv) ){
    rc = SQLITE4_IOERR;
  }else{
    rc = pEnv->xTestLock(pTestFile->pFile, iLock, nLock, eType);
  }
  return rc;
}

int testBtOsShmMap(bt_file *pFile, int iChunk, int sz, void **ppShm){
  BtTestFile *pTestFile = (BtTestFile*)pFile;
  bt_env *pEnv = pTestFile->pTestEnv->pEnv;
  int rc;

  if( testInjectIoerr(pTestFile->pTestEnv) ){
    rc = SQLITE4_IOERR_SHMMAP;
  }else{
    rc = pEnv->xShmMap(pTestFile->pFile, iChunk, sz, ppShm);
  }
  return rc;
}

void testBtOsShmBarrier(bt_file *pFile){
  BtTestFile *pTestFile = (BtTestFile*)pFile;
  bt_env *pEnv = pTestFile->pTestEnv->pEnv;

  pEnv->xShmBarrier(pTestFile->pFile);
}

int testBtOsShmUnmap(bt_file *pFile, int bDelete){
  BtTestFile *pTestFile = (BtTestFile*)pFile;
  bt_env *pEnv = pTestFile->pTestEnv->pEnv;
  int rc;

  rc = pEnv->xShmUnmap(pTestFile->pFile, bDelete);
  return rc;
}

static int testBtOsClose(bt_file *pFile){
  BtTestFile *pTestFile = (BtTestFile*)pFile;
  BtTestEnv *pTestEnv = pTestFile->pTestEnv;
  BtTestFile **pp;
  int rc;

  for(pp=&pTestEnv->pTestFile; *pp!=pTestFile; pp = &(*pp)->pNext);
  *pp = pTestFile->pNext;

  rc = pTestEnv->pEnv->xClose(pTestFile->pFile);
  ckfree(pTestFile);
  return rc;
}

/*
** Destructor for object created by tcl [btenv] command.
*/
static void test_btenv_del(void *ctx){
  BtTestEnv *p = (BtTestEnv*)ctx;
  ckfree(p);
}

/*
** Tcl command: BTENV method ...
*/
static int test_btenv_cmd(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  BtTestEnv *p = (BtTestEnv*)clientData;

  enum BtenvCmdSymbol {
    BTC_ATTACH,
    BTC_DELETE,
    BTC_IOERR,
    BTC_CRASH,
  };
  struct BtenvCmd {
    const char *zOpt;
    int eOpt;
    int nArg;
    const char *zErr;
  } aCmd[] = {
    { "attach", BTC_ATTACH, 3, "DBCMD" },
    { "delete", BTC_DELETE, 2, "" },
    { "ioerr",  BTC_IOERR,  4, "COUNT PERSISTENT" },
    { "crash",  BTC_CRASH,  4, "COUNT WAL" },
    { 0, 0 }
  };
  int rc = TCL_OK;
  int iOpt;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "sub-command ...");
    return TCL_ERROR;
  }
  rc = Tcl_GetIndexFromObjStruct(
      interp, objv[1], aCmd, sizeof(aCmd[0]), "sub-command", 0, &iOpt
  );
  if( rc!=TCL_OK ) return rc;

  if( objc!=aCmd[iOpt].nArg ){
    Tcl_WrongNumArgs(interp, 2, objv, aCmd[iOpt].zErr);
    return TCL_ERROR;
  }

  switch( aCmd[iOpt].eOpt ){
    case BTC_ATTACH: {
      if( p->pEnv ){
        Tcl_AppendResult(interp, "object has already been attached to db", 0);
        rc = TCL_ERROR;
      }else{
        sqlite4 *db = 0;
        rc = sqlite4TestDbHandle(interp, objv[2], &db);
        if( rc==TCL_OK ){
          void *pArg = (void*)(&p->pEnv);
          rc = sqlite4_kvstore_control(db, "main", BT_CONTROL_GETVFS, pArg);
          if( rc==SQLITE4_NOTFOUND ){
            Tcl_AppendResult(interp, "not a bt database", 0);
            rc = TCL_ERROR;
          }else{
            sqlite4_kvstore_control(db, "main", BT_CONTROL_SETVFS, (void*)p);
          }
        }
      }
      break;
    }

    case BTC_DELETE: {
      Tcl_DeleteCommand(interp, Tcl_GetString(objv[0]));
      break;
    }

    case BTC_IOERR: {
      int ret = p->nIoerrInjected;
      if( Tcl_GetIntFromObj(interp, objv[2], &p->nIoerrCnt)
       || Tcl_GetBooleanFromObj(interp, objv[3], &p->bIoerrPersist)
      ){
        rc = TCL_ERROR;
      }else{
        Tcl_SetObjResult(interp, Tcl_NewIntObj(ret));
      }

      break;
    }

    case BTC_CRASH: {
      if( Tcl_GetIntFromObj(interp, objv[2], &p->nCrashCnt)
       || Tcl_GetBooleanFromObj(interp, objv[3], &p->bWalCrash)
      ){
        rc = TCL_ERROR;
      }else{
        Tcl_SetObjResult(interp, Tcl_NewBooleanObj(p->bCrashed));
        p->bCrashed = 0;
      }
      break;
    }

    default:
      assert( 0 );
      break;
  }

  return rc;
}

/*
** Tcl command: btenv NAME
*/
static int test_btenv(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  BtTestEnv *p;
  const char *zName;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "NAME");
    return TCL_ERROR;
  }
  zName = Tcl_GetString(objv[1]);

  p = ckalloc(sizeof(BtTestEnv));
  memset(p, 0, sizeof(BtTestEnv));

  p->base.xFullpath = testBtOsFullpath;
  p->base.xOpen = testBtOsOpen;
  p->base.xSize = testBtOsSize;
  p->base.xRead = testBtOsRead;
  p->base.xWrite = testBtOsWrite;
  p->base.xTruncate = testBtOsTruncate;
  p->base.xSync = testBtOsSync;
  p->base.xSectorSize = testBtOsSectorSize;
  p->base.xClose = testBtOsClose;
  p->base.xUnlink = testBtOsUnlink;
  p->base.xLock = testBtOsLock;
  p->base.xTestLock = testBtOsTestLock;
  p->base.xShmMap = testBtOsShmMap;
  p->base.xShmBarrier = testBtOsShmBarrier;
  p->base.xShmUnmap = testBtOsShmUnmap;

  Tcl_CreateObjCommand(interp, zName, test_btenv_cmd, (void*)p, test_btenv_del);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(zName, -1));
  return TCL_OK;
}

int SqlitetestBt_Init(Tcl_Interp *interp){
  struct SyscallCmd {
    const char *zName;
    Tcl_ObjCmdProc *xCmd;
  } aCmd[] = {
    { "btenv",                  test_btenv },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xCmd, 0, 0);
  }
  return TCL_OK;
}


