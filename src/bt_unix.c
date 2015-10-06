/*
** 2011-12-03
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
** Unix-specific run-time environment implementation for bt.
*/
#if defined(__GNUC__) || defined(__TINYC__)
/* workaround for ftruncate() visibility on gcc. */
# ifndef _XOPEN_SOURCE
#  define _XOPEN_SOURCE 500
# endif
#endif

#include <unistd.h>
#include <sys/types.h>

#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>

#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <ctype.h>

#include <unistd.h>
#include <errno.h>

#include <sys/mman.h>
#include "btInt.h"

/* There is no fdatasync() call on Android */
#ifdef __ANDROID__
# define fdatasync(x) fsync(x)
#endif

/*
** An open file is an instance of the following object
*/
typedef struct BtPosixFile BtPosixFile;
struct BtPosixFile {
  sqlite4_env *pSqlEnv;
  bt_env *pEnv;                   /* The run-time environment */
  const char *zName;              /* Full path to file */
  int fd;                         /* The open file descriptor */
  int shmfd;                      /* Shared memory file-descriptor */
  int nShm;                       /* Number of entries in array apShm[] */
  void **apShm;                   /* Array of 32K shared memory segments */
};

static char *btPosixShmFile(BtPosixFile *p){
  char *zShm;
  int nName = strlen(p->zName);
  zShm = (char*)sqlite4_malloc(p->pSqlEnv, nName+4+1);
  if( zShm ){
    memcpy(zShm, p->zName, nName);
    memcpy(&zShm[nName], "-shm", 5);
  }
  return zShm;
}

static int btPosixOsOpen(
  sqlite4_env *pSqlEnv,
  bt_env *pEnv,
  const char *zFile,
  int flags,
  bt_file **ppFile
){
  int rc = SQLITE4_OK;
  BtPosixFile *p;

  p = (BtPosixFile*)sqlite4_malloc(pSqlEnv, sizeof(BtPosixFile));
  if( p==0 ){
    rc = btErrorBkpt(SQLITE4_NOMEM);
  }else{
    int bReadonly = (flags & BT_OPEN_READONLY);
    int oflags = (bReadonly ? O_RDONLY : (O_RDWR|O_CREAT));
    memset(p, 0, sizeof(BtPosixFile));
    p->zName = zFile;
    p->pEnv = pEnv;
    p->pSqlEnv = pSqlEnv;
    p->fd = open(zFile, oflags, 0644);
    if( p->fd<0 ){
      sqlite4_free(pSqlEnv, p);
      p = 0;
      rc = btErrorBkpt(SQLITE4_IOERR);
    }
  }

  *ppFile = (bt_file*)p;
  return rc;
}

static int btPosixOsSize(bt_file *pFile, i64 *pnByte){
  int rc = SQLITE4_OK;
  BtPosixFile *p = (BtPosixFile *)pFile;
  struct stat sBuf;

  if( fstat(p->fd, &sBuf)!=0 ){
    rc = SQLITE4_IOERR_FSTAT;
  }else{
    *pnByte = sBuf.st_size;
  }

  return rc;
}

static int btPosixOsWrite(
  bt_file *pFile,                 /* File to write to */
  i64 iOff,                       /* Offset to write to */
  void *pData,                    /* Write data from this buffer */
  int nData                       /* Bytes of data to write */
){
  int rc = SQLITE4_OK;
  BtPosixFile *p = (BtPosixFile *)pFile;
  off_t offset;

  offset = lseek(p->fd, (off_t)iOff, SEEK_SET);
  if( offset!=iOff ){
    rc = btErrorBkpt(SQLITE4_IOERR);
  }else{
    ssize_t prc = write(p->fd, pData, (size_t)nData);
    if( prc<0 ) rc = btErrorBkpt(SQLITE4_IOERR);
  }

  return rc;
}

static int btPosixOsTruncate(
  bt_file *pFile,                /* File to write to */
  i64 nSize                      /* Size to truncate file to */
){
  BtPosixFile *p = (BtPosixFile *)pFile;
  int rc = SQLITE4_OK;                /* Return code */
  int prc;                        /* Posix Return Code */
  struct stat sStat;              /* Result of fstat() invocation */
  
  prc = fstat(p->fd, &sStat);
  if( prc==0 && sStat.st_size>nSize ){
    prc = ftruncate(p->fd, (off_t)nSize);
  }
  if( prc<0 ) rc = btErrorBkpt(SQLITE4_IOERR);

  return rc;
}

static int btPosixOsRead(
  bt_file *pFile,                /* File to read from */
  i64 iOff,                      /* Offset to read from */
  void *pData,                    /* Read data into this buffer */
  int nData                       /* Bytes of data to read */
){
  int rc = SQLITE4_OK;
  BtPosixFile *p = (BtPosixFile *)pFile;
  off_t offset;

  offset = lseek(p->fd, (off_t)iOff, SEEK_SET);
  if( offset!=iOff ){
    rc = btErrorBkpt(SQLITE4_IOERR);
  }else{
    ssize_t prc = read(p->fd, pData, (size_t)nData);
    if( prc<0 ){ 
      rc = btErrorBkpt(SQLITE4_IOERR);
    }else if( prc<nData ){
      memset(&((u8 *)pData)[prc], 0, nData - prc);
    }
  }

  return rc;
}

static int btPosixOsSync(bt_file *pFile){
  int rc = SQLITE4_OK;
#ifndef SQLITE_NO_SYNC
  BtPosixFile *p = (BtPosixFile *)pFile;
  int prc = 0;

#if 0
  if( p->pMap ){
    prc = msync(p->pMap, p->nMap, MS_SYNC);
  }
#endif
  prc = fdatasync(p->fd);
  if( prc<0 ) rc = btErrorBkpt(SQLITE4_IOERR);
#else
  (void)pFile;
#endif

  return rc;
}

static int btPosixOsSectorSize(bt_file *pFile){
  return 512;
}

static int btPosixOsFullpath(
  sqlite4_env *pSqlEnv,
  bt_env *pEnv,
  const char *zName,
  char **pzOut
){
  int rc = SQLITE4_OK;
  char *zOut = 0;
  char *zCwd = 0;
  int nCwd = 0;

  if( zName[0]!='/' ){
    int nTmp = 512;
    char *zTmp = (char*)sqlite4_malloc(pSqlEnv, nTmp);
    while( zTmp ){
      zCwd = getcwd(zTmp, nTmp);
      if( zCwd || errno!=ERANGE ) break;
      sqlite4_free(pSqlEnv, zTmp);
      nTmp = nTmp*2;
      zTmp = sqlite4_malloc(pSqlEnv, nTmp);
    }
    if( zTmp==0 ){
      rc = btErrorBkpt(SQLITE4_NOMEM);
    }else if( zCwd==0 ){
      rc = btErrorBkpt(SQLITE4_IOERR);
    }else{
      assert( zCwd==zTmp );
      nCwd = strlen(zCwd);
    }
  }

  if( rc==SQLITE4_OK ){
    int nReq = nCwd + 1 + strlen(zName) + 1 + 4;
    zOut = sqlite4_malloc(pSqlEnv, nReq);
    if( zOut ){
      int nName = strlen(zName);
      if( nCwd ){
        memcpy(zOut, zCwd, nCwd);
        zOut[nCwd] = '/';
        memcpy(&zOut[nCwd+1], zName, nName+1);
      }else{
        memcpy(zOut, zName, nName+1);
      }
    }else{
      rc = btErrorBkpt(SQLITE4_NOMEM);
    }
  }

  sqlite4_free(pSqlEnv, zCwd);
  *pzOut = zOut;
  return rc;
}

static int btPosixOsUnlink(sqlite4_env *pEnv, bt_env *pVfs, const char *zFile){
  int prc = unlink(zFile);
  return prc ? btErrorBkpt(SQLITE4_IOERR) : SQLITE4_OK;
}

#define btPosixLockToByte(iLock) (100 + (iLock))

int btPosixOsLock(bt_file *pFile, int iLock, int eType){
  int rc = SQLITE4_OK;
  BtPosixFile *p = (BtPosixFile *)pFile;
  static const short aType[3] = { F_UNLCK, F_RDLCK, F_WRLCK };
  struct flock lock;

  assert( aType[BT_LOCK_UNLOCK]==F_UNLCK );
  assert( aType[BT_LOCK_SHARED]==F_RDLCK );
  assert( aType[BT_LOCK_EXCL]==F_WRLCK );
  assert( eType>=0 && eType<(sizeof(aType)/sizeof(aType[0])) );
  assert( iLock>=0 && iLock<=32 );

  memset(&lock, 0, sizeof(lock));
  lock.l_whence = SEEK_SET;
  lock.l_len = 1;
  lock.l_type = aType[eType];
  lock.l_start = btPosixLockToByte(iLock);

  if( fcntl(p->fd, F_SETLK, &lock) ){
    int e = errno;
    if( e==EACCES || e==EAGAIN ){
      rc = SQLITE4_BUSY;
    }else{
      rc = btErrorBkpt(SQLITE4_IOERR);
    }
  }

  return rc;
}

int btPosixOsTestLock(bt_file *pFile, int iLock, int nLock, int eType){
  int rc = SQLITE4_OK;
  BtPosixFile *p = (BtPosixFile *)pFile;
  static const short aType[3] = { 0, F_RDLCK, F_WRLCK };
  struct flock lock;

  assert( eType==BT_LOCK_SHARED || eType==BT_LOCK_EXCL );
  assert( aType[BT_LOCK_SHARED]==F_RDLCK );
  assert( aType[BT_LOCK_EXCL]==F_WRLCK );
  assert( eType>=0 && eType<(sizeof(aType)/sizeof(aType[0])) );
  assert( iLock>=0 && iLock<=32 );

  memset(&lock, 0, sizeof(lock));
  lock.l_whence = SEEK_SET;
  lock.l_len = nLock;
  lock.l_type = aType[eType];
  lock.l_start = btPosixLockToByte(iLock);

  if( fcntl(p->fd, F_GETLK, &lock) ){
    rc = btErrorBkpt(SQLITE4_IOERR);
  }else if( lock.l_type!=F_UNLCK ){
    rc = SQLITE4_BUSY;
  }

  return rc;
}

int btPosixOsShmMap(bt_file *pFile, int iChunk, int sz, void **ppShm){
  BtPosixFile *p = (BtPosixFile *)pFile;

  /* If the shared-memory file has not been opened, open it now. */
  if( p->shmfd<=0 ){
    char *zShm = btPosixShmFile(p);
    if( !zShm ) return btErrorBkpt(SQLITE4_NOMEM);
    p->shmfd = open(zShm, O_RDWR|O_CREAT, 0644);
    sqlite4_free(p->pSqlEnv, zShm);
    if( p->shmfd<0 ){ 
      return btErrorBkpt(SQLITE4_IOERR);
    }
  }

  if( ppShm==0 ){
    assert( p->nShm==0 );
    if( p->nShm ) return btErrorBkpt(SQLITE4_MISUSE);
    if( ftruncate(p->shmfd, 0) ){
      return btErrorBkpt(SQLITE4_IOERR);
    }
  }else{

    *ppShm = 0;
    assert( sz==BT_SHM_CHUNK_SIZE );
    if( iChunk>=p->nShm ){
      int i;
      void **apNew;
      int nNew = iChunk+1;
      off_t nReq = nNew * BT_SHM_CHUNK_SIZE;
      struct stat sStat;

      /* If the shared-memory file is not large enough to contain the 
       ** requested chunk, cause it to grow.  */
      if( fstat(p->shmfd, &sStat) ){
        return btErrorBkpt(SQLITE4_IOERR);
      }
      if( sStat.st_size<nReq ){
        if( ftruncate(p->shmfd, nReq) ){
          return btErrorBkpt(SQLITE4_IOERR);
        }
      }

      apNew = (void**)sqlite4_realloc(p->pSqlEnv, p->apShm, sizeof(void*)*nNew);
      if( !apNew ) return btErrorBkpt(SQLITE4_NOMEM);
      for(i=p->nShm; i<nNew; i++){
        apNew[i] = 0;
      }
      p->apShm = apNew;
      p->nShm = nNew;
    }

    if( p->apShm[iChunk]==0 ){
      p->apShm[iChunk] = mmap(0, BT_SHM_CHUNK_SIZE, 
          PROT_READ|PROT_WRITE, MAP_SHARED, p->shmfd, iChunk*BT_SHM_CHUNK_SIZE
      );
      if( p->apShm[iChunk]==0 ) return btErrorBkpt(SQLITE4_IOERR);
    }

    *ppShm = p->apShm[iChunk];
  }
  return SQLITE4_OK;
}

void btPosixOsShmBarrier(bt_file *pFile){
}

int btPosixOsShmUnmap(bt_file *pFile, int bDelete){
  BtPosixFile *p = (BtPosixFile *)pFile;
  if( p->shmfd>0 ){
    int i;
    for(i=0; i<p->nShm; i++){
      if( p->apShm[i] ){
        munmap(p->apShm[i], BT_SHM_CHUNK_SIZE);
        p->apShm[i] = 0;
      }
    }
    close(p->shmfd);
    p->shmfd = 0;
    if( bDelete ){
      char *zShm = btPosixShmFile(p);
      if( zShm ) unlink(zShm);
      sqlite4_free(p->pSqlEnv, zShm);
    }
  }
  return SQLITE4_OK;
}


static int btPosixOsClose(bt_file *pFile){
   BtPosixFile *p = (BtPosixFile *)pFile;
   btPosixOsShmUnmap(pFile, 0);
   close(p->fd);
   sqlite4_free(p->pSqlEnv, p->apShm);
   sqlite4_free(p->pSqlEnv, p);
   return SQLITE4_OK;
}

bt_env *sqlite4BtEnvDefault(void){
  static bt_env posix_env = {
    0,                            /* pVfsCtx */
    btPosixOsFullpath,            /* xFullpath */
    btPosixOsOpen,                /* xOpen */
    btPosixOsSize,                /* xSize */
    btPosixOsRead,                /* xRead */
    btPosixOsWrite,               /* xWrite */
    btPosixOsTruncate,            /* xTruncate */
    btPosixOsSync,                /* xSync */
    btPosixOsSectorSize,          /* xSectorSize */
    btPosixOsClose,               /* xClose */
    btPosixOsUnlink,              /* xUnlink */
    btPosixOsLock,                /* xLock */
    btPosixOsTestLock,            /* xTestLock */
    btPosixOsShmMap,              /* xShmMap */
    btPosixOsShmBarrier,          /* xShmBarrier */
    btPosixOsShmUnmap             /* xShmUnmap */
  };
  return &posix_env;
}
