/*
** 2013 October 19
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
#include <stddef.h>

/* Magic values identifying WAL file header */
#define BT_WAL_MAGIC   0xBEE1CA62
#define BT_WAL_VERSION 0x00000001

/* Wrap the log around if there is a block of this many free frames at
** the start of the file.  */
#define BT_NWRAPLOG    100

typedef struct BtCkptHdr BtCkptHdr;
typedef struct BtDbHdrCksum BtDbHdrCksum;
typedef struct BtFrameHdr BtFrameHdr;
typedef struct BtShm BtShm;
typedef struct BtShmHdr BtShmHdr;
typedef struct BtWalHdr BtWalHdr;

/*
** WAL file header. All u32 fields are stored in big-endian order on
** disk. A single WAL file may contain two of these headers - one at
** byte offset 0 and the other at offset <sector-size> (the start of
** the second disk sector in the file, according to the xSectorSize
** VFS method).
*/
struct BtWalHdr {
  u32 iMagic;                     /* Magic number (BT_WAL_MAGIC) */
  u32 iVersion;                   /* File format version */
  u32 iCnt;                       /* 0, 1 or 2 */
  u32 nSector;                    /* Sector size when header written */

  u32 nPgsz;                      /* Database page size in bytes */
  u32 nPg;                        /* Database size in pages at last commit */

  u32 padding;

  u32 iSalt1;                     /* Initial frame cksum-0 value */
  u32 iSalt2;                     /* Initial frame cksum-1 value */
  u32 iFirstFrame;                /* First frame of log (numbered from 1) */

  u32 aCksum[2];                  /* Checksum of all prior fields */
};

/*
** WAL Frame header. All fields are stored in big-endian order.
**
** pgno:
**   Page number for the frame.
**
** iNext:
**   Next frame in logical log.
**   
** nPg:
**   For non-commit frames, zero. For commit frames, the size of the
**   database file in pages at the time of commit (always at least 1).
*/
struct BtFrameHdr {
  u32 pgno;                       /* Page number of this frame */
  u32 iNext;                      /* Next frame pointer */
  u32 nPg;                        /* For commit frames, size of db file */
  u32 aCksum[2];                  /* Frame checksum */
};

#define BT_FRAME_COMMIT 0x80000000

/*
** A database header with checksum fields.
*/
struct BtDbHdrCksum {
  BtDbHdr hdr;
  u32 aCksum[2];
};


/*
** Shared memory header. Shared memory begins with two copies of
** this structure. All fields are stored in machine byte-order.
*/
struct BtShmHdr {
  u32 aLog[6];                    /* First/last frames for each log region */
  int nSector;                    /* Sector size assumed for WAL file */
  int iHashSide;                  /* Hash table side for region (c) of log */
  u32 aFrameCksum[2];             /* Checksum of previous frame */
  u32 iNextFrame;                 /* Location to write next log frame to */
  BtDbHdr dbhdr;                  /* Cached db-header values */

  u32 padding;
  u32 aCksum[2];                  /* Object checksum */
};

/*
** A single instance of this structure follows the two BtShmHdr structures 
** in shared memory.
**
** iWalHdr:
**   This field encodes two pieces of information: the location of the
**   current log file header (slot 0 or slot 1) and the value of the
**   BtWalHdr.iCnt field within it (0, 1 or 2). Encoded as:
**
**           ((iSlot << 2) + iCnt)
**
**   To decode, use:
**
**           iSlot = (ckpthdr.iWalHdr >> 2);
**           iCnt = (ckpthdr.iWalHdr & 0x03);& 0x03);
*/
struct BtCkptHdr {
  u32 iFirstRead;                 /* First uncheckpointed frame */
  u32 iWalHdr;                    /* Description of current wal header */
  u32 iFirstRecover;              /* First recovery frame */
};

struct BtShm {
  BtShmHdr hdr1;
  BtShmHdr hdr2;
  BtCkptHdr ckpt;
  BtReadSlot aReadlock[BT_NREADER];
};

/* 
** Log handle used by bt_pager.c to access functionality implemented by
** this module. 
*/
struct BtLog {
  BtLock *pLock;                  /* Lock object associated with this log */
  bt_file *pFd;                   /* File handle open on WAL file */
  BtShmHdr snapshot;              /* Current snapshot of shm-header */
  int nShm;                       /* Size of apShm[] array */
  u8 **apShm;                     /* Array of mapped shared-memory blocks */
  int nWrapLog;                   /* Wrap if this many free frames at start */
};

typedef u16 ht_slot;

/*
** Number of entries in each hash table bar the first. 
*/
#define HASHTABLE_NFRAME (BT_SHM_CHUNK_SIZE/(sizeof(u32) + 4*sizeof(ht_slot))) 

/* 
** Number of entries in first hash table. The first hash table is 
** smaller than the others as space for the BtShm structure is carved
** from the start of it. The two hash-slot arrays remain the same size, 
** but the number of entries in the page-number array is reduced to the
** value defined below.
*/
#define HASHTABLE_NFRAME_ONE (HASHTABLE_NFRAME - (sizeof(BtShm)/sizeof(u32)))

/*
** Number of slots in a hash table.
*/
#define HASHTABLE_NSLOT (HASHTABLE_NFRAME * 2)

#define HASHTABLE_OFFSET_1 (HASHTABLE_NFRAME * sizeof(u32))
#define HASHTABLE_OFFSET_2 (HASHTABLE_OFFSET_1+HASHTABLE_NSLOT*sizeof(ht_slot))

/*
** Used to calculate hash keys.
*/
#define HASHTABLE_KEY_MUL 383


/*
** The argument to this macro must be of type u32. On a little-endian
** architecture, it returns the u32 value that results from interpreting
** the 4 bytes as a big-endian value. On a big-endian architecture, it
** returns the value that would be produced by intepreting the 4 bytes
** of the input value as a little-endian integer.
*/
#define BYTESWAP32(x) ( \
  (((x)&0x000000FF)<<24) + (((x)&0x0000FF00)<<8)  \
  + (((x)&0x00FF0000)>>8)  + (((x)&0xFF000000)>>24) \
)

/* True if this is a little-endian build */
static const int btOne = 1;
#define BTLOG_LITTLE_ENDIAN (*(u8 *)(&btOne))

/*
** Generate or extend an 8 byte checksum based on the data in
** array aByte[] and the initial values of aIn[0] and aIn[1] (or
** initial values of 0 and 0 if aIn==NULL).
**
** The checksum is written back into aOut[] before returning.
**
** nByte must be a positive multiple of 8.
*/
static void btLogChecksum(
  int nativeCksum,                /* True for native byte-order, else false */
  u8 *a,                          /* Content to be checksummed */
  int nByte,                      /* Bytes of content in a[]. */
  const u32 *aIn,                 /* Initial checksum value input */
  u32 *aOut                       /* OUT: Final checksum value output */
){
  u32 s1, s2;
  u32 *aData = (u32 *)a;
  u32 *aEnd = (u32 *)&a[nByte];

  if( aIn ){
    s1 = aIn[0];
    s2 = aIn[1];
  }else{
    s1 = s2 = 0;
  }

  assert( nByte>=8 );
  assert( (nByte&0x00000007)==0 );

  if( nativeCksum ){
    do {
      s1 += *aData++ + s2;
      s2 += *aData++ + s1;
    }while( aData<aEnd );
  }else{
    do {
      s1 += BYTESWAP32(aData[0]) + s2;
      s2 += BYTESWAP32(aData[1]) + s1;
      aData += 2;
    }while( aData<aEnd );
  }

  aOut[0] = s1;
  aOut[1] = s2;
}

static void btLogChecksum32(
  int nativeCksum,                /* True for native byte-order, else false */
  u8 *a,                          /* Content to be checksummed */
  int nByte,                      /* Bytes of content in a[]. */
  const u32 *aIn,                 /* Initial checksum value input */
  u32 *aOut                       /* OUT: Final checksum value output */
){
  assert( nByte>=8 );
  if( nByte&0x00000007 ){
    btLogChecksum(nativeCksum, a, 8, aIn, aOut);
    btLogChecksum(nativeCksum, &a[4], nByte-4, aOut, aOut);
  }else{
    btLogChecksum(nativeCksum, a, nByte, aIn, aOut);
  }
}

#define BT_ALLOC_DEBUG   0
#define BT_PAGE_DEBUG    0
#define BT_VAL_DEBUG     0
#define BT_HDR_DEBUG     0
#define BT_RECOVER_DEBUG 0

static void btDebugTopology(BtLock *pLock, char *zStr, int iSide, u32 *aLog){
#if BT_PAGE_DEBUG
  fprintf(stderr, "%d:%s: (side=%d) %d..%d  %d..%d  %d..%d\n", 
      pLock->iDebugId, zStr, iSide,
      (int)aLog[0], (int)aLog[1], (int)aLog[2], 
      (int)aLog[3], (int)aLog[4], (int)aLog[5]
  );
  fflush(stderr);
#endif
}

static void btDebugDbhdr(BtLock *pLock, const char *zStr, BtDbHdr *pHdr){
#if BT_HDR_DEBUG
  static int nCall = 0;
  fprintf(stderr, "%d:%d: %s db-header "
      "(pgsz=%d nPg=%d iRoot=%d iCookie=%d iFreePg=%d iFreeBlk=%d)\n",
      pLock->iDebugId, nCall++,
      zStr, (int)pHdr->pgsz, (int)pHdr->nPg, (int)pHdr->iRoot, 
      (int)pHdr->iCookie, (int)pHdr->iFreePg, (int)pHdr->iFreeBlk
  );
  fflush(stderr);
#endif
}

static void btDebugRecoverFrame(BtLock *pLock, u32 iFrame, u32 pgno){
#if BT_RECOVER_DEBUG
  static int nCall = 0;
  fprintf(stderr, "%d:%d: recovered page %d from frame %d\n",
      pLock->iDebugId, nCall++, (int)pgno, (int)iFrame
  );
  fflush(stderr);
#endif
}

#ifndef NDEBUG
void sqlite4BtDebugReadlock(BtLock *pLock, u32 iFirst, u32 iLast){
#if BT_PAGE_DEBUG
  static int nCall = 0;
  fprintf(stderr, "%d:%d: readlock=(%d..%d)\n",
      pLock->iDebugId, nCall++, (int)iFirst, (int)iLast
  );
  fflush(stderr);
#endif
}
#endif

#ifndef NDEBUG
void sqlite4BtDebugPageAlloc(BtLock *pLock, const char *zStr, u32 pgno){
#if BT_ALLOC_DEBUG
  static int nCall = 0;
  fprintf(stderr, "%d:%d: allocate page %d from %s\n", 
      pLock->iDebugId, nCall++, pgno, zStr
  );
  fflush(stderr);
#endif
}
#endif

#ifndef NDEBUG
void sqlite4BtDebugPageFree(
  BtLock *pLock, int bBlock, const char *zStr, u32 pgno
){
#if BT_ALLOC_DEBUG
  static int nCall = 0;
  fprintf(stderr, "%d:%d: trim %s %d (%s)\n", 
      pLock->iDebugId, nCall++, bBlock ? "block" : "page", pgno, zStr
  );
  fflush(stderr);
  assert( pgno<1000000 );
#endif
}
#endif


#ifndef NDEBUG
static void btDebugCheckSnapshot(BtShmHdr *pHdr){
  u32 *aLog = pHdr->aLog;
  assert( pHdr->iNextFrame!=1 ||
      (aLog[0]==0 && aLog[1]==0 && aLog[2]==0 && aLog[3]==0)
  );

  /* Check that the three log regions do not overlap */
  assert( aLog[0]==0 || aLog[2]==0 || aLog[3]<aLog[0] || aLog[2]>aLog[1] );
  assert( aLog[0]==0 || aLog[4]==0 || aLog[5]<aLog[0] || aLog[4]>aLog[1] );
  assert( aLog[2]==0 || aLog[4]==0 || aLog[5]<aLog[2] || aLog[4]>aLog[3] );

  /* Check that the "next frame" does not overlap with any region */
  assert( aLog[0]==0 || pHdr->iNextFrame<aLog[0] ||  pHdr->iNextFrame>aLog[1] );
  assert( aLog[2]==0 || pHdr->iNextFrame<aLog[2] ||  pHdr->iNextFrame>aLog[3] );
  assert( aLog[4]==0 || pHdr->iNextFrame<aLog[4] ||  pHdr->iNextFrame>aLog[5] );
}
#else
#define btDebugCheckSnapshot(x)
#endif

#ifndef NDEBUG
static void btDebugLogSafepoint(BtLock *pLock, u32 iSafe){
#if BT_PAGE_DEBUG
  static int nCall = 0;
  fprintf(stderr, "%d:%d: checkpoint safepoint=%d\n",
      pLock->iDebugId, nCall++, (int)iSafe
  );
  fflush(stderr);
#endif
}
#else
#define btDebugLogSafepoint(x,y)
#endif

static void btDebugCkptPage(BtLock *pLock, u32 pgno, u8 *aData, int pgsz){
#if BT_PAGE_DEBUG
  static int nCall = 0;
  u32 aCksum[2];
  btLogChecksum(1, aData, pgsz, 0, aCksum);
  fprintf(stderr, "%d:%d: Ckpt page %d (cksum=%08x%08x)\n", 
      pLock->iDebugId, nCall++, (int)pgno, aCksum[0], aCksum[1]
  );
  fflush(stderr);
#endif
}

static void btDebugLogPage(
    BtLock *pLock, u32 pgno, u32 iFrame, u8 *aData, int pgsz, int bCommit
){
#if BT_PAGE_DEBUG
  static int nCall = 0;
  u32 aCksum[2];
  btLogChecksum(1, aData, pgsz, 0, aCksum);
  fprintf(stderr, "%d:%d: Log page %d to frame %d (cksum=%08x%08x)%s\n", 
      pLock->iDebugId, nCall++, (int)pgno, (int)iFrame, 
      aCksum[0], aCksum[1], (bCommit ? " commit" : "")
  );
  fflush(stderr);
#endif
}

#ifndef NDEBUG
void sqlite4BtDebugReadPage(BtLock *pLock, u32 pgno, u8 *aData, int pgsz){
#if BT_PAGE_DEBUG
  static int nCall = 0;
  u32 aCksum[2];
  btLogChecksum(1, aData, pgsz, 0, aCksum);
  fprintf(stderr, "%d:%d: Read page %d (cksum=%08x%08x)\n", pLock->iDebugId,
      nCall++, (int)pgno, aCksum[0], aCksum[1]
  );
  fflush(stderr);
#endif
}
#endif

#ifndef NDEBUG
#include <ctype.h>
#if BT_VAL_DEBUG
static void binToStr(u8 *pIn, int nIn, u8 *pOut, int nOut){
  int i;
  int nCopy = MIN(nIn, (nOut-1));
  for(i=0; i<nCopy; i++){
    if( isprint(pIn[i]) ){
      pOut[i] = pIn[i];
    }else{
      pOut[i] = '.';
    }
  }
  pOut[i] = '\0';
}
#endif
void sqlite4BtDebugKV(
    BtLock *pLock, const char *zStr, u8 *pK, int nK, u8 *pV, int nV
){
#if BT_VAL_DEBUG
  u8 aKBuf[40];
  u8 aVBuf[40];
  static int nCall = 0;

  binToStr(pK, nK, aKBuf, sizeof(aKBuf));
  if( nV<0 ){
    memcpy(aVBuf, "(delete)", 9);
  }else{
    binToStr(pV, nV, aVBuf, sizeof(aVBuf));
  }
  fprintf(stderr, "%d:%d: %s \"%s\" -> \"%s\" (%d bytes)\n", 
      pLock->iDebugId, nCall++, zStr, aKBuf, aVBuf, nV
  );

  fflush(stderr);
#endif
}
#endif

static void btDebugLogSearch(BtLock *pLock, u32 pgno, u32 iSafe, u32 iFrame){
#if BT_PAGE_DEBUG
  static int nCall = 0;
  fprintf(stderr, "%d:%d: Search log for page %d (safe=%d) - frame %d\n", 
      pLock->iDebugId, nCall++, (int)pgno, (int)iSafe, (int)iFrame
  );
  fflush(stderr);
#endif
}

static void btDebugSetPgno(BtLock *pLock, 
    int iHash, int iSide, u32 *aPgno, int iFrame, int iZero, u32 pgno
){
#if BT_PAGE_DEBUG
  static int nCall = 0;
  fprintf(stderr, "%d:%d: Set iHash=%d/%d aPgno=%p iFrame=%d iZero=%d pgno=%d\n"
      , pLock->iDebugId, nCall++, iHash, iSide,
      (void*)aPgno, iFrame, iZero, (int)pgno
  );
  fflush(stderr);
#endif
}

#ifndef NDEBUG
static void btDebugLogHeader(
    BtLock *pLock, const char *z, BtWalHdr *pHdr, int iHdr
){
#if BT_HDR_DEBUG
  static int nCall = 0;
  fprintf(stderr, 
      "%d:%d: %s log-header %d: (iCnt=%d iFirstFrame=%d nPgsz=%d)\n",
      pLock->iDebugId, nCall++, z, iHdr,
      (int)pHdr->iCnt, (int)pHdr->iFirstFrame, (int)pHdr->nPgsz
  );
  fflush(stderr);
#endif
}
#endif


#ifdef NDEBUG
# define btDebugLogHeader(a,b,c,d)
#endif

/*
** Ensure that shared-memory chunk iChunk is mapped and available in
** the BtLog.apShm[] array. If an error occurs, return an SQLite4 error
** code. Otherwise, SQLITE4_OK.
*/
static int btLogMapShm(BtLog *pLog, int iChunk){
  int rc = SQLITE4_OK;

  if( pLog->nShm<=iChunk ){
    sqlite4_env *pEnv = pLog->pLock->pEnv;
    u8 **apNew;
    int nNew = iChunk+1;

    apNew = (u8**)sqlite4_realloc(pEnv, pLog->apShm, sizeof(u8*)*nNew);
    if( apNew==0 ) return btErrorBkpt(SQLITE4_NOMEM);
    memset(&apNew[pLog->nShm], 0, (nNew-pLog->nShm) * sizeof(u8*));
    pLog->nShm = nNew;
    pLog->apShm = apNew;
  }

  if( pLog->apShm[iChunk]==0 ){
    u8 **pp = &pLog->apShm[iChunk];
    rc = sqlite4BtLockShmMap(pLog->pLock, iChunk, BT_SHM_CHUNK_SIZE, pp);
  }

  return rc;
}

static BtShm *btLogShm(BtLog *pLog){
  return (BtShm*)(pLog->apShm[0]);
}

static int btLogUpdateSharedHdr(BtLog *pLog){
  bt_env *pVfs = pLog->pLock->pVfs;
  BtShmHdr *p = &pLog->snapshot;
  BtShm *pShm = btLogShm(pLog);

  /* Calculate a checksum for the private snapshot object. */
  btLogChecksum32(1, (u8*)p, offsetof(BtShmHdr, aCksum), 0, p->aCksum);

  /* Update the shared object. */
  pVfs->xShmBarrier(pLog->pFd);
  memcpy(&pShm->hdr1, p, sizeof(BtShmHdr));
  pVfs->xShmBarrier(pLog->pFd);
  memcpy(&pShm->hdr2, p, sizeof(BtShmHdr));

  return SQLITE4_OK;
}

static void btLogZeroSnapshot(BtLog *pLog){
  bt_env *pVfs = pLog->pLock->pVfs;
  memset(&pLog->snapshot, 0, sizeof(BtShmHdr));
  pLog->snapshot.nSector = pVfs->xSectorSize(pLog->pFd);
}

/*
** Return the offset of frame iFrame within the log file.
*/
static i64 btLogFrameOffset(BtLog *pLog, int pgsz, u32 iFrame){
  return 
      (i64)pLog->snapshot.nSector*2 
    + (i64)(iFrame-1) * (i64)(pgsz + sizeof(BtFrameHdr));
}

static int btLogSyncFile(BtLog *pLog, bt_file *pFd){
  if( pLog->pLock->iSafetyLevel==BT_SAFETY_OFF ) return SQLITE4_OK;
  bt_env *pVfs = pLog->pLock->pVfs;
  return pVfs->xSync(pFd);
}

static int btLogWriteData(BtLog *pLog, i64 iOff, u8 *aData, int nData){
  bt_env *pVfs = pLog->pLock->pVfs;
  return pVfs->xWrite(pLog->pFd, iOff, aData, nData);
}

static int btLogReadData(BtLog *pLog, i64 iOff, u8 *aData, int nData){
  bt_env *pVfs = pLog->pLock->pVfs;
  return pVfs->xRead(pLog->pFd, iOff, aData, nData);
}

static int btLogReadHeader(BtLog *pLog, int iOff, BtWalHdr *pHdr){
  int rc = btLogReadData(pLog, (i64)iOff, (u8*)pHdr, sizeof(BtWalHdr));
  if( rc==SQLITE4_OK ){
    u32 aCksum[2];
    btLogChecksum(1, (u8*)pHdr, offsetof(BtWalHdr, aCksum), 0, aCksum);
    if( pHdr->iMagic!=BT_WAL_MAGIC 
     || aCksum[0]!=pHdr->aCksum[0] 
     || aCksum[1]!=pHdr->aCksum[1] 
    ){
      rc = SQLITE4_NOTFOUND;
    }else{
      btDebugLogHeader(pLog->pLock, "read", pHdr, iOff!=0);
    }
  }
  return rc;
}

/*
** This function is used as part of recovery. It reads the contents of
** the log file from disk and invokes the xFrame callback for each valid
** frame in the file.
*/
static int btLogTraverse(
  BtLog *pLog,                    /* Log module handle */
  BtWalHdr *pHdr,                 /* Log header read from file */
  int(*xFrame)(BtLog*, void*, u32, BtFrameHdr*), /* Frame callback */
  void *pCtx                      /* Passed as second argument to xFrame */
){
  sqlite4_env *pEnv = pLog->pLock->pEnv;
  const int pgsz = pHdr->nPgsz;
  u32 iFrame = pHdr->iFirstFrame;
  u32 aCksum[2];
  BtFrameHdr fhdr;                /* Frame header */
  u8 *aBuf;                       /* Buffer for frame data */
  int rc = SQLITE4_OK;

  aCksum[0] = pHdr->iSalt1;
  aCksum[1] = pHdr->iSalt2;

  aBuf = sqlite4_malloc(pEnv, pgsz);
  if( aBuf==0 ){
    rc = SQLITE4_NOMEM;
  }

  while( rc==SQLITE4_OK ){
    i64 iOff;
    iOff = btLogFrameOffset(pLog, pgsz, iFrame);

    rc = btLogReadData(pLog, iOff, (u8*)&fhdr, sizeof(BtFrameHdr));
    if( rc==SQLITE4_OK ){
      rc = btLogReadData(pLog, iOff+sizeof(BtFrameHdr), aBuf, pgsz);
    }
    if( rc==SQLITE4_OK ){
      btLogChecksum32(1, (u8*)&fhdr, offsetof(BtFrameHdr,aCksum),aCksum,aCksum);
      btLogChecksum(1, aBuf, pgsz, aCksum, aCksum);
      if( aCksum[0]!=fhdr.aCksum[0] || aCksum[1]!=fhdr.aCksum[1] ) break;
    }
    if( rc==SQLITE4_OK ){
      rc = xFrame(pLog, pCtx, iFrame, &fhdr);
    }

    iFrame = fhdr.iNext;
  }

  sqlite4_free(pEnv, aBuf);
  return rc;
}

/*
** Locate the iHash'th hash table in shared memory. Return it.
*/
static int btLogFindHash(
  BtLog *pLog,                    /* Log handle */
  int iSide,                      /* Which set of hash slots to return */
  int iHash,                      /* Hash table (numbered from 0) to find */
  ht_slot **paHash,               /* OUT: Pointer to hash slots */
  u32 **paPgno,                   /* OUT: Pointer to page number array */
  u32 *piZero                     /* OUT: Frame associated with *paPgno[0] */
){
  int rc;                         /* Return code */

  assert( iSide==0 || iSide==1 );

  rc = btLogMapShm(pLog, iHash);
  if( rc==SQLITE4_OK ){
    u8 *aChunk = pLog->apShm[iHash];
    u32 *aPgno;
    u32 iZero;

    *paHash = (ht_slot*)&aChunk[iSide?HASHTABLE_OFFSET_1:HASHTABLE_OFFSET_2];
    if( iHash==0 ){
      aPgno = (u32*)&aChunk[sizeof(BtShm)];
      iZero = 1;
    }else{
      aPgno = (u32*)aChunk;
      iZero = 1 + HASHTABLE_NFRAME_ONE + (HASHTABLE_NFRAME * (iHash-1));
    }
    *paPgno = aPgno;
    *piZero = iZero;
  }

  return rc;
}


/*
** Return the index of the hash table that contains the entry for frame
** iFrame. 
*/
static int btLogFrameHash(BtLog *pLog, u32 iFrame){
  if( iFrame<=HASHTABLE_NFRAME_ONE ) return 0;
  return 1 + ((iFrame - HASHTABLE_NFRAME_ONE - 1) / HASHTABLE_NFRAME);
}

/*
** Return a hash key for page number pgno.
*/
static int btLogHashKey(BtLog *pLog, u32 pgno){
  assert( pgno>=1 );
  return ((pgno * HASHTABLE_KEY_MUL) % HASHTABLE_NSLOT);
}

static int btLogHashNext(BtLog *pLog, int iSlot){
  return ((iSlot + 1) % HASHTABLE_NSLOT);
}

/*
** Add an entry mapping database page pgno to log frame iFrame to the
** the shared hash table. Return SQLITE4_OK if successful, or an SQLite4
** error code if an error occurs.
*/
static int btLogHashInsert(BtLog *pLog, u32 pgno, u32 iFrame){
  int iHash;                      /* Index of hash table to update */
  int rc = SQLITE4_OK;            /* Return code */
  ht_slot *aHash;                 /* Hash slots */
  u32 *aPgno;                     /* Page array for updated hash table */
  u32 iZero;                      /* Zero-offset of updated hash table */
  int iSide = pLog->snapshot.iHashSide;

  assert( iFrame>=1 && pgno>=1 );

  /* Find the required hash table */
  iHash = btLogFrameHash(pLog, iFrame);
  rc = btLogFindHash(pLog, iSide, iHash, &aHash, &aPgno, &iZero);

  /* Update the hash table */
  if( rc==SQLITE4_OK ){
    int iSlot;
    int nCollide = HASHTABLE_NSLOT*2;
    aPgno[iFrame-iZero] = pgno;
    btDebugSetPgno(pLog->pLock, iHash, iSide, aPgno, iFrame, iZero, pgno);

    if( iFrame==iZero ){
      memset(aHash, 0, sizeof(ht_slot) * HASHTABLE_NSLOT);
    }

    for(iSlot=btLogHashKey(pLog,pgno); ; iSlot=btLogHashNext(pLog, iSlot)){
      if( aHash[iSlot]==0 ){
        aHash[iSlot] = (iFrame-iZero+1);
        break;
      }
      if( (nCollide--)==0 ) return btErrorBkpt(SQLITE4_CORRUPT);
    }
  }

  return rc;
}

/*
** Remove everything following frame iFrame from the iHash'th hash table.
*/
static int btLogHashRollback(BtLog *pLog, int iHash, u32 iFrame){
  ht_slot *aHash;                 /* Hash slots */
  u32 *aPgno;                     /* Page array for updated hash table */
  u32 iZero;                      /* Zero-offset of updated hash table */
  int iSide = pLog->snapshot.iHashSide;
  int rc;

  rc = btLogFindHash(pLog, iSide, iHash, &aHash, &aPgno, &iZero);
  if( rc==SQLITE4_OK ){
    int i;
    ht_slot iMax;
    iMax = (iFrame - iZero) + 1;

    for(i=0; i<HASHTABLE_NSLOT; i++){
      if( aHash[i]>iMax ){
        aPgno[aHash[i]-1] = 0;
        aHash[i] = 0;
      }
    }
  }

  return rc;
}


/*
** Return true if log is completely empty (as it is if a file zero bytes
** in size has been opened or created).
*/
static int btLogIsEmpty(BtLog *pLog){
  return (pLog->snapshot.aLog[4]==0 && pLog->snapshot.iNextFrame==0);
}

typedef struct FrameRecoverCtx FrameRecoverCtx;
struct FrameRecoverCtx {
  u32 iLast;                      /* Frame containing last commit flag in log */
  u32 iNextFrame;                 /* Frame that follows frame iLast */
  u32 iPageOneFrame;              /* Frame containing most recent page 1 */
};

static int btLogRecoverFrame(
  BtLog *pLog,                    /* Log module handle */
  void *pCtx,                     /* Pointer to FrameRecoverCtx */
  u32 iFrame,                     /* Frame number */
  BtFrameHdr *pHdr                /* Frame header */
){
  FrameRecoverCtx *pFRC = (FrameRecoverCtx*)pCtx;

  btDebugRecoverFrame(pLog->pLock, iFrame, pHdr->pgno);

  if( btLogIsEmpty(pLog) ){
    /* This is the first frame recovered. It is therefore both the first
    ** and last frame of log region (c).  */
    pLog->snapshot.aLog[4] = iFrame;
    pLog->snapshot.aLog[5] = iFrame;
  }else{
    u32 iExpect = pLog->snapshot.aLog[5]+1;
    if( iFrame==iExpect ){
      pLog->snapshot.aLog[5] = iFrame;
    }else if( iFrame<iExpect ){
      assert( iFrame==1 );
      assert( pLog->snapshot.aLog[0]==0 && pLog->snapshot.aLog[1]==0 );
      pLog->snapshot.aLog[0] = pLog->snapshot.aLog[4];
      pLog->snapshot.aLog[1] = pLog->snapshot.aLog[5];
      pLog->snapshot.aLog[4] = iFrame;
      pLog->snapshot.aLog[5] = iFrame;
      pLog->snapshot.iHashSide = (pLog->snapshot.iHashSide + 1) % 2;
    }else{
      assert( pLog->snapshot.aLog[2]==0 && pLog->snapshot.aLog[3]==0 );
      pLog->snapshot.aLog[2] = pLog->snapshot.aLog[4];
      pLog->snapshot.aLog[3] = pLog->snapshot.aLog[5];
      pLog->snapshot.aLog[4] = iFrame;
      pLog->snapshot.aLog[5] = iFrame;
    }
  }

  btLogHashInsert(pLog, pHdr->pgno, iFrame);
  if( pHdr->nPg!=0 ){
    pFRC->iLast = iFrame;
    pFRC->iNextFrame = pHdr->iNext;
    memcpy(pLog->snapshot.aFrameCksum, pHdr->aCksum, sizeof(pHdr->aCksum));
    pLog->snapshot.dbhdr.nPg = pHdr->nPg;
  }

  if( pHdr->pgno==1 ){
    pFRC->iPageOneFrame = iFrame;
  }

#if 0
  fprintf(stderr, "recovered frame=%d pgno=%d\n", iFrame, pHdr->pgno);
  fflush(stderr);
#endif
  return 0;
}

/*
** This function is called as part of log recovery. The log file has 
** already been scanned and the log topology (pLog->snapshot.aLog[])
** shared-memory hash tables have been populated with data corresponding
** to the entire set of valid frames recovered from the log file -
** including uncommitted frames. This function removes the uncommitted
** frames from the log topology and shared hash tables.
*/
static int btLogRollbackRecovery(BtLog *pLog, FrameRecoverCtx *pCtx){
  u32 iLast = pCtx->iLast;        /* Last committed frame in log file */
  u32 *aLog = pLog->snapshot.aLog;/* Log file topology */

  while( iLast<aLog[4] || iLast>aLog[5] ){
    if( aLog[2] ){
      aLog[5] = aLog[3];
      aLog[4] = aLog[2];
      if( aLog[0] && aLog[0]<aLog[4] ){
        aLog[3] = aLog[1];
        aLog[2] = aLog[0];
        aLog[0] = aLog[1] = 0;
      }else{
        aLog[2] = aLog[3] = 0;
      }
    }else{
      aLog[5] = aLog[1];
      aLog[4] = aLog[0];
      aLog[0] = aLog[1] = 0;
      pLog->snapshot.iHashSide = (pLog->snapshot.iHashSide + 1) % 2;
    }
  }

  aLog[5] = iLast;
  return btLogHashRollback(pLog, btLogFrameHash(pLog, iLast), iLast);
}

static int btLogDecodeDbhdr(BtLog *pLog, u8 *aData, BtDbHdr *pHdr){
  BtDbHdrCksum hdr;
  u32 aCksum[2] = {0,0};

  if( aData ){
    memcpy(&hdr, aData, sizeof(BtDbHdrCksum));
    btLogChecksum32(1, (u8*)&hdr, offsetof(BtDbHdrCksum, aCksum), 0, aCksum);
  }

  if( aData==0 || aCksum[0]!=hdr.aCksum[0] || aCksum[1]!=hdr.aCksum[1] ){
    return SQLITE4_NOTFOUND;
  }

  memcpy(pHdr, &hdr, sizeof(BtDbHdr));
  return SQLITE4_OK;
}

static void btLogZeroDbhdr(BtLog *pLog, BtDbHdr *pHdr){
  assert( sizeof(pHdr->azStr)==strlen(BT_DBHDR_STRING) );

  memset(pHdr, 0, sizeof(BtDbHdr));
  memcpy(pHdr->azStr, BT_DBHDR_STRING, strlen(BT_DBHDR_STRING));
  pHdr->pgsz = pLog->pLock->nPgsz;
  pHdr->blksz = pLog->pLock->nBlksz;
  pHdr->nPg = 2;
  pHdr->iRoot = 2;
}

static int btLogReadDbhdr(BtLog *pLog, BtDbHdr *pHdr, u32 iFrame){
  BtLock *p = pLog->pLock;        /* BtLock handle */
  int rc;                         /* Return code */
  i64 nByte;                      /* Size of database file in byte */
  u8 aBuffer[sizeof(BtDbHdrCksum)];
  u8 *aData = 0;

  if( iFrame==0 ){
    rc = p->pVfs->xSize(p->pFd, &nByte);
    if( rc==SQLITE4_OK && nByte>0 ){
      rc = p->pVfs->xRead(p->pFd, 0, aBuffer, sizeof(BtDbHdrCksum));
      aData = aBuffer;
    }
  }else{
    i64 iOff = btLogFrameOffset(pLog, pLog->snapshot.dbhdr.pgsz, iFrame);
    iOff += sizeof(BtFrameHdr);
    rc = p->pVfs->xRead(pLog->pFd, iOff, aBuffer, sizeof(BtDbHdrCksum));
    aData = aBuffer;
  }

  if( rc==SQLITE4_OK ){
    rc = btLogDecodeDbhdr(pLog, aData, pHdr);
  }
  return rc;
}

static int btLogUpdateDbhdr(BtLog *pLog, u8 *aData){
  BtDbHdrCksum hdr;

  memcpy(&hdr.hdr, &pLog->snapshot.dbhdr, sizeof(BtDbHdr));
  btLogChecksum32(1, (u8*)&hdr, offsetof(BtDbHdrCksum, aCksum), 0, hdr.aCksum);
  btDebugDbhdr(pLog->pLock, "update", &pLog->snapshot.dbhdr);

  assert( hdr.hdr.iRoot==2 );
  assert( hdr.hdr.pgsz>0 );
  memcpy(aData, &hdr, sizeof(BtDbHdrCksum));

#ifndef NDEBUG
  {
    BtDbHdr tst;
    btLogDecodeDbhdr(pLog, aData, &tst);
    assert( 0==memcmp(&tst, &pLog->snapshot.dbhdr, sizeof(tst)) );
  }
#endif

  return SQLITE4_OK;
}


/*
** Run log recovery. In other words, read the log file from disk and 
** initialize the shared-memory accordingly.
*/
static int btLogRecover(BtLog *pLog){
  bt_env *pVfs = pLog->pLock->pVfs;
  i64 nByte = 0;                  /* Size of log file on disk */
  int rc;                         /* Return code */
  BtWalHdr *pHdr = 0;
  int iSlot = 0;
  FrameRecoverCtx ctx = {0, 0};
  BtWalHdr hdr1;
  BtWalHdr hdr2;

static int nCall = 0;
nCall++;

  /* Read a log file header from the start of the file. */
  rc = pVfs->xSize(pLog->pFd, &nByte);
  if( rc==SQLITE4_OK && nByte>0 ){
    rc = btLogReadHeader(pLog, 0, &hdr1);
    if( rc==SQLITE4_OK ){
      rc = btLogReadHeader(pLog, hdr1.nSector, &hdr2);
      if( rc==SQLITE4_NOTFOUND ){
        pHdr = &hdr1;
      }else if( rc==SQLITE4_OK ){
        int aGreater[3] = {1, 2, 0};
        pHdr = ((hdr2.iCnt==aGreater[hdr1.iCnt]) ? &hdr2 : &hdr1);
      }
      iSlot = (pHdr==&hdr2);
    }else if( rc==SQLITE4_NOTFOUND ){
      int iOff;
      for(iOff=256; iOff<=65536 && rc==SQLITE4_NOTFOUND; iOff=iOff*2){
        rc = btLogReadHeader(pLog, iOff, &hdr1);
      }
      if( rc==SQLITE4_OK ){
        pHdr = &hdr1;
        iSlot = 1;
      }
    }
    if( rc==SQLITE4_NOTFOUND ) rc = SQLITE4_OK;
  }

  /* If a header was successfully read from the file, attempt to 
  ** recover frames from the log file. */
  if( pHdr ){

    /* The following iterates through all readable frames in the log file.
    ** It populates pLog->snapshot.aLog[] with the log topology and the
    ** shared hash-tables with the pgno->frame mapping. The FrameRecoverCtx
    ** object is populated with the frame number and "next frame" pointer of
    ** the last commit-frame in the log (if any). Additionally, the
    ** pLog->snapshot.aFrameCksum[] variables are populated with the checksum
    ** beloging to the frame header of the last commit-frame in the log.  */
    rc = btLogTraverse(pLog, pHdr, btLogRecoverFrame, (void*)&ctx);

    if( rc==SQLITE4_OK && ctx.iLast>0 ){
      /* One or more transactions were recovered from the log file. */
      BtShm *pShm = btLogShm(pLog);
      pShm->ckpt.iWalHdr = (iSlot<<2) + pHdr->iCnt;
      pShm->ckpt.iFirstRead = pHdr->iFirstFrame;
      pShm->ckpt.iFirstRecover = pHdr->iFirstFrame;
      rc = btLogRollbackRecovery(pLog, &ctx);
      pLog->snapshot.iNextFrame = ctx.iNextFrame;
      pLog->snapshot.dbhdr.pgsz = pHdr->nPgsz;
      assert( pShm->ckpt.iFirstRead>0 );
    }
    assert( rc!=SQLITE4_NOTFOUND );

    /* Based on the wal-header, the page-size and number of pages in the
    ** database are now known and stored in snapshot.dbhdr. But the other
    ** header field values (iCookie, iRoot etc.) are still unknown. Read
    ** them from page 1 of the database file now.  */
    if( rc==SQLITE4_OK ){
      u32 nPg = pLog->snapshot.dbhdr.nPg;
      rc = btLogReadDbhdr(pLog, &pLog->snapshot.dbhdr, ctx.iPageOneFrame);
      if( ctx.iLast>0 ){
        pLog->snapshot.dbhdr.nPg = nPg;
      }
    }

  }else if( rc==SQLITE4_OK ){
    /* There is no data in the log file. Read the database header directly
    ** from offset 0 of the database file.  */
    btLogZeroSnapshot(pLog);
    rc = btLogReadDbhdr(pLog, &pLog->snapshot.dbhdr, 0);
  }

  if( rc==SQLITE4_NOTFOUND ){
    /* Check the size of the db file. If it is greater than zero bytes in
    ** size, refuse to open the file (as it is probably not a database
    ** file). Or, if it is exactly zero bytes in size, this is a brand
    ** new database.  */
    rc = pVfs->xSize(pLog->pLock->pFd, &nByte);
    if( rc==SQLITE4_OK ){
      if( nByte==0 ){
        btLogZeroDbhdr(pLog, &pLog->snapshot.dbhdr);
      }else{
        rc = btErrorBkpt(SQLITE4_NOTADB);
      }
    }
  }

  if( rc==SQLITE4_OK ){
    btDebugTopology(
        pLog->pLock, "recovered", pLog->snapshot.iHashSide, pLog->snapshot.aLog
    );

    btDebugDbhdr(pLog->pLock, "read", &pLog->snapshot.dbhdr);
  }
  return rc;
}

/*
** Open the log file for pager pPager. If successful, return the BtLog* 
** handle via output variable *ppLog. If parameter bRecover is true, then
** also run database recovery before returning. In this case, the caller
** has already obtained the required locks.
*/
int sqlite4BtLogOpen(BtPager *pPager, int bRecover, BtLog **ppLog){
  BtLock *pLock = (BtLock*)pPager;
  bt_env *pVfs = pLock->pVfs;
  sqlite4_env *pEnv = pLock->pEnv;
  int rc;                         /* Return code */
  const char *zWal;               /* Name of log file to open */
  BtLog *pLog;                    /* Log handle to return */
  int flags = BT_OPEN_LOG;

  pLog = sqlite4_malloc(pEnv, sizeof(BtLog));
  if( pLog==0 ){
    rc = SQLITE4_NOMEM;
    goto open_out;
  }
  memset(pLog, 0, sizeof(BtLog));
  pLog->pLock = (BtLock*)pPager;
  pLog->nWrapLog = BT_NWRAPLOG;

  zWal = sqlite4BtPagerFilename(pPager, BT_PAGERFILE_LOG);
  rc = pVfs->xOpen(pEnv, pVfs, zWal, flags, &pLog->pFd);

  if( rc==SQLITE4_OK && bRecover ){
    rc = btLogMapShm(pLog, 0);
    if( rc==SQLITE4_OK ){
      BtShm *pShm = btLogShm(pLog);
      memset(pShm, 0, sizeof(BtShm));
      pShm->ckpt.iFirstRead = 1;
      pShm->ckpt.iFirstRecover = 1;
      btLogZeroSnapshot(pLog);
      rc = btLogRecover(pLog);
    }
    if( rc==SQLITE4_OK ){
      rc = btLogUpdateSharedHdr(pLog);
    }
  }

 open_out:
  if( rc!=SQLITE4_OK ){
    sqlite4BtLogClose(pLog, 0);
    pLog = 0;
  }
  *ppLog = pLog;
  return rc;
}

/*
** Close the log file handle BtLog*. 
*/
int sqlite4BtLogClose(BtLog *pLog, int bCleanup){
  int rc = SQLITE4_OK;
  if( pLog ){
    sqlite4_env *pEnv = pLog->pLock->pEnv;
    bt_env *pVfs = pLog->pLock->pVfs;

    if( pLog->pFd ) pVfs->xClose(pLog->pFd);
    if( bCleanup ){
      BtPager *pPager = (BtPager*)pLog->pLock;
      const char *zWal = sqlite4BtPagerFilename(pPager, BT_PAGERFILE_LOG);
      rc = pVfs->xUnlink(pEnv, pVfs, zWal);
    }

    sqlite4_free(pEnv, pLog->apShm);
    sqlite4_free(pEnv, pLog);
  }

  return rc;
}

static int btLogWriteHeader(BtLog *pLog, int iHdr, BtWalHdr *pHdr){
  int rc;                         /* Return code */
  i64 iOff;                       /* File offset to write to */
  assert( iHdr==0 || iHdr==1 );

  btDebugLogHeader(pLog->pLock, "write", pHdr, iHdr);

  /* Calculate a checksum for the header */
  btLogChecksum(1, (u8*)pHdr, offsetof(BtWalHdr, aCksum), 0, pHdr->aCksum);

  /* Write the object to disk */
  iOff = iHdr * pLog->snapshot.nSector;
  rc = btLogWriteData(pLog, iOff, (u8*)pHdr, sizeof(BtWalHdr));

  return rc;
}

static int btLogHashSearch(
  BtLog *pLog,                    /* Log module handle */
  int iSide,                      /* 0 or 1 - the side of hash table to read */
  int iHash,                      /* Index of hash to query */
  u32 iHi,                        /* Consider no frames after this one */
  u32 pgno,                       /* query for this page number */
  u32 *piFrame                    /* OUT: Frame number for matching entry */
){
  ht_slot *aHash;
  u32 *aPgno;
  u32 iZero;
  int rc;

  rc = btLogFindHash(pLog, iSide, iHash, &aHash, &aPgno, &iZero);
  if( rc==SQLITE4_OK ){
    int nCollide = HASHTABLE_NSLOT*2;
    int iSlot;
    u32 iFrame = 0;
    
    iSlot = btLogHashKey(pLog, pgno); 
    for( ; aHash[iSlot]; iSlot=btLogHashNext(pLog, iSlot)){
      if( aPgno[aHash[iSlot]-1]==pgno ){
        u32 iCandidate = iZero + aHash[iSlot] - 1;
        if( iCandidate<=iHi ) iFrame = iCandidate;
      }
      if( (nCollide--)==0 ) return btErrorBkpt(SQLITE4_CORRUPT);
    }

    *piFrame = iFrame;
    if( iFrame==0 ){
      rc = SQLITE4_NOTFOUND;
    }
  }

  return rc;
}

/*
** If parameter iSafe is non-zero, then this function is being called as
** part of a checkpoint operation. In this case, if there exists a version
** of page pgno within the log at some point past frame iSafe, return
** SQLITE4_NOTFOUND.
*/
int btLogRead(BtLog *pLog, u32 pgno, u8 *aData, u32 iSafe){
  const int pgsz = pLog->snapshot.dbhdr.pgsz;
  int rc = SQLITE4_NOTFOUND;
  u32 iFrame = 0;
  int i;

  u32 *aLog = pLog->snapshot.aLog;
  int iSafeIdx = sqlite4BtLogFrameToIdx(aLog, iSafe);

  /* Loop through regions (c), (b) and (a) of the log file. In that order. */
  for(i=2; i>=0 && rc==SQLITE4_NOTFOUND; i--){
    u32 iLo = pLog->snapshot.aLog[i*2+0];
    if( iLo ){
      u32 iHi = pLog->snapshot.aLog[i*2+1];
      int iSide;
      int iHash;
      int iHashLast;

      iHash = btLogFrameHash(pLog, iHi);
      iHashLast = btLogFrameHash(pLog, iLo);
      iSide = (pLog->snapshot.iHashSide + (i==0)) % 2;

      for( ; rc==SQLITE4_NOTFOUND && iHash>=iHashLast; iHash--){
        rc = btLogHashSearch(pLog, iSide, iHash, iHi, pgno, &iFrame);
        if( rc==SQLITE4_OK ){
          if( iFrame<iLo || iFrame>iHi ){
            rc = SQLITE4_NOTFOUND;
          }else{
            assert( sqlite4BtLogFrameToIdx(aLog, iFrame)>=0 );
            if( iSafeIdx>=0 && sqlite4BtLogFrameToIdx(aLog, iFrame)>iSafeIdx ){
              return SQLITE4_NOTFOUND;
            }
          }
        }
      }
    }
  }

  btDebugLogSearch(pLog->pLock, pgno, iSafe, (rc==SQLITE4_OK ? iFrame : 0));

  if( rc==SQLITE4_OK ){
    bt_env *pVfs = pLog->pLock->pVfs;
    i64 iOff;
    assert( rc==SQLITE4_OK );
    iOff = btLogFrameOffset(pLog, pgsz, iFrame);
    rc = pVfs->xRead(pLog->pFd, iOff + sizeof(BtFrameHdr), aData, pgsz);

#if 0
    fprintf(stderr, "read page %d from offset %d\n", (int)pgno, (int)iOff);
    fflush(stderr);
#endif
  }

  return rc;
}

/*
** Attempt to read data for page pgno from the log file. If successful,
** the data is written into buffer aData[] (which must be at least as
** large as a database page). In this case SQLITE4_OK is returned.
**
** If the log does not contain any version of page pgno, SQLITE4_NOTFOUND
** is returned and the contents of buffer aData[] are not modified.
**
** If any other error occurs, an SQLite4 error code is returned. The final
** state of buffer aData[] is undefined in this case.
*/
int sqlite4BtLogRead(BtLog *pLog, u32 pgno, u8 *aData){
  if( pLog->snapshot.aLog[4]==0 ){
    assert( pLog->snapshot.aLog[0]==0 && pLog->snapshot.aLog[2]==0 );
    return SQLITE4_NOTFOUND;
  }
  return btLogRead(pLog, pgno, aData, 0);
}

static int btLogZeroHash(BtLog *pLog, int iHash){
  int iSide = pLog->snapshot.iHashSide;
  ht_slot *aHash;
  u32 *aPgno;
  u32 iZero;
  int rc;

  rc = btLogFindHash(pLog, iSide, iHash, &aHash, &aPgno, &iZero);
  if( rc==SQLITE4_OK ){
    memset(aHash, 0, sizeof(ht_slot)*HASHTABLE_NSLOT);
  }
  return rc;
}

static int btLogWriteFrame(BtLog *pLog, int nPad, u32 pgno, u8 *aData, u32 nPg){
  const int pgsz = pLog->snapshot.dbhdr.pgsz;
  u32 *aLog = pLog->snapshot.aLog;
  int rc = SQLITE4_OK;            /* Return code */
  u32 iFrame;                     /* Write this frame (numbered from 1) */
  u32 iNextFrame;                 /* Frame to write following this one */
  i64 iOff;                       /* Offset of log file to write to */
  BtFrameHdr frame;               /* Header for new frame */

  /* Figure out the offset to write the current frame to. */
  iFrame = pLog->snapshot.iNextFrame;
  iOff = btLogFrameOffset(pLog, pgsz, iFrame);

  /* The current frame will be written to location pLog->snapshot.iNextFrame.
  ** This code determines where the following frame will be stored. There
  ** are three possibilities:
  **
  **   1) The next frame follows the current frame (this is the usual case).
  **   2) The next frame is frame 1 - the log wraps around.
  **   3) Following the current frame is a block of frames still in use.
  **      So the next frame will immediately follow this block.
  */
  iNextFrame = pLog->snapshot.iNextFrame + 1;
  if( iFrame!=1 && iFrame==aLog[5]+1
   && aLog[0]==0 && aLog[2]==0 
   && aLog[4]!=0 && aLog[4]>pLog->nWrapLog 
  ){
    /* Case 2) It is possible to wrap the log around */
    iNextFrame = 1;
  }else if( (iNextFrame+nPad)>=aLog[0] && iNextFrame<=aLog[1] ){

    /* Case 3) It is necessary to jump over some existing log. */
    iNextFrame = aLog[1]+nPad+1;
    assert( iNextFrame!=1 );

    if( btLogFrameHash(pLog, iNextFrame)!=btLogFrameHash(pLog, iFrame) ){
      rc = btLogZeroHash(pLog, btLogFrameHash(pLog, iNextFrame));
    }
  }

  if( rc==SQLITE4_OK ){
    if( iNextFrame & 0x80000000 ){
      rc = SQLITE4_FULL;
    }else{
      u32 *a;                     /* Pointer to cksum of previous frame */

      /* Populate the frame header object. */
      memset(&frame, 0, sizeof(frame));
      frame.pgno = pgno;
      frame.iNext = iNextFrame;
      frame.nPg = nPg;
      a = pLog->snapshot.aFrameCksum;
      btLogChecksum32(1,(u8*)&frame,offsetof(BtFrameHdr,aCksum),a,frame.aCksum);
      btLogChecksum(1, aData, pgsz, frame.aCksum, frame.aCksum);

      btDebugLogPage(pLog->pLock, pgno, iFrame, aData, pgsz, nPg);

      /* Write the frame header to the log file. */
      rc = btLogWriteData(pLog, iOff, (u8*)&frame, sizeof(frame));
    }
    pLog->snapshot.iNextFrame = iNextFrame;
  }

  /* Write the frame contents to the log file. */
  if( rc==SQLITE4_OK ){
    rc = btLogWriteData(pLog, iOff+sizeof(frame), aData, pgsz);
  }

  /* Update the wal index hash tables with the (pgno -> iFrame) record. 
  ** If this is a commit frame, update the nPg field as well. */
  if( rc==SQLITE4_OK ){
    if( iFrame==1 ){
      pLog->snapshot.iHashSide = (pLog->snapshot.iHashSide+1) % 2;
    }
    if( nPg ) pLog->snapshot.dbhdr.nPg = nPg;

    rc = btLogHashInsert(pLog, pgno, iFrame);
  }

  /* Update the private copy of the shm-header */
  btDebugCheckSnapshot(&pLog->snapshot);
  BtShmHdr hdr;
  memcpy(&hdr, &pLog->snapshot, sizeof(BtShmHdr));
  if( rc==SQLITE4_OK ){
    if( btLogIsEmpty(pLog) ){
      assert( iFrame==1 );
      aLog[4] = iFrame;
    }else if( iFrame==1 ){
      assert( aLog[0]==0 && aLog[1]==0 && aLog[2]==0 && aLog[3]==0 );
      aLog[0] = aLog[4];
      aLog[1] = aLog[5];
      aLog[4] = iFrame;
    }else if( iFrame!=aLog[5]+1 ){
      assert( iFrame>aLog[5] );
      assert( aLog[2]==0 && aLog[3]==0 );
      aLog[2] = aLog[4];
      aLog[3] = aLog[5];
      aLog[4] = iFrame;
    }

    aLog[5] = iFrame;
    memcpy(pLog->snapshot.aFrameCksum, frame.aCksum, sizeof(frame.aCksum));
  }
  btDebugCheckSnapshot(&pLog->snapshot);

  return rc;
}

/*
** Write a frame to the log file.
*/
int sqlite4BtLogWrite(BtLog *pLog, u32 pgno, u8 *aData, u32 nPg){
  const int pgsz = pLog->snapshot.dbhdr.pgsz;
  int rc = SQLITE4_OK;

  int nPad = 0;
  if( pLog->pLock->iSafetyLevel==BT_SAFETY_FULL ){
    nPad = (pLog->snapshot.nSector + pgsz-1) / pgsz;
  }

  /* If this is a commit frame and the size of the database has changed,
  ** ensure that the log file contains at least one copy of page 1 written
  ** since the last checkpoint. This is required as a future checkpoint
  ** will need to update the nPg field in the database header located on
  ** page 1. */
  if( nPg /* && nPg!=pLog->snapshot.dbhdr.nPg */ ){
    BtPager *pPager = (BtPager *)(pLog->pLock);
    BtPage *pOne = 0;
    rc = sqlite4BtPageGet(pPager, 1, &pOne);
    if( rc==SQLITE4_OK ){
      rc = sqlite4BtLogWrite(pLog, 1, sqlite4BtPageData(pOne), 0);
      sqlite4BtPageRelease(pOne);
    }
    if( rc!=SQLITE4_OK ) return rc;
  }

  /* Handle a special case - if the log file is completely empty then
  ** this writer must write the first header into the WAL file. */
  if( btLogIsEmpty(pLog) ){
    BtWalHdr hdr;
    memset(&hdr, 0, sizeof(BtWalHdr));

    hdr.iMagic = BT_WAL_MAGIC;
    hdr.iVersion = BT_WAL_VERSION;
    hdr.nSector = pLog->snapshot.nSector;
    hdr.nPgsz = pgsz;
    hdr.iSalt1 = 22;
    hdr.iSalt2 = 23;
    hdr.iFirstFrame = 1;

    rc = btLogWriteHeader(pLog, 0, &hdr);
    if( rc!=SQLITE4_OK ) return rc;

    pLog->snapshot.aFrameCksum[0] = hdr.iSalt1;
    pLog->snapshot.aFrameCksum[1] = hdr.iSalt2;
    pLog->snapshot.iNextFrame = 1;
  }
  btDebugCheckSnapshot(&pLog->snapshot);

  rc = btLogWriteFrame(pLog, nPad, pgno, aData, nPg);

  /* If this is a COMMIT, sync the log and update the shared shm-header. */
  if( nPg ){
    int i;
    for(i=0; i<nPad && rc==SQLITE4_OK; i++){
      rc = btLogWriteFrame(pLog, nPad, pgno, aData, nPg);
    }
    if( rc==SQLITE4_OK && pLog->pLock->iSafetyLevel==BT_SAFETY_FULL ){
      rc = btLogSyncFile(pLog, pLog->pFd);
    }
    if( rc==SQLITE4_OK ) rc = btLogUpdateSharedHdr(pLog);
  }

  return rc;
}

/*
** Return true if the checksum in BtShmHdr.aCksum[] matches the rest
** of the object.
*/
static int btLogChecksumOk(BtShmHdr *pHdr){
  u32 aCksum[2];
  btLogChecksum32(1, (u8*)pHdr, offsetof(BtShmHdr, aCksum), 0, aCksum);
  return (aCksum[0]==pHdr->aCksum[0] && aCksum[1]==pHdr->aCksum[1]);
}

static int btLogSnapshot(BtLog *pLog, BtShmHdr *pHdr){
  int rc;

  rc = btLogMapShm(pLog, 0);
  if( rc==SQLITE4_OK ){
    BtShm *pShm = btLogShm(pLog);
    int nAttempt = 500;

    while( (nAttempt--)>0 ){
      memcpy(pHdr, &pShm->hdr1, sizeof(BtShmHdr));
      if( btLogChecksumOk(pHdr) ) break;
      memcpy(pHdr, &pShm->hdr2, sizeof(BtShmHdr));
      if( btLogChecksumOk(pHdr) ) break;
    }

    if( nAttempt==0 ) rc = SQLITE4_PROTOCOL;
  }

  return rc;
}

static void btLogSnapshotTrim(u32 *aLog, u32 iFirst){
  if( iFirst ){
    int iRegion;
    for(iRegion=0; iRegion<3; iRegion++){
      if( aLog[iRegion*2] ){
        if( iFirst>=aLog[iRegion*2] && iFirst<=aLog[iRegion*2+1] ){
          aLog[iRegion*2] = iFirst;
          break;
        }else{
          aLog[iRegion*2] = 0;
          aLog[iRegion*2+1] = 0;
        }
      }
    }
  }
}

int sqlite4BtLogSnapshotOpen(BtLog *pLog){
  u32 *aLog = pLog->snapshot.aLog;
  int rc = SQLITE4_NOTFOUND;
  BtShmHdr shmhdr;
  u32 iFirstRead = 0;

  while( rc==SQLITE4_NOTFOUND ){
    BtShm *pShm;

    /* Attempt to read a copy of the BtShmHdr from shared-memory. */
    rc = btLogSnapshot(pLog, &pLog->snapshot);
    btDebugCheckSnapshot(&pLog->snapshot);

    /* Take a read lock on the database */
    if( rc==SQLITE4_OK ){
      BtReadSlot *aReadlock;
      pShm = btLogShm(pLog);

      aReadlock = pShm->aReadlock;
      iFirstRead = pShm->ckpt.iFirstRead;
      rc = sqlite4BtLockReader(pLog->pLock, aLog, iFirstRead, aReadlock);
    }

    /* Check that the BtShmHdr in shared-memory has not changed. If it has,
    ** drop the read-lock and re-attempt the entire operation. */
    if( rc==SQLITE4_OK ){
      rc = btLogSnapshot(pLog, &shmhdr);
    }
    if( rc==SQLITE4_OK ){
      if( iFirstRead!=pShm->ckpt.iFirstRead 
       || memcmp(&shmhdr, &pLog->snapshot, sizeof(BtShmHdr)) 
      ){
        rc = SQLITE4_NOTFOUND;
      }
    }
    
    if( rc!=SQLITE4_OK ){
      sqlite4BtLockReaderUnlock(pLog->pLock);
    }
  }

  if( rc==SQLITE4_OK ){
    btDebugTopology(
        pLog->pLock, "snapshotA", pLog->snapshot.iHashSide, pLog->snapshot.aLog
    );
  }

  /* If a snapshot was successfully read, adjust it so that the aLog[] 
  ** array specifies that no frames before iFirstRead is ever read from 
  ** the log file.  */
  if( rc==SQLITE4_OK ){
    btLogSnapshotTrim(aLog, iFirstRead);
  }

  if( rc==SQLITE4_OK ){
    btDebugTopology(
        pLog->pLock, "snapshotB", pLog->snapshot.iHashSide, pLog->snapshot.aLog
    );
  }

  return rc;
}

int sqlite4BtLogSnapshotClose(BtLog *pLog){
  sqlite4BtLockReaderUnlock(pLog->pLock);
  return SQLITE4_OK;
}

/*
** The log handle has already successfully opened a read-only snapshot
** when this function is called. This function attempts to upgrade it
** to a read-write snapshot. 
*/
int sqlite4BtLogSnapshotWrite(BtLog *pLog){
  BtLock *pLock = pLog->pLock;
  int rc;

  rc = sqlite4BtLockWriter(pLock);
  if( rc==SQLITE4_OK ){
    BtShm *pShm = btLogShm(pLog);
    BtShmHdr shmhdr;

    /* Check if this connection is currently reading from the latest
    ** database snapshot. Set rc to SQLITE4_BUSY if it is not.  */
    rc = btLogSnapshot(pLog, &shmhdr);
    if( rc==SQLITE4_OK && memcmp(&pShm->hdr1, &pShm->hdr2, sizeof(BtShmHdr)) ){
      memcpy(&pShm->hdr1, &shmhdr, sizeof(BtShmHdr));
      memcpy(&pShm->hdr2, &shmhdr, sizeof(BtShmHdr));
    }
    if( rc==SQLITE4_OK && pLog->snapshot.iNextFrame!=shmhdr.iNextFrame ){
      rc = SQLITE4_BUSY;
    }

    /* Currently, pLog->snapshot.aLog[] contains a map of the frames
    ** that this connection was required to consider in order to read
    ** from the read-only snapshot. The following block edits this so
    ** that it contains a map of all frames that are currently in use
    ** by any reader, or may be used by any future reader or recovery
    ** process.  */
    if( rc==SQLITE4_OK ){
      u32 *aLog = shmhdr.aLog;
      u32 iRecover = pShm->ckpt.iFirstRecover;
      u32 iRead = 0;

      btDebugTopology(pLog->pLock, "snapshotC", shmhdr.iHashSide, aLog);

      assert( shmhdr.iHashSide==pLog->snapshot.iHashSide );
      btDebugCheckSnapshot(&pLog->snapshot);

      rc = sqlite4BtLockReaderQuery(pLock, aLog, pShm->aReadlock, &iRead, 0);

      if( rc==SQLITE4_OK ){
        /* Now "trim" the snapshot so that it accesses nothing earlier than
        ** either iRecover or iRead (whichever occurs first in the log). */
        u32 iTrim = iRecover;
        if( iRead ){
          int iIdxRead = sqlite4BtLogFrameToIdx(aLog, iRead);
          if( sqlite4BtLogFrameToIdx(aLog, iRecover)>iIdxRead ) iTrim = iRead;
        }

        if( iTrim==0 || iTrim==shmhdr.iNextFrame || btLogIsEmpty(pLog) ){
          memset(aLog, 0, sizeof(u32)*6);
        }else{
          int i;
          for(i=0; i<3; i++){
            int bIn = (aLog[2*i]<=iTrim && iTrim<=aLog[2*i+1]);
            if( bIn ){
              aLog[2*i] = iTrim;
              break;
            }else{
              aLog[2*i] = aLog[2*i+1] = 0;
            }
          }
        }
      }

      btDebugTopology(pLog->pLock, "snapshotD", shmhdr.iHashSide, aLog);

      if( rc==SQLITE4_OK ){
        memcpy(pLog->snapshot.aLog, aLog, sizeof(u32)*6);
      }
      btDebugCheckSnapshot(&pLog->snapshot);
    }
  }

  return rc;
}

int sqlite4BtLogSnapshotEndWrite(BtLog *pLog){
  return sqlite4BtLockWriterUnlock(pLog->pLock);
}

static void btLogMergeInplace(
  u32 *aLeft, int nLeft,          /* Left hand input array */
  u32 *aRight, int nRight,        /* Right hand input array */
  u32 *aSpace,                    /* Temporary space */
  int *pnOut                      /* OUT: Size of aLeft[] after merge */
){
  int iLeft = 0;
  int iRight = 0;
  int iOut = 0;

  while( iLeft<nLeft || iRight<nRight ){
    u32 v;
    if( iRight==nRight || (iLeft<nLeft && aLeft[iLeft]<aRight[iRight]) ){
      assert( iLeft<nLeft );
      v = aLeft[iLeft++];
    }else{
      assert( iRight<nRight );
      v = aRight[iRight++];
    }
    if( v && (iOut==0 || v!=aSpace[iOut-1]) ) aSpace[iOut++] = v;
  }

  memcpy(aLeft, aSpace, iOut*sizeof(u32));
  memset(&aLeft[iOut], 0, ((nLeft+nRight)-iOut) * sizeof(u32));
  *pnOut = iOut;
}

static void btLogMergeSort(
  u32 *aPgno,                     /* Array to sort */
  int *pnPgno,                    /* IN/OUT: Number of entries in aPgno[] */
  u32 *aSpace                     /* Temporary space */
){
  int nMerge;
  int nPgno = *pnPgno;

  for(nMerge=1; nMerge<nPgno; nMerge=nMerge*2){
    int iLeft;
    for(iLeft=0; iLeft<nPgno; iLeft+=(nMerge*2)){
      u32 *aLeft = &aPgno[iLeft];
      int nLeft = MIN(nMerge, nPgno-iLeft);
      u32 *aRight = &aPgno[iLeft+nMerge];
      int nRight = MIN(nMerge, nPgno-iLeft-nLeft);
      btLogMergeInplace(aLeft, nLeft, aRight, nRight, aSpace, pnPgno);
    }
  }
}

int sqlite4BtLogFrameToIdx(u32 *aLog, u32 iFrame){
  int i;
  int iRet = 0;
  for(i=0; i<3; i++){
    u32 iFirst = aLog[i*2];
    u32 iLast = aLog[i*2+1];
    if( iFirst ){
      if( iFrame>=iFirst && iFrame<=iLast ){
        iRet += (iFrame - iFirst);
        return iRet;
      }else{
        iRet += (iLast - iFirst) + 1;
      }
    }
  }
  if( i==3 ) return -1;
  return iRet;
}

/*
** Parameters iFirst and iLast are frame numbers for frames that are part 
** of the current log. This function scans the wal-index from iFirst to
** iLast (inclusive) and records the set of page numbers that occur once.
** This set is sorted in ascending order and returned via the output 
** variables *paPgno and *pnPgno.
*/
static int btLogGatherPgno(
  BtLog *pLog,                    /* Log module handle */
  int nFrameBuffer,
  u32 **paPgno,                   /* OUT: s4_malloc'd array of sorted pgno */
  int *pnPgno,                    /* OUT: Number of entries in *paPgno */
  u32 *piLastFrame                /* OUT: Last frame checkpointed */
){
  BtShm *pShm = btLogShm(pLog);
  BtLock *pLock = pLog->pLock;
  u32 *aLog = pLog->snapshot.aLog;/* Log file topology */
  u32 i;
  u32 *aPgno;                     /* Returned array */
  int nPgno;                      /* Elements in aPgno[] */
  u32 *aSpace;                    /* Temporary space used by merge-sort */
  int nMax;
  int rc = SQLITE4_OK;
  int iRegion;
  int bLocked;
  u32 iSafe;                      /* Last frame in log it is safe to gather */

  int iSafeIdx = -1;
  int iFirstIdx = -1;
  int iBufIdx;
  int iIdx = 0;

  *paPgno = 0;
  *pnPgno = 0;
  *piLastFrame = 0;

  rc = sqlite4BtLockReaderQuery(pLock, aLog, pShm->aReadlock, &iSafe, &bLocked);
  if( rc!=SQLITE4_OK || bLocked ) return rc;
  btDebugLogSafepoint(pLock, iSafe);
  btDebugTopology(
      pLock, "checkpointer", pLog->snapshot.iHashSide, pLog->snapshot.aLog
  );

  iFirstIdx = sqlite4BtLogFrameToIdx(aLog, pShm->ckpt.iFirstRecover);
  iSafeIdx = sqlite4BtLogFrameToIdx(aLog, iSafe);
  iBufIdx = sqlite4BtLogFrameToIdx(aLog, pLog->snapshot.aLog[5]) - nFrameBuffer;
  if( iSafeIdx<0 || iBufIdx<iSafeIdx ) iSafeIdx = iBufIdx;
  if( iSafeIdx<0 || (iFirstIdx>=0 && iSafeIdx<iFirstIdx) ) return rc;

  /* Determine an upper limit on the number of distinct page numbers. This
  ** limit is used to allocate space for the returned array.  */
  nMax = iSafeIdx - iFirstIdx +1;

  /* Allocate space to collect all page numbers. */
  aPgno = (u32*)sqlite4_malloc(pLog->pLock->pEnv, sizeof(u32)*nMax*2);
  if( aPgno==0 ) rc = btErrorBkpt(SQLITE4_NOMEM);
  aSpace = &aPgno[nMax];
  nPgno = 0;

  /* Copy the required page numbers into the allocated array */
  for(iRegion=0; iRegion<3; iRegion++){
    u32 iFirst = aLog[iRegion*2];
    u32 iLast = aLog[iRegion*2+1];
    if( iFirst ){

      for(i=iFirst; rc==SQLITE4_OK && i<=iLast; i++, iIdx++){
        int iHash = btLogFrameHash(pLog, i);
        u32 *aPage;
        ht_slot *aHash;
        u32 iZero;

        /* Ensure that the checkpoint does not read any frames from the 
        ** log that occur earlier than iFirstRecover. This is not just 
        ** an optimization - there is a chance that such frames may be 
        ** overwritten by a writer running concurrently with this 
        ** checkpoint.  */
        if( (iFirstIdx>=0 && iIdx<iFirstIdx) 
         || (iSafeIdx>=0 && iIdx>iSafeIdx) 
        ){
          continue;
        }
        *piLastFrame = i;

        /* It doesn't matter which 'side' of the hash table is requested here,
        ** as only the page-number array, not the aHash[] table, will be used.
        ** And it is the same for both sides. Hence the constant 0 passed as
        ** the second argument to btLogFindHash().  */
        rc = btLogFindHash(pLog, 0, iHash, &aHash, &aPage, &iZero);
        if( rc==SQLITE4_OK ){
          aPgno[nPgno++] = aPage[i-iZero];
        }
      }
    }
  }

  /* Sort the contents of the array in ascending order. This step also 
  ** eliminates any  duplicate page numbers. */
  if( rc==SQLITE4_OK ){
    btLogMergeSort(aPgno, &nPgno, aSpace);
    *pnPgno = nPgno;
    *paPgno = aPgno;
  }else{
    sqlite4_free(pLog->pLock->pEnv, aPgno);
    *paPgno = 0;
    *pnPgno = 0;
  }

  return rc;
}

/*
** Return the number of frames in the log file that have not yet been
** copied into the database file, according to the current snapshot.
**
** todo: adjust result for iFirstRead/iFirstRecover.
*/
int sqlite4BtLogSize(BtLog *pLog){
  return 
      (int)pLog->snapshot.aLog[1] - (int)pLog->snapshot.aLog[0]
    + (pLog->snapshot.aLog[0]!=0)
    + (int)pLog->snapshot.aLog[3] - (int)pLog->snapshot.aLog[2]
    + (pLog->snapshot.aLog[3]!=0)
    + (int)pLog->snapshot.aLog[5] - (int)pLog->snapshot.aLog[4]
    + (pLog->snapshot.aLog[5]!=0)
  ;
}

static int btLogMerge(BtLog *pLog, u8 *aBuf){
  bt_db *db = (bt_db*)sqlite4BtPagerExtra((BtPager*)pLog->pLock);
  return sqlite4BtMerge(db, &pLog->snapshot.dbhdr, aBuf);
}

int sqlite4BtLogCheckpoint(BtLog *pLog, int nFrameBuffer){
  BtLock *pLock = pLog->pLock;
  int rc;

  /* Take the CHECKPOINTER lock. */
  rc = sqlite4BtLockCkpt(pLock);
  if( rc==SQLITE4_OK ){
    int pgsz;
    bt_env *pVfs = pLock->pVfs;
    bt_file *pFd = pLock->pFd;
    BtShm *pShm;                  /* Pointer to shared-memory region */
    u32 iLast;                    /* Last frame to checkpoint */
    BtFrameHdr fhdr;              /* Frame header of frame iLast */
    u32 *aPgno = 0;               /* Array of page numbers to checkpoint */
    int nPgno;                    /* Number of entries in aPgno[] */
    int i;                        /* Used to loop through aPgno[] */
    u8 *aBuf;                     /* Buffer to load page data into */
    u32 iFirstRead;               /* First frame not checkpointed */

    rc = btLogSnapshot(pLog, &pLog->snapshot);
    sqlite4BtPagerSetDbhdr((BtPager*)pLock, &pLog->snapshot.dbhdr);
    pgsz = pLog->snapshot.dbhdr.pgsz;

    if( rc==SQLITE4_OK ){
      /* Allocate space to load log data into */
      aBuf = sqlite4_malloc(pLock->pEnv, pgsz);
      if( aBuf==0 ) rc = btErrorBkpt(SQLITE4_NOMEM);
    }
    
    /* Figure out the set of page numbers stored in the part of the log 
    ** file being checkpointed. Remove any duplicates and sort them in 
    ** ascending order.  */
    if( rc==SQLITE4_OK ){
      rc = btLogGatherPgno(pLog, nFrameBuffer, &aPgno, &nPgno, &iLast);
    }

    if( rc==SQLITE4_OK && nPgno>0 ){
      i64 iOff = btLogFrameOffset(pLog, pgsz, iLast);

      /* Ensure the log has been synced to disk */
      if( rc==SQLITE4_OK ){
        rc = btLogSyncFile(pLog, pLog->pFd);
      }

      rc = btLogReadData(pLog, iOff, (u8*)&fhdr, sizeof(BtFrameHdr));
      iFirstRead = fhdr.iNext;

      /* Copy data from the log file to the database file. */
      for(i=0; rc==SQLITE4_OK && i<nPgno; i++){
        u32 pgno = aPgno[i];
        rc = btLogRead(pLog, pgno, aBuf, iLast);
        if( rc==SQLITE4_OK ){
          i64 iOff = (i64)pgsz * (pgno-1);
          if( pgno==1 ){
            rc = btLogUpdateDbhdr(pLog, aBuf);
          }else if( pgno==pLog->snapshot.dbhdr.iSRoot ){
            rc = btLogMerge(pLog, aBuf);
          }
          if( rc==SQLITE4_OK ){
            btDebugCkptPage(pLog->pLock, pgno, aBuf, pgsz);
            rc = pVfs->xWrite(pFd, iOff, aBuf, pgsz);
          }
        }else if( rc==SQLITE4_NOTFOUND ){
          rc = SQLITE4_OK;
        }
      }

      /* Sync the database file to disk. */
      if( rc==SQLITE4_OK ){
        rc = btLogSyncFile(pLog, pLog->pLock->pFd);
      }

      /* Update the first field of the checkpoint-header. This tells readers
      ** that they need not consider anything that in the log before this
      ** point (since the data has already been copied into the database
      ** file).  */
      if( rc==SQLITE4_OK ){
        assert( iFirstRead>0 );
        pShm = btLogShm(pLog);
        pShm->ckpt.iFirstRead = iFirstRead;
        pVfs->xShmBarrier(pLog->pFd);
      }

      /* Write a new header into the log file. This tells any future recovery
      ** where it should start reading the log. Once this new header is synced
      ** to disk, the space cleared by this checkpoint operation can be 
      ** reused.  */
      if( rc==SQLITE4_OK ){
        int iSlot = ((pShm->ckpt.iWalHdr >> 2) + 1) % 2;
        BtWalHdr hdr;

        memset(&hdr, 0, sizeof(BtWalHdr));
        hdr.iMagic = BT_WAL_MAGIC;
        hdr.iVersion = BT_WAL_VERSION;
        hdr.iCnt = (((pShm->ckpt.iWalHdr & 0x03) + 1) % 3);
        hdr.nSector = pLog->snapshot.nSector;
        hdr.nPgsz = pgsz;
        hdr.iFirstFrame = iFirstRead;

        hdr.iSalt1 = fhdr.aCksum[0];
        hdr.iSalt2 = fhdr.aCksum[1];
        rc = btLogWriteHeader(pLog, iSlot, &hdr);
        if( rc==SQLITE4_OK ){
          rc = btLogSyncFile(pLog, pLog->pFd);
        }
        if( rc==SQLITE4_OK ){
          pShm->ckpt.iWalHdr = (iSlot<<2) + hdr.iCnt;
        }
      }

      /* Update the second field of the checkpoint header. This tells future
      ** writers that it is now safe to recycle pages before this point
      ** (assuming all live readers are cleared).  */
      if( rc==SQLITE4_OK ){
        pShm->ckpt.iFirstRecover = iFirstRead;
        pVfs->xShmBarrier(pLog->pFd);
      }
    }

    /* Free buffers and drop the checkpointer lock */
    sqlite4_free(pLock->pEnv, aBuf);
    sqlite4_free(pLock->pEnv, aPgno);
    sqlite4BtLockCkptUnlock(pLock);
    sqlite4BtPagerSetDbhdr((BtPager*)pLock, 0);
  }

  return rc;
}

#if 0
/*
** Return the database page size in bytes.
*/
int sqlite4BtLogPagesize(BtLog *pLog){
  return pLog->snapshot.dbhdr.pgsz;
}

/*
** Return the number of pages in the database at last commit.
*/
int sqlite4BtLogPagecount(BtLog *pLog){
  return (pLog->snapshot.dbhdr.nPg==1 ? 2 : pLog->snapshot.dbhdr.nPg);
}

/*
** Return the current value of the user cookie.
*/
u32 sqlite4BtLogCookie(BtLog *pLog){
  return pLog->snapshot.dbhdr.iCookie;
}
#endif

BtDbHdr *sqlite4BtLogDbhdr(BtLog *pLog){
  return &pLog->snapshot.dbhdr;
}

void sqlite4BtLogReloadDbHdr(BtLog *pLog){
  BtShm *pShm = btLogShm(pLog);
  memcpy(&pLog->snapshot, &pShm->hdr1, sizeof(BtShmHdr));
}

int sqlite4BtLogDbhdrFlush(BtLog *pLog){
  BtPager *pPager = (BtPager *)(pLog->pLock);
  BtPage *pOne = 0;
  int rc;

  rc = sqlite4BtPageGet(pPager, 1, &pOne);
  if( rc==SQLITE4_OK ){
    rc = sqlite4BtPageWrite(pOne);
  }
  if( rc==SQLITE4_OK ){
    btLogUpdateDbhdr(pLog, sqlite4BtPageData(pOne));
  }
  sqlite4BtPageRelease(pOne);

  return rc;
} 

/*
** Set the value of the user cookie.
*/
int sqlite4BtLogSetCookie(BtLog *pLog, u32 iCookie){
  BtPager *pPager = (BtPager *)(pLog->pLock);
  BtPage *pOne = 0;
  int rc;

  rc = sqlite4BtPageGet(pPager, 1, &pOne);
  if( rc==SQLITE4_OK ){
    rc = sqlite4BtPageWrite(pOne);
  }
  if( rc==SQLITE4_OK ){
    pLog->snapshot.dbhdr.iCookie = iCookie;
    btLogUpdateDbhdr(pLog, sqlite4BtPageData(pOne));
  }
  sqlite4BtPageRelease(pOne);

  return rc;
}

