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

#include "bt.h"

/* #define BT_STDERR_DEBUG 1 */

typedef sqlite4_int64 i64;
typedef sqlite4_uint64 u64;
typedef unsigned int u32;
typedef unsigned short u16;
typedef unsigned char u8;

typedef struct BtDbHdr BtDbHdr;

/* 
** Special error codes used internally. These must be distinct from SQLite4
** error codes. Which is easy - SQLite4 error codes are all greater than or
** equal to zero.
*/
#define BT_BLOCKFULL -1


/* Number of elements in an array object. */
#define array_size(x) (sizeof(x)/sizeof(x[0]))

/* Number of read-lock slots in shared memory */
#define BT_NREADER 4

#ifndef MIN
# define MIN(a,b) (((a)<(b))?(a):(b))
#endif
#ifndef MAX
# define MAX(a,b) (((a)>(b))?(a):(b))
#endif

/* By default pages are 1024 bytes in size. */
#define BT_DEFAULT_PGSZ 1024

/* By default blocks are 512K bytes in size. */
#define BT_DEFAULT_BLKSZ (512*1024)

/* Default cache size in pages */
#define BT_DEFAULT_CACHESZ 1000

/*
** This structure is the in-memory representation of all data stored in
** the database header at the start of the db file.
**
** pgsz, blksz:
**   Byte offset 0 of the database file is the first byte of both page 1
**   and block 1. Each page is pHdr->pgsz bytes in size. Each block is
**   pHdr->blksz bytes in size. It is guaranteed that the block-size is
**   an integer multiple of the page-size.
**
** iSubBlock, nSubPg:
**   These are likely a stop-gap. When a user executes a 'fast-insert' write,
**   the new key-value pair (or delete marker) is written into a b-tree 
**   stored within block iSubBlock. The root of the tree is always the first
**   page in the block. 
**
**   Variable nSubPg contains the number of pages currently used by the
**   sub-tree, not including any overflow pages. Pages (except overflow 
**   pages) are always allocated contiguously within the block.
**
**   The reason these are likely a stop-gap is that the write-magnification
**   caused by using a b-tree for to populate level-0 sub-trees is too 
**   expensive.
*/
#define BT_DBHDR_STRING "SQLite4 bt database 0001"
struct BtDbHdr {
  char azStr[24];                 /* Copy of BT_DBHDR_STRING */
  u32 pgsz;                       /* Page size in bytes */
  u32 blksz;                      /* Block size in bytes */
  u32 nPg;                        /* Size of database file in pages */

  u32 iRoot;                      /* B-tree root page */
  u32 iMRoot;                     /* Root page of meta-tree */
  u32 iSRoot;                     /* Root page of schedule-tree */

  u32 iSubBlock;                  /* Block containing current sub-tree */
  u32 nSubPg;                     /* Number of non-overflow pages in sub-tree */

  u32 iCookie;                    /* Current value of schema cookie */
  u32 iFreePg;                    /* First page in free-page list trunk */
  u32 iFreeBlk;                   /* First page in free-block list trunk */
};


/*
** This struct defines the format of database "schedule" pages.
**
** eBusy:
*/
typedef struct BtSchedule BtSchedule;
struct BtSchedule {
  u32 eBusy;                      /* One of the BT_SCHEDULE_XXX constants */
  u32 iAge;                       /* Age of input segments */
  u32 iMinLevel;                  /* Minimum level of input segments */
  u32 iMaxLevel;                  /* Maximum level of input segments */
  u32 iOutLevel;                  /* Level at which to write output */
  u32 aBlock[32];                 /* Allocated blocks */

  u32 iNextPg;                    /* Page that contains next input key */
  u32 iNextCell;                  /* Cell that contains next input key */
  u32 iFreeList;                  /* First page of new free-list (if any) */
  u32 aRoot[32];                  /* Root pages for populated blocks */
};

#define BT_SCHEDULE_EMPTY 0
#define BT_SCHEDULE_BUSY  1
#define BT_SCHEDULE_DONE  2

int sqlite4BtMerge(bt_db *db, BtDbHdr *pHdr, u8 *aSched);

/*************************************************************************
** Interface to bt_pager.c functionality.
*/
typedef struct BtPage BtPage;
typedef struct BtPager BtPager;

/*
** Open and close a pager database connection.
*/
int sqlite4BtPagerNew(sqlite4_env*, int nExtra, BtPager **pp);
int sqlite4BtPagerClose(BtPager*);
void *sqlite4BtPagerExtra(BtPager*);

/*
** Attach a database file to a pager object.
*/
int sqlite4BtPagerOpen(BtPager*, const char *zFilename);

/*
** Transactions. These methods are more or less the same as their 
** counterparts in bt.h.
*/
int sqlite4BtPagerBegin(BtPager*, int iLevel);
int sqlite4BtPagerCommit(BtPager*, int iLevel);
int sqlite4BtPagerRevert(BtPager*, int iLevel);
int sqlite4BtPagerRollback(BtPager*, int iLevel);
int sqlite4BtPagerTransactionLevel(BtPager*);

/*
** Query for the database page size. Requires an open read transaction.
*/
int sqlite4BtPagerPagesize(BtPager*);

/* 
** Query for the db header values. Requires an open read transaction or
** an active checkpoint.
*/
BtDbHdr *sqlite4BtPagerDbhdr(BtPager*);
void sqlite4BtPagerDbhdrDirty(BtPager*);

/*
** Used by checkpointers to specify the header to use during a checkpoint.
*/
void sqlite4BtPagerSetDbhdr(BtPager *, BtDbHdr *);

/*
** Read, write and trim existing database pages.
*/
int sqlite4BtPageGet(BtPager*, u32 pgno, BtPage **ppPage);
int sqlite4BtPageTrimPgno(BtPager*, u32 pgno);
int sqlite4BtPageWrite(BtPage*);
int sqlite4BtPageTrim(BtPage*);
int sqlite4BtPageRelease(BtPage*);
void sqlite4BtPageReference(BtPage*);

/*
** Allocate new database pages or blocks.
*/
int sqlite4BtPageAllocate(BtPager*, BtPage **ppPage);
int sqlite4BtBlockAllocate(BtPager*, int nBlk, u32 *piBlk);

/* Block trim */
int sqlite4BtBlockTrim(BtPager*, u32);

/*
** Query page references.
*/
u32 sqlite4BtPagePgno(BtPage*);
void *sqlite4BtPageData(BtPage*);

/*
** Debugging only. Return number of outstanding page references.
*/
int sqlite4BtPagerRefcount(BtPager*);

/* 
** Read/write the schema cookie value.
*/
int sqlite4BtPagerSetCookie(BtPager*, u32 iVal);
int sqlite4BtPagerGetCookie(BtPager*, u32 *piVal);

/*
** Return a pointer to a buffer containing the name of the pager log file.
*/
#define BT_PAGERFILE_DATABASE 0
#define BT_PAGERFILE_LOG      1
#define BT_PAGERFILE_SHM      2
const char *sqlite4BtPagerFilename(BtPager*, int ePagerfile);

bt_env *sqlite4BtPagerGetEnv(BtPager*);
void sqlite4BtPagerSetEnv(BtPager*, bt_env*);

void sqlite4BtPagerSetSafety(BtPager*, int*);
void sqlite4BtPagerSetAutockpt(BtPager*, int*);

void sqlite4BtPagerLogsize(BtPager*, int*);
void sqlite4BtPagerMultiproc(BtPager *pPager, int *piVal);
void sqlite4BtPagerLogsizeCb(BtPager *pPager, bt_logsizecb*);
int sqlite4BtPagerCheckpoint(BtPager *pPager, bt_checkpoint*);

int sqlite4BtPagerHdrdump(BtPager *pPager, sqlite4_buffer *pBuf);

/*
** Write a page buffer directly to the database file.
*/
int sqlite4BtPagerRawWrite(BtPager *pPager, u32 pgno, u8 *aBuf);

/*
** End of bt_pager.c interface.
*************************************************************************/

/*************************************************************************
** File-system interface.
*/

/* Candidate values for the 3rd argument to bt_env.xLock() */
#define BT_LOCK_UNLOCK 0
#define BT_LOCK_SHARED 1
#define BT_LOCK_EXCL   2

/* Size of shared-memory chunks - 48KB. */
#define BT_SHM_CHUNK_SIZE (48*1024)

/* Find the default VFS */
bt_env *sqlite4BtEnvDefault(void);

/*
** End of file system interface.
*************************************************************************/

/*************************************************************************
** Interface to bt_varint.c functionality.
**
** All this is just copied from SQLite4 proper. It is a bit ridiculous.
*/
int sqlite4BtVarintPut32(u8 *, int);
int sqlite4BtVarintGet32(u8 *, int *);
int sqlite4BtVarintPut64(u8 *aData, i64 iVal);
int sqlite4BtVarintGet64(const u8 *aData, i64 *piVal);
int sqlite4BtVarintLen32(int);
int sqlite4BtVarintSize(u8 c);
/*
** End of bt_varint.c interface.
*************************************************************************/

/*************************************************************************
** Interface to bt_log.c functionality.
*/
typedef struct BtLog BtLog;
int sqlite4BtLogOpen(BtPager*, int bRecover, BtLog**);
int sqlite4BtLogClose(BtLog*, int bCleanup);

int sqlite4BtLogRead(BtLog*, u32 pgno, u8 *aData);
int sqlite4BtLogWrite(BtLog*, u32 pgno, u8 *aData, u32 nPg);

int sqlite4BtLogSnapshotOpen(BtLog*);
int sqlite4BtLogSnapshotClose(BtLog*);

int sqlite4BtLogSnapshotWrite(BtLog*);
int sqlite4BtLogSnapshotEndWrite(BtLog*);

int sqlite4BtLogSize(BtLog*);
int sqlite4BtLogCheckpoint(BtLog*, int);

int sqlite4BtLogFrameToIdx(u32 *aLog, u32 iFrame);

#if 0
int sqlite4BtLogPagesize(BtLog*);
int sqlite4BtLogPagecount(BtLog*);
u32 sqlite4BtLogCookie(BtLog*);
#endif
BtDbHdr *sqlite4BtLogDbhdr(BtLog*);

int sqlite4BtLogSetCookie(BtLog*, u32 iCookie);
int sqlite4BtLogDbhdrFlush(BtLog*);
void sqlite4BtLogReloadDbHdr(BtLog*);

/*
** End of bt_log.c interface.
*************************************************************************/

/*************************************************************************
** Interface to bt_lock.c functionality.
*/
typedef struct BtShared BtShared;
typedef struct BtLock BtLock;
typedef struct BtReadSlot BtReadSlot;
typedef struct BtFile BtFile;

struct BtLock {
  /* These three are set by the bt_pager module and thereafter used by 
  ** the bt_lock, bt_pager and bt_log modules. */
  sqlite4_env *pEnv;              /* SQLite environment */
  bt_env *pVfs;                   /* Bt environment */
  bt_file *pFd;                   /* Database file descriptor */
  int iDebugId;                   /* Sometimes useful when debugging */

  /* Global configuration settings:
  **
  ** nAutoCkpt:
  **   If a transaction is committed and there are this many frames in the
  **   log file, automatically run a checkpoint operation.
  **
  ** iSafetyLevel:
  **   Current safety level. 0==off, 1==normal, 2=full.
  */
  int iSafetyLevel;               /* 0==OFF, 1==NORMAL, 2==FULL */
  int nAutoCkpt;                  /* Auto-checkpoint when log is this large */
  int bRequestMultiProc;          /* Request multi-proc support */
  int nBlksz;                     /* Requested block-size in bytes */
  int nPgsz;                      /* Requested page-size in bytes */

  /* These are used only by the bt_lock module. */
  BtShared *pShared;              /* Shared by all handles on this file */
  BtLock *pNext;                  /* Next connection using pShared */
  u32 mExclLock;                  /* Mask of exclusive locks held */
  u32 mSharedLock;                /* Mask of shared locks held */
  BtFile *pBtFile;                /* Used to defer close if necessary */

  u8 *aUsed;
};

struct BtReadSlot {
  u32 iFirst;
  u32 iLast;
};

/* Connect and disconnect procedures */
int sqlite4BtLockConnect(BtLock*, int (*xRecover)(BtLock*));
int sqlite4BtLockDisconnect(BtLock*, int(*xCkpt)(BtLock*), int(*xDel)(BtLock*));

/* Obtain and release the WRITER lock */
int sqlite4BtLockWriter(BtLock*);
int sqlite4BtLockWriterUnlock(BtLock*);

/* Obtain and release CHECKPOINTER lock */
int sqlite4BtLockCkpt(BtLock*);
int sqlite4BtLockCkptUnlock(BtLock*);

/* Obtain and release READER locks.  */
int sqlite4BtLockReader(BtLock*, u32 *aLog, u32 iFirst, BtReadSlot *aLock);
int sqlite4BtLockReaderUnlock(BtLock*);

/* Query READER locks.  */
int sqlite4BtLockReaderQuery(BtLock*, u32*, BtReadSlot*, u32*, int*);

/* Obtain pointers to shared-memory chunks */
int sqlite4BtLockShmMap(BtLock*, int iChunk, int nByte, u8 **ppOut);

/*
** End of bt_lock.c interface.
*************************************************************************/

/*************************************************************************
** Utility functions.
*/
void sqlite4BtPutU32(u8 *a, u32 i);
u32 sqlite4BtGetU32(const u8 *a);
void sqlite4BtBufAppendf(sqlite4_buffer *pBuf, const char *zFormat, ...);

/*
** End of utility interface.
*************************************************************************/

#ifdef NDEBUG
# define sqlite4BtDebugReadPage(a,b,c,d)
# define sqlite4BtDebugKV(a,b,c,d,e,f)
# define sqlite4BtDebugReadlock(a,b,c)
# define sqlite4BtDebugPageAlloc(a,b,c)
# define sqlite4BtDebugPageFree(a,b,c,d)
# define btErrorBkpt(x) x
#else
void sqlite4BtDebugReadPage(BtLock *pLock, u32 pgno, u8 *aData, int pgsz);
void sqlite4BtDebugKV(BtLock*, const char*,u8 *pK, int nK, u8 *pV, int nV);
void sqlite4BtDebugReadlock(BtLock *pLock, u32 iFirst, u32 iLast);
void sqlite4BtDebugPageAlloc(BtLock *pLock, const char*, u32);
void sqlite4BtDebugPageFree(BtLock *pLock, int, const char*, u32);
int btErrorBkpt(int rc);
#endif

