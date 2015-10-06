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

#ifndef __BT_H
#define __BT_H

#include <sqlite4.h>

typedef struct bt_db bt_db;
typedef struct bt_cursor bt_cursor;

/* 
** Values that may be passed as the 4th parameter to BtCsrSeek() 
*/
#define BT_SEEK_LEFAST   -2
#define BT_SEEK_LE       -1
#define BT_SEEK_EQ        0
#define BT_SEEK_GE        1

/*
** Open and close a database connection.
*/
int sqlite4BtNew(sqlite4_env*, int nExtra, bt_db **ppDb);
int sqlite4BtClose(bt_db*);
void *sqlite4BtExtra(bt_db*);

/*
** Attach a database connection to a database file.
*/
int sqlite4BtOpen(bt_db*, const char *zFilename);

/*
** Begin(), Commit() and Revert() methods. These functions work in the
** same way as the corresponding sqlite4_kv_methods functions. As follows:
**
** Part of the state maintained by a database handle is the transaction
** level - a non-negative integer. If the transaction level is 0, then
** no read or write transaction is currently open. If the transaction
** level is 1, then a read-only transaction is open. If the transaction
** level is 2 or greater, then (L-1) nested write transactions are open,
** where L is the transaction level. Summary:
**
**   L==0    (no transaction is open)
**   L==1    (a read-only transaction is open)
**   L>=2    ((L-1) nested write transactions are open)
**
** If successful, all three methods set the transaction level to the value
** passed as the second argument. They differ as follows:
**
** Begin(): If (L>=iLevel), this is a no-op. Otherwise, open the read and/or
**          write transactions required to set L to iLevel.
**
** Commit(): If (L<=iLevel), this is a no-op. Otherwise, close as many
**           transactions as required to set L to iLevel. Commit any write
**           transactions closed by this action.
**
** Rollback(): If (L<=iLevel), this is a no-op. Otherwise, rollback and 
**             close as many transactions as required to set L to iLevel. 
**             Then, if iLevel is greater than or equal to 2, rollback
**             (but do not close) the innermost remaining sub-transaction.
**
**    SAVEPOINT one;              -- Begin(2)
**      SAVEPOINT two;            -- Begin(3)
**      ROLLBACK TO one;          -- Rollback(2)
**      SAVEPOINT two;            -- Begin(3)
**        SAVEPOINT three;        -- Begin(4)
**        ROLLBACK TO three;      -- Rollback(5)
**        RELEASE three;          -- Commit(3)
**    RELEASE one;                -- Commit(1)
**
** The TransactionLevel() method returns the current transaction level.
*/
int sqlite4BtBegin(bt_db*, int iLevel);
int sqlite4BtCommit(bt_db*, int iLevel);
int sqlite4BtRollback(bt_db*, int iLevel);
int sqlite4BtTransactionLevel(bt_db*);

/*
** Open and close a database cursor.
*/
int sqlite4BtCsrOpen(bt_db*, int nExtra, bt_cursor **ppCsr);
int sqlite4BtCsrClose(bt_cursor *pCsr);
void *sqlite4BtCsrExtra(bt_cursor *pCsr);

/*
** Return values:
**
**   SQLITE4_OK:
**   SQLITE4_NOTFOUND:
**   SQLITE4_INEXACT:
**   other:
*/
int sqlite4BtCsrSeek(bt_cursor *pCsr, const void *pK, int nK, int eSeek);

int sqlite4BtCsrFirst(bt_cursor *pCsr);
int sqlite4BtCsrLast(bt_cursor *pCsr);

int sqlite4BtCsrNext(bt_cursor *pCsr);
int sqlite4BtCsrPrev(bt_cursor *pCsr);

int sqlite4BtCsrKey(bt_cursor *pCsr, const void **ppK, int *pnK);
int sqlite4BtCsrData(bt_cursor *pCsr, int, int, const void **ppV, int *pnV);

int sqlite4BtReplace(bt_db*, const void *pK, int nK, const void *pV, int nV);
int sqlite4BtDelete(bt_cursor*);

int sqlite4BtSetCookie(bt_db*, unsigned int iVal);
int sqlite4BtGetCookie(bt_db*, unsigned int *piVal);


/*
** kvstore xControl() method.
**
** BT_CONTROL_INFO:
**   If the second argument to sqlite4BtControl() is BT_CONTROL_INFO, then
**   the third is expected to be a pointer to an instance of type bt_info.
**   The "output" buffer must already be initialized. Before 
**   sqlite4BtControl() returns it appends debugging information to the
**   buffer. The specific information appended depends on the eType and
**   pgno member variables.
**
** BT_CONTROL_SETVFS:
**   The third argument is assumed to be a pointer to an instance of type
**   bt_env. The database handle takes a copy of this pointer (not a copy 
**   of the object) and uses it for all subsequent IO. It is the 
**   responsibility of the caller to ensure that the pointer is valid for
**   the lifetime of the database connection.
**
** BT_CONTROL_GETVFS:
**   The third argument is assumed to be of type (bt_env**). Before 
**   returning, the value pointed to is populated with a pointer to 
**   to the current bt_env object.
**
** BT_CONTROL_SAFETY:
**   The third argument is interpreted as a pointer to type (int). If
**   the value stored in the (int) location is 0, 1 or 2, then the current
**   b-tree safety level is set to 0, 1 or 2, respectively. Otherwise, the
**   integer value is set to the current safety level.
**
** BT_CONTROL_AUTOCKPT:
**   The third argument is interpreted as a pointer to type (int). If
**   the indicated value is greater than or equal to zero, then the 
**   database connection auto-checkpoint value is set accordingly. If
**   the indicated value is less than zero, it is set to the current
**   auto-checkpoint value before returning.
**
** BT_CONTROL_LOGSIZE:
**   The third argument is interpreted as a pointer to type (int). The
**   value pointer to is set to the number of uncheckpointed frames
**   that stored in the log file according to the snapshot used by the
**   most recently completed transaction or checkpoint operation.
**
** BT_CONTROL_MULTIPROC:
**   The third argument is interpreted as a pointer to type (int).
**
** BT_CONTROL_LOGSIZECB:
**
** BT_CONTROL_CHECKPOINT:
**
** BT_CONTROL_FAST_INSERT_OP:
**   The third argument is currently unused. This file-control causes the 
**   next call to sqlite4BtReplace() or sqlite4BtCsrOpen() to write to or
**   open a cursor on the "fast-insert" tree. Subsequent operations are
**   unaffected.
**  
**   In other words, an app that uses the fast-insert tree exclusively 
**   must execute this file-control before every call to CsrOpen() or 
**   Replace().
*/
#define BT_CONTROL_INFO           7706389
#define BT_CONTROL_SETVFS         7706390
#define BT_CONTROL_GETVFS         7706391
#define BT_CONTROL_SAFETY         7706392
#define BT_CONTROL_AUTOCKPT       7706393
#define BT_CONTROL_LOGSIZE        7706394
#define BT_CONTROL_MULTIPROC      7706395
#define BT_CONTROL_LOGSIZECB      7706396
#define BT_CONTROL_CHECKPOINT     7706397
#define BT_CONTROL_FAST_INSERT_OP 7706498
#define BT_CONTROL_BLKSZ          7706499
#define BT_CONTROL_PAGESZ         7706500

int sqlite4BtControl(bt_db*, int op, void *pArg);

#define BT_SAFETY_OFF    0
#define BT_SAFETY_NORMAL 1
#define BT_SAFETY_FULL   2

typedef struct bt_info bt_info;
struct bt_info {
  int eType;
  unsigned int pgno;
  sqlite4_buffer output;
};

#define BT_INFO_PAGEDUMP       1
#define BT_INFO_FILENAME       2
#define BT_INFO_HDRDUMP        3
#define BT_INFO_PAGEDUMP_ASCII 4
#define BT_INFO_BLOCK_FREELIST 5
#define BT_INFO_PAGE_FREELIST  6
#define BT_INFO_PAGE_LEAKS     7

typedef struct bt_logsizecb bt_logsizecb;
struct bt_logsizecb {
  void *pCtx;                     /* A copy of this is passed to xLogsize() */
  void (*xLogsize)(void*, int);   /* Callback function */
};

typedef struct bt_checkpoint bt_checkpoint;
struct bt_checkpoint {
  int nFrameBuffer;               /* Minimum number of frames to leave in log */
  int nCkpt;                      /* OUT: Number of frames checkpointed */
};

/*
** File-system interface.
*/
typedef struct bt_env bt_env;
typedef struct bt_file bt_file;

/*
** xFullpath:
*/
struct bt_env {
  void *pVfsCtx;
  int (*xFullpath)(sqlite4_env*,bt_env*, const char *, char **);
  int (*xOpen)(sqlite4_env*,bt_env*, const char *, int flags, bt_file**);
  int (*xSize)(bt_file*, sqlite4_int64*);
  int (*xRead)(bt_file*, sqlite4_int64, void *, int);
  int (*xWrite)(bt_file*, sqlite4_int64, void *, int);
  int (*xTruncate)(bt_file*, sqlite4_int64);
  int (*xSync)(bt_file*);
  int (*xSectorSize)(bt_file*);
  int (*xClose)(bt_file*);
  int (*xUnlink)(sqlite4_env*,bt_env*, const char *);
  int (*xLock)(bt_file*, int, int);
  int (*xTestLock)(bt_file*, int, int, int);
  int (*xShmMap)(bt_file*, int, int, void **);
  void (*xShmBarrier)(bt_file*);
  int (*xShmUnmap)(bt_file*, int);
};

/*
** Flags for xOpen
*/
#define BT_OPEN_DATABASE   0x0001
#define BT_OPEN_LOG        0x0002
#define BT_OPEN_SHARED     0x0004
#define BT_OPEN_READONLY   0x0008

#endif /* ifndef __BT_H */

