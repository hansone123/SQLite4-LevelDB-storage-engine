/* 
 * File:   kvldb.h
 * Author: hanson
 *
 * Created on September 29, 2015, 6:50 PM
 */

#ifndef KVLDB_H
#define	KVLDB_H

#include "c.h"

#define LDB_SEEK_LEFAST   -2
#define LDB_SEEK_LE       -1
#define LDB_SEEK_EQ        0
#define LDB_SEEK_GE        1
        
/* Forward declarations of objects */
typedef struct KVLdb KVLdb;
typedef struct KVLdbCsr KVLdbCsr;


static int kvldbBegin(KVStore *pKVStore, int iLevel);
static int kvldbCommitPhaseOne(KVStore *pKVStore, int iLevel);
static int kvldbCommitPhaseTwo(KVStore *pKVStore, int iLevel);
static int kvldbRollback(KVStore *pKVStore, int iLevel);
static int kvldbRevert(KVStore *pKVStore, int iLevel);
static int kvldbReplace(
  KVStore *pKVStore,
  const KVByteArray *aKey, KVSize nKey,
  const KVByteArray *aData, KVSize nData
);
static int kvldbOpenCursor(KVStore *pKVStore, KVCursor **ppKVCursor);
static int kvldbReset(KVCursor *pKVCursor);
static int kvldbCloseCursor(KVCursor *pKVCursor);
static int kvldbNextEntry(KVCursor *pKVCursor);
static int kvldbPrevEntry(KVCursor *pKVCursor);
static int kvldbSeek(
  KVCursor *pKVCursor, 
  const KVByteArray *aKey,
  KVSize nKey,
  int dir
);
static int kvldbDelete(KVCursor *pKVCursor);
static int kvldbKey(
  KVCursor *pKVCursor,         /* The cursor whose key is desired */
  const KVByteArray **paKey,   /* Make this point to the key */
  KVSize *pN                   /* Make this point to the size of the key */
);
static int kvldbData(
  KVCursor *pKVCursor,         /* The cursor from which to take the data */
  KVSize ofst,                 /* Offset into the data to begin reading */
  KVSize n,                    /* Number of bytes requested */
  const KVByteArray **paData,  /* Pointer to the data written here */
  KVSize *pNData               /* Number of bytes delivered */
);
static int kvldbClose(KVStore *pKVStore);
static int kvldbControl(KVStore *pKVStore, int op, void *pArg);
static int kvldbGetMeta(KVStore *pKVStore, unsigned int *piVal);
static int kvldbPutMeta(KVStore *pKVStore, unsigned int iVal);
static void kvldbPragmaDestroy(void *p);
static void kvldbPragma(sqlite4_context *ctx, int nArg, sqlite4_value **apArg);
static int kvldbGetMethod(
  sqlite4_kvstore *pKVStore, 
  const char *zMethod, 
  void **ppArg,
  void (**pxFunc)(sqlite4_context *, int, sqlite4_value **),
  void (**pxDestroy)(void *)
);

    
#endif	/* KVLDB_H */

