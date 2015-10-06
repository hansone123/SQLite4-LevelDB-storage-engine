/*
** 2013 March 1
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains C code for a program that links against SQLite
** versions 3 and 4. It contains a few simple performance test routines
** that can be run against either database system.
*/

#include "sqlite4.h"
#include "sqlite3.h"
#include "lsm.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>

#define SQLITE3_DB_FILE "test.db3"
#define SQLITE4_DB_FILE "test.db4"

#include "lsmtest_util.c"

/*
** Unlink database zDb and its supporting files (wal, shm, journal, and log).
** This function works with both lsm and sqlite3 databases.
*/
static int unlink_db(const char *zDb){
  int i;
  char *zBase = (char *)zDb;
  const char *azExt[] = { "", "-shm", "-wal", "-journal", "-log", 0 };

  if( 0==sqlite4_strnicmp("file:", zDb, 5) ){
    for(i=5; zDb[i] && zDb[i]!='?'; i++);
    zBase = sqlite4_mprintf(0, "%.*s", i-5, &zDb[5]);
  }

  for(i=0; azExt[i]; i++){
    char *zFile = sqlite4_mprintf(0, "%s%s", zBase, azExt[i]);
    unlink(zFile);
    sqlite4_free(0, zFile);
  }

  if( zBase!=zDb ){
    sqlite4_free(0, zBase);
  }
  return 0;
}

/*
** Allocate and return a buffer containing the text of CREATE TABLE 
** and CREATE INDEX statements to use as the database schema for the [insert]
** and [select] tests. The schema is similar to:
**
**     CREATE TABLE t1(k PRIMARY KEY, c0 BLOB, c1 BLOB, v BLOB);
**     CREATE INDEX i0 ON t1(c0);
**     CREATE INDEX i1 ON t1(c1);
**
** where the number of "c" columns (and indexes) is determined by the nIdx
** argument. The example above is as would be returned for nIdx==2.
*/
static char *create_schema_sql(int nIdx, int bWithoutRowid){
  char *zSchema;
  int i;

  zSchema = sqlite4_mprintf(0, "CREATE TABLE t1(k PRIMARY KEY,");
  for(i=0; i<nIdx; i++){
    zSchema = sqlite4_mprintf(0, "%z c%d BLOB,", zSchema, i);
  }
  zSchema = sqlite4_mprintf(0, "%z v BLOB)%s;", zSchema,
      (bWithoutRowid ? " WITHOUT ROWID" : "")
  );

  for(i=0; i<nIdx; i++){
    zSchema = sqlite4_mprintf(
      0, "%z\nCREATE INDEX i%d ON t1 (c%d);", zSchema, i, i
    );
  }

  return zSchema;
}

static char *create_insert_sql(int nIdx){
  char *zInsert;
  int i;

  zInsert = sqlite4_mprintf(0, "INSERT INTO t1 VALUES(rblob(:1, 8, 20),");
  for(i=0; i<nIdx; i++){
    zInsert = sqlite4_mprintf(0, "%z rblob((:1<<%d)+:1, 8, 20),", zInsert, i);
  }
  zInsert = sqlite4_mprintf(0, "%z rblob((:1<<%d)+:1, 100, 150));", zInsert, i);

  return zInsert;
}

static char *create_select_sql(int iIdx){
  char *zSql;
  if( iIdx==0 ){
    zSql = sqlite4_mprintf(0, "SELECT * FROM t1 WHERE k = rblob(:1, 8, 20)");
  }else{
    int iCol = iIdx-1;
    zSql = sqlite4_mprintf(0, 
        "SELECT * FROM t1 WHERE c%d = rblob((:1<<%d)+:1, 8, 20)", iCol, iCol
    );
  }
  return zSql;
}

static int do_explode(const char *zLine, int rc, int iLine){
  if( rc ){
    fprintf(stderr, "ERROR: \"%s\" at line %d failed. rc=%d\n", 
        zLine, iLine, rc
    );
    exit(-1);
  }
  return 0;
}
#define EXPLODE(rc) do_explode(#rc, rc, __LINE__)


/*************************************************************************
** Implementations of the rblob(nMin, nMax) function. One for src4 and
** one for sqlite3.
*/

/* src4 implementation */
static void rblobFunc4(sqlite4_context *ctx, int nArg, sqlite4_value **apArg){
  unsigned char aBlob[1000];

  int iSeed = sqlite4_value_int(apArg[0]);
  int nMin = sqlite4_value_int(apArg[1]);
  int nMax = sqlite4_value_int(apArg[2]);
  int nByte;

  nByte = testPrngValue(iSeed + 1000000) & 0x7FFFFFFF;
  nByte = (nByte % (nMax+1-nMin)) + nMin;
  assert( nByte>=nMin && nByte<=nMax );
  if( nByte>sizeof(aBlob) ) nByte = sizeof(aBlob);
  testPrngArray(iSeed, (unsigned int *)aBlob, (nByte+3)/4);

  sqlite4_result_blob(ctx, aBlob, nByte, SQLITE4_TRANSIENT, 0);
}
static void install_rblob_function4(sqlite4 *db){
  testPrngInit();
  sqlite4_create_function(db, "rblob", 3, 0, rblobFunc4, 0, 0, 0);
}

/* sqlite3 implementation */
static void rblobFunc3(sqlite3_context *ctx, int nArg, sqlite3_value **apArg){
  unsigned char aBlob[1000];

  int iSeed = sqlite3_value_int(apArg[0]);
  int nMin = sqlite3_value_int(apArg[1]);
  int nMax = sqlite3_value_int(apArg[2]);
  int nByte;

  nByte = testPrngValue(iSeed + 1000000) & 0x7FFFFFFF;
  nByte = (nByte % (nMax+1-nMin)) + nMin;
  assert( nByte>=nMin && nByte<=nMax );
  if( nByte>sizeof(aBlob) ) nByte = sizeof(aBlob);
  testPrngArray(iSeed, (unsigned int *)aBlob, (nByte+3)/4);

  sqlite3_result_blob(ctx, aBlob, nByte, SQLITE_TRANSIENT);
}
static void install_rblob_function3(sqlite3 *db){
  testPrngInit();
  sqlite3_create_function(db, "rblob", 3, SQLITE_UTF8, 0, rblobFunc3, 0, 0);
}
/*
** End of rblob() implementations.
*************************************************************************/

/*************************************************************************
** Implementations of the rint(iSeed, nRange) function. One for src4 and
** one for sqlite3.
*/

/* sqlite3 implementation */
static void rintFunc3(sqlite3_context *ctx, int nArg, sqlite3_value **apArg){
  int iVal;
  int iSeed = sqlite3_value_int(apArg[0]);
  int nRange = sqlite3_value_int(apArg[1]);

  iVal = testPrngValue(iSeed) & 0x7FFFFFFF;
  if( nRange>0 ){
    iVal = iVal % nRange;
  }
  sqlite3_result_int(ctx, iVal);
}
static void install_rint_function3(sqlite3 *db){
  testPrngInit();
  sqlite3_create_function(db, "rint", 2, SQLITE_UTF8, 0, rintFunc3, 0, 0);
}

/* src4 implementation */
static void rintFunc4(sqlite4_context *ctx, int nArg, sqlite4_value **apArg){
  int iVal;
  int iSeed = sqlite4_value_int(apArg[0]);
  int nRange = sqlite4_value_int(apArg[1]);

  iVal = testPrngValue(iSeed) & 0x7FFFFFFF;
  if( nRange>0 ){
    iVal = iVal % nRange;
  }
  sqlite4_result_int(ctx, iVal);
}
static void install_rint_function4(sqlite4 *db){
  testPrngInit();
  sqlite4_create_function(db, "rint", 2, 0, rintFunc4, 0, 0, 0);
}
/*
** End of rint() implementations.
*************************************************************************/

/*************************************************************************
** Integer query functions for sqlite3 and src4.
*/
static int integer_query4(sqlite4 *db, const char *zSql){
  int iRet;
  sqlite4_stmt *pStmt;

  EXPLODE( sqlite4_prepare(db, zSql, -1, &pStmt, 0) );
  EXPLODE( SQLITE_ROW!=sqlite4_step(pStmt) );
  iRet = sqlite4_column_int(pStmt, 0);
  EXPLODE( sqlite4_finalize(pStmt) );

  return iRet;
}
static int integer_query3(sqlite3 *db, const char *zSql){
  int iRet;
  sqlite3_stmt *pStmt;

  EXPLODE( sqlite3_prepare(db, zSql, -1, &pStmt, 0) );
  EXPLODE( SQLITE_ROW!=sqlite3_step(pStmt) );
  iRet = sqlite3_column_int(pStmt, 0);
  EXPLODE( sqlite3_finalize(pStmt) );

  return iRet;
}
/*
** End of integer query implementations.
*************************************************************************/


static int bt_open(sqlite4_env *pEnv, const char *zFile, sqlite4 **pDb){
  char *zUri = sqlite3_mprintf("file:%s?kv=bt", zFile);
  int rc = sqlite4_open(pEnv, zUri, pDb);
  sqlite3_free(zUri);
  return rc;
}

static int do_insert1_test4(
  const char *zFile,
  int nRow,                       /* Number of rows to insert in total */
  int nRowPerTrans,               /* Number of rows per transaction */
  int nIdx,                       /* Number of aux indexes (aside from PK) */
  int iSync                       /* PRAGMA synchronous value (0, 1 or 2) */
){
  char *zCreateTbl;               /* Create table statement */
  char *zInsert;                  /* INSERT statement */
  sqlite4_stmt *pInsert;          /* Compiled INSERT statement */
  sqlite4 *db = 0;                /* Database handle */
  int i;                          /* Counter to count nRow rows */
  int nMs;                        /* Test time in ms */

  lsm_db *pLsm;

  if( zFile==0 ) zFile = SQLITE4_DB_FILE;
  unlink_db(zFile);
  EXPLODE(  bt_open(0, zFile, &db)  );
  sqlite4_kvstore_control(db, "main", SQLITE4_KVCTRL_LSM_HANDLE, &pLsm);
  i = iSync;
  lsm_config(pLsm, LSM_CONFIG_SAFETY, &i);
  assert( i==iSync );

  install_rblob_function4(db);

  zCreateTbl = create_schema_sql(nIdx, 0);
  zInsert = create_insert_sql(nIdx);

  /* Create the db schema and prepare the INSERT statement */
  EXPLODE(  sqlite4_exec(db, zCreateTbl, 0, 0)  );
  EXPLODE(  sqlite4_prepare(db, zInsert, -1, &pInsert, 0)  );

  /* Run the test */
  testTimeInit();
  for(i=0; i<nRow; i++){
    if( (i % nRowPerTrans)==0 ){
      if( i!=0 ) EXPLODE(  sqlite4_exec(db, "COMMIT", 0, 0)  );
      EXPLODE(  sqlite4_exec(db, "BEGIN", 0, 0)  );
    }
    sqlite4_bind_int(pInsert, 1, i);
    sqlite4_step(pInsert);
    EXPLODE(  sqlite4_reset(pInsert)  );
  }
  EXPLODE(  sqlite4_exec(db, "COMMIT", 0, 0)  );

  /* Free all the stuff allocated above */
  sqlite4_finalize(pInsert);
  sqlite4_free(0, zCreateTbl);
  sqlite4_free(0, zInsert);
  sqlite4_close(db, 0);
  nMs = testTimeGet();

  /* Print out the time taken by the test */
  printf("%.3f seconds\n", (double)nMs / 1000.0);
  return 0;
}
static int do_insert1_test3(
  const char *zFile,
  int nRow,                       /* Number of rows to insert in total */
  int nRowPerTrans,               /* Number of rows per transaction */
  int nIdx,                       /* Number of aux indexes (aside from PK) */
  int iSync                       /* PRAGMA synchronous value (0, 1 or 2) */
){
  char *zCreateTbl;               /* Create table statement */
  char *zInsert;                  /* INSERT statement */
  char *zSync;                    /* "PRAGMA synchronous=" statement */
  sqlite3_stmt *pInsert;          /* Compiled INSERT statement */
  sqlite3 *db = 0;                /* Database handle */
  int i;                          /* Counter to count nRow rows */
  int nMs;                        /* Test time in ms */

  if( zFile==0 ) zFile = SQLITE3_DB_FILE;
  unlink_db(zFile);
  EXPLODE( sqlite3_open(zFile, &db) );
  EXPLODE( sqlite3_exec(db, "PRAGMA journal_mode=WAL", 0, 0, 0) );
  zSync = sqlite4_mprintf(0, "PRAGMA synchronous=%d", iSync);
  EXPLODE( sqlite3_exec(db, zSync, 0, 0, 0) );
  sqlite4_free(0, zSync);

  install_rblob_function3(db);

  zCreateTbl = create_schema_sql(nIdx, 1);
  zInsert = create_insert_sql(nIdx);

  /* Create the db schema and prepare the INSERT statement */
  EXPLODE(  sqlite3_exec(db, zCreateTbl, 0, 0, 0)  );
  EXPLODE(  sqlite3_prepare(db, zInsert, -1, &pInsert, 0)  );

  /* Run the test */
  testTimeInit();
  for(i=0; i<nRow; i++){
    if( (i % nRowPerTrans)==0 ){
      if( i!=0 ) EXPLODE(  sqlite3_exec(db, "COMMIT", 0, 0, 0)  );
      EXPLODE(  sqlite3_exec(db, "BEGIN", 0, 0, 0)  );
    }
    sqlite3_bind_int(pInsert, 1, i);
    sqlite3_step(pInsert);
    EXPLODE(  sqlite3_reset(pInsert)  );
  }
  EXPLODE(  sqlite3_exec(db, "COMMIT", 0, 0, 0)  );

  /* Finalize the statement and close the db. */
  sqlite3_finalize(pInsert);
  sqlite3_close(db);
  nMs = testTimeGet();

  /* Free the stuff allocated above */
  sqlite4_free(0, zCreateTbl);
  sqlite4_free(0, zInsert);

  /* Print out the time taken by the test */
  printf("%.3f seconds\n", (double)nMs / 1000.0);
  return 0;
}

static int do_insert1(int argc, char **argv){
  struct Insert1Arg {
    const char *zArg;
    int nMin;
    int nMax;
  } aArg[] = { 
    {"-db",           3,    4}, 
    {"-rows",         1,    10000000}, 
    {"-rowspertrans", 1,    10000000}, 
    {"-indexes",      0,    20}, 
    {"-sync",         0,    2}, 
    {"-file",         -1,    -1}, 
    {0,0,0}
  };
  int i;

  int iDb = 4;                    /* SQLite 3 or 4 */
  int nRow = 50000;               /* Total rows: 50000 */
  int nRowPerTrans = 10;          /* Total rows each transaction: 50000 */
  int nIdx = 3;                   /* Number of auxilliary indexes */
  int iSync = 1;                  /* PRAGMA synchronous setting */
  const char *zFile = 0;

  for(i=0; i<argc; i++){
    int iSel;
    int iVal;
    int rc;

    rc = testArgSelectX(aArg, "argument", sizeof(aArg[0]), argv[i], &iSel);
    if( rc!=0 ) return -1;
    if( i==argc-1 ){
      fprintf(stderr, "option %s requires an argument\n", aArg[iSel].zArg);
      return -1;
    }
    if( 0==strcmp("-file", aArg[iSel].zArg) ){
      zFile = argv[++i];
    }else{
      iVal = atoi(argv[++i]);
      if( iVal<aArg[iSel].nMin || iVal>aArg[iSel].nMax ){
        fprintf(stderr, "option %s out of range (%d..%d)\n", 
            aArg[iSel].zArg, aArg[iSel].nMin, aArg[iSel].nMax 
            );
        return -1;
      }

      switch( iSel ){
        case 0: iDb = iVal;          break;
        case 1: nRow = iVal;         break;
        case 2: nRowPerTrans = iVal; break;
        case 3: nIdx = iVal;         break;
        case 4: iSync = iVal;        break;
      }
    }
  }

  printf("insert1: db=%d rows=%d rowspertrans=%d indexes=%d sync=%d ... ", 
      iDb, nRow, nRowPerTrans, nIdx, iSync
  );
  fflush(stdout);
  if( iDb==3 ){
    do_insert1_test3(zFile, nRow, nRowPerTrans, nIdx, iSync);
  }else{
    do_insert1_test4(zFile, nRow, nRowPerTrans, nIdx, iSync);
  }

  return 0;
}

static int do_select1_test4(
  const char *zFile,
  int nRow,                       /* Number of rows to read in total */
  int nRowPerTrans,               /* Number of rows per transaction */
  int iIdx
){
  int nMs = 0;
  sqlite4_stmt *pSelect = 0;
  char *zSelect;
  sqlite4 *db;
  int i;
  int nTblRow;

  if( zFile==0 ) zFile = SQLITE4_DB_FILE;
  EXPLODE( bt_open(0, zFile, &db) );
  install_rblob_function4(db);

  nTblRow = integer_query4(db, "SELECT count(*) FROM t1");

  /* Create the db schema and prepare the INSERT statement */
  zSelect = create_select_sql(iIdx);
  EXPLODE(  sqlite4_prepare(db, zSelect, -1, &pSelect, 0)  );

  testTimeInit();
  for(i=0; i<nRow; i++){
    if( (i % nRowPerTrans)==0 ){
      if( i!=0 ) EXPLODE(  sqlite4_exec(db, "COMMIT", 0, 0)  );
      EXPLODE(  sqlite4_exec(db, "BEGIN", 0, 0)  );
    }
    sqlite4_bind_int(pSelect, 1, (i*211)%nTblRow);
    EXPLODE(  SQLITE_ROW!=sqlite4_step(pSelect)  );
    EXPLODE(  sqlite4_reset(pSelect)  );
  }
  EXPLODE(  sqlite4_exec(db, "COMMIT", 0, 0)  );
  nMs = testTimeGet();

  sqlite4_finalize(pSelect);
  sqlite4_close(db, 0);
  sqlite4_free(0, zSelect);

  printf("%.3f seconds\n", (double)nMs / 1000.0);
  return 0;
}
static int do_select1_test3(
  const char *zFile,
  int nRow,                       /* Number of rows to read in total */
  int nRowPerTrans,               /* Number of rows per transaction */
  int iIdx
){
  int nMs = 0;
  sqlite3_stmt *pSelect = 0;
  char *zSelect;
  sqlite3 *db;
  int i;
  int nTblRow;
  
  if( zFile==0 ) zFile = SQLITE3_DB_FILE;
  EXPLODE( sqlite3_open(zFile, &db) );
  install_rblob_function3(db);

  nTblRow = integer_query3(db, "SELECT count(*) FROM t1");

  /* Create the db schema and prepare the INSERT statement */
  zSelect = create_select_sql(iIdx);
  EXPLODE(  sqlite3_prepare(db, zSelect, -1, &pSelect, 0)  );

  testTimeInit();
  for(i=0; i<nRow; i++){
    if( (i % nRowPerTrans)==0 ){
      if( i!=0 ) EXPLODE(  sqlite3_exec(db, "COMMIT", 0, 0, 0)  );
      EXPLODE(  sqlite3_exec(db, "BEGIN", 0, 0, 0)  );
    }
    sqlite3_bind_int(pSelect, 1, (i*211)%nTblRow);
    EXPLODE(  SQLITE_ROW!=sqlite3_step(pSelect)  );
    EXPLODE(  sqlite3_reset(pSelect)  );
  }
  EXPLODE(  sqlite3_exec(db, "COMMIT", 0, 0, 0)  );
  nMs = testTimeGet();

  sqlite3_finalize(pSelect);
  sqlite3_close(db);
  sqlite4_free(0, zSelect);

  printf("%.3f seconds\n", (double)nMs / 1000.0);
  return 0;
}

static int do_select1(int argc, char **argv){
  struct Insert1Arg {
    const char *zArg;
    int nMin;
    int nMax;
  } aArg[] = {
    {"-db",           3,    4}, 
    {"-rows",         1,    10000000}, 
    {"-rowspertrans", 1,    10000000}, 
    {"-index",        0,    21}, 
    {"-file",        -1,    -1}, 
    {0,0,0}
  };
  int i;

  int iDb = 4;                    /* SQLite 3 or 4 */
  int nRow = 50000;               /* Total rows: 50000 */
  int nRowPerTrans = 10;          /* Total rows each transaction: 50000 */
  int iIdx = 0;
  const char *zFile = 0;

  for(i=0; i<argc; i++){
    int iSel;
    int rc;

    rc = testArgSelectX(aArg, "argument", sizeof(aArg[0]), argv[i], &iSel);
    if( rc!=0 ) return -1;
    if( i==argc-1 ){
      fprintf(stderr, "option %s requires an argument\n", aArg[iSel].zArg);
      return -1;
    }
    if( 0==strcmp("-file", aArg[iSel].zArg) ){
      zFile = argv[++i];
    }else{
      int iVal = atoi(argv[++i]);
      if( iVal<aArg[iSel].nMin || iVal>aArg[iSel].nMax ){
        fprintf(stderr, "option %s out of range (%d..%d)\n", 
            aArg[iSel].zArg, aArg[iSel].nMin, aArg[iSel].nMax 
        );
        return -1;
      }

      switch( iSel ){
        case 0: iDb = iVal;          break;
        case 1: nRow = iVal;         break;
        case 2: nRowPerTrans = iVal; break;
        case 3: iIdx = iVal;         break;
      }
    }
  }

  printf("select1: db=%d rows=%d rowspertrans=%d index=%d ... ", 
      iDb, nRow, nRowPerTrans, iIdx
  );
  fflush(stdout);
  if( iDb==3 ){
    do_select1_test3(zFile, nRow, nRowPerTrans, iIdx);
  }else{
    do_select1_test4(zFile, nRow, nRowPerTrans, iIdx);
  }

  return 0;
}

typedef struct SqlDatabase  SqlDatabase;
typedef struct SqlStmt SqlStmt;
typedef struct SqlDatabase3 SqlDatabase3;
typedef struct SqlDatabase4 SqlDatabase4;
struct SqlStmt {
  char *zStmt;
  void *pStmt;
  SqlStmt *pNext;
};
struct SqlDatabase {
  int iDb;                        /* SQLite version (3 or 4) */
  SqlStmt *pSqlStmt;
};
struct SqlDatabase3 {
  SqlDatabase x;                  /* Must be first */
  sqlite3 *db;
};
struct SqlDatabase4 {
  SqlDatabase x;                  /* Must be first */
  sqlite4 *db;
};

static int open_database(
  int iDb, 
  const char *zConfig,
  const char *zFile, 
  int bNew,
  SqlDatabase **pp
){
  int rc = 0;

  assert( iDb==3 || iDb==4 );
  if( zFile==0 ){
    if( iDb==3 ){
      zFile = SQLITE3_DB_FILE;
    }else{
      zFile = SQLITE4_DB_FILE;
    }
  }

  if( bNew ){
    unlink_db(zFile);
  }

  if( iDb==3 ){
    SqlDatabase3 *p = sqlite4_malloc(0, sizeof(SqlDatabase4));
    memset(p, 0, sizeof(SqlDatabase3));
    p->x.iDb = 3;
    rc = sqlite3_open(zFile, &p->db);
    if( rc!=SQLITE_OK ){
      sqlite4_free(0, p);
      p = 0;
    }else{
      sqlite3_exec(p->db, "PRAGMA synchronous=NORMAL", 0, 0, 0);
      sqlite3_exec(p->db, "PRAGMA journal_mode=WAL", 0, 0, 0);
      install_rint_function3(p->db);
      if( zConfig ) {
        rc = sqlite3_exec(p->db, zConfig, 0, 0, 0);
      }
    }
    *pp = (SqlDatabase *)p;
  }else{
    SqlDatabase4 *p = sqlite4_malloc(0, sizeof(SqlDatabase4));
    memset(p, 0, sizeof(SqlDatabase4));
    p->x.iDb = 4;
    rc = bt_open(0, zFile, &p->db);
    if( rc!=SQLITE4_OK ){
      sqlite4_free(0, p);
      p = 0;
    }else{
      install_rint_function4(p->db);
      if( zConfig ) {
        rc = sqlite4_exec(p->db, zConfig, 0, 0);
      }
    }
    *pp = (SqlDatabase *)p;
  }

  return rc;
}

static void close_database(SqlDatabase *pDb){
  assert( pDb->iDb==3 || pDb->iDb==4 );
  if( pDb->iDb==3 ){
    SqlDatabase3 *p = (SqlDatabase3 *)pDb;
    SqlStmt *pSql;
    SqlStmt *pNext;
    for(pSql=pDb->pSqlStmt; pSql; pSql=pNext){
      pNext = pSql->pNext;
      sqlite3_finalize((sqlite3_stmt *)pSql->pStmt);
      sqlite4_free(0, pSql);
    }
    sqlite3_close(p->db);
    sqlite4_free(0, p);
  }else{
    SqlDatabase4 *p = (SqlDatabase4 *)pDb;
    SqlStmt *pSql;
    SqlStmt *pNext;
    for(pSql=pDb->pSqlStmt; pSql; pSql=pNext){
      pNext = pSql->pNext;
      sqlite4_finalize((sqlite4_stmt *)pSql->pStmt);
      sqlite4_free(0, pSql);
    }
    sqlite4_close(p->db, 0);
    sqlite4_free(0, p);
  }
}

static int exec_sql(SqlDatabase *pDb, const char *zSql, const char *zBind, ...){
  int rc = 0;
  void *pNewStmt = 0;
  SqlStmt *pSql;

  /* Search for an existing prepared statement. */
  for(pSql=pDb->pSqlStmt; pSql && strcmp(pSql->zStmt, zSql); pSql=pSql->pNext);

  assert( pDb->iDb==3 || pDb->iDb==4 );
  if( pDb->iDb==3 ){
    int *piOut = 0;
    sqlite3_stmt *pStmt = 0;
    SqlDatabase3 *p = (SqlDatabase3 *)pDb;

    if( pSql==0 ){
      rc = sqlite3_prepare(p->db, zSql, -1, &pStmt, 0);
    }else{
      pStmt = (sqlite3_stmt *)(pSql->pStmt);
    }

    if( zBind ){
      int i;
      va_list ap;

      va_start(ap, zBind);
      for(i=0; zBind[i] && rc==SQLITE_OK; i++){
        switch( zBind[i] ){
          case 'O': {
            piOut = va_arg(ap, int *);
            break;
          }

          case 'i': {
            int iVal = va_arg(ap, int);
            rc = sqlite3_bind_int(pStmt, i+1, iVal);
            break;
          };
          default:
            rc = -1;
            break;
        }
      }
      va_end(ap);
    }

    if( rc==SQLITE_OK ){
      while( SQLITE_ROW==sqlite3_step(pStmt) ){
        if( piOut ) *piOut = sqlite3_column_int(pStmt, 0);
      }
      rc = sqlite3_reset(pStmt);
      pNewStmt = pStmt;
    }else{
      sqlite3_finalize(pStmt);
    }
  }else{
    int *piOut = 0;
    sqlite4_stmt *pStmt = 0;
    SqlDatabase4 *p = (SqlDatabase4 *)pDb;

    if( pSql==0 ){
      rc = sqlite4_prepare(p->db, zSql, -1, &pStmt, 0);
    }else{
      pStmt = (sqlite4_stmt *)(pSql->pStmt);
    }

    if( zBind ){
      int i;
      va_list ap;

      va_start(ap, zBind);
      for(i=0; zBind[i] && rc==SQLITE4_OK; i++){
        switch( zBind[i] ){
          case 'O': {
            piOut = va_arg(ap, int *);
            break;
          }
          case 'i': {
            int iVal = va_arg(ap, int);
            rc = sqlite4_bind_int(pStmt, i+1, iVal);
            break;
          };
          default:
            rc = -1;
            break;
        }
      }
      va_end(ap);
    }

    if( rc==SQLITE4_OK ){
      while( SQLITE4_ROW==sqlite4_step(pStmt) ){
        if( piOut ) *piOut = sqlite4_column_int(pStmt, 0);
      }
      rc = sqlite4_reset(pStmt);
      pNewStmt = pStmt;
    }else{
      sqlite4_finalize(pStmt);
    }
  }

  if( pSql==0 && pNewStmt ){
    int nSql = strlen(zSql);
    pSql = sqlite4_malloc(0, sizeof(SqlStmt) + nSql + 1);
    memset(pSql, 0, sizeof(SqlStmt));
    pSql->zStmt = (char *)&pSql[1];
    memcpy(pSql->zStmt, zSql, nSql+1);
    pSql->pStmt = pNewStmt;
    pSql->pNext = pDb->pSqlStmt;
    pDb->pSqlStmt = pSql;
  }

  return rc;
}

static int do_int_insert_test(
  int iDb,
  const char *zCfg, 
  const char *zFile, 
  int nRow, 
  int nRowPerTrans
){
  int nMs = 0;
  SqlDatabase *db;
  int i;

  EXPLODE( open_database(iDb, zCfg, zFile, 1, &db) );
  EXPLODE( exec_sql(db, 
        "CREATE TABLE t1(k INTEGER PRIMARY KEY, v INTEGER)", 0
  ));

  testTimeInit();
  for(i=0; i<nRow; i++){
    if( (i % nRowPerTrans)==0 ){
      if( i!=0 ) EXPLODE( exec_sql(db, "COMMIT", 0) );
      EXPLODE( exec_sql(db, "BEGIN", 0) );
    }
    EXPLODE( exec_sql(db, 
          "INSERT INTO t1 VALUES(?+1, rint(?,0))", "ii", i, i
    ));
  }
  EXPLODE( exec_sql(db, "COMMIT", 0) );

  nMs = testTimeGet();

  close_database(db);
  printf("%.3f seconds\n", (double)nMs / 1000.0);
  return 0;
}

static int do_int_update_test(
  int iDb,
  const char *zCfg,
  const char *zFile,
  int nRow,
  int nRowPerTrans
){
  int nMs = 0;
  SqlDatabase *db;
  int i;
  int k = 0;
  int v = 0;
  int nTblRow = 0;

  EXPLODE( open_database(iDb, zCfg, zFile, 0, &db) );
  EXPLODE( exec_sql(db, "SELECT count(*) FROM t1", "O", &nTblRow) );

  testTimeInit();
  for(i=0; i<nRow; i++){
    if( (i % nRowPerTrans)==0 ){
      if( i!=0 ) EXPLODE( exec_sql(db, "COMMIT", 0) );
      EXPLODE( exec_sql(db, "BEGIN", 0) );
    }
    k = (testPrngValue(v+i) % nTblRow);
    v = (testPrngValue(k+i) % nTblRow);
    EXPLODE( exec_sql(db, "UPDATE t1 SET v = ? WHERE k = (?+1)", "ii", v, k));
  }
  EXPLODE( exec_sql(db, "COMMIT", 0) );

  nMs = testTimeGet();

  close_database(db);
  printf("%.3f seconds\n", (double)nMs / 1000.0);
  return 0;
}

static int get_int_test_args(
  int argc, char **argv,
  int *piDb,
  int *pnRow,
  int *pnRowPerTrans,
  const char **pzCfg,
  const char **pzFile
){
  struct Insert1Arg {
    const char *zArg;
    int nMin;
    int nMax;
  } aArg[] = {
    {"-db",           3,    4}, 
    {"-rows",         1,    10000000}, 
    {"-rowspertrans", 1,    10000000}, 
    {"-file",        -1,    -1}, 
    {"-config",      -1,    -1}, 
    {0,0,0}
  };
  int i;

  int iDb = 4;                    /* SQLite 3 or 4 */
  int nRow = 50000;               /* Total rows: 50000 */
  int nRowPerTrans = 10;          /* Total rows each transaction: 50000 */
  const char *zFile = 0;
  const char *zCfg = 0;

  for(i=0; i<argc; i++){
    int iSel;
    int rc;

    rc = testArgSelectX(aArg, "argument", sizeof(aArg[0]), argv[i], &iSel);
    if( rc!=0 ) return -1;
    if( i==argc-1 ){
      fprintf(stderr, "option %s requires an argument\n", aArg[iSel].zArg);
      return -1;
    }
    if( 0==strcmp("-config", aArg[iSel].zArg) ){
      zCfg = argv[++i];
    }else if( 0==strcmp("-file", aArg[iSel].zArg) ){
      zFile = argv[++i];
    }else{
      int iVal = atoi(argv[++i]);
      if( iVal<aArg[iSel].nMin || iVal>aArg[iSel].nMax ){
        fprintf(stderr, "option %s out of range (%d..%d)\n", 
            aArg[iSel].zArg, aArg[iSel].nMin, aArg[iSel].nMax 
        );
        return -1;
      }

      switch( iSel ){
        case 0: iDb = iVal;          break;
        case 1: nRow = iVal;         break;
        case 2: nRowPerTrans = iVal; break;
      }
    }
  }

  *piDb = iDb;
  *pnRow = nRow;
  *pnRowPerTrans = nRowPerTrans;
  *pzCfg = zCfg;
  *pzFile = zFile;
  return 0;
}

static int do_int_insert(int argc, char **argv){
  int iDb;
  int nRow;
  int nRowPerTrans;
  const char *zFile;
  const char *zCfg;
  int rc;

  rc = get_int_test_args(argc, argv, &iDb, &nRow, &nRowPerTrans, &zCfg, &zFile);
  if( rc!=0 ) return rc;

  printf("int_insert: db=%d rows=%d rowspertrans=%d ... ", 
      iDb, nRow, nRowPerTrans
  );
  fflush(stdout);
  do_int_insert_test(iDb, zCfg, zFile, nRow, nRowPerTrans);

  return 0;
}

static int do_int_update(int argc, char **argv){
  int iDb;
  int nRow;
  int nRowPerTrans;
  const char *zFile;
  const char *zCfg;
  int rc;

  rc = get_int_test_args(argc, argv, &iDb, &nRow, &nRowPerTrans, &zCfg, &zFile);
  if( rc!=0 ) return rc;

  printf("int_update: db=%d rows=%d rowspertrans=%d ... ", 
      iDb, nRow, nRowPerTrans
  );
  fflush(stdout);
  do_int_update_test(iDb, zCfg, zFile, nRow, nRowPerTrans);

  return 0;
}

int main(int argc, char **argv){
  struct SqltestArg {
    const char *zPrg;
    int (*xPrg)(int, char **);
  } aArg[] = { 
    {"select", do_select1},
    {"insert", do_insert1},
    {"int_insert", do_int_insert},
    {"int_update", do_int_update},
    {0, 0}
  };
  int iSel;
  int rc;

  if( argc<2 ){
    fprintf(stderr, "Usage: %s sub-program...\n", argv[0]);
    return -1;
  }

  rc = testArgSelectX(aArg, "sub-program", sizeof(aArg[0]), argv[1], &iSel);
  if( rc!=0 ) return -1;

  aArg[iSel].xPrg(argc-2, argv+2);
  return 0;
}
