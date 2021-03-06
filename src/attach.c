/*
** 2003 April 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to implement the ATTACH and DETACH commands.
*/
#include "sqliteInt.h"

#ifndef SQLITE4_OMIT_ATTACH
/*
** Resolve an expression that was part of an ATTACH or DETACH statement. This
** is slightly different from resolving a normal SQL expression, because simple
** identifiers are treated as strings, not possible column names or aliases.
**
** i.e. if the parser sees:
**
**     ATTACH DATABASE abc AS def
**
** it treats the two expressions as literal strings 'abc' and 'def' instead of
** looking for columns of the same name.
**
** This only applies to the root node of pExpr, so the statement:
**
**     ATTACH DATABASE abc||def AS 'db2'
**
** will fail because neither abc or def can be resolved.
*/
static int resolveAttachExpr(NameContext *pName, Expr *pExpr)
{
  int rc = SQLITE4_OK;
  if( pExpr ){
    if( pExpr->op!=TK_ID ){
      rc = sqlite4ResolveExprNames(pName, pExpr);
      if( rc==SQLITE4_OK && !sqlite4ExprIsConstant(pExpr) ){
        sqlite4ErrorMsg(pName->pParse, "invalid name: \"%s\"", pExpr->u.zToken);
        return SQLITE4_ERROR;
      }
    }else{
      pExpr->op = TK_STRING;
    }
  }
  return rc;
}

/*
** An SQL user-function registered to do the work of an ATTACH statement. The
** three arguments to the function come directly from an attach statement:
**
**     ATTACH DATABASE x AS y KEY z
**
**     SELECT sqlite_attach(x, y, z)
**
** If the optional "KEY z" syntax is omitted, an SQL NULL is passed as the
** third argument.
*/
static void attachFunc(
  sqlite4_context *context,
  int NotUsed,
  sqlite4_value **argv
){
  int i;
  int rc = 0;
  sqlite4 *db = sqlite4_context_db_handle(context);
  const char *zName;
  const char *zFile;
  char *zPath = 0;
  char *zErr = 0;
  unsigned int flags;
  Db *aNew;
  char *zErrDyn = 0;

  UNUSED_PARAMETER(NotUsed);

  zFile = (const char *)sqlite4_value_text(argv[0], 0);
  zName = (const char *)sqlite4_value_text(argv[1], 0);
  if( zFile==0 ) zFile = "";
  if( zName==0 ) zName = "";

  /* Check for the following errors:
  **
  **     * Too many attached databases,
  **     * Transaction currently open
  **     * Specified database name already being used.
  */
  if( db->nDb>=db->aLimit[SQLITE4_LIMIT_ATTACHED]+2 ){
    zErrDyn = sqlite4MPrintf(db, "too many attached databases - max %d", 
      db->aLimit[SQLITE4_LIMIT_ATTACHED]
    );
    goto attach_error;
  }
  if( db->pSavepoint ){
    zErrDyn = sqlite4MPrintf(db, "cannot ATTACH database within transaction");
    goto attach_error;
  }
  for(i=0; i<db->nDb; i++){
    char *z = db->aDb[i].zName;
    assert( z && zName );
    if( sqlite4_stricmp(z, zName)==0 ){
      zErrDyn = sqlite4MPrintf(db, "database %s is already in use", zName);
      goto attach_error;
    }
  }

  /* Allocate the new entry in the db->aDb[] array and initialise the schema
  ** hash tables.
  */
  if( db->aDb==db->aDbStatic ){
    aNew = sqlite4DbMallocRaw(db, sizeof(db->aDb[0])*3 );
    if( aNew==0 ) return;
    memcpy(aNew, db->aDb, sizeof(db->aDb[0])*2);
  }else{
    aNew = sqlite4DbRealloc(db, db->aDb, sizeof(db->aDb[0])*(db->nDb+1) );
    if( aNew==0 ) return;
  }
  db->aDb = aNew;
  aNew = &db->aDb[db->nDb];
  memset(aNew, 0, sizeof(*aNew));

  /* Open the database file. If the btree is successfully opened, use
  ** it to obtain the database schema. At this point the schema may
  ** or may not be initialised.
  */
  flags = db->openFlags;
  rc = sqlite4ParseUri(db->pEnv, zFile, &flags, &zPath, &zErr);
  if( rc!=SQLITE4_OK ){
    if( rc==SQLITE4_NOMEM ) db->mallocFailed = 1;
    sqlite4_result_error(context, zErr, -1);
    sqlite4_free(db->pEnv, zErr);
    return;
  }
  rc = sqlite4KVStoreOpen(db, zName, zPath, &aNew->pKV, flags);
  sqlite4_free(db->pEnv, zPath);
  db->nDb++;
  if( rc==SQLITE4_CONSTRAINT ){
    rc = SQLITE4_ERROR;
    zErrDyn = sqlite4MPrintf(db, "database is already attached");
  }else if( rc==SQLITE4_OK ){
    aNew->pSchema = sqlite4SchemaGet(db);
    if( !aNew->pSchema ){
      rc = SQLITE4_NOMEM;
    }else if( aNew->pSchema->file_format && aNew->pSchema->enc!=ENC(db) ){
      zErrDyn = sqlite4MPrintf(db, 
        "attached databases must use the same text encoding as main database");
      rc = SQLITE4_ERROR;
    }
  }
  aNew->zName = sqlite4DbStrDup(db, zName);
  if( rc==SQLITE4_OK && aNew->zName==0 ){
    rc = SQLITE4_NOMEM;
  }

  /* If the file was opened successfully, read the schema for the new database.
  ** If this fails, or if opening the file failed, then close the file and 
  ** remove the entry from the db->aDb[] array. i.e. put everything back the way
  ** we found it.
  */
  if( rc==SQLITE4_OK ){
    rc = sqlite4Init(db, &zErrDyn);
  }
  if( rc ){
    int iDb = db->nDb - 1;
    assert( iDb>=2 );
    if( db->aDb[iDb].pKV ){
      sqlite4KVStoreClose(db->aDb[iDb].pKV);
      db->aDb[iDb].pKV = 0;
      db->aDb[iDb].pSchema = 0;
    }
    sqlite4ResetInternalSchema(db, -1);
    db->nDb = iDb;
    if( rc==SQLITE4_NOMEM || rc==SQLITE4_IOERR_NOMEM ){
      db->mallocFailed = 1;
      sqlite4DbFree(db, zErrDyn);
      zErrDyn = sqlite4MPrintf(db, "out of memory");
    }else if( zErrDyn==0 ){
      zErrDyn = sqlite4MPrintf(db, "unable to open database: %s", zFile);
    }
    goto attach_error;
  }
  
  return;

attach_error:
  /* Return an error if we get here */
  if( zErrDyn ){
    sqlite4_result_error(context, zErrDyn, -1);
    sqlite4DbFree(db, zErrDyn);
  }
  if( rc ) sqlite4_result_error_code(context, rc);
}

/*
** An SQL user-function registered to do the work of an DETACH statement. The
** three arguments to the function come directly from a detach statement:
**
**     DETACH DATABASE x
**
**     SELECT sqlite_detach(x)
*/
static void detachFunc(
  sqlite4_context *context,
  int NotUsed,
  sqlite4_value **argv
){
  const char *zName = (const char *)sqlite4_value_text(argv[0], 0);
  sqlite4 *db = sqlite4_context_db_handle(context);
  int i;
  Db *pDb = 0;
  char zErr[128];

  UNUSED_PARAMETER(NotUsed);

  if( zName==0 ) zName = "";
  for(i=0; i<db->nDb; i++){
    pDb = &db->aDb[i];
    if( pDb->pKV==0 ) continue;
    if( sqlite4_stricmp(pDb->zName, zName)==0 ) break;
  }

  if( i>=db->nDb ){
    sqlite4_snprintf(zErr,sizeof(zErr), "no such database: %s", zName);
    goto detach_error;
  }
  if( i<2 ){
    sqlite4_snprintf(zErr,sizeof(zErr), "cannot detach database %s", zName);
    goto detach_error;
  }
  if( db->pSavepoint ){
    sqlite4_snprintf(zErr,sizeof(zErr),
                     "cannot DETACH database within transaction");
    goto detach_error;
  }
  if( pDb->pKV->iTransLevel ){
    sqlite4_snprintf(zErr,sizeof(zErr), "database %s is locked", zName);
    goto detach_error;
  }

  sqlite4KVStoreClose(pDb->pKV);
  pDb->pKV = 0;
  sqlite4SchemaClear(db->pEnv, pDb->pSchema);
  sqlite4DbFree(db, pDb->pSchema);
  pDb->pSchema = 0;
  sqlite4ResetInternalSchema(db, -1);
  return;

detach_error:
  sqlite4_result_error(context, zErr, -1);
}

/*
** This procedure generates VDBE code for a single invocation of either the
** sqlite_detach() or sqlite_attach() SQL user functions.
*/
static void codeAttach(
  Parse *pParse,       /* The parser context */
  int type,            /* Either SQLITE4_ATTACH or SQLITE4_DETACH */
  FuncDef const *pFunc,/* FuncDef wrapper for detachFunc() or attachFunc() */
  Expr *pAuthArg,      /* Expression to pass to authorization callback */
  Expr *pFilename,     /* Name of database file */
  Expr *pDbname,       /* Name of the database to use internally */
  Expr *pKey           /* Database key for encryption extension */
){
  int rc;
  NameContext sName;
  Vdbe *v;
  sqlite4* db = pParse->db;
  int regArgs;

  memset(&sName, 0, sizeof(NameContext));
  sName.pParse = pParse;

  if( 
      SQLITE4_OK!=(rc = resolveAttachExpr(&sName, pFilename)) ||
      SQLITE4_OK!=(rc = resolveAttachExpr(&sName, pDbname)) ||
      SQLITE4_OK!=(rc = resolveAttachExpr(&sName, pKey))
  ){
    pParse->nErr++;
    goto attach_end;
  }

#ifndef SQLITE4_OMIT_AUTHORIZATION
  if( pAuthArg ){
    char *zAuthArg;
    if( pAuthArg->op==TK_STRING ){
      zAuthArg = pAuthArg->u.zToken;
    }else{
      zAuthArg = 0;
    }
    rc = sqlite4AuthCheck(pParse, type, zAuthArg, 0, 0);
    if(rc!=SQLITE4_OK ){
      goto attach_end;
    }
  }
#endif /* SQLITE4_OMIT_AUTHORIZATION */


  v = sqlite4GetVdbe(pParse);
  regArgs = sqlite4GetTempRange(pParse, 4);
  sqlite4ExprCode(pParse, pFilename, regArgs);
  sqlite4ExprCode(pParse, pDbname, regArgs+1);
  sqlite4ExprCode(pParse, pKey, regArgs+2);

  assert( v || db->mallocFailed );
  if( v ){
    sqlite4VdbeAddOp3(v, OP_Function, 0, regArgs+3-pFunc->nArg, regArgs+3);
    assert( pFunc->nArg==-1 || (pFunc->nArg&0xff)==pFunc->nArg );
    sqlite4VdbeChangeP5(v, (u8)(pFunc->nArg));
    sqlite4VdbeChangeP4(v, -1, (char *)pFunc, P4_FUNCDEF);

    /* Code an OP_Expire. For an ATTACH statement, set P1 to true (expire this
    ** statement only). For DETACH, set it to false (expire all existing
    ** statements).
    */
    sqlite4VdbeAddOp1(v, OP_Expire, (type==SQLITE4_ATTACH));
  }
  
attach_end:
  sqlite4ExprDelete(db, pFilename);
  sqlite4ExprDelete(db, pDbname);
  sqlite4ExprDelete(db, pKey);
}

/*
** Called by the parser to compile a DETACH statement.
**
**     DETACH pDbname
*/
void sqlite4Detach(Parse *pParse, Expr *pDbname){
  static const FuncDef detach_func = {
    1,                /* nArg */
    0,                /* flags */
    0,                /* pUserData */
    0,                /* pNext */
    detachFunc,       /* xFunc */
    0,                /* xStep */
    0,                /* xFinalize */
    "sqlite_detach",  /* zName */
    0,                /* pHash */
    0                 /* pDestructor */
  };
  codeAttach(pParse, SQLITE4_DETACH, &detach_func, pDbname, 0, 0, pDbname);
}

/*
** Called by the parser to compile an ATTACH statement.
**
**     ATTACH p AS pDbname KEY pKey
*/
void sqlite4Attach(Parse *pParse, Expr *p, Expr *pDbname, Expr *pKey){
  static const FuncDef attach_func = {
    3,                /* nArg */
    0,                /* flags */
    0,                /* pUserData */
    0,                /* pNext */
    attachFunc,       /* xFunc */
    0,                /* xStep */
    0,                /* xFinalize */
    "sqlite_attach",  /* zName */
    0,                /* pHash */
    0                 /* pDestructor */
  };
  codeAttach(pParse, SQLITE4_ATTACH, &attach_func, p, p, pDbname, pKey);
}
#endif /* SQLITE4_OMIT_ATTACH */

/*
** Initialize a DbFixer structure.  This routine must be called prior
** to passing the structure to one of the sqliteFixAAAA() routines below.
**
** The return value indicates whether or not fixation is required.  TRUE
** means we do need to fix the database references, FALSE means we do not.
*/
int sqlite4FixInit(
  DbFixer *pFix,      /* The fixer to be initialized */
  Parse *pParse,      /* Error messages will be written here */
  int iDb,            /* This is the database that must be used */
  const char *zType,  /* "view", "trigger", or "index" */
  const Token *pName  /* Name of the view, trigger, or index */
){
  sqlite4 *db;

  if( NEVER(iDb<0) || iDb==1 ) return 0;
  db = pParse->db;
  assert( db->nDb>iDb );
  pFix->pParse = pParse;
  pFix->zDb = db->aDb[iDb].zName;
  pFix->zType = zType;
  pFix->pName = pName;
  return 1;
}

/*
** The following set of routines walk through the parse tree and assign
** a specific database to all table references where the database name
** was left unspecified in the original SQL statement.  The pFix structure
** must have been initialized by a prior call to sqlite4FixInit().
**
** These routines are used to make sure that an index, trigger, or
** view in one database does not refer to objects in a different database.
** (Exception: indices, triggers, and views in the TEMP database are
** allowed to refer to anything.)  If a reference is explicitly made
** to an object in a different database, an error message is added to
** pParse->zErrMsg and these routines return non-zero.  If everything
** checks out, these routines return 0.
*/
int sqlite4FixSrcList(
  DbFixer *pFix,       /* Context of the fixation */
  SrcList *pList       /* The Source list to check and modify */
){
  int i;
  const char *zDb;
  SrcListItem *pItem;

  if( NEVER(pList==0) ) return 0;
  zDb = pFix->zDb;
  for(i=0, pItem=pList->a; i<pList->nSrc; i++, pItem++){
    if( pItem->zDatabase==0 ){
      pItem->zDatabase = sqlite4DbStrDup(pFix->pParse->db, zDb);
    }else if( sqlite4_stricmp(pItem->zDatabase,zDb)!=0 ){
      sqlite4ErrorMsg(pFix->pParse,
         "%s %T cannot reference objects in database %s",
         pFix->zType, pFix->pName, pItem->zDatabase);
      return 1;
    }
#if !defined(SQLITE4_OMIT_VIEW) || !defined(SQLITE4_OMIT_TRIGGER)
    if( sqlite4FixSelect(pFix, pItem->pSelect) ) return 1;
    if( sqlite4FixExpr(pFix, pItem->pOn) ) return 1;
#endif
  }
  return 0;
}
#if !defined(SQLITE4_OMIT_VIEW) || !defined(SQLITE4_OMIT_TRIGGER)
int sqlite4FixSelect(
  DbFixer *pFix,       /* Context of the fixation */
  Select *pSelect      /* The SELECT statement to be fixed to one database */
){
  while( pSelect ){
    if( sqlite4FixExprList(pFix, pSelect->pEList) ){
      return 1;
    }
    if( sqlite4FixSrcList(pFix, pSelect->pSrc) ){
      return 1;
    }
    if( sqlite4FixExpr(pFix, pSelect->pWhere) ){
      return 1;
    }
    if( sqlite4FixExpr(pFix, pSelect->pHaving) ){
      return 1;
    }
    pSelect = pSelect->pPrior;
  }
  return 0;
}
int sqlite4FixExpr(
  DbFixer *pFix,     /* Context of the fixation */
  Expr *pExpr        /* The expression to be fixed to one database */
){
  while( pExpr ){
    if( ExprHasAnyProperty(pExpr, EP_TokenOnly) ) break;
    if( ExprHasProperty(pExpr, EP_xIsSelect) ){
      if( sqlite4FixSelect(pFix, pExpr->x.pSelect) ) return 1;
    }else{
      if( sqlite4FixExprList(pFix, pExpr->x.pList) ) return 1;
    }
    if( sqlite4FixExpr(pFix, pExpr->pRight) ){
      return 1;
    }
    pExpr = pExpr->pLeft;
  }
  return 0;
}
int sqlite4FixExprList(
  DbFixer *pFix,     /* Context of the fixation */
  ExprList *pList    /* The expression to be fixed to one database */
){
  int i;
  ExprListItem *pItem;
  if( pList==0 ) return 0;
  for(i=0, pItem=pList->a; i<pList->nExpr; i++, pItem++){
    if( sqlite4FixExpr(pFix, pItem->pExpr) ){
      return 1;
    }
  }
  return 0;
}
#endif

#ifndef SQLITE4_OMIT_TRIGGER
int sqlite4FixTriggerStep(
  DbFixer *pFix,     /* Context of the fixation */
  TriggerStep *pStep /* The trigger step be fixed to one database */
){
  while( pStep ){
    if( sqlite4FixSelect(pFix, pStep->pSelect) ){
      return 1;
    }
    if( sqlite4FixExpr(pFix, pStep->pWhere) ){
      return 1;
    }
    if( sqlite4FixExprList(pFix, pStep->pExprList) ){
      return 1;
    }
    pStep = pStep->pNext;
  }
  return 0;
}
#endif
