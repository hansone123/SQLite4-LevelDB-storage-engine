/*
** 2003 January 11
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to implement the sqlite4_authorizer_push()
** and sqlite4_authorizer_pop() APIs. This facility is an optional feature 
** of the library. Embedded systems that do not need this facility may omit 
** it by recompiling the library with -DSQLITE4_OMIT_AUTHORIZATION=1
*/
#include "sqliteInt.h"

/*
** All of the code in this file may be omitted by defining a single
** macro.
*/
#ifndef SQLITE4_OMIT_AUTHORIZATION

/*
** Each authorizer callback is stored in an instance of this structure.
** The structures themselves are stored in a linked list headed at
** sqlite4.pAuth.
*/
struct Authorizer {
  void *pCtx;
  int (*xAuth)(void*,int,const char*,const char*,const char*,const char*);
  void (*xDestroy)(void*);
  Authorizer *pNext;
};

/*
** Push an authorizer callback onto the stack.
*/
int sqlite4_authorizer_push(
  sqlite4 *db,
  void *pCtx,
  int (*xAuth)(void*,int,const char*,const char*,const char*,const char*),
  void (*xDestroy)(void*)
){
  int rc = SQLITE4_OK;
  Authorizer *pNew;

  sqlite4_mutex_enter(db->mutex);

  pNew = (Authorizer *)sqlite4DbMallocZero(db, sizeof(Authorizer));
  if( pNew==0 ){
    rc = SQLITE4_NOMEM;
    if( xDestroy ) xDestroy(pCtx);
  }else{
    pNew->pCtx = pCtx;
    pNew->xAuth = xAuth;
    pNew->xDestroy = xDestroy;
    pNew->pNext = db->pAuth;
    db->pAuth = pNew;
    sqlite4ExpirePreparedStatements(db);
  }

  sqlite4_mutex_leave(db->mutex);
  return rc;
}

/*
** Pop an authorizer callback from the stack. This version assumes that
** the stack is not empty and that the database handle mutex is held.
*/
static void authPopStack(sqlite4 *db){
  Authorizer *pAuth = db->pAuth;
  db->pAuth = pAuth->pNext;
  if( pAuth->xDestroy ){
    pAuth->xDestroy(pAuth->pCtx);
  }
  sqlite4DbFree(db, pAuth);
}

/*
** Pop an authorizer callback from the stack.
*/
int sqlite4_authorizer_pop(sqlite4 *db){
  int rc = SQLITE4_OK;
  sqlite4_mutex_enter(db->mutex);

  if( db->pAuth==0 ){
    rc = SQLITE4_ERROR;
  }else{
    authPopStack(db);
  }
  sqlite4ExpirePreparedStatements(db);
  sqlite4_mutex_leave(db->mutex);
  return rc;
}

/*
** Free the entire authorization callback stack. This function is called
** as part of closing the database handle.
*/
void sqlite4AuthFreeAll(sqlite4 *db){
  while( db->pAuth ){
    authPopStack(db);
  }
}

/*
** Write an error message into pParse->zErrMsg that explains that the
** user-supplied authorization function returned an illegal value.
*/
static void authBadReturnCode(Parse *pParse){
  sqlite4ErrorMsg(pParse, "authorizer malfunction");
  pParse->rc = SQLITE4_ERROR;
}

/*
** Invoke the authorization callback stack with the supplied parameters.
** If no error occurs, return SQLITE4_OK, SQLITE4_IGNORE or SQLITE4_DENY.
**
** If an authorizer function returns an invalid value, return SQLITE4_DENY
** and leave an error message in pParse.
*/
static int authInvokeStack(
  Parse *pParse,
  int eAuth,                      /* Action code */
  const char *z1,                 /* Third argument for auth callbacks */
  const char *z2,                 /* Fourth argument for auth callbacks */
  const char *z3,                 /* Fifth argument for auth callbacks */
  const char *z4                  /* Sixth argument for auth callbacks */
){
  int rc = SQLITE4_OK;
  Authorizer *p;
  
  for(p=pParse->db->pAuth; p; p=p->pNext){
    int rcauth = p->xAuth(p->pCtx, eAuth, z1, z2, z3, z4);
    if( rcauth!=SQLITE4_OK ){
      switch( rcauth ){
        case SQLITE4_IGNORE:
        case SQLITE4_DENY:
          rc = rcauth;
          /* fall through */

        case SQLITE4_ALLOW:
          break;

        default:
          authBadReturnCode(pParse);
          rc = SQLITE4_DENY;
      }
      break;
    }
  }

  assert( rc==SQLITE4_OK || rc==SQLITE4_DENY || rc==SQLITE4_IGNORE );
  return rc;
}

/*
** Invoke the authorization callback for permission to read column zCol from
** table zTab in database zDb. This function assumes that an authorization
** callback has been registered (i.e. that sqlite4.xAuth is not NULL).
**
** If SQLITE4_IGNORE is returned and pExpr is not NULL, then pExpr is changed
** to an SQL NULL expression. Otherwise, if pExpr is NULL, then SQLITE4_IGNORE
** is treated as SQLITE4_DENY. In this case an error is left in pParse.
*/
int sqlite4AuthReadCol(
  Parse *pParse,                  /* The parser context */
  const char *zTab,               /* Table name */
  const char *zCol,               /* Column name */
  int iDb                         /* Index of containing database. */
){
  const char *zAuthContext = pParse->zAuthContext;
  sqlite4 *db = pParse->db;       /* Database handle */
  char *zDb = db->aDb[iDb].zName; /* Name of attached database */
  int rc;                         /* Auth callback return code */

  rc = authInvokeStack(pParse, SQLITE4_READ, zTab, zCol, zDb, zAuthContext);
  if( rc==SQLITE4_DENY && pParse->rc==SQLITE4_OK ){
    if( db->nDb>2 || iDb!=0 ){
      sqlite4ErrorMsg(pParse, "access to %s.%s.%s is prohibited",zDb,zTab,zCol);
    }else{
      sqlite4ErrorMsg(pParse, "access to %s.%s is prohibited", zTab, zCol);
    }
    pParse->rc = SQLITE4_AUTH;
  }

  return rc;
}

/*
** The pExpr should be a TK_COLUMN expression.  The table referred to
** is in pTabList or else it is the NEW or OLD table of a trigger.  
** Check to see if it is OK to read this particular column.
**
** If the auth function returns SQLITE4_IGNORE, change the TK_COLUMN 
** instruction into a TK_NULL.  If the auth function returns SQLITE4_DENY,
** then generate an error.
*/
void sqlite4AuthRead(
  Parse *pParse,        /* The parser context */
  Expr *pExpr,          /* The expression to check authorization on */
  Schema *pSchema,      /* The schema of the expression */
  SrcList *pTabList     /* All table that pExpr might refer to */
){
  sqlite4 *db = pParse->db;
  Table *pTab = 0;      /* The table being read */
  const char *zCol;     /* Name of the column of the table */
  int iSrc;             /* Index in pTabList->a[] of table being read */
  int iDb;              /* The index of the database the expression refers to */
  int iCol;             /* Index of column in table */

  if( db->pAuth==0 ) return;
  iDb = sqlite4SchemaToIndex(pParse->db, pSchema);
  if( iDb<0 ){
    /* An attempt to read a column out of a subquery or other
    ** temporary table. */
    return;
  }

  assert( pExpr->op==TK_COLUMN || pExpr->op==TK_TRIGGER );
  if( pExpr->op==TK_TRIGGER ){
    pTab = pParse->pTriggerTab;
  }else{
    assert( pTabList );
    for(iSrc=0; ALWAYS(iSrc<pTabList->nSrc); iSrc++){
      if( pExpr->iTable==pTabList->a[iSrc].iCursor ){
        pTab = pTabList->a[iSrc].pTab;
        break;
      }
    }
  }
  iCol = pExpr->iColumn;
  if( NEVER(pTab==0) ) return;

  if( iCol>=0 ){
    assert( iCol<pTab->nCol );
    zCol = pTab->aCol[iCol].zName;
  }else{
    zCol = "ROWID";
  }
  assert( iDb>=0 && iDb<db->nDb );
  if( SQLITE4_IGNORE==sqlite4AuthReadCol(pParse, pTab->zName, zCol, iDb) ){
    pExpr->op = TK_NULL;
  }
}

/*
** Do an authorization check using the code and arguments given.  Return
** either SQLITE4_OK (zero) or SQLITE4_IGNORE or SQLITE4_DENY.  If SQLITE4_DENY
** is returned, then the error count and error message in pParse are
** modified appropriately.
*/
int sqlite4AuthCheck(
  Parse *pParse,
  int code,
  const char *zArg1,
  const char *zArg2,
  const char *zArg3
){
  sqlite4 *db = pParse->db;
  int rc;

  /* Don't do any authorization checks if the database is initialising
  ** or if the parser is being invoked from within sqlite4_declare_vtab.
  */
  if( db->init.busy || IN_DECLARE_VTAB ){
    return SQLITE4_OK;
  }

  rc = authInvokeStack(pParse, code, zArg1, zArg2, zArg3, pParse->zAuthContext);
  if( rc==SQLITE4_DENY && pParse->rc==SQLITE4_OK ){
    sqlite4ErrorMsg(pParse, "not authorized");
    pParse->rc = SQLITE4_AUTH;
  }

  return rc;
}

/*
** Push an authorization context.  After this routine is called, the
** zArg3 argument to authorization callbacks will be zContext until
** popped.  Or if pParse==0, this routine is a no-op.
*/
void sqlite4AuthContextPush(
  Parse *pParse,
  AuthContext *pContext, 
  const char *zContext
){
  assert( pParse );
  pContext->pParse = pParse;
  pContext->zAuthContext = pParse->zAuthContext;
  pParse->zAuthContext = zContext;
}

/*
** Pop an authorization context that was previously pushed
** by sqlite4AuthContextPush
*/
void sqlite4AuthContextPop(AuthContext *pContext){
  if( pContext->pParse ){
    pContext->pParse->zAuthContext = pContext->zAuthContext;
    pContext->pParse = 0;
  }
}

#endif /* SQLITE4_OMIT_AUTHORIZATION */
