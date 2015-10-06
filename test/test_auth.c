/*
** 2013 May 8
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*/
#include <stdio.h>
#include <assert.h>
#include <string.h>

#include "sqlite4.h"
#include "testInt.h"

typedef struct TestAuth TestAuth;
struct TestAuth {
  Tcl_Interp *interp;
  Tcl_Obj *pScript;
  Tcl_Obj *pDestroy;
  sqlite4 *db;
};

static const char *sqlite4TestAuthCode(int rcauth){
  switch( rcauth ){
    case SQLITE4_CREATE_INDEX: return "SQLITE4_CREATE_INDEX";
    case SQLITE4_CREATE_TABLE: return "SQLITE4_CREATE_TABLE";
    case SQLITE4_CREATE_TEMP_INDEX: return "SQLITE4_CREATE_TEMP_INDEX";
    case SQLITE4_CREATE_TEMP_TABLE: return "SQLITE4_CREATE_TEMP_TABLE";
    case SQLITE4_CREATE_TEMP_TRIGGER: return "SQLITE4_CREATE_TEMP_TRIGGER";
    case SQLITE4_CREATE_TEMP_VIEW: return "SQLITE4_CREATE_TEMP_VIEW";
    case SQLITE4_CREATE_TRIGGER: return "SQLITE4_CREATE_TRIGGER";
    case SQLITE4_CREATE_VIEW: return "SQLITE4_CREATE_VIEW";
    case SQLITE4_DELETE: return "SQLITE4_DELETE";
    case SQLITE4_DROP_INDEX: return "SQLITE4_DROP_INDEX";
    case SQLITE4_DROP_TABLE: return "SQLITE4_DROP_TABLE";
    case SQLITE4_DROP_TEMP_INDEX: return "SQLITE4_DROP_TEMP_INDEX";
    case SQLITE4_DROP_TEMP_TABLE: return "SQLITE4_DROP_TEMP_TABLE";
    case SQLITE4_DROP_TEMP_TRIGGER: return "SQLITE4_DROP_TEMP_TRIGGER";
    case SQLITE4_DROP_TEMP_VIEW: return "SQLITE4_DROP_TEMP_VIEW";
    case SQLITE4_DROP_TRIGGER: return "SQLITE4_DROP_TRIGGER";
    case SQLITE4_DROP_VIEW: return "SQLITE4_DROP_VIEW";
    case SQLITE4_INSERT: return "SQLITE4_INSERT";
    case SQLITE4_PRAGMA: return "SQLITE4_PRAGMA";
    case SQLITE4_READ: return "SQLITE4_READ";
    case SQLITE4_SELECT: return "SQLITE4_SELECT";
    case SQLITE4_TRANSACTION: return "SQLITE4_TRANSACTION";
    case SQLITE4_UPDATE: return "SQLITE4_UPDATE";
    case SQLITE4_ATTACH: return "SQLITE4_ATTACH";
    case SQLITE4_DETACH: return "SQLITE4_DETACH";
    case SQLITE4_ALTER_TABLE: return "SQLITE4_ALTER_TABLE";
    case SQLITE4_REINDEX: return "SQLITE4_REINDEX";
    case SQLITE4_ANALYZE: return "SQLITE4_ANALYZE";
    case SQLITE4_CREATE_VTABLE: return "SQLITE4_CREATE_VTABLE";
    case SQLITE4_DROP_VTABLE: return "SQLITE4_DROP_VTABLE";
    case SQLITE4_FUNCTION: return "SQLITE4_FUNCTION";
    case SQLITE4_SAVEPOINT: return "SQLITE4_SAVEPOINT";
  }
  return "unknown";
}

static void testauth_xDel(void *pCtx){
  TestAuth *p = (TestAuth *)pCtx;
  if( p->pDestroy ){
    Tcl_EvalObjEx(p->interp, p->pDestroy, TCL_EVAL_GLOBAL);
    Tcl_DecrRefCount(p->pDestroy);
  }
  Tcl_DecrRefCount(p->pScript);
  ckfree(p);
}

static int testauth_xAuth(
  void *pCtx,
  int code,
  const char *z1,
  const char *z2,
  const char *z3,
  const char *z4
){
  TestAuth *p = (TestAuth *)pCtx;
  Tcl_Interp *interp = p->interp;
  Tcl_Obj *pEval;
  Tcl_Obj *pCode;
  int rc;

  pEval = Tcl_DuplicateObj(p->pScript);
  Tcl_IncrRefCount(pEval);

  pCode = Tcl_NewStringObj(sqlite4TestAuthCode(code), -1);
  if( TCL_OK!=Tcl_ListObjAppendElement(interp, pEval, pCode)
   || TCL_OK!=Tcl_ListObjAppendElement(interp, pEval, Tcl_NewStringObj(z1, -1))
   || TCL_OK!=Tcl_ListObjAppendElement(interp, pEval, Tcl_NewStringObj(z2, -1))
   || TCL_OK!=Tcl_ListObjAppendElement(interp, pEval, Tcl_NewStringObj(z3, -1))
   || TCL_OK!=Tcl_ListObjAppendElement(interp, pEval, Tcl_NewStringObj(z4, -1))
  ){
    rc = -1;
  }else{
    if( TCL_OK!=Tcl_EvalObjEx(p->interp, pEval, TCL_EVAL_GLOBAL) ){
      rc = -2;
    }else{
      const char *zRes = Tcl_GetStringResult(p->interp);
      rc = -3;
      if( strcmp(zRes, "SQLITE4_OK")==0 ) rc = SQLITE4_OK;
      if( strcmp(zRes, "SQLITE4_ALLOW")==0 ) rc = SQLITE4_ALLOW;
      if( strcmp(zRes, "SQLITE4_IGNORE")==0 ) rc = SQLITE4_IGNORE;
      if( strcmp(zRes, "SQLITE4_DENY")==0 ) rc = SQLITE4_DENY;
    }
  }

  Tcl_DecrRefCount(pEval);
  return rc;
}

/*
** sqlite4_authorizer_push DB SCRIPT ?DESTRUCTOR-SCRIPT?
*/
static int testauth_push(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  TestAuth *pNew;
  sqlite4 *db;
  int rc;

  if( objc!=3 && objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB SCRIPT ?DESTRUCTOR-SCRIPT?");
    return TCL_ERROR;
  }
  rc = sqlite4TestDbHandle(interp, objv[1], &db);
  if( rc!=TCL_OK ) return rc;

  pNew = (TestAuth *)ckalloc(sizeof(TestAuth));
  memset(pNew, 0, sizeof(TestAuth));

  pNew->interp = interp;
  pNew->db = db;
  pNew->pScript = Tcl_DuplicateObj(objv[2]);
  Tcl_IncrRefCount(pNew->pScript);
  if( objc==4 ){
    pNew->pDestroy = Tcl_DuplicateObj(objv[3]);
    Tcl_IncrRefCount(pNew->pDestroy);
  }

  rc = sqlite4_authorizer_push(db, (void *)pNew, testauth_xAuth, testauth_xDel);
  if( rc!=SQLITE4_OK ){
    sqlite4TestSetResult(interp, rc);
    return TCL_ERROR;
  }
  Tcl_ResetResult(interp);
  return TCL_OK;
}

/*
** sqlite4_authorizer_pop DB
*/
static int testauth_pop(
  void *clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;
  sqlite4 *db;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  rc = sqlite4TestDbHandle(interp, objv[1], &db);
  if( rc!=TCL_OK ) return rc;

  rc = sqlite4_authorizer_pop(db);
  if( rc ){
    sqlite4TestSetResult(interp, rc);
    return TCL_ERROR;
  }
  Tcl_ResetResult(interp);
  return TCL_OK;
}

int Sqlitetest_auth_init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "sqlite4_authorizer_push", testauth_push, 0, 0);
  Tcl_CreateObjCommand(interp, "sqlite4_authorizer_pop", testauth_pop, 0, 0);
  return TCL_OK;
}

