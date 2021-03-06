/*
** 2007 March 29
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
** This file contains obscure tests of the C-interface required
** for completeness. Test code is written in C for these cases
** as there is not much point in binding to Tcl.
*/
#include "sqliteInt.h"
#include "tcl.h"
#include <stdlib.h>
#include <string.h>

/*
** c_collation_test
*/
static int c_collation_test(
  ClientData clientData, /* Pointer to sqlite4_enable_XXX function */
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int objc,              /* Number of arguments */
  Tcl_Obj *CONST objv[]  /* Command arguments */
){
  const char *zErrFunction = "N/A";
  sqlite4 *db;

  int rc;
  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  /* Open a database. */
  rc = sqlite4_open(0, ":memory:", &db, 0);
  if( rc!=SQLITE4_OK ){
    zErrFunction = "sqlite4_open";
    goto error_out;
  }

  sqlite4_close(db, 0);
  return TCL_OK;

error_out:
  Tcl_ResetResult(interp);
  Tcl_AppendResult(interp, "Error testing function: ", zErrFunction, 0);
  return TCL_ERROR;
}

/*
** c_realloc_test
*/
static int c_realloc_test(
  ClientData clientData, /* Pointer to sqlite4_enable_XXX function */
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int objc,              /* Number of arguments */
  Tcl_Obj *CONST objv[]  /* Command arguments */
){
  void *p;
  const char *zErrFunction = "N/A";

  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  p = sqlite4_malloc(0, 5);
  if( !p ){
    zErrFunction = "sqlite4_malloc";
    goto error_out;
  }

  /* Test that realloc()ing a block of memory to a negative size is
  ** the same as free()ing that memory.
  */
  p = sqlite4_realloc(0, p, -1);
  if( p ){
    zErrFunction = "sqlite4_realloc";
    goto error_out;
  }

  return TCL_OK;

error_out:
  Tcl_ResetResult(interp);
  Tcl_AppendResult(interp, "Error testing function: ", zErrFunction, 0);
  return TCL_ERROR;
}


/*
** c_misuse_test
*/
static int c_misuse_test(
  ClientData clientData, /* Pointer to sqlite4_enable_XXX function */
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int objc,              /* Number of arguments */
  Tcl_Obj *CONST objv[]  /* Command arguments */
){
  const char *zErrFunction = "N/A";
  sqlite4 *db = 0;
  sqlite4_stmt *pStmt;
  int rc;

  if( objc!=1 ){
    Tcl_WrongNumArgs(interp, 1, objv, "");
    return TCL_ERROR;
  }

  /* Open a database. Then close it again. We need to do this so that
  ** we have a "closed database handle" to pass to various API functions.
  */
  rc = sqlite4_open(0, ":memory:", &db, 0);
  if( rc!=SQLITE4_OK ){
    zErrFunction = "sqlite4_open";
    goto error_out;
  }
  sqlite4_close(db, 0);


  rc = sqlite4_errcode(db);
  if( rc!=SQLITE4_MISUSE ){
    zErrFunction = "sqlite4_errcode";
    goto error_out;
  }

  pStmt = (sqlite4_stmt*)1234;
  rc = sqlite4_prepare(db, 0, 0, &pStmt, 0);
  if( rc!=SQLITE4_MISUSE ){
    zErrFunction = "sqlite4_prepare";
    goto error_out;
  }
  assert( pStmt==0 ); /* Verify that pStmt is zeroed even on a MISUSE error */

  return TCL_OK;

error_out:
  Tcl_ResetResult(interp);
  Tcl_AppendResult(interp, "Error testing function: ", zErrFunction, 0);
  return TCL_ERROR;
}

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest9_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
     void *clientData;
  } aObjCmd[] = {
     { "c_misuse_test",    c_misuse_test, 0 },
     { "c_realloc_test",   c_realloc_test, 0 },
     { "c_collation_test", c_collation_test, 0 },
  };
  int i;
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, 
        aObjCmd[i].xProc, aObjCmd[i].clientData, 0);
  }
  return TCL_OK;
}
