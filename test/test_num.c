/*
** 2013 May 24
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Code for testing the sqlite4_num_xxx() functions used by SQLite for
** base 10 arithmatic.
*/

#include "sqlite4.h"
#include <tcl.h>
#include <string.h>

#define NUM_FORMAT "sign:%d approx:%d e:%d m:%llu"

/* Append a return value representing a sqlite4_num.
*/
static void append_num_result( Tcl_Interp *interp, sqlite4_num A ){
  char buf[100];
  sprintf( buf, NUM_FORMAT, A.sign, A.approx, A.e, A.m );
  Tcl_AppendResult(interp, buf, 0);
}

/* Convert a string either representing a sqlite4_num (listing its fields as
** returned by append_num_result) or that can be parsed as one. Invalid
** strings become NaN.
*/
static sqlite4_num test_parse_num( char *arg ){
  sqlite4_num A;
  int sign, approx, e;
  if( sscanf( arg, NUM_FORMAT, &sign, &approx, &e, &A.m)==4 ){
    A.sign = sign;
    A.approx = approx;
    A.e = e;
    return A;
  } else {
    return sqlite4_num_from_text(arg, -1, 0, 0);
  }
}

/* Convert return values of sqlite4_num to strings that will be readable in
** the tests.
*/
static char *describe_num_comparison( int code ){
  switch( code ){
    case 0: return "incomparable";
    case 1: return "lesser";
    case 2: return "equal";
    case 3: return "greater";
    default: return "error"; 
  }
}

/* Compare two numbers A and B. Returns "incomparable", "lesser", "equal",
** "greater", or "error".
*/
static int test_num_compare(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite4_num A, B;
  int cmp;
  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " NUM NUM\"", 0);
    return TCL_ERROR;
  }
  
  A = test_parse_num( argv[1] );
  B = test_parse_num( argv[2] );
  cmp = sqlite4_num_compare(A, B);
  Tcl_AppendResult( interp, describe_num_comparison( cmp ), 0);
  return TCL_OK; 
}

/* Create a sqlite4_num from a string. The optional second argument specifies
** how many bytes may be read.
*/
static int test_num_from_text(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite4_num A;
  int len;
  int input_len;
  int flags;
  int encoding = 0;
  int i;
  char *utf16_text = 0;
  char *text;
  if( argc<2 || argc>4 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
      " STRING\" or \"", argv[0], " STRING INTEGER\" or \"",
      argv[0], " STRING INTEGER STRING\"", 0);
    return TCL_ERROR;
  }

  if( argc>=3 ){
    if ( Tcl_GetInt(interp, argv[2], &len) ) return TCL_ERROR; 
  }else{
    len = -1;
  }

  flags = 0;
  if( argc>=4 ){
    if( strchr(argv[3], 'w') ) flags |= SQLITE4_IGNORE_WHITESPACE;
    if( strchr(argv[3], 'p') ) flags |= SQLITE4_PREFIX_ONLY; 
    if( strchr(argv[3], 'b') ) encoding = SQLITE4_UTF16BE; 
    if( strchr(argv[3], 'l') ) encoding = SQLITE4_UTF16LE; 
  }
  
 if( encoding==SQLITE4_UTF16BE || encoding==SQLITE4_UTF16LE ){
    flags |= encoding;
    input_len = strlen(argv[1]); 
    utf16_text = sqlite4_malloc(0, 2*(input_len+1));
    if( !utf16_text ){
      Tcl_AppendResult(interp, "utf16 string allocation failed");
      return TCL_ERROR;
    }
    for( i=0; i<2*(input_len+1); i++ ){
      utf16_text[i] = 0;
    }
    for( i=0; i<input_len; i++ ){
      utf16_text[ i*2+ (encoding==SQLITE4_UTF16BE) ] = argv[1][i];
    }
    text = utf16_text;
  }else{
    text = argv[1];
  }

  A = sqlite4_num_from_text(text, len, flags, 0);
  append_num_result(interp, A);
  if( utf16_text ){
    sqlite4_free(0, utf16_text);
  }
  return TCL_OK;
}

static int test_num_text_is_real(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  int pbReal;
  int len;
  if( argc!=2 && argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
      " STRING\" or \"", argv[0], " STRING INTEGER\"", 0);
    return TCL_ERROR;
  }

  if( argc==3 ){
    if ( Tcl_GetInt(interp, argv[2], &len) ) return TCL_ERROR; 
  }else{
    len = -1;
  }

  sqlite4_num_from_text(argv[1], len, 0, &pbReal);
  Tcl_AppendResult( interp, pbReal ? "true" : "false", 0 );
  return TCL_OK;
}

static int test_num_to_text(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  char text[30];
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
      " NUM\"", 0);
    return TCL_ERROR;
  }
  sqlite4_num_to_text( test_parse_num( argv[1] ), text, 0 );
  Tcl_AppendResult( interp, text, 0 );
  return TCL_OK;
}

static int test_num_binary_op(
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv,            /* Text of each argument */
  sqlite4_num (*op) (sqlite4_num, sqlite4_num)
){
  sqlite4_num A, B, R;
  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
      " NUM NUM\"", 0);
    return TCL_ERROR;
  }
  A = test_parse_num(argv[1]);
  B = test_parse_num(argv[2]);
  R = op(A, B);
  append_num_result(interp, R);
  return TCL_OK;
}

static int test_num_add(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  return test_num_binary_op( interp, argc, argv, sqlite4_num_add );
}

static int test_num_sub(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  return test_num_binary_op( interp, argc, argv, sqlite4_num_sub );
}

static int test_num_mul(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  return test_num_binary_op( interp, argc, argv, sqlite4_num_mul );
}

static int test_num_div(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  return test_num_binary_op( interp, argc, argv, sqlite4_num_div );
}

static int test_num_predicate(
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv,            /* Text of each argument */
  int (*pred) (sqlite4_num)
){
  sqlite4_num A;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
      " NUM\"", 0);
    return TCL_ERROR;
  }
  A = test_parse_num(argv[1]);
  Tcl_AppendResult(interp, pred(A) ? "true" : "false", 0);  
  return TCL_OK;
}

static int test_num_isinf(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  return test_num_predicate( interp, argc, argv, sqlite4_num_isinf );
}

static int test_num_isnan(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  return test_num_predicate( interp, argc, argv, sqlite4_num_isnan );
}


static int test_num_from_double(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  sqlite4_num ret;
  double val;
  
  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "NUMBER");
    return TCL_ERROR;
  }
  if( Tcl_GetDoubleFromObj(interp, objv[1], &val) ){
    return TCL_ERROR;
  }

  ret = sqlite4_num_from_double(val);

  Tcl_ResetResult(interp);
  append_num_result(interp, ret);
  return TCL_OK;
}


static int test_num_to_int32(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite4_num A;
  int lossy;
  int iVal;
  char buf[50];
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
      " NUM\"", 0);
    return TCL_ERROR;
  }
  A = test_parse_num(argv[1]);
  iVal = sqlite4_num_to_int32(A, &lossy);
  sprintf( buf, "%s%d", lossy?"~":"", iVal );
  Tcl_AppendResult(interp, buf, 0);
  return TCL_OK;
}

static int test_num_to_int64(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite4_num A;
  int lossy;
  sqlite4_int64 iVal;
  char buf[50];
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
      " NUM\"", 0);
    return TCL_ERROR;
  }
  A = test_parse_num(argv[1]);
  iVal = sqlite4_num_to_int64(A, &lossy);
  sprintf( buf, "%s%lld", lossy?"~":"", iVal );
  Tcl_AppendResult(interp, buf, 0);
  return TCL_OK;
}


/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest_num_init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_CmdProc *xProc;
  } aCmd[] = {
     { "sqlite4_num_compare",           (Tcl_CmdProc*)test_num_compare      }, 
     { "sqlite4_num_from_text",         (Tcl_CmdProc*)test_num_from_text    }, 
     { "sqlite4_num_text_is_real",      (Tcl_CmdProc*)test_num_text_is_real }, 
     { "sqlite4_num_to_text",           (Tcl_CmdProc*)test_num_to_text      },
     { "sqlite4_num_add",               (Tcl_CmdProc*)test_num_add          },
     { "sqlite4_num_sub",               (Tcl_CmdProc*)test_num_sub          },
     { "sqlite4_num_mul",               (Tcl_CmdProc*)test_num_mul          },
     { "sqlite4_num_div",               (Tcl_CmdProc*)test_num_div          },
     { "sqlite4_num_isinf",             (Tcl_CmdProc*)test_num_isinf        },
     { "sqlite4_num_isnan",             (Tcl_CmdProc*)test_num_isnan        },
     { "sqlite4_num_to_int32",          (Tcl_CmdProc*)test_num_to_int32     },
     { "sqlite4_num_to_int64",          (Tcl_CmdProc*)test_num_to_int64     },
  };

  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
     void *clientData;
  } aObjCmd[] = {
     { "sqlite4_num_from_double", test_num_from_double, 0 },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateCommand(interp, aCmd[i].zName, aCmd[i].xProc, 0, 0);
  }
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, aObjCmd[i].xProc, 0, 0);
  }

  return TCL_OK;
}


