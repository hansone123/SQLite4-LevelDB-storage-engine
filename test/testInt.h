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

#ifndef __TEST_INT_H
#define __TEST_INT_H

#include <tcl.h>


/* test_auth.c */
int Sqlitetest_auth_init(Tcl_Interp *interp);


/* test_func.c */
int Sqlitetest_func_Init(Tcl_Interp *);

/* test_main.c */
void sqlite4TestInit(Tcl_Interp*);
void *sqlite4TestTextToPtr(const char *z);
int sqlite4TestDbHandle(Tcl_Interp *, Tcl_Obj *, sqlite4 **);
int sqlite4TestSetResult(Tcl_Interp *interp, int rc);

/* test_mem.c */

/*
** The following values are interpreted by the xCtrl() methods of the 
** special sqlite4_mm objects used for testing.
**
** TESTMEM_CTRL_REPORT:
**   Write a report concerning all mallocs, outstanding and otherwise,
**   to the open file (type FILE*) passed as the third argument.
**
** TESTMEM_CTRL_FAULTCONFIG:
**   Used to configure the OOM fault injector. The third and fourth
**   arguments passed to the sqlite4_mm_control() call should be of
**   type int. 
**
**   The first is the number of allocation requests that will succeed,
**   plus one, before the next failure is injected. In other words, 
**   passing a value of 1 means the very next allocation attempt will
**   fail. If the second argument is non-zero, then all allocations
**   following a failure also fail (persistent OOM). Otherwise, subsequent
**   allocations succeed (transient OOM).
**
**   Passing zero as the first integer argument effectively disables
**   fault injection - no OOM faults will be injected until after 
**   TESTMEM_CTRL_FAULTCONFIG is called again.
**
** TESTMEM_CTRL_FAULTREPORT:
**   Used to query the OOM fault injector. The third and fourth arguments
**   should both be of type (int*). Before returning, the value pointed
**   to by the third argument is set to the total number of faults injected
**   since the most recent call to FAULTCONFIG. The location pointed to
**   by the fourth argument is set to the number of "benign" faults that 
**   occurred.
*/
#define TESTMEM_CTRL_REPORT         62930001
#define TESTMEM_CTRL_FAULTCONFIG    62930002
#define TESTMEM_CTRL_FAULTREPORT    62930003

sqlite4_mm *test_mm_debug(sqlite4_mm *p);
sqlite4_mm *test_mm_faultsim(sqlite4_mm *p);

/* test_num.c */
int Sqlitetest_num_init(Tcl_Interp *interp);

/* test_bt.c */
int SqlitetestBt_Init(Tcl_Interp *interp);

#endif

