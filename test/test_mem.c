/*
** 2012 July 7
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

#include "sqliteInt.h"
#include "testInt.h"

#if defined(__GLIBC__)
  extern int backtrace(void**,int);
  extern void backtrace_symbols_fd(void*const*,int,int);
# define TM_BACKTRACE 12
#else
# define backtrace(A,B) 1
# define backtrace_symbols_fd(A,B,C)
#endif

typedef struct TmBlockHdr TmBlockHdr;
typedef struct TmAgg TmAgg;
typedef struct Testmem Testmem;

/*
** The object that implements the sqlite4_mm interface for this allocator.
*/
struct Testmem {
  sqlite4_mm base;                /* Base class.  Must be first. */
  sqlite4_mm *p;                  /* Underlying allocator object */
  sqlite4_mutex *mutex;           /* Mutex protecting this object (or NULL) */

  TmBlockHdr *pFirst;             /* List of all outstanding allocations */
#ifdef TM_BACKTRACE
  TmAgg *aHash[10000];            /* Table of all allocations by backtrace() */
#endif
};

struct TmBlockHdr {
  TmBlockHdr *pNext;
  TmBlockHdr *pPrev;
  int nByte;
#ifdef TM_BACKTRACE
  TmAgg *pAgg;
#endif
  u32 iForeGuard;
};

#ifdef TM_BACKTRACE
struct TmAgg {
  int nAlloc;                     /* Number of allocations at this path */
  int nByte;                      /* Total number of bytes allocated */
  int nOutAlloc;                  /* Number of outstanding allocations */
  int nOutByte;                   /* Number of outstanding bytes */
  void *aFrame[TM_BACKTRACE];     /* backtrace() output */
  TmAgg *pNext;                   /* Next object in hash-table collision */
};
#endif

#define FOREGUARD 0x80F5E153
#define REARGUARD 0xE4676B53
static const u32 rearguard = REARGUARD;

#define ROUND8(x) (((x)+7)&~7)
#define BLOCK_HDR_SIZE (ROUND8( sizeof(TmBlockHdr) ))

/*
** Given a user data pointer, return a pointer to the associated 
** TmBlockHdr structure.
*/
static TmBlockHdr *userToBlock(const void *p){
  return (TmBlockHdr *)(((const u8 *)p) - BLOCK_HDR_SIZE);
}

/*
** sqlite4_mm_methods.xMalloc method.
*/
static void *mmDebugMalloc(sqlite4_mm *pMM, sqlite4_size_t nByte){
  Testmem *pTest = (Testmem *)pMM;
  TmBlockHdr *pNew;               /* New allocation header block */
  u8 *pUser;                      /* Return value */
  int nReq;                       /* Total number of bytes requested */

  /* Allocate space for the users allocation, the TmBlockHdr object that
  ** located immediately before the users allocation in memory, and the
  ** 4-byte 'rearguard' located immediately following it. */
  assert( sizeof(rearguard)==4 );
  nReq = BLOCK_HDR_SIZE + nByte + 4;
  pNew = (TmBlockHdr *)sqlite4_mm_malloc(pTest->p, nReq);
  memset(pNew, 0, sizeof(TmBlockHdr));

  sqlite4_mutex_enter(pTest->mutex);

  pNew->iForeGuard = FOREGUARD;
  pNew->nByte = nByte;
  pNew->pNext = pTest->pFirst;

  if( pTest->pFirst ){
    pTest->pFirst->pPrev = pNew;
  }
  pTest->pFirst = pNew;

  pUser = &((u8 *)pNew)[BLOCK_HDR_SIZE];
  memset(pUser, 0x56, nByte);
  memcpy(&pUser[nByte], &rearguard, 4);

#ifdef TM_BACKTRACE
  {
    TmAgg *pAgg;
    int i;
    u32 iHash = 0;
    void *aFrame[TM_BACKTRACE];
    memset(aFrame, 0, sizeof(aFrame));
    backtrace(aFrame, TM_BACKTRACE);

    for(i=0; i<ArraySize(aFrame); i++){
      iHash += (u64)(aFrame[i]) + (iHash<<3);
    }
    iHash = iHash % ArraySize(pTest->aHash);

    for(pAgg=pTest->aHash[iHash]; pAgg; pAgg=pAgg->pNext){
      if( memcmp(pAgg->aFrame, aFrame, sizeof(aFrame))==0 ) break;
    }
    if( !pAgg ){
      pAgg = (TmAgg *)sqlite4_mm_malloc(pTest->p, sizeof(TmAgg));
      memset(pAgg, 0, sizeof(TmAgg));
      memcpy(pAgg->aFrame, aFrame, sizeof(aFrame));
      pAgg->pNext = pTest->aHash[iHash];
      pTest->aHash[iHash] = pAgg;
    }
    pAgg->nAlloc++;
    pAgg->nByte += nByte;
    pAgg->nOutAlloc++;
    pAgg->nOutByte += nByte;
    pNew->pAgg = pAgg;
  }
#endif

  sqlite4_mutex_leave(pTest->mutex);
  return pUser;
}

/*
** sqlite4_mm_methods.xFree method.
*/
static void mmDebugFree(sqlite4_mm *pMM, void *p){
  Testmem *pTest = (Testmem *)pMM;
  if( p ){
    TmBlockHdr *pHdr = userToBlock(p);
    u8 *pUser = (u8 *)p;

    sqlite4_mutex_enter(pTest->mutex);

    assert( pHdr->iForeGuard==FOREGUARD );
    assert( 0==memcmp(&pUser[pHdr->nByte], &rearguard, 4) );

    /* Unlink the TmBlockHdr object from the (Testmem.pFirst) list. */
    if( pHdr->pPrev ){
      assert( pHdr->pPrev->pNext==pHdr );
      pHdr->pPrev->pNext = pHdr->pNext;
    }else{
      assert( pHdr==pTest->pFirst );
      pTest->pFirst = pHdr->pNext;
    }
    if( pHdr->pNext ){
      assert( pHdr->pNext->pPrev==pHdr );
      pHdr->pNext->pPrev = pHdr->pPrev;
    }

#ifdef TM_BACKTRACE
    pHdr->pAgg->nOutAlloc--;
    pHdr->pAgg->nOutByte -= pHdr->nByte;
#endif

    sqlite4_mutex_leave(pTest->mutex);

    memset(pUser, 0x58, pHdr->nByte);
    memset(pHdr, 0x57, sizeof(TmBlockHdr));
    sqlite4_mm_free(pTest->p, pHdr);
  }
}

/*
** sqlite4_mm_methods.xRealloc method.
*/
static void *mmDebugRealloc(sqlite4_mm *pMM, void *p, int nByte){
  void *pNew;

  pNew = sqlite4_mm_malloc(pMM, nByte);
  if( pNew && p ){
    int nOrig = sqlite4_mm_msize(pMM, p);
    memcpy(pNew, p, MIN(nByte, nOrig));
    sqlite4_mm_free(pMM, p);
  }

  return pNew;
}

/*
** sqlite4_mm_methods.xMsize method.
*/
static sqlite4_size_t mmDebugMsize(sqlite4_mm *pMM, void *p){
  if( p==0 ) return 0;
  TmBlockHdr *pHdr = userToBlock(p);
  return pHdr->nByte;
}

/*
** sqlite4_mm_methods.xMember method.
*/
static int mmDebugMember(sqlite4_mm *pMM, const void *p){
  Testmem *pTest = (Testmem *)pMM;
  return sqlite4_mm_member(pTest->p, (const void *)userToBlock(p));
}

/*
** sqlite4_mm_methods.xBenign method.
*/
static void mmDebugBenign(sqlite4_mm *pMM, int bBenign){
  Testmem *pTest = (Testmem *)pMM;
  sqlite4_mm_benign_failures(pTest->p, bBenign);
}

/*
** sqlite4_mm_methods.xStat method.
*/
static sqlite4_int64 mmDebugStat(
  sqlite4_mm *pMM, 
  unsigned int eType, 
  unsigned int flags
){
  Testmem *pTest = (Testmem *)pMM;
  return sqlite4_mm_stat(pTest->p, eType, flags);
}

/*
** Write a report of all mallocs, outstanding and otherwise, to the
** file passed as the second argument. 
*/
static void mmDebugReport(Testmem *pTest, FILE *pFile){
#ifdef TM_BACKTRACE
  int i;
  fprintf(pFile, "LEAKS\n");
  for(i=0; i<ArraySize(pTest->aHash); i++){
    TmAgg *pAgg;
    for(pAgg=pTest->aHash[i]; pAgg; pAgg=pAgg->pNext){
      if( pAgg->nOutAlloc ){
        int j;
        fprintf(pFile, "%d %d ", pAgg->nOutByte, pAgg->nOutAlloc);
        for(j=0; j<TM_BACKTRACE; j++){
          fprintf(pFile, "%p ", pAgg->aFrame[j]);
        }
        fprintf(pFile, "\n");
      }
    }
  }
  fprintf(pFile, "\nALLOCATIONS\n");
  for(i=0; i<ArraySize(pTest->aHash); i++){
    TmAgg *pAgg;
    for(pAgg=pTest->aHash[i]; pAgg; pAgg=pAgg->pNext){
      int j;
      fprintf(pFile, "%d %d ", pAgg->nByte, pAgg->nAlloc);
      for(j=0; j<TM_BACKTRACE; j++) fprintf(pFile, "%p ", pAgg->aFrame[j]);
      fprintf(pFile, "\n");
    }
  }
#else
  (void)pFile;
  (void)pTest;
#endif
}

static int mmDebugCtrl(sqlite4_mm *pMM, unsigned int eType, va_list ap){
  Testmem *pTest = (Testmem *)pMM;
  int rc = SQLITE4_OK;
  if( eType==TESTMEM_CTRL_REPORT ){
    FILE *pFile = va_arg(ap, FILE*);
    mmDebugReport(pTest, pFile);
  }else{
    rc = sqlite4_mm_control_va(pTest->p, eType, ap);
  }
  return rc;
}

/*
** Destroy the allocator object passed as the first argument.
*/
static void mmDebugFinal(sqlite4_mm *pMM){
  Testmem *pTest = (Testmem *)pMM;
  sqlite4_mm *p = pTest->p;
  sqlite4_mm_free(p, (void *)pTest);
  sqlite4_mm_destroy(p);
}

/*
** Create a new debug allocator wrapper around the allocator passed as the
** first argument.
*/
sqlite4_mm *test_mm_debug(sqlite4_mm *p){
  static const sqlite4_mm_methods mmDebugMethods = {
    /* iVersion */    1,
    /* xMalloc  */    mmDebugMalloc,
    /* xRealloc */    mmDebugRealloc,
    /* xFree    */    mmDebugFree,
    /* xMsize   */    mmDebugMsize,
    /* xMember  */    mmDebugMember,
    /* xBenign  */    mmDebugBenign,
    /* xStat    */    mmDebugStat,
    /* xCtrl    */    mmDebugCtrl,
    /* xFinal   */    mmDebugFinal
  };
  Testmem *pTest;
  pTest = (Testmem *)sqlite4_mm_malloc(p, sizeof(Testmem));
  if( pTest ){
    memset(pTest, 0, sizeof(Testmem));
    pTest->base.pMethods = &mmDebugMethods;
    pTest->p = p;
  }
  return (sqlite4_mm *)pTest;
}

/**************************************************************************
** Start of "Fault MM" implementation.
*/
typedef struct FaultMM FaultMM;
struct FaultMM {
  sqlite4_mm base;                /* Base class. Must be first. */
  sqlite4_mm *p;                  /* Underlying allocator object */
  sqlite4_mutex *mutex;           /* Mutex protecting this object (or NULL) */

  int iCnt;                       /* First FAULTCONFIG parameter */
  int bPersistent;                /* Second FAULTCONFIG parameter */
  int bBenign;                    /* Most recent value passed to xBenign */
  int nFault;                     /* Number of faults that have occurred */
  int nBenignFault;               /* Number of benign faults */
};

static int injectOOM(FaultMM *pFault){
  if( pFault->nFault && pFault->bPersistent ) return 1;
  if( pFault->iCnt>0 ){
    pFault->iCnt--;
    if( pFault->iCnt==0 ) return 1;
  }
  return 0;
}

/*
** sqlite4_mm_methods.xMalloc method.
*/
static void *mmFaultMalloc(sqlite4_mm *pMM, sqlite4_size_t nByte){
  FaultMM *pFault = (FaultMM *)pMM;
  if( injectOOM(pFault) ){
    pFault->nFault++;
    if( pFault->bBenign ) pFault->nBenignFault++;
    return 0;
  }
  return sqlite4_mm_malloc(pFault->p, nByte);
}

/*
** sqlite4_mm_methods.xFree method.
*/
static void mmFaultFree(sqlite4_mm *pMM, void *p){
  FaultMM *pFault = (FaultMM *)pMM;
  sqlite4_mm_free(pFault->p, p);
}

/*
** sqlite4_mm_methods.xRealloc method.
*/
static void *mmFaultRealloc(sqlite4_mm *pMM, void *p, int nByte){
  FaultMM *pFault = (FaultMM *)pMM;
  if( injectOOM(pFault) ){
    pFault->nFault++;
    if( pFault->bBenign ) pFault->nBenignFault++;
    return 0;
  }
  return sqlite4_mm_realloc(pFault->p, p, nByte);
}

/*
** sqlite4_mm_methods.xMsize method.
*/
static sqlite4_size_t mmFaultMsize(sqlite4_mm *pMM, void *p){
  FaultMM *pFault = (FaultMM *)pMM;
  return sqlite4_mm_msize(pFault->p, p);
}

/*
** sqlite4_mm_methods.xMember method.
*/
static int mmFaultMember(sqlite4_mm *pMM, const void *p){
  FaultMM *pFault = (FaultMM *)pMM;
  return sqlite4_mm_member(pFault->p, p);
}

/*
** sqlite4_mm_methods.xBenign method.
*/
static void mmFaultBenign(sqlite4_mm *pMM, int bBenign){
  FaultMM *pFault = (FaultMM *)pMM;
  pFault->bBenign = bBenign;
  sqlite4_mm_benign_failures(pFault->p, bBenign);
}

/*
** sqlite4_mm_methods.xStat method.
*/
static sqlite4_int64 mmFaultStat(
  sqlite4_mm *pMM, 
  unsigned int eType, 
  unsigned int flags
){
  FaultMM *pFault = (FaultMM *)pMM;
  return sqlite4_mm_stat(pFault->p, eType, flags);
}

static int mmFaultCtrl(sqlite4_mm *pMM, unsigned int eType, va_list ap){
  FaultMM *pFault = (FaultMM *)pMM;
  int rc = SQLITE4_OK;

  switch( eType ){
    case TESTMEM_CTRL_FAULTCONFIG: {
      int iCnt = va_arg(ap, int);
      int bPersistent = va_arg(ap, int);
      pFault->iCnt = iCnt;
      pFault->bPersistent = bPersistent;
      pFault->nFault = 0;
      pFault->nBenignFault = 0;
      break;
    }

    case TESTMEM_CTRL_FAULTREPORT: {
      int *pnFault = va_arg(ap, int*);
      int *pnBenign = va_arg(ap, int*);
      *pnFault = pFault->nFault;
      *pnBenign = pFault->nBenignFault;
      break;
    }

    default:
      rc = sqlite4_mm_control_va(pFault->p, eType, ap);
      break;
  }

  return rc;
}

/*
** Destroy the allocator object passed as the first argument.
*/
static void mmFaultFinal(sqlite4_mm *pMM){
  Testmem *pTest = (Testmem *)pMM;
  sqlite4_mm *p = pTest->p;
  sqlite4_mm_free(p, (void *)pTest);
  sqlite4_mm_destroy(p);
}

sqlite4_mm *test_mm_faultsim(sqlite4_mm *p){
  static const sqlite4_mm_methods mmFaultMethods = {
    /* iVersion */    1,
    /* xMalloc  */    mmFaultMalloc,
    /* xRealloc */    mmFaultRealloc,
    /* xFree    */    mmFaultFree,
    /* xMsize   */    mmFaultMsize,
    /* xMember  */    mmFaultMember,
    /* xBenign  */    mmFaultBenign,
    /* xStat    */    mmFaultStat,
    /* xCtrl    */    mmFaultCtrl,
    /* xFinal   */    mmFaultFinal
  };
  Testmem *pTest;
  pTest = (Testmem *)sqlite4_mm_malloc(p, sizeof(Testmem));
  if( pTest ){
    memset(pTest, 0, sizeof(Testmem));
    pTest->base.pMethods = &mmFaultMethods;
    pTest->p = p;
  }
  return (sqlite4_mm *)pTest;
}

