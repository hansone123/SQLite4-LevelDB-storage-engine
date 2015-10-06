/*
** 2013-01-01
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
** This file contains the implementation of the "sqlite4_mm" memory
** allocator object.
*/
#include "sqliteInt.h"

/*************************************************************************
** The SQLITE4_MM_SYSTEM memory allocator.  This allocator uses the
** malloc/realloc/free from the system library.  It also tries to use
** the memory allocation sizer from the system library if such a routine
** exists.  If there is no msize in the system library, then each allocation
** is increased in size by 8 bytes and the size of the allocation is stored
** in those initial 8 bytes.
**
** C-preprocessor macro summary:
**
**    HAVE_MALLOC_USABLE_SIZE     The configure script sets this symbol if
**                                the malloc_usable_size() interface exists
**                                on the target platform.  Or, this symbol
**                                can be set manually, if desired.
**                                If an equivalent interface exists by
**                                a different name, using a separate -D
**                                option to rename it.  This symbol will
**                                be enabled automatically on windows
**                                systems, and malloc_usable_size() will
**                                be redefined to _msize(), unless the
**                                SQLITE4_WITHOUT_MSIZE macro is defined.
**    
**    SQLITE4_WITHOUT_ZONEMALLOC   Some older macs lack support for the zone
**                                memory allocator.  Set this symbol to enable
**                                building on older macs.
**
**    SQLITE4_WITHOUT_MSIZE        Set this symbol to disable the use of
**                                _msize() on windows systems.  This might
**                                be necessary when compiling for Delphi,
**                                for example.
*/

/*
** Windows systems have malloc_usable_size() but it is called _msize().
** The use of _msize() is automatic, but can be disabled by compiling
** with -DSQLITE4_WITHOUT_MSIZE
*/
#if !defined(HAVE_MALLOC_USABLE_SIZE) && SQLITE4_OS_WIN \
      && !defined(SQLITE4_WITHOUT_MSIZE)
# define HAVE_MALLOC_USABLE_SIZE 1
# define SQLITE4_MALLOCSIZE _msize
#endif

#if defined(__APPLE__) && !defined(SQLITE4_WITHOUT_ZONEMALLOC)

/*
** Use the zone allocator available on apple products unless the
** SQLITE4_WITHOUT_ZONEMALLOC symbol is defined.
*/
#include <sys/sysctl.h>
#include <malloc/malloc.h>
#include <libkern/OSAtomic.h>
static malloc_zone_t* _sqliteZone_;
#define SQLITE4_MALLOC(x) malloc_zone_malloc(_sqliteZone_, (x))
#define SQLITE4_FREE(x) malloc_zone_free(_sqliteZone_, (x));
#define SQLITE4_REALLOC(x,y) malloc_zone_realloc(_sqliteZone_, (x), (y))
#define SQLITE4_MALLOCSIZE(x) \
        (_sqliteZone_ ? _sqliteZone_->size(_sqliteZone_,x) : malloc_size(x))

#else /* if not __APPLE__ */

/*
** Use standard C library malloc and free on non-Apple systems.  
** Also used by Apple systems if SQLITE4_WITHOUT_ZONEMALLOC is defined.
*/
#define SQLITE4_MALLOC(x)    malloc(x)
#define SQLITE4_FREE(x)      free(x)
#define SQLITE4_REALLOC(x,y) realloc((x),(y))

#ifdef HAVE_MALLOC_USABLE_SIZE
# ifndef SQLITE4_MALLOCSIZE
#  include <malloc.h>
#  define SQLITE4_MALLOCSIZE(x) malloc_usable_size(x)
# endif
#else
# undef SQLITE4_MALLOCSIZE
#endif

#endif /* __APPLE__ or not __APPLE__ */

/*
** Implementations of core routines
*/
static void *mmSysMalloc(sqlite4_mm *pMM, sqlite4_size_t iSize){
#ifdef SQLITE4_MALLOCSIZE
  return SQLITE4_MALLOC(iSize);
#else
  unsigned char *pRes = SQLITE4_MALLOC(iSize+8);
  if( pRes ){
    *(sqlite4_size_t*)pRes = iSize;
    pRes += 8;
  }
  return pRes;
#endif
}
static void *mmSysRealloc(sqlite4_mm *pMM, void *pOld, sqlite4_size_t iSz){
#ifdef SQLITE4_MALLOCSIZE
  return SQLITE4_REALLOC(pOld, iSz);
#else
  unsigned char *pRes;
  if( pOld==0 ) return mmSysMalloc(pMM, iSz);
  pRes = (unsigned char*)pOld;
  pRes -= 8;
  pRes = SQLITE4_REALLOC(pRes, iSz+8);
  if( pRes ){
    *(sqlite4_size_t*)pRes = iSz;
    pRes += 8;
  }
  return pRes;
#endif 
}
static void mmSysFree(sqlite4_mm *pNotUsed, void *pOld){
#ifdef SQLITE4_MALLOCSIZE
  SQLITE4_FREE(pOld);
#else
  unsigned char *pRes;
  if( pOld==0 ) return;
  pRes = (unsigned char *)pOld;
  pRes -= 8;
  SQLITE4_FREE(pRes);
#endif
}
static sqlite4_size_t mmSysMsize(sqlite4_mm *pNotUsed, void *pOld){
#ifdef SQLITE4_MALLOCSIZE
  return SQLITE4_MALLOCSIZE(pOld);
#else
  unsigned char *pX;
  if( pOld==0 ) return 0;
  pX = (unsigned char *)pOld;
  pX -= 8;
  return *(sqlite4_size_t*)pX;
#endif
}

static const sqlite4_mm_methods mmSysMethods = {
  /* iVersion */    1,
  /* xMalloc  */    mmSysMalloc,
  /* xRealloc */    mmSysRealloc,
  /* xFree    */    mmSysFree,
  /* xMsize   */    mmSysMsize,
  /* xMember  */    0,
  /* xBenign  */    0,
  /* xStat    */    0,
  /* xCtrl    */    0,
  /* xFinal   */    0
};
sqlite4_mm sqlite4MMSystem =  {
  /* pMethods */    &mmSysMethods
};

/* The system memory allocator is the default. */
sqlite4_mm *sqlite4_mm_default(void){ return &sqlite4MMSystem; }

/*************************************************************************
** The SQLITE4_MM_OVERFLOW memory allocator.
**
** This memory allocator has two child memory allocators, A and B.  Always
** try to fulfill the request using A first, then overflow to B if the request
** on A fails.  The A allocator must support the xMember method.
*/
struct mmOvfl {
  sqlite4_mm base;    /* Base class - must be first */
  int (*xMemberOfA)(sqlite4_mm*, const void*);
  sqlite4_mm *pA;     /* Primary memory allocator */
  sqlite4_mm *pB;     /* Backup memory allocator in case pA fails */
};

static void *mmOvflMalloc(sqlite4_mm *pMM, sqlite4_size_t iSz){
  const struct mmOvfl *pOvfl = (const struct mmOvfl*)pMM;
  void *pRes;
  pRes = pOvfl->pA->pMethods->xMalloc(pOvfl->pA, iSz);
  if( pRes==0 ){
    pRes = pOvfl->pB->pMethods->xMalloc(pOvfl->pB, iSz);
  }
  return pRes;
}
static void *mmOvflRealloc(sqlite4_mm *pMM, void *pOld, sqlite4_size_t iSz){
  const struct mmOvfl *pOvfl;
  void *pRes, *pAlt;
  if( pOld==0 ) return mmOvflMalloc(pMM, iSz);
  pOvfl = (const struct mmOvfl*)pMM;
  if( pOvfl->xMemberOfA(pOvfl->pA, pOld) ){
    pRes = pOvfl->pA->pMethods->xRealloc(pOvfl->pA, pOld, iSz);
    if( pRes==0 && (pAlt = pOvfl->pB->pMethods->xMalloc(pOvfl->pB, iSz))!=0 ){
      sqlite4_size_t nOld = pOvfl->pA->pMethods->xMsize(pOvfl->pA, pOld);
      assert( nOld<iSz );
      memcpy(pAlt, pOld, (size_t)nOld);
      pOvfl->pA->pMethods->xFree(pOvfl->pA, pOld);
      pRes = pAlt;
    }
  }else{
    pRes = pOvfl->pB->pMethods->xRealloc(pOvfl->pB, pOld, iSz);
  }
  return pRes;
}
static void mmOvflFree(sqlite4_mm *pMM, void *pOld){
  const struct mmOvfl *pOvfl;
  if( pOld==0 ) return;
  pOvfl = (const struct mmOvfl*)pMM;
  if( pOvfl->xMemberOfA(pOvfl->pA, pOld) ){
    pOvfl->pA->pMethods->xFree(pOvfl->pA, pOld);
  }else{
    pOvfl->pB->pMethods->xFree(pOvfl->pB, pOld);
  }
}
static sqlite4_size_t mmOvflMsize(sqlite4_mm *pMM, void *pOld){
  const struct mmOvfl *pOvfl;
  sqlite4_size_t iSz;
  if( pOld==0 ) return 0;
  pOvfl = (const struct mmOvfl*)pMM;
  if( pOvfl->xMemberOfA(pOvfl->pA, pOld) ){
    iSz = sqlite4_mm_msize(pOvfl->pA, pOld);
  }else{
    iSz = sqlite4_mm_msize(pOvfl->pB, pOld);
  }
  return iSz;
}
static int mmOvflMember(sqlite4_mm *pMM, const void *pOld){
  const struct mmOvfl *pOvfl;
  int iRes;
  if( pOld==0 ) return 0;
  pOvfl = (const struct mmOvfl*)pMM;
  if( pOvfl->xMemberOfA(pOvfl->pA, pOld) ){
    iRes = 1;
  }else{
    iRes = sqlite4_mm_member(pOvfl->pB, pOld);
  }
  return iRes;
}
static void mmOvflBenign(sqlite4_mm *pMM, int bEnable){
  struct mmOvfl *pOvfl = (struct mmOvfl*)pMM;
  sqlite4_mm_benign_failures(pOvfl->pA, bEnable);
  sqlite4_mm_benign_failures(pOvfl->pB, bEnable);
}
static void mmOvflFinal(sqlite4_mm *pMM){
  struct mmOvfl *pOvfl = (struct mmOvfl*)pMM;
  sqlite4_mm *pA = pOvfl->pA;
  sqlite4_mm *pB = pOvfl->pB;
  mmOvflFree(pMM, pMM);
  sqlite4_mm_destroy(pA);
  sqlite4_mm_destroy(pB);
}
static const sqlite4_mm_methods mmOvflMethods = {
  /* iVersion */    1,
  /* xMalloc  */    mmOvflMalloc,
  /* xRealloc */    mmOvflRealloc,
  /* xFree    */    mmOvflFree,
  /* xMsize   */    mmOvflMsize,
  /* xMember  */    mmOvflMember,
  /* xBenign  */    mmOvflBenign,
  /* xStat    */    0,
  /* xCtrl    */    0,
  /* xFinal   */    mmOvflFinal
};
static sqlite4_mm *mmOvflNew(sqlite4_mm *pA, sqlite4_mm *pB){
  struct mmOvfl *pOvfl;
  if( pA->pMethods->xMember==0 ) return 0;
  pOvfl = sqlite4_mm_malloc(pA, sizeof(*pOvfl));
  if( pOvfl==0 ){
    pOvfl = sqlite4_mm_malloc(pB, sizeof(*pOvfl));
  }
  if( pOvfl ){
    pOvfl->base.pMethods = &mmOvflMethods;
    pOvfl->xMemberOfA = pA->pMethods->xMember;
    pOvfl->pA = pA;
    pOvfl->pB = pB;
  }
  return &pOvfl->base;
}

/*************************************************************************
** The SQLITE4_MM_STATS memory allocator.
*/

/*
** Number of available statistics. Statistics are assigned ids starting at
** one, not zero.
*/
#define MM_STATS_NSTAT 8

struct mmStats {
  sqlite4_mm base;                /* Base class.  Must be first. */
  sqlite4_mm *p;                  /* Underlying allocator object */
  sqlite4_mutex *mutex;           /* Mutex protecting aStat[] (or NULL) */

  i64 nOut;                       /* Number of bytes outstanding */
  i64 nOutHw;                     /* Highwater mark of nOut */
  i64 nUnit;                      /* Number of allocations outstanding */
  i64 nUnitHw;                    /* Highwater mark of nUnit */
  i64 nMaxRequest;                /* Largest request seen so far */
  i64 nFault;                     /* Number of malloc or realloc failures */
};

static void updateStatsMalloc(
  struct mmStats *pStats, 
  void *pNew, 
  sqlite4_size_t iSz
){
  /* Statistic SQLITE4_MMSTAT_SIZE records the largest allocation request
  ** that has been made so far. So if iSz is larger than the current value,
  ** set MMSTAT_SIZE to iSz now. This statistic is updated regardless of 
  ** whether or not the allocation succeeded.  */ 
  if( iSz>pStats->nMaxRequest ){
    pStats->nMaxRequest = iSz;
  }

  /* If the allocation succeeded, increase the number of allocations and
  ** bytes outstanding accordingly. Also update the highwater marks if
  ** required. If the allocation failed, increment the fault count.  */
  if( pNew ){
    pStats->nOut += sqlite4_mm_msize(pStats->p, pNew);
    pStats->nUnit += 1;
    if( pStats->nOut>pStats->nOutHw ) pStats->nOutHw = pStats->nOut;
    if( pStats->nUnit>pStats->nUnitHw ) pStats->nUnitHw = pStats->nUnit;
  }else{
    pStats->nFault++;
  }
}

static void *mmStatsMalloc(sqlite4_mm *pMM, sqlite4_size_t iSz){
  struct mmStats *pStats = (struct mmStats*)pMM;
  void *pRet;

  pRet = sqlite4_mm_malloc(pStats->p, iSz);
  sqlite4_mutex_enter(pStats->mutex);
  updateStatsMalloc(pStats, pRet, iSz);
  sqlite4_mutex_leave(pStats->mutex);
  return pRet;
}

static void mmStatsFree(sqlite4_mm *pMM, void *pOld){
  struct mmStats *pStats = (struct mmStats*)pMM;
  sqlite4_mutex_enter(pStats->mutex);
  if( pOld ){
    sqlite4_size_t nByte = sqlite4_mm_msize(pMM, pOld);
    pStats->nOut -= nByte;
    pStats->nUnit -= 1;
  }
  sqlite4_mutex_leave(pStats->mutex);
  sqlite4_mm_free(pStats->p, pOld);
}

static void *mmStatsRealloc(sqlite4_mm *pMM, void *pOld, sqlite4_size_t iSz){
  struct mmStats *pStats = (struct mmStats*)pMM;
  sqlite4_size_t nOrig = (pOld ? sqlite4_mm_msize(pStats->p, pOld) : 0);
  void *pRet;

  pRet = sqlite4_mm_realloc(pStats->p, pOld, iSz);

  sqlite4_mutex_enter(pStats->mutex);
  if( pRet ){
    pStats->nOut -= nOrig;
    if( pOld ) pStats->nUnit--;
  }
  updateStatsMalloc(pStats, pRet, iSz);
  sqlite4_mutex_leave(pStats->mutex);

  return pRet;
}

static sqlite4_size_t mmStatsMsize(sqlite4_mm *pMM, void *pOld){
  struct mmStats *pStats = (struct mmStats*)pMM;
  return sqlite4_mm_msize(pStats->p, pOld);
}

static int mmStatsMember(sqlite4_mm *pMM, const void *pOld){
  struct mmStats *pStats = (struct mmStats*)pMM;
  return sqlite4_mm_member(pStats->p, pOld);
}

/*
** sqlite4_mm_methods.xBenign method.
*/
static void mmStatsBenign(sqlite4_mm *pMM, int bBenign){
  struct mmStats *pStats = (struct mmStats *)pMM;
  sqlite4_mm_benign_failures(pStats->p, bBenign);
}


static sqlite4_int64 mmStatsStat(
  sqlite4_mm *pMM, 
  unsigned int eType, 
  unsigned int flags
){
  struct mmStats *pStats = (struct mmStats*)pMM;
  i64 iRet = 0;
  sqlite4_mutex_enter(pStats->mutex);
  switch( eType ){
    case SQLITE4_MMSTAT_OUT: {
      iRet = pStats->nOut;
      break;
    }
    case SQLITE4_MMSTAT_OUT_HW: {
      iRet = pStats->nOutHw;
      if( flags & SQLITE4_MMSTAT_RESET ) pStats->nOutHw = pStats->nOut;
      break;
    }
    case SQLITE4_MMSTAT_UNITS: {
      iRet = pStats->nUnit;
      break;
    }
    case SQLITE4_MMSTAT_UNITS_HW: {
      iRet = pStats->nUnitHw;
      if( flags & SQLITE4_MMSTAT_RESET ) pStats->nUnitHw = pStats->nUnit;
      break;
    }
    case SQLITE4_MMSTAT_SIZE: {
      iRet = pStats->nMaxRequest;
      if( flags & SQLITE4_MMSTAT_RESET ) pStats->nMaxRequest = 0;
      break;
    }
    case SQLITE4_MMSTAT_MEMFAULT:
    case SQLITE4_MMSTAT_FAULT: {
      iRet = pStats->nFault;
      if( flags & SQLITE4_MMSTAT_RESET ) pStats->nFault = 0;
      break;
    }
  }
  sqlite4_mutex_leave(pStats->mutex);
  return iRet;
}

static int mmStatsCtrl(sqlite4_mm *pMM, unsigned int eType, va_list ap){
  struct mmStats *pStats = (struct mmStats*)pMM;
  return sqlite4_mm_control_va(pStats->p, eType, ap);
}

/*
** Destroy the allocator object passed as the first argument.
*/
static void mmStatsFinal(sqlite4_mm *pMM){
  struct mmStats *pStats = (struct mmStats*)pMM;
  sqlite4_mm *p = pStats->p;
  sqlite4_mm_free(p, pStats);
  sqlite4_mm_destroy(p);
}

static const sqlite4_mm_methods mmStatsMethods = {
  /* iVersion */    1,
  /* xMalloc  */    mmStatsMalloc,
  /* xRealloc */    mmStatsRealloc,
  /* xFree    */    mmStatsFree,
  /* xMsize   */    mmStatsMsize,
  /* xMember  */    mmStatsMember,
  /* xBenign  */    mmStatsBenign,
  /* xStat    */    mmStatsStat,
  /* xCtrl    */    mmStatsCtrl,
  /* xFinal   */    mmStatsFinal
};

/*
** Allocate a new stats allocator.
*/
static sqlite4_mm *mmStatsNew(sqlite4_mm *p){
  struct mmStats *pNew;

  pNew = (struct mmStats *)sqlite4_mm_malloc(p, sizeof(*pNew));
  if( pNew ){
    memset(pNew, 0, sizeof(*pNew));
    pNew->p = p;
    pNew->base.pMethods = &mmStatsMethods;
  }

  return (sqlite4_mm *)pNew;
}


/*************************************************************************
** The SQLITE4_MM_ONESIZE memory allocator.
**
** All memory allocations are rounded up to a single size, "sz".  A request
** for an allocation larger than sz bytes fails.  All allocations come out
** of a single initial buffer with "cnt" chunks of "sz" bytes each.
**
** Space to hold the sqlite4_mm object comes from the first block in the
** allocation space.
*/
struct mmOnesz {
  sqlite4_mm base;            /* Base class.  Must be first. */
  const void *pSpace;         /* Space to allocate */
  const void *pLast;          /* Last possible allocation */
  struct mmOneszBlock *pFree; /* List of free blocks */
  int sz;                     /* Size of each allocation */
  unsigned nFailSize;         /* Failures due to size */
  unsigned nFailMem;          /* Failures due to OOM */
  unsigned nSlot;             /* Number of available slots */
  unsigned nUsed;             /* Current number of slots in use */
  unsigned nUsedHw;           /* Highwater mark for slots in use */
  sqlite4_size_t mxSize;      /* Maximum request size */
};

/* A free block in the buffer */
struct mmOneszBlock {
  struct mmOneszBlock *pNext;  /* Next on the freelist */
};

static void *mmOneszMalloc(sqlite4_mm *pMM, sqlite4_size_t iSz){
  struct mmOnesz *pOnesz = (struct mmOnesz*)pMM;
  void *pRes;
  if( iSz>pOnesz->mxSize ) pOnesz->mxSize = iSz;
  if( iSz>pOnesz->sz ){ pOnesz->nFailSize++; return 0; }
  if( pOnesz->pFree==0 ){ pOnesz->nFailMem++;  return 0; }
  pOnesz->nUsed++;
  if( pOnesz->nUsed>pOnesz->nUsedHw ) pOnesz->nUsedHw = pOnesz->nUsed;
  pRes = pOnesz->pFree;
  pOnesz->pFree = pOnesz->pFree->pNext;
  return pRes;
}
static void mmOneszFree(sqlite4_mm *pMM, void *pOld){
  struct mmOnesz *pOnesz = (struct mmOnesz*)pMM;
  if( pOld ){
    struct mmOneszBlock *pBlock = (struct mmOneszBlock*)pOld;
    pBlock->pNext = pOnesz->pFree;
    pOnesz->pFree = pBlock;
    pOnesz->nUsed--;
  }
}
static void *mmOneszRealloc(sqlite4_mm *pMM, void *pOld, sqlite4_size_t iSz){
  struct mmOnesz *pOnesz = (struct mmOnesz*)pMM;
  if( pOld==0 ) return mmOneszMalloc(pMM, iSz);
  if( iSz<=0 ){
    mmOneszFree(pMM, pOld);
    return 0;
  }
  if( iSz>pOnesz->sz ) return 0;
  return pOld;
}
static sqlite4_size_t mmOneszMsize(sqlite4_mm *pMM, void *pOld){
  struct mmOnesz *pOnesz = (struct mmOnesz*)pMM;
  return pOld ? pOnesz->sz : 0;  
}
static int mmOneszMember(sqlite4_mm *pMM, const void *pOld){
  struct mmOnesz *pOnesz = (struct mmOnesz*)pMM;
  return pOld && pOld>=pOnesz->pSpace && pOld<=pOnesz->pLast;
}
static sqlite4_int64 mmOneszStat(
  sqlite4_mm *pMM, 
  unsigned int eType, 
  unsigned int flgs
){
  struct mmOnesz *pOnesz = (struct mmOnesz*)pMM;
  sqlite4_int64 x = -1;
  switch( eType ){
    case SQLITE4_MMSTAT_OUT: {
      x = pOnesz->nUsed*pOnesz->sz;
      break;
    }
    case SQLITE4_MMSTAT_OUT_HW: {
      x = pOnesz->nUsedHw*pOnesz->sz;
      if( flgs & SQLITE4_MMSTAT_RESET ) pOnesz->nUsedHw = pOnesz->nUsed;
      break;
    }
    case SQLITE4_MMSTAT_UNITS: {
      x = pOnesz->nUsed;
      break;
    }
    case SQLITE4_MMSTAT_UNITS_HW: {
      x = pOnesz->nUsedHw;
      if( flgs & SQLITE4_MMSTAT_RESET ) pOnesz->nUsedHw = pOnesz->nUsed;
      break;
    }
    case SQLITE4_MMSTAT_SIZE: {
      x = pOnesz->mxSize;
      if( flgs & SQLITE4_MMSTAT_RESET ) pOnesz->mxSize = 0;
      break;
    }
    case SQLITE4_MMSTAT_SZFAULT: {
      x = pOnesz->nFailSize;
      if( flgs & SQLITE4_MMSTAT_RESET ) pOnesz->nFailSize = 0;
      break;
    }
    case SQLITE4_MMSTAT_MEMFAULT: {
      x = pOnesz->nFailMem;
      if( flgs & SQLITE4_MMSTAT_RESET ) pOnesz->nFailMem = 0;
      break;
    }
    case SQLITE4_MMSTAT_FAULT: {
      x = pOnesz->nFailSize + pOnesz->nFailMem;
      if( flgs & SQLITE4_MMSTAT_RESET ){
        pOnesz->nFailSize = 0;
        pOnesz->nFailMem = 0;
      }
      break;
    }
  }
  return x;
}
static const sqlite4_mm_methods mmOneszMethods = {
  /* iVersion */    1,
  /* xMalloc  */    mmOneszMalloc,
  /* xRealloc */    mmOneszRealloc,
  /* xFree    */    mmOneszFree,
  /* xMsize   */    mmOneszMsize,
  /* xMember  */    mmOneszMember,
  /* xBenign  */    0,
  /* xStat    */    mmOneszStat,
  /* xCtrl    */    0,
  /* xFinal   */    0
};
static sqlite4_mm *mmOneszNew(void *pSpace, int sz, int cnt){
  struct mmOnesz *pOnesz;
  unsigned char *pMem;
  int n;
  if( sz<sizeof(struct mmOneszBlock) ) return 0;
  pMem = (unsigned char*)pSpace;
  pOnesz = (struct mmOnesz*)pMem;
  n = (sizeof(*pOnesz) + sz - 1)/sz;
  pMem += sz*n;
  cnt -= n;
  if( cnt<2 ) return 0;
  memset(pOnesz, 0, sizeof(*pOnesz));
  pOnesz->base.pMethods = &mmOneszMethods;
  pOnesz->pSpace = (const void*)pMem;
  pOnesz->sz = sz;
  pOnesz->pLast = (const void*)(pMem + sz*(cnt-2));
  pOnesz->pFree = 0;
  while( cnt ){
    struct mmOneszBlock *pBlock = (struct mmOneszBlock*)pMem;
    pBlock->pNext = pOnesz->pFree;
    pOnesz->pFree = pBlock;
    cnt--;
    pMem += sz;
  }
  return &pOnesz->base;
}

/*************************************************************************
** Main interfaces.
*/
void *sqlite4_mm_malloc(sqlite4_mm *pMM, sqlite4_size_t iSize){
  if( pMM==0 ) pMM = &sqlite4MMSystem;
  return pMM->pMethods->xMalloc(pMM,iSize);
}
void *sqlite4_mm_realloc(sqlite4_mm *pMM, void *pOld, sqlite4_size_t iSize){
  if( pMM==0 ) pMM = &sqlite4MMSystem;
  return pMM->pMethods->xRealloc(pMM,pOld,iSize);
}
void sqlite4_mm_free(sqlite4_mm *pMM, void *pOld){
  if( pMM==0 ) pMM = &sqlite4MMSystem;
  pMM->pMethods->xFree(pMM,pOld);
}
sqlite4_size_t sqlite4_mm_msize(sqlite4_mm *pMM, void *pOld){
  if( pMM==0 ) pMM = &sqlite4MMSystem;
  return pMM->pMethods->xMsize(pMM,pOld);
}
int sqlite4_mm_member(sqlite4_mm *pMM, const void *pOld){
  return (pMM && pMM->pMethods->xMember!=0) ?
            pMM->pMethods->xMember(pMM,pOld) : -1;
}
void sqlite4_mm_benign_failures(sqlite4_mm *pMM, int bEnable){
  if( pMM && pMM->pMethods->xBenign ){
    pMM->pMethods->xBenign(pMM, bEnable);
  }
}
sqlite4_int64 sqlite4_mm_stat(sqlite4_mm *pMM, int eStatType, unsigned flags){
  if( pMM==0 ) return -1;
  if( pMM->pMethods->xStat==0 ) return -1;
  return pMM->pMethods->xStat(pMM, eStatType, flags);
}
int sqlite4_mm_control_va(sqlite4_mm *pMM, int eCtrlType, va_list ap){
  if( pMM==0 || pMM->pMethods->xCtrl==0 ) return SQLITE4_NOTFOUND;
  return pMM->pMethods->xCtrl(pMM, eCtrlType, ap);
}
int sqlite4_mm_control(sqlite4_mm *pMM, int eCtrlType, ...){
  int rc;
  va_list ap;
  va_start(ap, eCtrlType);
  rc = sqlite4_mm_control_va(pMM, eCtrlType, ap);
  va_end(ap);
  return rc;
}
void sqlite4_mm_destroy(sqlite4_mm *pMM){
  if( pMM && pMM->pMethods->xFinal ) pMM->pMethods->xFinal(pMM);
}

/*
** Create a new memory allocation object.  eType determines the type of
** memory allocator and the arguments.
*/
sqlite4_mm *sqlite4_mm_new(sqlite4_mm_type eType, ...){
  va_list ap;
  sqlite4_mm *pMM;

  va_start(ap, eType);
  switch( eType ){
    case SQLITE4_MM_SYSTEM: {
      pMM = &sqlite4MMSystem;
      break;
    }
    case SQLITE4_MM_OVERFLOW: {
      sqlite4_mm *pA = va_arg(ap, sqlite4_mm*);
      sqlite4_mm *pB = va_arg(ap, sqlite4_mm*);
      pMM = mmOvflNew(pA, pB);
      break;
    }
    case SQLITE4_MM_ONESIZE: {
      void *pSpace = va_arg(ap, void*);
      int sz = va_arg(ap, int);
      int cnt = va_arg(ap, int);
      pMM = mmOneszNew(pSpace, sz, cnt);
      break;
    }
    case SQLITE4_MM_STATS: {
      sqlite4_mm *p = va_arg(ap, sqlite4_mm*);
      pMM = mmStatsNew(p);
      break;
    }
    default: {
      pMM = 0;
      break;
    }
  }
  va_end(ap);
  return pMM;
}

/*************************************************************************
** sqlite4_buffer implementation.
*/

void sqlite4_buffer_init(sqlite4_buffer *pBuf, sqlite4_mm *pMM){
  memset(pBuf, 0, sizeof(*pBuf));
  pBuf->pMM = pMM;
}

int sqlite4_buffer_resize(sqlite4_buffer *pBuf, sqlite4_size_t nReq){
  sqlite4_size_t nCurrent;                  /* Current buffer size */
  nCurrent = sqlite4_mm_msize(pBuf->pMM, pBuf->p);
  if( nCurrent<nReq ){
    void *pNew = sqlite4_mm_realloc(pBuf->pMM, pBuf->p, nReq);
    if( pNew==0 ) return SQLITE4_NOMEM;
    pBuf->p = pNew;
  }
  pBuf->n = nReq;
  return SQLITE4_OK;
}

int sqlite4_buffer_append(
  sqlite4_buffer *pBuf, 
  const void *p, 
  sqlite4_size_t n
){
  int rc;                         /* Return code */
  sqlite4_size_t nOrig = pBuf->n; /* Initial buffer size in bytes */

  rc = sqlite4_buffer_resize(pBuf, nOrig+n);
  if( rc==SQLITE4_OK ){
    memcpy(&((u8 *)pBuf->p)[nOrig], p, n);
  }
  return rc;
}

int sqlite4_buffer_set(
  sqlite4_buffer *pBuf, 
  const void *p, 
  sqlite4_size_t n
){
  pBuf->n = 0;
  return sqlite4_buffer_append(pBuf, p, n);
}

void sqlite4_buffer_clear(sqlite4_buffer *pBuf){
  sqlite4_mm_free(pBuf->pMM, pBuf->p);
  sqlite4_buffer_init(pBuf, pBuf->pMM);
}
