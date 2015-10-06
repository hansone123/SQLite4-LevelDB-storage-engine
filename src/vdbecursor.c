/*
** 2012-02-16
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
** This file contains methods for the VdbeCursor object.
**
** A VdbeCursor is an abstraction of the KVCursor that includes knowledge
** about different "tables" in the key space.  A VdbeCursor is only active
** over a particular table.  Thus, for example, sqlite4VdbeNext() will
** return SQLITE4_NOTFOUND when advancing off the end of a table into the
** next table whereas the lower-level sqlite4KVCursorNext() routine will
** not return SQLITE4_NOTFOUND until it is advanced off the end of the very
** last table in the database.
*/
#include "sqliteInt.h"
#include "vdbeInt.h"


/*
** Move a VDBE cursor to the first or to the last element of its table.  The
** first element is sought if iEnd==+1 and the last element if iEnd==-1.
**
** Return SQLITE4_OK on success. Return SQLITE4_NOTFOUND if the table is empty.
** Other error codes are also possible for various kinds of errors.
*/
int sqlite4VdbeSeekEnd(VdbeCursor *pC, int iEnd){
  KVCursor *pCur = pC->pKVCur;
  int rc;
  KVSize nProbe;
  KVByteArray aProbe[16];

  assert( iEnd==(+1) || iEnd==(-1) || iEnd==(-2) );  
  if( pC->iRoot==KVSTORE_ROOT ){
    if( iEnd>0 ){
      rc = sqlite4KVCursorSeek(pCur, (const KVByteArray *)"\00", 1, iEnd);
    }else{
      nProbe = sqlite4PutVarint64(aProbe, LARGEST_INT64);
      rc = sqlite4KVCursorSeek(pCur, aProbe, nProbe, iEnd);
    }
    if( rc==SQLITE4_INEXACT ) rc = SQLITE4_OK;
  }else{
    const KVByteArray *aKey;
    KVSize nKey;

    nProbe = sqlite4PutVarint64(aProbe, pC->iRoot);
    aProbe[nProbe] = 0xFF;

    rc = sqlite4KVCursorSeek(pCur, aProbe, nProbe+(iEnd<0), iEnd);
    if( rc==SQLITE4_OK ){
      rc = SQLITE4_CORRUPT_BKPT;
    }else if( rc==SQLITE4_INEXACT ){
      rc = sqlite4KVCursorKey(pCur, &aKey, &nKey);
      if( rc==SQLITE4_OK && (nKey<nProbe || memcmp(aKey, aProbe, nProbe)!=0) ){
        rc = SQLITE4_NOTFOUND;
      }
    }
    pC->rowChnged = 1;
  }

  return rc;
}

/*
** Move a VDBE cursor to the next element in its table.
** Return SQLITE4_NOTFOUND if the seek falls of the end of the table.
*/
int sqlite4VdbeNext(VdbeCursor *pC){
  KVCursor *pCur = pC->pKVCur;
  const KVByteArray *aKey;
  KVSize nKey;
  int rc;
  sqlite4_uint64 iTabno;

  rc = sqlite4KVCursorNext(pCur);
  if( rc==SQLITE4_OK && pC->iRoot!=KVSTORE_ROOT ){
    rc = sqlite4KVCursorKey(pCur, &aKey, &nKey);
    if( rc==SQLITE4_OK ){
      iTabno = 0;
      sqlite4GetVarint64(aKey, nKey, &iTabno);
      if( iTabno!=pC->iRoot ) rc = SQLITE4_NOTFOUND;
    }
  }
  pC->rowChnged = 1;
  return rc;
}

/*
** Move a VDBE cursor to the previous element in its table.
** Return SQLITE4_NOTFOUND if the seek falls of the end of the table.
*/
int sqlite4VdbePrevious(VdbeCursor *pC){
  KVCursor *pCur = pC->pKVCur;
  const KVByteArray *aKey;
  KVSize nKey;
  int rc;
  sqlite4_uint64 iTabno;

  rc = sqlite4KVCursorPrev(pCur);
  if( rc==SQLITE4_OK && pC->iRoot!=KVSTORE_ROOT ){
    rc = sqlite4KVCursorKey(pCur, &aKey, &nKey);
    if( rc==SQLITE4_OK ){
      iTabno = 0;
      sqlite4GetVarint64(aKey, nKey, &iTabno);
      if( iTabno!=pC->iRoot ) rc = SQLITE4_NOTFOUND;
    }
  }
  pC->rowChnged = 1;
  return rc;
}


/*
** Close a VDBE cursor and release all the resources that cursor 
** happens to hold.
*/
void sqlite4VdbeFreeCursor(VdbeCursor *pCx){
  if( pCx==0 ){
    return;
  }
  sqlite4Fts5Close(pCx->pFts);
  if( pCx->pKVCur ){
    sqlite4KVCursorClose(pCx->pKVCur);
  }
  if( pCx->pTmpKV ){
    sqlite4KVStoreClose(pCx->pTmpKV);
  }
  if( pCx->pDecoder ){
    sqlite4VdbeDecoderDestroy(pCx->pDecoder);
    pCx->pDecoder = 0;
  }
  sqlite4_buffer_clear(&pCx->sSeekKey);
#ifndef SQLITE4_OMIT_VIRTUALTABLE
  if( pCx->pVtabCursor ){
    sqlite4_vtab_cursor *pVtabCursor = pCx->pVtabCursor;
    const sqlite4_module *pModule = pCx->pModule;
    p->inVtabMethod = 1;
    pModule->xClose(pVtabCursor);
    p->inVtabMethod = 0;
  }
#endif
}


/*
** Cursor pPk is open on a primary key index. If there is currently a
** deferred seek pending on the cursor, do the actual seek now.
**
** If the operation is a success, SQLITE4_OK is returned. Or, if the
** required entry is not found in the PK index, SQLITE4_CORRUPT. Or if 
** some other error occurs, an error code is returned.
*/
int sqlite4VdbeCursorMoveto(VdbeCursor *pPk){
  int rc = SQLITE4_OK;            /* Return code */
  if( pPk->sSeekKey.n!=0 ){
    assert( pPk->pKeyInfo->nPK==0 );
    rc = sqlite4KVCursorSeek(pPk->pKVCur, pPk->sSeekKey.p, pPk->sSeekKey.n, 0);
    if( rc==SQLITE4_NOTFOUND ){
      rc = SQLITE4_CORRUPT_BKPT;
    }
    pPk->nullRow = 0;
    pPk->sSeekKey.n = 0;
    pPk->rowChnged = 1;
  }
  return rc;
}
