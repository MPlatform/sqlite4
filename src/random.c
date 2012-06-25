/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code to implement a pseudo-random number
** generator (PRNG) for SQLite.
**
** Random numbers are used by some of the database backends in order
** to generate random integer keys for tables or random filenames.
*/
#include "sqliteInt.h"


/*
** Get a single 8-bit random value from the PRNG.  The Mutex
** must be held while executing this routine.
*/
static u8 randomByte(sqlite4_env *pEnv){
  pEnv->prngX = (pEnv->prngX>>1) ^ ((-(pEnv->prngX&1)) & 0xd0000001);
  pEnv->prngY = pEnv->prngY*1103515245 + 12345;
  return (u8)((pEnv->prngX ^ pEnv->prngY)&0xff);
}

/*
** Return N random bytes.
*/
void sqlite4_randomness(sqlite4_env *pEnv, int N, void *pBuf){
  unsigned char *zBuf = pBuf;
  if( pEnv==0 ) pEnv = &sqlite4DefaultEnv;
  sqlite4_mutex_enter(pEnv->pPrngMutex);
  while( N-- ){
    *(zBuf++) = randomByte(pEnv);
  }
  sqlite4_mutex_leave(pEnv->pPrngMutex);
}
