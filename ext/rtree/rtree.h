/*
** 2008 May 26
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This header file is used by programs that want to link against the
** RTREE library.  All it does is declare the sqlite4RtreeInit() interface.
*/
#include "sqlite4.h"

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

int sqlite4RtreeInit(sqlite4 *db);

#ifdef __cplusplus
}  /* extern "C" */
#endif  /* __cplusplus */
