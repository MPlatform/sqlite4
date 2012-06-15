
#include "lsmtest.h"


int do_work(int nArg, char **azArg){
  struct Option {
    const char *zName;
  } aOpt [] = {
    { "-optimize" },
    { "-npage" },
    { 0 }
  };

  lsm_db *pDb;
  int rc;
  int i;
  const char *zDb;
  int flags = LSM_WORK_CHECKPOINT;
  int nWork = (1<<30);

  if( nArg==0 ) goto usage;
  zDb = azArg[nArg-1];
  for(i=0; i<(nArg-1); i++){
    int iSel;
    rc = testArgSelect(aOpt, "option", azArg[i], &iSel);
    if( rc ) return rc;
    switch( iSel ){
      case 0:
        flags |= LSM_WORK_OPTIMIZE;
        break;
      case 1:
        i++;
        if( i==(nArg-1) ) goto usage;
        nWork = atoi(azArg[i]);
        break;
    }
  }

  rc = lsm_new(0, &pDb);
  if( rc!=LSM_OK ){
    testPrintError("lsm_open(): rc=%d\n", rc);
  }else{
    rc = lsm_open(pDb, zDb);
    if( rc!=LSM_OK ){
      testPrintError("lsm_open(): rc=%d\n", rc);
    }else{
      rc = lsm_work(pDb, flags, nWork, 0);
      if( rc!=LSM_OK ){
        testPrintError("lsm_work(): rc=%d\n", rc);
      }
    }
  }

  lsm_close(pDb);
  return rc;

 usage:
  testPrintUsage("?-optimize? ?-n N? DATABASE");
  return -1;
}


/*
**   lsmtest show DATABASE ?array|page PGNO?
*/
int do_show(int nArg, char **azArg){
  lsm_db *pDb;
  int rc;
  const char *zDb;

  int eOpt = LSM_INFO_DB_STRUCTURE;
  unsigned int iPg = 0;

  struct Option {
    const char *zName;
    int eOpt;
  } aOpt [] = { 
    { "array",      LSM_INFO_ARRAY_STRUCTURE },
    { "page-ascii", LSM_INFO_PAGE_ASCII_DUMP },
    { "page-hex",   LSM_INFO_PAGE_HEX_DUMP },
    { 0, 0 } 
  };

  char *z = 0;

  if( nArg!=1 && nArg!=3 ){
    testPrintUsage("DATABASE ?array|page PGNO?");
    return -1;
  }
  if( nArg==3 ){
    rc = testArgSelect(aOpt, "option", azArg[1], &eOpt);
    if( rc!=0 ) return rc;
    eOpt = aOpt[eOpt].eOpt;
    iPg = atoi(azArg[2]);
  }
  zDb = azArg[0];

  rc = lsm_new(0, &pDb);
  if( rc!=LSM_OK ){
    testPrintError("lsm_new(): rc=%d\n", rc);
  }else{
    rc = lsm_open(pDb, zDb);
    if( rc!=LSM_OK ){
      testPrintError("lsm_open(): rc=%d\n", rc);
    }
  }

  if( rc==LSM_OK ){
    switch( eOpt ){
      case LSM_INFO_DB_STRUCTURE:
        rc = lsm_info(pDb, LSM_INFO_DB_STRUCTURE, &z);
        break;
      case LSM_INFO_ARRAY_STRUCTURE:
      case LSM_INFO_PAGE_ASCII_DUMP:
      case LSM_INFO_PAGE_HEX_DUMP:
        rc = lsm_info(pDb, eOpt, iPg, &z);
        break;
      default:
        assert( !"no chance" );
    }

    if( rc==LSM_OK ){
      printf("%s\n", z);
      fflush(stdout);
    }
    lsm_free(lsm_get_env(pDb), z);
  }

  lsm_close(pDb);
  return rc;
}



