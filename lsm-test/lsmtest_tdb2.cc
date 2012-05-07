

#include "lsmtest.h"
#include <stdlib.h>

#ifdef HAVE_LEVELDB
#include "leveldb/db.h"
extern "C" {
  struct LevelDb {
    TestDb base;
    leveldb::DB* db;
    std::string *pRes;
  };
}

int test_leveldb_open(const char *zFilename, int bClear, TestDb **ppDb){
  LevelDb *pLevelDb;

  if( bClear ){
    char *zCmd = sqlite3_mprintf("rm -rf %s\n", zFilename);
    system(zCmd);
    sqlite3_free(zCmd);
  }

  pLevelDb = (LevelDb *)malloc(sizeof(LevelDb));
  memset(pLevelDb, 0, sizeof(LevelDb));

  leveldb::Options options;
  options.create_if_missing = 1;
  leveldb::Status s = leveldb::DB::Open(options, zFilename, &pLevelDb->db);

  if( s.ok() ){
    *ppDb = (TestDb *)pLevelDb;
    pLevelDb->pRes = new std::string("");
  }else{
    test_leveldb_close((TestDb *)pLevelDb);
    *ppDb = 0;
    return 1;
  }
  return 0;
}

int test_leveldb_close(TestDb *pDb){
  LevelDb *pLevelDb = (LevelDb *)pDb;
  if( pDb ){
    if( pLevelDb->db ) delete pLevelDb->db;
  }
  free((void *)pDb);
  return 0;
}

int test_leveldb_write(TestDb *pDb, void *pKey, int nKey, void *pVal, int nVal){
  LevelDb *pLevelDb = (LevelDb *)pDb;
  leveldb::Status s = pLevelDb->db->Put(leveldb::WriteOptions(), 
      leveldb::Slice((const char *)pKey, nKey), 
      leveldb::Slice((const char *)pVal, nVal) 
  );
  return !s.ok();
}

int test_leveldb_delete(TestDb *pDb, void *pKey, int nKey){
  LevelDb *pLevelDb = (LevelDb *)pDb;
  leveldb::Status s = pLevelDb->db->Delete(leveldb::WriteOptions(), 
      leveldb::Slice((const char *)pKey, nKey)
  );
  return !s.ok();
}

int test_leveldb_fetch(
  TestDb *pDb, 
  void *pKey, 
  int nKey, 
  void **ppVal, 
  int *pnVal
){
  LevelDb *pLevelDb = (LevelDb *)pDb;
  pLevelDb->pRes->assign("");
  leveldb::Status s = pLevelDb->db->Get(
      leveldb::ReadOptions(), 
      leveldb::Slice((const char *)pKey, nKey), 
      pLevelDb->pRes
  );
  if( s.ok() ){
    *ppVal = (void *)pLevelDb->pRes->data();
    *pnVal = (int)pLevelDb->pRes->length();
  }else{
    *pnVal = -1;
    *ppVal = 0;
  }
  return 0;
}

int test_leveldb_scan(
  TestDb *pDb,                    /* Database handle */
  void *pCtx,                     /* Context pointer to pass to xCallback */
  int bReverse,                   /* True to iterate in reverse order */
  void *pKey1, int nKey1,         /* Start of search */
  void *pKey2, int nKey2,         /* End of search */
  void (*xCallback)(void *pCtx, void *pKey, int nKey, void *pVal, int nVal)
){
  LevelDb *pLevelDb = (LevelDb *)pDb;
  leveldb::Iterator *pIter;

  leveldb::Slice sKey1((const char *)pKey1, nKey1);
  leveldb::Slice sKey2((const char *)pKey2, nKey2);

  pIter = pLevelDb->db->NewIterator(leveldb::ReadOptions());
  if( bReverse==0 ){
    if( pKey1 ){
      pIter->Seek(sKey1);
    }else{
      pIter->SeekToFirst();
    }
  }else{
    if( pKey2 ){
      pIter->Seek(sKey2);
      if( pIter->Valid()==0 ){
        pIter->SeekToLast();
      }else{
        int res = pIter->key().compare(sKey2);
        assert( res>=0 );
        if( res>0 ){
          pIter->Prev();
        }
        assert( pIter->Valid()==0 || pIter->key().compare(sKey2)<=0 );
      }
    }else{
      pIter->SeekToLast();
    }
  }

  while( pIter->Valid() ){
    if( (bReverse==0 && pKey2 && pIter->key().compare(sKey2)>0) ) break;
    if( (bReverse!=0 && pKey1 && pIter->key().compare(sKey1)<0) ) break;

    leveldb::Slice k = pIter->key();
    leveldb::Slice v = pIter->value();
    xCallback(pCtx, (void *)k.data(), k.size(), (void *)v.data(), v.size());

    if( bReverse==0 ){
      pIter->Next();
    }else{
      pIter->Prev();
    }
  }

  delete pIter;
  return LSM_OK;
}
#endif

#ifdef HAVE_KYOTOCABINET
#include "kcpolydb.h"
extern "C" {
  struct KcDb {
    TestDb base;
    kyotocabinet::TreeDB* db;
    char *pVal;
  };
}

int test_kc_open(const char *zFilename, int bClear, TestDb **ppDb){
  KcDb *pKcDb;
  int ok;
  int rc = 0;

  if( bClear ){
    char *zCmd = sqlite3_mprintf("rm -rf %s\n", zFilename);
    system(zCmd);
    sqlite3_free(zCmd);
  }

  pKcDb = (KcDb *)malloc(sizeof(KcDb));
  memset(pKcDb, 0, sizeof(KcDb));


  pKcDb->db = new kyotocabinet::TreeDB();
  pKcDb->db->tune_page(TESTDB_DEFAULT_PAGE_SIZE);
  pKcDb->db->tune_page_cache(
      TESTDB_DEFAULT_PAGE_SIZE * TESTDB_DEFAULT_CACHE_SIZE
  );
  ok = pKcDb->db->open(zFilename,
      kyotocabinet::PolyDB::OWRITER | kyotocabinet::PolyDB::OCREATE
  );
  if( ok==0 ){
    free(pKcDb);
    pKcDb = 0;
    rc = 1;
  }

  *ppDb = (TestDb *)pKcDb;
  return rc;
}

int test_kc_close(TestDb *pDb){
  KcDb *pKcDb = (KcDb *)pDb;
  if( pKcDb->pVal ){
    delete [] pKcDb->pVal;
  }
  pKcDb->db->close();
  delete pKcDb->db;
  free(pKcDb);
  return 0;
}

int test_kc_write(TestDb *pDb, void *pKey, int nKey, void *pVal, int nVal){
  KcDb *pKcDb = (KcDb *)pDb;
  int ok;

  ok = pKcDb->db->set((const char *)pKey, nKey, (const char *)pVal, nVal);
  return (ok ? 0 : 1);
}

int test_kc_delete(TestDb *pDb, void *pKey, int nKey){
  KcDb *pKcDb = (KcDb *)pDb;
  int ok;

  ok = pKcDb->db->remove((const char *)pKey, nKey);
  return (ok ? 0 : 1);
}

int test_kc_fetch(
  TestDb *pDb, 
  void *pKey, 
  int nKey, 
  void **ppVal,
  int *pnVal
){
  KcDb *pKcDb = (KcDb *)pDb;
  size_t nVal;

  if( pKcDb->pVal ){
    delete [] pKcDb->pVal;
    pKcDb->pVal = 0;
  }

  pKcDb->pVal = pKcDb->db->get((const char *)pKey, nKey, &nVal);
  if( pKcDb->pVal ){
    *ppVal = pKcDb->pVal;
    *pnVal = nVal;
  }else{
    *ppVal = 0;
    *pnVal = -1;
  }

  return 0;
}

int test_kc_scan(
  TestDb *pDb,                    /* Database handle */
  void *pCtx,                     /* Context pointer to pass to xCallback */
  int bReverse,                   /* True for a reverse order scan */
  void *pKey1, int nKey1,         /* Start of search */
  void *pKey2, int nKey2,         /* End of search */
  void (*xCallback)(void *pCtx, void *pKey, int nKey, void *pVal, int nVal)
){
  KcDb *pKcDb = (KcDb *)pDb;
  kyotocabinet::DB::Cursor* pCur = pKcDb->db->cursor();
  int res;

  if( bReverse==0 ){
    if( pKey1 ){
      res = pCur->jump((const char *)pKey1, nKey1);
    }else{
      res = pCur->jump();
    }
  }else{
    if( pKey2 ){
      res = pCur->jump_back((const char *)pKey2, nKey2);
    }else{
      res = pCur->jump_back();
    }
  }

  while( res ){
    const char *pKey; size_t nKey;
    const char *pVal; size_t nVal;
    pKey = pCur->get(&nKey, &pVal, &nVal);

    if( bReverse==0 && pKey2 ){
      res = memcmp(pKey, pKey2, MIN((size_t)nKey2, nKey));
      if( res>0 || (res==0 && (size_t)nKey2<nKey) ){
        delete [] pKey;
        break;
      }
    }else if( bReverse!=0 && pKey1 ){
      res = memcmp(pKey, pKey1, MIN((size_t)nKey1, nKey));
      if( res<0 || (res==0 && (size_t)nKey1>nKey) ){
        delete [] pKey;
        break;
      }
    }

    xCallback(pCtx, (void *)pKey, (int)nKey, (void *)pVal, (int)nVal);
    delete [] pKey;

    if( bReverse ){
      res = pCur->step_back();
    }else{
      res = pCur->step();
    }
  }

  delete pCur;
  return 0;
}
#endif

