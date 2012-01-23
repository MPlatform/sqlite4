/*
** 2006 June 7
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This header file defines the SQLite interface for use by
** shared libraries that want to be imported as extensions into
** an SQLite instance.  Shared libraries that intend to be loaded
** as extensions by SQLite should #include this file instead of 
** sqlite4.h.
*/
#ifndef _SQLITE3EXT_H_
#define _SQLITE3EXT_H_
#include "sqlite4.h"

typedef struct sqlite4_api_routines sqlite4_api_routines;

/*
** The following structure holds pointers to all of the SQLite API
** routines.
**
** WARNING:  In order to maintain backwards compatibility, add new
** interfaces to the end of this structure only.  If you insert new
** interfaces in the middle of this structure, then older different
** versions of SQLite will not be able to load each others' shared
** libraries!
*/
struct sqlite4_api_routines {
  void * (*aggregate_context)(sqlite4_context*,int nBytes);
  int  (*aggregate_count)(sqlite4_context*);
  int  (*bind_blob)(sqlite4_stmt*,int,const void*,int n,void(*)(void*));
  int  (*bind_double)(sqlite4_stmt*,int,double);
  int  (*bind_int)(sqlite4_stmt*,int,int);
  int  (*bind_int64)(sqlite4_stmt*,int,sqlite_int64);
  int  (*bind_null)(sqlite4_stmt*,int);
  int  (*bind_parameter_count)(sqlite4_stmt*);
  int  (*bind_parameter_index)(sqlite4_stmt*,const char*zName);
  const char * (*bind_parameter_name)(sqlite4_stmt*,int);
  int  (*bind_text)(sqlite4_stmt*,int,const char*,int n,void(*)(void*));
  int  (*bind_text16)(sqlite4_stmt*,int,const void*,int,void(*)(void*));
  int  (*bind_value)(sqlite4_stmt*,int,const sqlite4_value*);
  int  (*busy_handler)(sqlite4*,int(*)(void*,int),void*);
  int  (*busy_timeout)(sqlite4*,int ms);
  int  (*changes)(sqlite4*);
  int  (*close)(sqlite4*);
  int  (*collation_needed)(sqlite4*,void*,void(*)(void*,sqlite4*,
                           int eTextRep,const char*));
  int  (*collation_needed16)(sqlite4*,void*,void(*)(void*,sqlite4*,
                             int eTextRep,const void*));
  const void * (*column_blob)(sqlite4_stmt*,int iCol);
  int  (*column_bytes)(sqlite4_stmt*,int iCol);
  int  (*column_bytes16)(sqlite4_stmt*,int iCol);
  int  (*column_count)(sqlite4_stmt*pStmt);
  const char * (*column_database_name)(sqlite4_stmt*,int);
  const void * (*column_database_name16)(sqlite4_stmt*,int);
  const char * (*column_decltype)(sqlite4_stmt*,int i);
  const void * (*column_decltype16)(sqlite4_stmt*,int);
  double  (*column_double)(sqlite4_stmt*,int iCol);
  int  (*column_int)(sqlite4_stmt*,int iCol);
  sqlite_int64  (*column_int64)(sqlite4_stmt*,int iCol);
  const char * (*column_name)(sqlite4_stmt*,int);
  const void * (*column_name16)(sqlite4_stmt*,int);
  const char * (*column_origin_name)(sqlite4_stmt*,int);
  const void * (*column_origin_name16)(sqlite4_stmt*,int);
  const char * (*column_table_name)(sqlite4_stmt*,int);
  const void * (*column_table_name16)(sqlite4_stmt*,int);
  const unsigned char * (*column_text)(sqlite4_stmt*,int iCol);
  const void * (*column_text16)(sqlite4_stmt*,int iCol);
  int  (*column_type)(sqlite4_stmt*,int iCol);
  sqlite4_value* (*column_value)(sqlite4_stmt*,int iCol);
  void * (*commit_hook)(sqlite4*,int(*)(void*),void*);
  int  (*complete)(const char*sql);
  int  (*complete16)(const void*sql);
  int  (*create_collation)(sqlite4*,const char*,int,void*,
                           int(*)(void*,int,const void*,int,const void*),
                           int(*)(void*,int,const void*,int,const void*),
                           void(*)(void*));
  int  (*create_function)(sqlite4*,const char*,int,int,void*,
                          void (*xFunc)(sqlite4_context*,int,sqlite4_value**),
                          void (*xStep)(sqlite4_context*,int,sqlite4_value**),
                          void (*xFinal)(sqlite4_context*));
  int  (*create_function16)(sqlite4*,const void*,int,int,void*,
                            void (*xFunc)(sqlite4_context*,int,sqlite4_value**),
                            void (*xStep)(sqlite4_context*,int,sqlite4_value**),
                            void (*xFinal)(sqlite4_context*));
  int (*create_module)(sqlite4*,const char*,const sqlite4_module*,void*);
  int  (*data_count)(sqlite4_stmt*pStmt);
  sqlite4 * (*db_handle)(sqlite4_stmt*);
  int (*declare_vtab)(sqlite4*,const char*);
  int  (*enable_shared_cache)(int);
  int  (*errcode)(sqlite4*db);
  const char * (*errmsg)(sqlite4*);
  const void * (*errmsg16)(sqlite4*);
  int  (*exec)(sqlite4*,const char*,sqlite4_callback,void*,char**);
  int  (*expired)(sqlite4_stmt*);
  int  (*finalize)(sqlite4_stmt*pStmt);
  void  (*free)(void*);
  void  (*free_table)(char**result);
  int  (*get_autocommit)(sqlite4*);
  void * (*get_auxdata)(sqlite4_context*,int);
  int  (*get_table)(sqlite4*,const char*,char***,int*,int*,char**);
  int  (*global_recover)(void);
  void  (*interruptx)(sqlite4*);
  sqlite_int64  (*last_insert_rowid)(sqlite4*);
  const char * (*libversion)(void);
  int  (*libversion_number)(void);
  void *(*malloc)(int);
  char * (*mprintf)(const char*,...);
  int  (*open)(const char*,sqlite4**);
  int  (*open16)(const void*,sqlite4**);
  int  (*prepare)(sqlite4*,const char*,int,sqlite4_stmt**,const char**);
  int  (*prepare16)(sqlite4*,const void*,int,sqlite4_stmt**,const void**);
  void * (*profile)(sqlite4*,void(*)(void*,const char*,sqlite_uint64),void*);
  void  (*progress_handler)(sqlite4*,int,int(*)(void*),void*);
  void *(*realloc)(void*,int);
  int  (*reset)(sqlite4_stmt*pStmt);
  void  (*result_blob)(sqlite4_context*,const void*,int,void(*)(void*));
  void  (*result_double)(sqlite4_context*,double);
  void  (*result_error)(sqlite4_context*,const char*,int);
  void  (*result_error16)(sqlite4_context*,const void*,int);
  void  (*result_int)(sqlite4_context*,int);
  void  (*result_int64)(sqlite4_context*,sqlite_int64);
  void  (*result_null)(sqlite4_context*);
  void  (*result_text)(sqlite4_context*,const char*,int,void(*)(void*));
  void  (*result_text16)(sqlite4_context*,const void*,int,void(*)(void*));
  void  (*result_text16be)(sqlite4_context*,const void*,int,void(*)(void*));
  void  (*result_text16le)(sqlite4_context*,const void*,int,void(*)(void*));
  void  (*result_value)(sqlite4_context*,sqlite4_value*);
  void * (*rollback_hook)(sqlite4*,void(*)(void*),void*);
  int  (*set_authorizer)(sqlite4*,int(*)(void*,int,const char*,const char*,
                         const char*,const char*),void*);
  void  (*set_auxdata)(sqlite4_context*,int,void*,void (*)(void*));
  char * (*snprintf)(int,char*,const char*,...);
  int  (*step)(sqlite4_stmt*);
  int  (*table_column_metadata)(sqlite4*,const char*,const char*,const char*,
                                char const**,char const**,int*,int*,int*);
  void  (*thread_cleanup)(void);
  int  (*total_changes)(sqlite4*);
  void * (*trace)(sqlite4*,void(*xTrace)(void*,const char*),void*);
  int  (*transfer_bindings)(sqlite4_stmt*,sqlite4_stmt*);
  void * (*update_hook)(sqlite4*,void(*)(void*,int ,char const*,char const*,
                                         sqlite_int64),void*);
  void * (*user_data)(sqlite4_context*);
  const void * (*value_blob)(sqlite4_value*);
  int  (*value_bytes)(sqlite4_value*);
  int  (*value_bytes16)(sqlite4_value*);
  double  (*value_double)(sqlite4_value*);
  int  (*value_int)(sqlite4_value*);
  sqlite_int64  (*value_int64)(sqlite4_value*);
  int  (*value_numeric_type)(sqlite4_value*);
  const unsigned char * (*value_text)(sqlite4_value*);
  const void * (*value_text16)(sqlite4_value*);
  const void * (*value_text16be)(sqlite4_value*);
  const void * (*value_text16le)(sqlite4_value*);
  int  (*value_type)(sqlite4_value*);
  char *(*vmprintf)(const char*,va_list);
  /* Added ??? */
  int (*overload_function)(sqlite4*, const char *zFuncName, int nArg);
  /* Added by 3.3.13 */
  int (*prepare_v2)(sqlite4*,const char*,int,sqlite4_stmt**,const char**);
  int (*prepare16_v2)(sqlite4*,const void*,int,sqlite4_stmt**,const void**);
  int (*clear_bindings)(sqlite4_stmt*);
  /* Added by 3.4.1 */
  int (*create_module_v2)(sqlite4*,const char*,const sqlite4_module*,void*,
                          void (*xDestroy)(void *));
  /* Added by 3.5.0 */
  int (*bind_zeroblob)(sqlite4_stmt*,int,int);
  int (*blob_bytes)(sqlite4_blob*);
  int (*blob_close)(sqlite4_blob*);
  int (*blob_open)(sqlite4*,const char*,const char*,const char*,sqlite4_int64,
                   int,sqlite4_blob**);
  int (*blob_read)(sqlite4_blob*,void*,int,int);
  int (*blob_write)(sqlite4_blob*,const void*,int,int);
  int (*file_control)(sqlite4*,const char*,int,void*);
  sqlite4_int64 (*memory_highwater)(int);
  sqlite4_int64 (*memory_used)(void);
  sqlite4_mutex *(*mutex_alloc)(int);
  void (*mutex_enter)(sqlite4_mutex*);
  void (*mutex_free)(sqlite4_mutex*);
  void (*mutex_leave)(sqlite4_mutex*);
  int (*mutex_try)(sqlite4_mutex*);
  int (*open_v2)(const char*,sqlite4**,int,const char*);
  int (*release_memory)(int);
  void (*result_error_nomem)(sqlite4_context*);
  void (*result_error_toobig)(sqlite4_context*);
  int (*sleep)(int);
  void (*soft_heap_limit)(int);
  sqlite4_vfs *(*vfs_find)(const char*);
  int (*vfs_register)(sqlite4_vfs*,int);
  int (*vfs_unregister)(sqlite4_vfs*);
  int (*xthreadsafe)(void);
  void (*result_zeroblob)(sqlite4_context*,int);
  void (*result_error_code)(sqlite4_context*,int);
  int (*test_control)(int, ...);
  void (*randomness)(int,void*);
  sqlite4 *(*context_db_handle)(sqlite4_context*);
  int (*extended_result_codes)(sqlite4*,int);
  int (*limit)(sqlite4*,int,int);
  sqlite4_stmt *(*next_stmt)(sqlite4*,sqlite4_stmt*);
  const char *(*sql)(sqlite4_stmt*);
  int (*status)(int,int*,int*,int);
  int (*backup_finish)(sqlite4_backup*);
  sqlite4_backup *(*backup_init)(sqlite4*,const char*,sqlite4*,const char*);
  int (*backup_pagecount)(sqlite4_backup*);
  int (*backup_remaining)(sqlite4_backup*);
  int (*backup_step)(sqlite4_backup*,int);
  const char *(*compileoption_get)(int);
  int (*compileoption_used)(const char*);
  int (*create_function_v2)(sqlite4*,const char*,int,int,void*,
                            void (*xFunc)(sqlite4_context*,int,sqlite4_value**),
                            void (*xStep)(sqlite4_context*,int,sqlite4_value**),
                            void (*xFinal)(sqlite4_context*),
                            void(*xDestroy)(void*));
  int (*db_config)(sqlite4*,int,...);
  sqlite4_mutex *(*db_mutex)(sqlite4*);
  int (*db_status)(sqlite4*,int,int*,int*,int);
  int (*extended_errcode)(sqlite4*);
  void (*log)(int,const char*,...);
  sqlite4_int64 (*soft_heap_limit64)(sqlite4_int64);
  const char *(*sourceid)(void);
  int (*stmt_status)(sqlite4_stmt*,int,int);
  int (*strnicmp)(const char*,const char*,int);
  int (*unlock_notify)(sqlite4*,void(*)(void**,int),void*);
  int (*wal_autocheckpoint)(sqlite4*,int);
  int (*wal_checkpoint)(sqlite4*,const char*);
  void *(*wal_hook)(sqlite4*,int(*)(void*,sqlite4*,const char*,int),void*);
  int (*blob_reopen)(sqlite4_blob*,sqlite4_int64);
  int (*vtab_config)(sqlite4*,int op,...);
  int (*vtab_on_conflict)(sqlite4*);
};

/*
** The following macros redefine the API routines so that they are
** redirected throught the global sqlite4_api structure.
**
** This header file is also used by the loadext.c source file
** (part of the main SQLite library - not an extension) so that
** it can get access to the sqlite4_api_routines structure
** definition.  But the main library does not want to redefine
** the API.  So the redefinition macros are only valid if the
** SQLITE_CORE macros is undefined.
*/
#ifndef SQLITE_CORE
#define sqlite4_aggregate_context      sqlite4_api->aggregate_context
#ifndef SQLITE_OMIT_DEPRECATED
#define sqlite4_aggregate_count        sqlite4_api->aggregate_count
#endif
#define sqlite4_bind_blob              sqlite4_api->bind_blob
#define sqlite4_bind_double            sqlite4_api->bind_double
#define sqlite4_bind_int               sqlite4_api->bind_int
#define sqlite4_bind_int64             sqlite4_api->bind_int64
#define sqlite4_bind_null              sqlite4_api->bind_null
#define sqlite4_bind_parameter_count   sqlite4_api->bind_parameter_count
#define sqlite4_bind_parameter_index   sqlite4_api->bind_parameter_index
#define sqlite4_bind_parameter_name    sqlite4_api->bind_parameter_name
#define sqlite4_bind_text              sqlite4_api->bind_text
#define sqlite4_bind_text16            sqlite4_api->bind_text16
#define sqlite4_bind_value             sqlite4_api->bind_value
#define sqlite4_busy_handler           sqlite4_api->busy_handler
#define sqlite4_busy_timeout           sqlite4_api->busy_timeout
#define sqlite4_changes                sqlite4_api->changes
#define sqlite4_close                  sqlite4_api->close
#define sqlite4_collation_needed       sqlite4_api->collation_needed
#define sqlite4_collation_needed16     sqlite4_api->collation_needed16
#define sqlite4_column_blob            sqlite4_api->column_blob
#define sqlite4_column_bytes           sqlite4_api->column_bytes
#define sqlite4_column_bytes16         sqlite4_api->column_bytes16
#define sqlite4_column_count           sqlite4_api->column_count
#define sqlite4_column_database_name   sqlite4_api->column_database_name
#define sqlite4_column_database_name16 sqlite4_api->column_database_name16
#define sqlite4_column_decltype        sqlite4_api->column_decltype
#define sqlite4_column_decltype16      sqlite4_api->column_decltype16
#define sqlite4_column_double          sqlite4_api->column_double
#define sqlite4_column_int             sqlite4_api->column_int
#define sqlite4_column_int64           sqlite4_api->column_int64
#define sqlite4_column_name            sqlite4_api->column_name
#define sqlite4_column_name16          sqlite4_api->column_name16
#define sqlite4_column_origin_name     sqlite4_api->column_origin_name
#define sqlite4_column_origin_name16   sqlite4_api->column_origin_name16
#define sqlite4_column_table_name      sqlite4_api->column_table_name
#define sqlite4_column_table_name16    sqlite4_api->column_table_name16
#define sqlite4_column_text            sqlite4_api->column_text
#define sqlite4_column_text16          sqlite4_api->column_text16
#define sqlite4_column_type            sqlite4_api->column_type
#define sqlite4_column_value           sqlite4_api->column_value
#define sqlite4_commit_hook            sqlite4_api->commit_hook
#define sqlite4_complete               sqlite4_api->complete
#define sqlite4_complete16             sqlite4_api->complete16
#define sqlite4_create_collation       sqlite4_api->create_collation
#define sqlite4_create_function        sqlite4_api->create_function
#define sqlite4_create_function16      sqlite4_api->create_function16
#define sqlite4_create_module          sqlite4_api->create_module
#define sqlite4_create_module_v2       sqlite4_api->create_module_v2
#define sqlite4_data_count             sqlite4_api->data_count
#define sqlite4_db_handle              sqlite4_api->db_handle
#define sqlite4_declare_vtab           sqlite4_api->declare_vtab
#define sqlite4_enable_shared_cache    sqlite4_api->enable_shared_cache
#define sqlite4_errcode                sqlite4_api->errcode
#define sqlite4_errmsg                 sqlite4_api->errmsg
#define sqlite4_errmsg16               sqlite4_api->errmsg16
#define sqlite4_exec                   sqlite4_api->exec
#ifndef SQLITE_OMIT_DEPRECATED
#define sqlite4_expired                sqlite4_api->expired
#endif
#define sqlite4_finalize               sqlite4_api->finalize
#define sqlite4_free                   sqlite4_api->free
#define sqlite4_free_table             sqlite4_api->free_table
#define sqlite4_get_autocommit         sqlite4_api->get_autocommit
#define sqlite4_get_auxdata            sqlite4_api->get_auxdata
#define sqlite4_get_table              sqlite4_api->get_table
#ifndef SQLITE_OMIT_DEPRECATED
#define sqlite4_global_recover         sqlite4_api->global_recover
#endif
#define sqlite4_interrupt              sqlite4_api->interruptx
#define sqlite4_last_insert_rowid      sqlite4_api->last_insert_rowid
#define sqlite4_libversion             sqlite4_api->libversion
#define sqlite4_libversion_number      sqlite4_api->libversion_number
#define sqlite4_malloc                 sqlite4_api->malloc
#define sqlite4_mprintf                sqlite4_api->mprintf
#define sqlite4_open                   sqlite4_api->open
#define sqlite4_open16                 sqlite4_api->open16
#define sqlite4_prepare                sqlite4_api->prepare
#define sqlite4_prepare16              sqlite4_api->prepare16
#define sqlite4_prepare_v2             sqlite4_api->prepare_v2
#define sqlite4_prepare16_v2           sqlite4_api->prepare16_v2
#define sqlite4_profile                sqlite4_api->profile
#define sqlite4_progress_handler       sqlite4_api->progress_handler
#define sqlite4_realloc                sqlite4_api->realloc
#define sqlite4_reset                  sqlite4_api->reset
#define sqlite4_result_blob            sqlite4_api->result_blob
#define sqlite4_result_double          sqlite4_api->result_double
#define sqlite4_result_error           sqlite4_api->result_error
#define sqlite4_result_error16         sqlite4_api->result_error16
#define sqlite4_result_int             sqlite4_api->result_int
#define sqlite4_result_int64           sqlite4_api->result_int64
#define sqlite4_result_null            sqlite4_api->result_null
#define sqlite4_result_text            sqlite4_api->result_text
#define sqlite4_result_text16          sqlite4_api->result_text16
#define sqlite4_result_text16be        sqlite4_api->result_text16be
#define sqlite4_result_text16le        sqlite4_api->result_text16le
#define sqlite4_result_value           sqlite4_api->result_value
#define sqlite4_rollback_hook          sqlite4_api->rollback_hook
#define sqlite4_set_authorizer         sqlite4_api->set_authorizer
#define sqlite4_set_auxdata            sqlite4_api->set_auxdata
#define sqlite4_snprintf               sqlite4_api->snprintf
#define sqlite4_step                   sqlite4_api->step
#define sqlite4_table_column_metadata  sqlite4_api->table_column_metadata
#define sqlite4_thread_cleanup         sqlite4_api->thread_cleanup
#define sqlite4_total_changes          sqlite4_api->total_changes
#define sqlite4_trace                  sqlite4_api->trace
#ifndef SQLITE_OMIT_DEPRECATED
#define sqlite4_transfer_bindings      sqlite4_api->transfer_bindings
#endif
#define sqlite4_update_hook            sqlite4_api->update_hook
#define sqlite4_user_data              sqlite4_api->user_data
#define sqlite4_value_blob             sqlite4_api->value_blob
#define sqlite4_value_bytes            sqlite4_api->value_bytes
#define sqlite4_value_bytes16          sqlite4_api->value_bytes16
#define sqlite4_value_double           sqlite4_api->value_double
#define sqlite4_value_int              sqlite4_api->value_int
#define sqlite4_value_int64            sqlite4_api->value_int64
#define sqlite4_value_numeric_type     sqlite4_api->value_numeric_type
#define sqlite4_value_text             sqlite4_api->value_text
#define sqlite4_value_text16           sqlite4_api->value_text16
#define sqlite4_value_text16be         sqlite4_api->value_text16be
#define sqlite4_value_text16le         sqlite4_api->value_text16le
#define sqlite4_value_type             sqlite4_api->value_type
#define sqlite4_vmprintf               sqlite4_api->vmprintf
#define sqlite4_overload_function      sqlite4_api->overload_function
#define sqlite4_prepare_v2             sqlite4_api->prepare_v2
#define sqlite4_prepare16_v2           sqlite4_api->prepare16_v2
#define sqlite4_clear_bindings         sqlite4_api->clear_bindings
#define sqlite4_bind_zeroblob          sqlite4_api->bind_zeroblob
#define sqlite4_blob_bytes             sqlite4_api->blob_bytes
#define sqlite4_blob_close             sqlite4_api->blob_close
#define sqlite4_blob_open              sqlite4_api->blob_open
#define sqlite4_blob_read              sqlite4_api->blob_read
#define sqlite4_blob_write             sqlite4_api->blob_write
#define sqlite4_file_control           sqlite4_api->file_control
#define sqlite4_memory_highwater       sqlite4_api->memory_highwater
#define sqlite4_memory_used            sqlite4_api->memory_used
#define sqlite4_mutex_alloc            sqlite4_api->mutex_alloc
#define sqlite4_mutex_enter            sqlite4_api->mutex_enter
#define sqlite4_mutex_free             sqlite4_api->mutex_free
#define sqlite4_mutex_leave            sqlite4_api->mutex_leave
#define sqlite4_mutex_try              sqlite4_api->mutex_try
#define sqlite4_open_v2                sqlite4_api->open_v2
#define sqlite4_release_memory         sqlite4_api->release_memory
#define sqlite4_result_error_nomem     sqlite4_api->result_error_nomem
#define sqlite4_result_error_toobig    sqlite4_api->result_error_toobig
#define sqlite4_sleep                  sqlite4_api->sleep
#define sqlite4_soft_heap_limit        sqlite4_api->soft_heap_limit
#define sqlite4_vfs_find               sqlite4_api->vfs_find
#define sqlite4_vfs_register           sqlite4_api->vfs_register
#define sqlite4_vfs_unregister         sqlite4_api->vfs_unregister
#define sqlite4_threadsafe             sqlite4_api->xthreadsafe
#define sqlite4_result_zeroblob        sqlite4_api->result_zeroblob
#define sqlite4_result_error_code      sqlite4_api->result_error_code
#define sqlite4_test_control           sqlite4_api->test_control
#define sqlite4_randomness             sqlite4_api->randomness
#define sqlite4_context_db_handle      sqlite4_api->context_db_handle
#define sqlite4_extended_result_codes  sqlite4_api->extended_result_codes
#define sqlite4_limit                  sqlite4_api->limit
#define sqlite4_next_stmt              sqlite4_api->next_stmt
#define sqlite4_sql                    sqlite4_api->sql
#define sqlite4_status                 sqlite4_api->status
#define sqlite4_backup_finish          sqlite4_api->backup_finish
#define sqlite4_backup_init            sqlite4_api->backup_init
#define sqlite4_backup_pagecount       sqlite4_api->backup_pagecount
#define sqlite4_backup_remaining       sqlite4_api->backup_remaining
#define sqlite4_backup_step            sqlite4_api->backup_step
#define sqlite4_compileoption_get      sqlite4_api->compileoption_get
#define sqlite4_compileoption_used     sqlite4_api->compileoption_used
#define sqlite4_create_function_v2     sqlite4_api->create_function_v2
#define sqlite4_db_config              sqlite4_api->db_config
#define sqlite4_db_mutex               sqlite4_api->db_mutex
#define sqlite4_db_status              sqlite4_api->db_status
#define sqlite4_extended_errcode       sqlite4_api->extended_errcode
#define sqlite4_log                    sqlite4_api->log
#define sqlite4_soft_heap_limit64      sqlite4_api->soft_heap_limit64
#define sqlite4_sourceid               sqlite4_api->sourceid
#define sqlite4_stmt_status            sqlite4_api->stmt_status
#define sqlite4_strnicmp               sqlite4_api->strnicmp
#define sqlite4_unlock_notify          sqlite4_api->unlock_notify
#define sqlite4_wal_autocheckpoint     sqlite4_api->wal_autocheckpoint
#define sqlite4_wal_checkpoint         sqlite4_api->wal_checkpoint
#define sqlite4_wal_hook               sqlite4_api->wal_hook
#define sqlite4_blob_reopen            sqlite4_api->blob_reopen
#define sqlite4_vtab_config            sqlite4_api->vtab_config
#define sqlite4_vtab_on_conflict       sqlite4_api->vtab_on_conflict
#endif /* SQLITE_CORE */

#define SQLITE_EXTENSION_INIT1     const sqlite4_api_routines *sqlite4_api = 0;
#define SQLITE_EXTENSION_INIT2(v)  sqlite4_api = v;

#endif /* _SQLITE3EXT_H_ */
