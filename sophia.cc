
#include <sophia.h>
#include <string.h>
#include <stdlib.h>
#include "sophia-cc.h"

namespace sophia {

/**
 * Cursor list `free` callback.
 */

static void
DestroyCursor(void *node) {
  if (node) sp_destroy(node);
}

Sophia::Sophia(const char *path) : path(path) {
  env = NULL;
  db = NULL;
  // cursor = NULL;
  cursors = list_new();
  cursors->free = DestroyCursor;
  open = false;
}

Sophia::~Sophia() {
  if (cursors) list_destroy(cursors);
  if (db) sp_destroy(db);
  if (env) sp_destroy(env);
}

bool
Sophia::IsOpen() {
  return open;
}

SophiaReturnCode
Sophia::Open(
    bool create_if_missing
  , bool read_only
  , int page_size
  , int merge_watermark
  , bool gc
) {
  uint32_t flags = 0;

  if (!(env = sp_env())) {
    return SOPHIA_ENV_ALLOC_ERROR;
  }

  if (create_if_missing) flags |= SPO_CREAT;

  if (read_only) {
    flags |= SPO_RDONLY;
  } else {
    flags |= SPO_RDWR;
  }

  if (-1 == sp_ctl(env, SPDIR, flags, path)) {
    free(env);
    return SOPHIA_OPEN_ERROR;
  }

  if (gc && -1 == sp_ctl(env, SPGC, 1)) {
    free(env);
    return SOPHIA_GC_ERROR;
  }

  if (-1 == sp_ctl(env, SPMERGEWM, merge_watermark)) {
    free(env);
    return SOPHIA_MW_ERROR;
  }

  if (-1 == sp_ctl(env, SPPAGE, page_size)) {
    free(env);
    return SOPHIA_PAGE_ERROR;
  }

  if (!(db = sp_open(env))) {
    free(env);
    return SOPHIA_ENV_ERROR;
  }

  open = true;

  return SOPHIA_SUCCESS;
}

SophiaReturnCode
Sophia::Close() {
  // noop if we're already closed
  if (!open) return SOPHIA_SUCCESS;

  if (cursors) list_destroy(cursors);
  cursors = NULL;

  if (db && -1 == sp_destroy(db)) {
    return SOPHIA_DESTROY_ERROR;
  }
  db = NULL;

  if (env && -1 == sp_destroy(env)) {
    return SOPHIA_DESTROY_ERROR;
  }
  env = NULL;

  open = false;

  return SOPHIA_SUCCESS;
}

SophiaReturnCode
Sophia::Set(
    const char *key
  , size_t keysize
  , const char *value
  , size_t valuesize
) {
  if (-1 == sp_set(db, key, keysize, value, valuesize)) {
    return SOPHIA_DB_ERROR;
  }
  return SOPHIA_SUCCESS;
}

SophiaReturnCode
Sophia::Set(const char *key, const char *value) {
  size_t keysize = strlen(key) + 1;
  size_t valuesize = strlen(value) + 1;
  return Set(key, keysize, value, valuesize);
}

char *
Sophia::Get(const char *key, size_t keysize) {
  void *ref = NULL;
  char *value = NULL;
  size_t valuesize;

  if (-1 == sp_get(db, key, keysize, &ref, &valuesize)) {
    return NULL;
  }

  if (NULL == ref) return NULL;

  value = (char *) ref;
  return value;
}

char *
Sophia::Get(const char *key) {
  size_t keysize = strlen(key) + 1;
  return Get(key, keysize);
}

SophiaReturnCode
Sophia::Delete(const char *key, size_t keysize) {
  if (-1 == sp_delete(db, key, keysize)) {
    return SOPHIA_DB_ERROR;
  }
  return SOPHIA_SUCCESS;
}

SophiaReturnCode
Sophia::Delete(const char *key) {
  size_t keysize = strlen(key) + 1;
  return Delete(key, keysize);
}

SophiaReturnCode
Sophia::Count(size_t *n) {
  size_t count = 0;
  list_node_t *cursor_node = NULL;

  void *cursor = sp_cursor(db, SPGT, NULL, 0);
  if (NULL == cursor) {
    return SOPHIA_DB_ERROR;
  }

  cursor_node = list_node_new(cursor);
  list_rpush(cursors, cursor_node);
  while (sp_fetch(cursor)) {
    if (sp_key(cursor)) {
      count++;
    }
  }

  // remove and destroy the cursor
  list_remove(cursors, cursor_node);
  *n = count;

  return SOPHIA_SUCCESS;
}

/**
 * Internal store for key/size.
 */

typedef struct {
  const char *key;
  size_t size;
} ClearKey;

SophiaReturnCode
Sophia::Clear() {
  SophiaReturnCode rc;
  size_t count;
  void *cursor = NULL;
  list_node_t *cursor_node = NULL;

  rc = Count(&count);
  if (SOPHIA_SUCCESS != rc) {
    return rc;
  }

  cursor = sp_cursor(db, SPGT, NULL, 0);
  if (NULL == cursor) return SOPHIA_DB_ERROR;
  cursor_node = list_node_new(cursor);
  list_rpush(cursors, cursor_node);

  ClearKey keys[count];
  int i = 0;
  while (sp_fetch(cursor)) {
    keys[i].key = sp_key(cursor);
    keys[i].size = sp_keysize(cursor);
    i++;
  }

  // remove and destroy the cursor
  list_remove(cursors, cursor_node);

  for (int i = 0; i < (int) count; i++) {
    rc = Delete(keys[i].key, keys[i].size);
    if (SOPHIA_SUCCESS != rc) return rc;
  }

  return SOPHIA_SUCCESS;
}

const char *
Sophia::Error(SophiaReturnCode rc) {
  char *err = NULL;
  switch (rc) {
    case SOPHIA_SUCCESS:
      return NULL;

    case SOPHIA_ENV_ALLOC_ERROR:
      return "Failed to allocate environment";
    case SOPHIA_OPEN_ERROR:
      return "Failed to open/create repository";
    case SOPHIA_GC_ERROR:
      return "Failed to enable GC";
    case SOPHIA_MW_ERROR:
      return "Failed to set merge watermark";
    case SOPHIA_PAGE_ERROR:
      return "Failed to set page size";
    case SOPHIA_DESTROY_ERROR:
      return "Failed to destroy environment";
    case SOPHIA_CURSOR_ALREADY_OPEN_ERROR:
      return "An existing cursor is open";

    case SOPHIA_TRANSACTION_ALLOC_ERROR:
      return "Failed to allocate transaction operation";
    case SOPHIA_TRANSACTION_OPERATION_ERROR:
      return "Failed to add operation to transaction stack";
    case SOPHIA_TRANSACTION_NOT_OPEN_ERROR:
      return "Transaction not open";

    case SOPHIA_DATABASE_NOT_OPEN_ERROR:
      return "Database not open";

    case SOPHIA_ENV_ERROR:
      if (!env || !(err = sp_error(env))) {
        return "Unknown environment error";
      }
      return err;
    case SOPHIA_DB_ERROR:
      if (!db || !(err = sp_error(db))) {
        return "Unknown database error";
      }
      return err;
  }
  return NULL;
}

/**
 * Operation list `free` callback.
 */

static void
DestroyOperation(void *node) {
  TransactionOperation *operation = (TransactionOperation *) node;
  if (operation->key) free(operation->key);
  if (operation->value) free(operation->value);
  delete operation;
}


/**
 * Sophia transaction.
 */

Transaction::Transaction(Sophia *sp) : sp(sp) {
  operations = list_new();
  operations->free = DestroyOperation;
}

Transaction::~Transaction() {
  if (operations) {
    list_destroy(operations);
  }
}

SophiaReturnCode
Transaction::Begin() {
  if (!sp->IsOpen()) return SOPHIA_DATABASE_NOT_OPEN_ERROR;
  if (-1 == sp_begin(sp->db)) return SOPHIA_DB_ERROR;
  return SOPHIA_SUCCESS;
}

SophiaReturnCode
Transaction::Set(
    const char *key
  , size_t keysize
  , const char *value
  , size_t valuesize
) {
  TransactionOperation *operation = new TransactionOperation;
  operation->type = TRANSACTION_OPERATION_SET;
  operation->key = strdup(key);
  operation->keysize = keysize;
  operation->value = strdup(value);
  operation->valuesize = valuesize;
  return AddOperation(operation);
}

SophiaReturnCode
Transaction::Set(const char *key, const char *value) {
  size_t keysize = strlen(key) + 1;
  size_t valuesize = strlen(value) + 1;
  return this->Set(key, keysize, value, valuesize);
}

SophiaReturnCode
Transaction::Delete(const char *key, size_t keysize) {
  TransactionOperation *operation = new TransactionOperation;
  operation->type = TRANSACTION_OPERATION_DELETE;
  operation->key = strdup(key);
  operation->keysize = keysize;
  operation->value = NULL;
  operation->valuesize = -1;
  return AddOperation(operation);
}

SophiaReturnCode
Transaction::Delete(const char *key) {
  size_t keysize = strlen(key) + 1;
  return Delete(key, keysize);
}

SophiaReturnCode
Transaction::AddOperation(TransactionOperation *operation) {
  // cannot write to a committed/bad transaction
  if (!operations) return SOPHIA_TRANSACTION_NOT_OPEN_ERROR;

  list_node_t *node = list_node_new(operation);
  if (!node) return SOPHIA_TRANSACTION_ALLOC_ERROR;

  list_rpush(operations, node);
  return SOPHIA_SUCCESS;
}

SophiaReturnCode
Transaction::Commit() {
  list_node_t *node;
  list_iterator_t *iterator = list_iterator_new(operations, LIST_HEAD);

  while ((node = list_iterator_next(iterator))) {
    TransactionOperation *operation = (TransactionOperation *) node->val;
    int rc;

    if (TRANSACTION_OPERATION_SET == operation->type) {
      rc = sp->Set(
          operation->key
        , operation->keysize
        , operation->value
        , operation->valuesize
      );
    } else {
      rc = sp->Delete(
          operation->key
        , operation->keysize
      );
    }

    if (SOPHIA_SUCCESS != rc) {
      list_iterator_destroy(iterator);
      list_destroy(operations);
      operations = NULL;
      return SOPHIA_DB_ERROR;
    }
  }

  list_iterator_destroy(iterator);
  list_destroy(operations);
  operations = NULL;

  if (-1 == sp_commit(sp->db)) {
    return SOPHIA_DB_ERROR;
  }

  return SOPHIA_SUCCESS;
}

SophiaReturnCode
Transaction::Rollback() {
  if (-1 == sp_rollback(sp->db)) {
    return SOPHIA_DB_ERROR;
  }

  if (operations) {
    list_destroy(operations);
    operations = NULL;
  }

  return SOPHIA_SUCCESS;
}

/**
 * Iterator.
 */

Iterator::Iterator(Sophia *sp) : sp(sp) {
  order = SPGT;
  start = NULL;
  startsize = 0;
  end = NULL;
  endsize = 0;
  cursor = NULL;
}

Iterator::Iterator(
    Sophia *sp
  , sporder order
) : sp(sp), order(order) {
  start = NULL;
  startsize = 0;
  end = NULL;
  endsize = 0;
  cursor = NULL;
}

Iterator::Iterator(
    Sophia *sp
  , sporder order
  , const char *start
) : sp(sp), order(order), start(start) {
  startsize = start ? strlen(start) + 1 : 0;
  end = NULL;
  endsize = 0;
  cursor = NULL;
}

Iterator::Iterator(
    Sophia *sp
  , sporder order
  , const char *start
  , size_t startsize
) : sp(sp), order(order), start(start), startsize(startsize) {
  end = NULL;
  endsize = 0;
  cursor = NULL;
}

Iterator::Iterator(
    Sophia *sp
  , sporder order
  , const char *start
  , const char *end
) : sp(sp), order(order), start(start), end(end) {
  startsize = start ? strlen(start) + 1 : 0;
  endsize = strlen(end) + 1;
  end = NULL;
  endsize = 0;
  cursor = NULL;
}

Iterator::Iterator(
    Sophia *sp
  , sporder order
  , const char *start
  , size_t startsize
  , const char *end
  , size_t endsize
) : sp(sp)
  , order(order)
  , start(start)
  , startsize(startsize)
  , end(end)
  , endsize(endsize) {
  cursor = NULL;
}

Iterator::~Iterator() {
  if (cursor) {
    list_remove(sp->cursors, cursor_node);
    cursor = NULL;
    cursor_node = NULL;
  }
}

SophiaReturnCode
Iterator::Begin() {
  if (!sp->IsOpen()) return SOPHIA_DATABASE_NOT_OPEN_ERROR;
  cursor = sp_cursor(sp->db, order, start, startsize);
  if (NULL == cursor) return SOPHIA_DB_ERROR;
  cursor_node = list_node_new(cursor);
  list_rpush(sp->cursors, cursor_node);
  return SOPHIA_SUCCESS;
}

IteratorResult *
Iterator::Next() {
  size_t keysize;
  size_t valuesize;
  const char *k;
  const char *v;
  IteratorResult *result = NULL;

  if (0 == sp_fetch(cursor)) return NULL;

  // TODO: could failure here ever indicate error?
  if (!(k = sp_key(cursor)) || !(v = sp_value(cursor))) {
    return NULL;
  }

  keysize = sp_keysize(cursor);
  valuesize = sp_valuesize(cursor);

  // don't go past end
  if (end && 0 == strcmp(end, k)) {
    return NULL;
  }

  result = new IteratorResult;
  result->key = k;
  result->value = v;
  return result;
}

SophiaReturnCode
Iterator::End() {
  if (cursor) {
    list_remove(sp->cursors, cursor_node);
    cursor = NULL;
  }
  return SOPHIA_SUCCESS;
}

} // namespace sophia
