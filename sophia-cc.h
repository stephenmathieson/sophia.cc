
#ifndef SOPHIA_CC_H
#define SOPHIA_CC_H 1

#include <list.h>
#include <string.h>
#include <sophia.h>

namespace sophia {

/**
 * Return codes.
 */

typedef enum {
    SOPHIA_SUCCESS = 0

  , SOPHIA_ENV_ALLOC_ERROR = -1
  , SOPHIA_OPEN_ERROR = -2
  , SOPHIA_GC_ERROR = -3
  , SOPHIA_MW_ERROR = -4
  , SOPHIA_PAGE_ERROR = -5
  , SOPHIA_DESTROY_ERROR = -6
  , SOPHIA_CURSOR_ALREADY_OPEN_ERROR = -7

  , SOPHIA_TRANSACTION_ALLOC_ERROR = -8
  , SOPHIA_TRANSACTION_OPERATION_ERROR = -9
  , SOPHIA_TRANSACTION_NOT_OPEN_ERROR = -10

  , SOPHIA_DATABASE_NOT_OPEN_ERROR = - 11

  , SOPHIA_ENV_ERROR = -200
  , SOPHIA_DB_ERROR = -300
} SophiaReturnCode;

/**
 * Operation types.
 */

typedef enum {
    TRANSACTION_OPERATION_SET = 0
  , TRANSACTION_OPERATION_DELETE = 1
} TransactionOperationType;

/**
 * Operation.
 */

typedef struct {
  char *key;
  size_t keysize;
  char *value;
  size_t valuesize;
  TransactionOperationType type;
} TransactionOperation;

/**
 * Iterator->Next() result.
 */

typedef struct {
  const char *key;
  const char *value;
} IteratorResult;

/**
 * Iterator direction.
 */

typedef enum {
    ITERATOR_FORWARD = 0
  , ITERATOR_REVERSE = 1
} IteratorDirection;

// forward defs
class Transaction;
class Iterator;

/**
 * Sophia wrapper.
 */

class Sophia {
  public:

    /**
     * Sophia db.
     */

    void *db;

    /**
     * Sophia env.
     */

    void *env;

    /**
     * Sophia cursors.
     */

    list_t *cursors;

    /**
     * Create a new Sophia instance for db `path`.
     */

    Sophia(const char *path);

    /**
     * Destroy.
     */

    ~Sophia();

    /**
     * Check if the db is open.
     */

    bool
    IsOpen();

    /**
     * Open/create the database.
     */

    SophiaReturnCode
    Open(
        bool create_if_missing = true
      , bool read_only = false
      , int page_size = 2048
      , int merge_watermark = 100000
      , bool gc = true
    );

    /**
     * Close the database.
     */

    SophiaReturnCode
    Close();

    /**
     * Set `key` of `keysize` to `value` of `valuesize`.
     */

    SophiaReturnCode
    Set(
        const char *key
      , size_t keysize
      , const char *value
      , size_t valuesize
    );

    /**
     * Set `key` = `value` using the default
     * (`strlen(ptr) + 1`) algorithm to calculate
     * key/value sizes.
     */

    SophiaReturnCode
    Set(const char *key, const char *value);

    /**
     * Get the value of `key` using the given `keysize`.
     *
     * `free` the result when done.
     */

    char *
    Get(const char *key, size_t keysize);

    /**
     * Get the value of `key` using the default
     * (`strlen(ptr) + 1`) algorithm to calculate
     * key size.
     *
     * `free` the result when done.
     */

    char *
    Get(const char *key);

    /**
     * Get the error string associated with return code `rc`.
     *
     * Do not destroy the result.
     */

    const char *
    Error(SophiaReturnCode rc);

    /**
     * Delete `key` of `keysize`.
     */

    SophiaReturnCode
    Delete(const char *key, size_t keysize);

    /**
     * Delete `key` using the default (`strlen(ptr) + 1`)
     * algorithm to calculate key size.
     */

    SophiaReturnCode
    Delete(const char *key);

    /**
     * Put the number of keys in `n`.
     */

    SophiaReturnCode
    Count(size_t *n);

    /**
     * Clear *all* keys in the database.
     */

    SophiaReturnCode
    Clear();

  private:

    /**
     * Open flag.
     */

    bool open;

    /**
     * Path to db.
     */

    const char *path;
};

/**
 * Transaction wrapper.
 */

class Transaction {
  public:

    Transaction(Sophia *sp);
    ~Transaction();

    /**
     * Begin the transaction.
     */

    SophiaReturnCode
    Begin();

    /**
     * Create a pending set (`key` = `value`) using the
     * default (`strlen(ptr) + 1`) algorithm to calculate
     * key/value sizes.
     */

    SophiaReturnCode
    Set(const char *key, const char *value);

    /**
     * Create a pending set (`key` = `value`) using the
     * given key/value sizes.
     */

    SophiaReturnCode
    Set(
        const char *key
      , size_t keysize
      , const char *value
      , size_t valuesize
    );

    /**
     * Create a pending delete (`key` = `NULL`) using the
     * default (`strlen(ptr) + 1`) algorithm to calculate
     * key size.
     */

    SophiaReturnCode
    Delete(const char *key);

    /**
     * Create a pending delete (`key` = `NULL`) using the
     * given key size.
     */

    SophiaReturnCode
    Delete(const char *key, size_t keysize);

    /**
     * Commit the transaction.
     */

    SophiaReturnCode
    Commit();

    /**
     * Rollback the transaction.
     */

    SophiaReturnCode
    Rollback();

  private:

    /**
     * Pointer to the Sophia class.
     */

    Sophia *sp;

    /**
     * All pending operations.
     */

    list_t *operations;

    /**
     * Add an operation to the stack.
     */

    SophiaReturnCode
    AddOperation(TransactionOperation *operation);
};

/**
 * Iterator.
 */

class Iterator {
  public:

    Iterator(Sophia *sp);
    Iterator(Sophia *sp, sporder order);
    Iterator(
        Sophia *sp
      , sporder order
      , const char *start
    );
    Iterator(
        Sophia *sp
      , sporder order
      , const char *start
      , size_t keysize
    );
    Iterator(
        Sophia *sp
      , sporder order
      , const char *start
      , const char *end
    );
    Iterator(
        Sophia *sp
      , sporder order
      , const char *start
      , size_t keysize
      , const char *end
      , size_t endsize
    );

    ~Iterator();

    /**
     * Begin the iterator.
     */

    SophiaReturnCode
    Begin();

    /**
     * Get the next result.
     */

    IteratorResult *
    Next();

    /**
     * End the iterator.
     */

    SophiaReturnCode
    End();

  private:

    /**
     * Owner Sophia instance.
     */

    Sophia *sp;

    /**
     * Iterator order.
     */

    sporder order;

    /**
     * Cursor.
     */

    void *cursor;

    /**
     * Cursor node in Sophia's list.
     */

    list_node_t *cursor_node;

    /**
     * Start key.
     */

    const char *start;

    /**
     * Start keysize.
     */

    size_t startsize;

    /**
     * End key.
     */

    const char *end;

    /**
     * End keysize.
     */

    size_t endsize;
};

} // namespace sophia

#endif
