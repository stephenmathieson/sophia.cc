
#include "sophia-cc.h"
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

using namespace sophia;

#define SUITE(title) \
  printf("\n  \e[36m%s\e[0m\n", title)

#define TEST(suite, name) \
  static void Test##suite##name(void)

#define RUN_TEST(suite, test) \
  Test##suite##test(); \
  printf("    \e[92m✓ \e[90m"#suite "::" #test "\e[0m\n");

#define SOPHIA_ASSERT(rc) \
  if (SOPHIA_SUCCESS != rc) { \
    fprintf( \
        stderr \
      , "Error: %s (%d / %s at line %d)\n" \
      , sp->Error(rc) \
      , rc \
      , __PRETTY_FUNCTION__ \
      , __LINE__ \
    ); \
    exit(1); \
  }

/**
 * Sophia tests.
 */

TEST(Sophia, Set) {
  Sophia *sp = new Sophia("testdb");
  // shouldn't segfault
  assert(SOPHIA_DATABASE_NOT_OPEN_ERROR == sp->Set("foo", "bar"));

  SOPHIA_ASSERT(sp->Open());

  for (int i = 0; i < 100; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%03d", i);
    sprintf(value, "value%03d", i);
    SOPHIA_ASSERT(sp->Set(key, value));
  }

  SOPHIA_ASSERT(sp->Close());
  delete sp;
}

TEST(Sophia, Get) {
  Sophia *sp = new Sophia("testdb");
  // shouldn't segfault
  assert(!sp->Get("foo"));

  SOPHIA_ASSERT(sp->Open());

  for (int i = 0; i < 100; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%03d", i);
    sprintf(value, "value%03d", i);
    char *_actual = sp->Get(key);
    assert(0 == strcmp(value, _actual));
    free(_actual);
  }

  assert(NULL == sp->Get("asdf"));
  assert(NULL == sp->Get("lkjh"));

  SOPHIA_ASSERT(sp->Close());
  delete sp;
}

TEST(Sophia, Delete) {
  Sophia *sp = new Sophia("testdb");
  // shouldn't segfault
  assert(SOPHIA_DATABASE_NOT_OPEN_ERROR == sp->Delete("foo"));
  SOPHIA_ASSERT(sp->Open());

  for (int i = 0; i < 100; i += 2) {
    char key[100];
    sprintf(key, "key%03d", i);
    SOPHIA_ASSERT(sp->Delete(key));
    assert(NULL == sp->Get(key));
  }

  SOPHIA_ASSERT(sp->Close());
  delete sp;
}

TEST(Sophia, Error) {
  Sophia *sp = new Sophia("/1/2/3");
  assert(0 == strcmp(
      "Unknown environment error"
    , sp->Error(SOPHIA_ENV_ERROR)
  ));
  assert(0 == strcmp(
      "Unknown database error"
    , sp->Error(SOPHIA_DB_ERROR)
  ));
  delete sp;
}

TEST(Sophia, IsOpen) {
  Sophia *sp = new Sophia("testdb");

  SOPHIA_ASSERT(sp->Open());
  assert(true == sp->IsOpen());
  SOPHIA_ASSERT(sp->Close());
  assert(false == sp->IsOpen());

  delete sp;
}

TEST(Sophia, Clear) {
  Sophia *sp = new Sophia("testdb");
  // shouldn't segfault
  assert(SOPHIA_DATABASE_NOT_OPEN_ERROR == sp->Clear());
  SOPHIA_ASSERT(sp->Open());
  SOPHIA_ASSERT(sp->Clear());
  for (int i = 0; i < 100; i++) {
    char key[100];
    sprintf(key, "key%03d", i);
    assert(NULL == sp->Get(key));
  }
  SOPHIA_ASSERT(sp->Close());
  delete sp;
}

TEST(Sophia, Count) {
  Sophia *sp = new Sophia("testdb");
  size_t count;

  // shouldn't segfault
  assert(SOPHIA_DATABASE_NOT_OPEN_ERROR == sp->Count(&count));

  SOPHIA_ASSERT(sp->Open());

  for (int i = 0; i < 5000; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%05d", i);
    sprintf(value, "value%05d", i);
    SOPHIA_ASSERT(sp->Set(key, value));
  }

  SOPHIA_ASSERT(sp->Count(&count));
  assert(5000 == count);

  SOPHIA_ASSERT(sp->Close());
  delete sp;
}

/**
 * Iterator tests.
 */

TEST(Iterator, Begin) {
  Sophia *sp = new Sophia("testdb");
  Iterator *it = NULL;
  IteratorResult *res = NULL;

  it = new Iterator(sp);
  assert(SOPHIA_DATABASE_NOT_OPEN_ERROR == it->Begin());
  delete it;

  SOPHIA_ASSERT(sp->Open());

  // test reverse

  it = new Iterator(sp, SPLT);
  SOPHIA_ASSERT(it->Begin());

  res = it->Next();
  assert(0 == strcmp("key04999", res->key));
  assert(0 == strcmp("value04999", res->value));
  delete res;

  SOPHIA_ASSERT(it->End());
  delete it;

  // test start

  it = new Iterator(sp, SPGT, "key03999");
  SOPHIA_ASSERT(it->Begin());

  res = it->Next();
  assert(0 == strcmp("key04000", res->key));
  assert(0 == strcmp("value04000", res->value));

  delete res;
  SOPHIA_ASSERT(it->End());
  delete it;

  // test start/end

  it = new Iterator(sp, SPGT, "key03999", "key04001");
  SOPHIA_ASSERT(it->Begin());

  res = it->Next();
  assert(0 == strcmp("key04000", res->key));
  assert(0 == strcmp("value04000", res->value));
  delete res;

  assert(NULL == it->Next());
  SOPHIA_ASSERT(it->End());
  delete it;

  // test end

  it = new Iterator(sp, SPGT, NULL, "key00002");
  SOPHIA_ASSERT(it->Begin());
  res = it->Next();
  assert(0 == strcmp("key00000", res->key));
  assert(0 == strcmp("value00000", res->value));
  delete res;

  res = it->Next();
  assert(0 == strcmp("key00001", res->key));
  assert(0 == strcmp("value00001", res->value));
  delete res;

  assert(NULL == it->Next());
  SOPHIA_ASSERT(it->End());
  delete it;

  SOPHIA_ASSERT(sp->Close());
  delete sp;
}

TEST(Iterator, Next) {
  Sophia *sp = new Sophia("testdb");
  Iterator *it = NULL;
  Iterator *it2 = NULL;
  IteratorResult *res = NULL;
  int i = 0;

  SOPHIA_ASSERT(sp->Open());
  it = new Iterator(sp, SPGT, "key00100", "key00500");

  assert(0 == it->Begin());

  i = 100;
  while ((res = it->Next())) {
    i++;
    char key[100];
    char value[100];
    sprintf(key, "key%05d", i);
    sprintf(value, "value%05d", i);

    assert(0 == strcmp(key, res->key));
    assert(0 == strcmp(value, res->value));

    delete res;
  }

  assert(499 == i);
  SOPHIA_ASSERT(it->End());
  delete it;

  // multiple iterators

  it = new Iterator(sp, SPGT, "key00000", "key00100");
  it2 = new Iterator(sp, SPLT, "key00100", "key00000");

  SOPHIA_ASSERT(it->Begin());
  SOPHIA_ASSERT(it2->Begin());

  res = it->Next();
  assert(0 == strcmp("key00001", res->key));
  assert(0 == strcmp("value00001", res->value));
  delete res;

  res = it2->Next();
  assert(0 == strcmp("key00099", res->key));
  assert(0 == strcmp("value00099", res->value));
  delete res;

  SOPHIA_ASSERT(it->End());
  SOPHIA_ASSERT(it2->End());

  SOPHIA_ASSERT(sp->Close());
  delete sp;
}

/**
 * Transaction tests.
 */

TEST(Transaction, Begin) {
  Sophia *sp = new Sophia("testdb");

  Transaction *t = new Transaction(sp);

  assert(SOPHIA_DATABASE_NOT_OPEN_ERROR == t->Begin());

  SOPHIA_ASSERT(sp->Open());
  SOPHIA_ASSERT(sp->Clear());

  SOPHIA_ASSERT(t->Begin());
  SOPHIA_ASSERT(t->Rollback());
  delete t;

  SOPHIA_ASSERT(sp->Close());
  delete sp;
}

TEST(Transaction, Set) {
  Sophia *sp = new Sophia("testdb");
  Transaction *t = new Transaction(sp);
  SOPHIA_ASSERT(sp->Open());
  SOPHIA_ASSERT(t->Begin());

  for (int i = 0; i < 5000; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%05d", i);
    sprintf(value, "value%05d", i);
    SOPHIA_ASSERT(t->Set(key, value));
  }

  SOPHIA_ASSERT(t->Commit());
  delete t;

  size_t count;
  SOPHIA_ASSERT(sp->Count(&count));
  assert(5000 == count);

  SOPHIA_ASSERT(sp->Close());
  delete sp;
}

TEST(Transaction, Delete) {
  Sophia *sp = new Sophia("testdb");
  Transaction *t = new Transaction(sp);
  SOPHIA_ASSERT(sp->Open());
  SOPHIA_ASSERT(t->Begin());

  for (int i = 0; i < 5000; i += 2) {
    char key[100];
    sprintf(key, "key%05d", i);
    SOPHIA_ASSERT(t->Delete(key));
  }

  SOPHIA_ASSERT(t->Commit());
  delete t;

  size_t count;
  SOPHIA_ASSERT(sp->Count(&count));
  assert(2500 == count);

  SOPHIA_ASSERT(sp->Close());
  delete sp;
}

TEST(Transaction, Commit) {
  Sophia *sp = new Sophia("testdb");
  Transaction *t = new Transaction(sp);
  SOPHIA_ASSERT(sp->Open());
  SOPHIA_ASSERT(sp->Clear());
  SOPHIA_ASSERT(t->Begin());

  size_t sets = 0;
  size_t dels = 0;

  for (int i = 0; i < 10000; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%05d", i);
    sprintf(value, "value%05d", i);

    int n = (rand() % 3) + 1;
    if (2 == n) {
      SOPHIA_ASSERT(t->Delete(key));
      dels++;
    } else {
      SOPHIA_ASSERT(t->Set(key, value));
      sets++;
    }
  }

  SOPHIA_ASSERT(t->Commit());

  size_t count;
  SOPHIA_ASSERT(sp->Count(&count));
  assert(sets == count);

  SOPHIA_ASSERT(sp->Close());
  delete sp;
}

int
main(void) {
  srand(time(0));

  SUITE("Sophia");
  RUN_TEST(Sophia, Set);
  RUN_TEST(Sophia, Get);
  RUN_TEST(Sophia, Delete);
  RUN_TEST(Sophia, Error);
  RUN_TEST(Sophia, IsOpen);
  RUN_TEST(Sophia, Clear);
  RUN_TEST(Sophia, Count);

  SUITE("Iterator");
  RUN_TEST(Iterator, Begin);
  RUN_TEST(Iterator, Next);

  SUITE("Transaction");
  RUN_TEST(Transaction, Begin);
  RUN_TEST(Transaction, Set);
  RUN_TEST(Transaction, Delete);
  RUN_TEST(Transaction, Commit);

  printf("\n");
}
