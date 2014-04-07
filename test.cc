
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


/**
 * Sophia tests.
 */

TEST(Sophia, Set) {
  Sophia *sp = new Sophia("testdb");
  assert(SOPHIA_SUCCESS == sp->Open());

  for (int i = 0; i < 100; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%03d", i);
    sprintf(value, "value%03d", i);
    assert(SOPHIA_SUCCESS == sp->Set(key, value));
  }

  assert(SOPHIA_SUCCESS == sp->Close());
  delete sp;
}

TEST(Sophia, Get) {
  Sophia *sp = new Sophia("testdb");
  assert(SOPHIA_SUCCESS == sp->Open());

  for (int i = 0; i < 100; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%03d", i);
    sprintf(value, "value%03d", i);
    char *_actual = sp->Get(key);
    assert(0 == strcmp(value, _actual));
    delete _actual;
  }

  assert(NULL == sp->Get("asdf"));
  assert(NULL == sp->Get("lkjh"));

  assert(SOPHIA_SUCCESS == sp->Close());
  delete sp;
}

TEST(Sophia, Delete) {
  Sophia *sp = new Sophia("testdb");
  assert(SOPHIA_SUCCESS == sp->Open());

  for (int i = 0; i < 100; i += 2) {
    char key[100];
    sprintf(key, "key%03d", i);
    assert(SOPHIA_SUCCESS == sp->Delete(key));
    assert(NULL == sp->Get(key));
  }

  assert(SOPHIA_SUCCESS == sp->Close());
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

  assert(SOPHIA_SUCCESS == sp->Open());
  assert(true == sp->IsOpen());
  assert(SOPHIA_SUCCESS == sp->Close());
  assert(false == sp->IsOpen());

  delete sp;
}

TEST(Sophia, Clear) {
  Sophia *sp = new Sophia("testdb");
  assert(SOPHIA_SUCCESS == sp->Open());
  assert(SOPHIA_SUCCESS == sp->Clear());
  for (int i = 0; i < 100; i++) {
    char key[100];
    sprintf(key, "key%03d", i);
    assert(NULL == sp->Get(key));
  }
  assert(SOPHIA_SUCCESS == sp->Close());
  delete sp;
}

TEST(Sophia, Count) {
  Sophia *sp = new Sophia("testdb");
  assert(SOPHIA_SUCCESS == sp->Open());

  for (int i = 0; i < 5000; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%05d", i);
    sprintf(value, "value%05d", i);
    assert(SOPHIA_SUCCESS == sp->Set(key, value));
  }

  size_t count;
  assert(SOPHIA_SUCCESS == sp->Count(&count));
  assert(5000 == count);

  assert(SOPHIA_SUCCESS == sp->Close());
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

  assert(SOPHIA_SUCCESS == sp->Open());

  // test reverse

  it = new Iterator(sp, SPLT);
  assert(SOPHIA_SUCCESS == it->Begin());

  res = it->Next();
  assert(0 == strcmp("key04999", res->key));
  assert(0 == strcmp("value04999", res->value));
  delete res;

  assert(SOPHIA_SUCCESS == it->End());
  delete it;

  // test start

  it = new Iterator(sp, SPGT, "key03999");
  assert(SOPHIA_SUCCESS == it->Begin());

  res = it->Next();
  assert(0 == strcmp("key04000", res->key));
  assert(0 == strcmp("value04000", res->value));

  delete res;
  assert(SOPHIA_SUCCESS == it->End());
  delete it;

  // test start/end

  it = new Iterator(sp, SPGT, "key03999", "key04001");
  assert(SOPHIA_SUCCESS == it->Begin());

  res = it->Next();
  assert(0 == strcmp("key04000", res->key));
  assert(0 == strcmp("value04000", res->value));
  delete res;

  assert(NULL == it->Next());
  assert(SOPHIA_SUCCESS == it->End());
  delete it;

  // test end

  it = new Iterator(sp, SPGT, NULL, "key00002");
  assert(SOPHIA_SUCCESS == it->Begin());
  res = it->Next();
  assert(0 == strcmp("key00000", res->key));
  assert(0 == strcmp("value00000", res->value));
  delete res;

  res = it->Next();
  assert(0 == strcmp("key00001", res->key));
  assert(0 == strcmp("value00001", res->value));
  delete res;

  assert(NULL == it->Next());
  assert(SOPHIA_SUCCESS == it->End());
  delete it;

  assert(SOPHIA_SUCCESS == sp->Close());
  delete sp;
}

TEST(Iterator, Next) {
  Sophia *sp = new Sophia("testdb");
  Iterator *it = NULL;
  IteratorResult *res = NULL;
  int i = 0;

  assert(SOPHIA_SUCCESS == sp->Open());
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
  assert(SOPHIA_SUCCESS == it->End());
  delete it;

  assert(SOPHIA_SUCCESS == sp->Close());
  delete sp;
}

/**
 * Transaction tests.
 */

TEST(Transaction, Begin) {
  Sophia *sp = new Sophia("testdb");

  Transaction *t = new Transaction(sp);

  assert(SOPHIA_DATABASE_NOT_OPEN_ERROR == t->Begin());

  assert(SOPHIA_SUCCESS == sp->Open());
  assert(SOPHIA_SUCCESS == sp->Clear());

  assert(SOPHIA_SUCCESS == t->Begin());
  assert(SOPHIA_SUCCESS == t->Rollback());
  delete t;

  assert(SOPHIA_SUCCESS == sp->Close());
  delete sp;
}

TEST(Transaction, Set) {
  Sophia *sp = new Sophia("testdb");
  Transaction *t = new Transaction(sp);
  assert(SOPHIA_SUCCESS == sp->Open());
  assert(SOPHIA_SUCCESS == t->Begin());

  for (int i = 0; i < 5000; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%05d", i);
    sprintf(value, "value%05d", i);
    assert(SOPHIA_SUCCESS == t->Set(key, value));
  }

  assert(SOPHIA_SUCCESS == t->Commit());
  delete t;

  size_t count;
  assert(SOPHIA_SUCCESS == sp->Count(&count));
  assert(5000 == count);

  assert(SOPHIA_SUCCESS == sp->Close());
  delete sp;
}

TEST(Transaction, Delete) {
  Sophia *sp = new Sophia("testdb");
  Transaction *t = new Transaction(sp);
  assert(SOPHIA_SUCCESS == sp->Open());
  assert(SOPHIA_SUCCESS == t->Begin());

  for (int i = 0; i < 5000; i += 2) {
    char key[100];
    sprintf(key, "key%05d", i);
    assert(SOPHIA_SUCCESS == t->Delete(key));
  }

  assert(SOPHIA_SUCCESS == t->Commit());
  delete t;

  size_t count;
  assert(SOPHIA_SUCCESS == sp->Count(&count));
  assert(2500 == count);

  assert(SOPHIA_SUCCESS == sp->Close());
  delete sp;
}

TEST(Transaction, Commit) {
  Sophia *sp = new Sophia("testdb");
  Transaction *t = new Transaction(sp);
  assert(SOPHIA_SUCCESS == sp->Open());
  assert(SOPHIA_SUCCESS == sp->Clear());
  assert(SOPHIA_SUCCESS == t->Begin());

  int sets = 0;
  int dels = 0;

  for (int i = 0; i < 10000; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%05d", i);
    sprintf(value, "value%05d", i);

    int n = (rand() % 3) + 1;
    if (2 == n) {
      assert(SOPHIA_SUCCESS == t->Delete(key));
      dels++;
    } else {
      assert(SOPHIA_SUCCESS == t->Set(key, value));
      sets++;
    }
  }

  assert(SOPHIA_SUCCESS == t->Commit());

  size_t count;
  assert(SOPHIA_SUCCESS == sp->Count(&count));
  assert(sets == count);

  assert(SOPHIA_SUCCESS == sp->Close());
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
