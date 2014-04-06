
#include "sophia-cc.h"
#include <assert.h>
#include <string.h>
#include <stdio.h>

int
main(void) {
  sophia::Sophia *sp = new sophia::Sophia("testdb");
  sophia::SophiaReturnCode rc;

  assert(0 == sp->Open());

  for (int i = 0; i < 100; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%03d", i);
    sprintf(value, "value%03d", i);
    assert(0 == sp->Set(key, value));
  }

  size_t count;
  assert(0 == sp->Count(&count));
  assert(100 == count);

  sophia::Transaction *t = new sophia::Transaction(sp);
  assert(0 == t->Begin());

  for (int i = 100; i < 1000; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%03d", i);
    sprintf(value, "value%03d", i);
    assert(0 == t->Set(key, value));
  }

  assert(0 == sp->Count(&count));
  assert(100 == count);

  assert(0 == t->Commit());
  assert(0 == sp->Count(&count));
  assert(1000 == count);

  rc = t->Set("foo", "bar");
  assert(sophia::SOPHIA_TRANSACTION_NOT_OPEN == rc);
  assert(sp->Error(rc));
  delete t;

  for (int i = 0; i < 1000; i++) {
    char key[100];
    char value[100];
    sprintf(key, "key%03d", i);
    sprintf(value, "value%03d", i);
    char *actual = sp->Get(key);
    assert(actual && 0 == strcmp(value, actual));
    free(actual);
  }

  sophia::Transaction *t2 = new sophia::Transaction(sp);
  assert(0 == t2->Begin());
  for (int i = 100; i < 1000; i++) {
    char key[100];
    sprintf(key, "key%03d", i);
    assert(0 == t2->Delete(key));
  }
  assert(0 == t2->Rollback());
  delete t2;

  assert(0 == sp->Count(&count));
  assert(1000 == count);

  sophia::Iterator *iterator = new sophia::Iterator(
      sp
    , SPGTE
    , "key400"
    , "key500"
  );
  int i = 400;
  assert(0 == iterator->Begin());

  sophia::IteratorResult *result;
  while ((result = iterator->Next())) {
    char key[100];
    char value[100];
    sprintf(key, "key%03d", i);
    sprintf(value, "value%03d", i);

    assert(0 == strcmp(key, result->key));
    assert(0 == strcmp(value, result->value));

    i++;
    delete result;
  }

  assert(0 == iterator->End());
  delete iterator;

  assert(0 == sp->Clear());
  assert(0 == sp->Count(&count));
  assert(0 == count);

  assert(0 == sp->Close());
  delete sp;

  return 0;
}
