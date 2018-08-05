#include "data.h"

typedef enum {
  META_SUCCESS,
  META_UNRECOGNIZED
} meta_result;

meta_result meta(char*, table*);
