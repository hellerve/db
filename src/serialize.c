#include <string.h>

#include "serialize.h"

#define size_of_attr(s, a) (sizeof(((s*)0)->a))
const uint32_t ID_SIZE = size_of_attr(row, id);
const uint32_t USERNAME_SIZE = size_of_attr(row, username);
const uint32_t EMAIL_SIZE = size_of_attr(row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

void serialize_row(row* src, unsigned char* dest) {
  memcpy(dest + ID_OFFSET, &src->id, ID_SIZE);
  memcpy(dest + USERNAME_OFFSET, &src->username, USERNAME_SIZE);
  memcpy(dest + EMAIL_OFFSET, &src->email, EMAIL_SIZE);
}

void deserialize_row(unsigned char* src, row* dest) {
  memcpy(&dest->id, src + ID_OFFSET, ID_SIZE);
  memcpy(&dest->username, src + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&dest->email, src + EMAIL_OFFSET, EMAIL_SIZE);
}
