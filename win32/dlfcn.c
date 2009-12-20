#include <stdio.h>
#include <windows.h>
#include <dlfcn.h>

void* dlopen(const char* path, int mode) {
  char *buf = malloc(strlen(path) + 20);
  sprintf(buf, "%s.dll", path);
  void *lib = (void*) LoadLibrary(buf);
  free(buf);
  return lib;
}

void* dlsym(void* handle, const char* symbol) {
  (void*) GetProcAddress((HINSTANCE) handle, symbol);
}

int dlclose(void* handle) {
  // dlclose returns zero on success.
  // FreeLibrary returns nonzero on success.
  (int) (FreeLibrary((HINSTANCE) handle) != 0);
}

static char dlerror_buf[200];

const char *dlerror(void) {
  DWORD err = GetLastError();
  sprintf(dlerror_buf, "%x", err);
  return dlerror_buf;
}

