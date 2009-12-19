/* dlfcn.h
 */

void* dlopen(const char* path, int mode);
void* dlsym(void* handle, const char* symbol);
int dlclose(void* handle);
const char *dlerror(void);

#define RTLD_LAZY 1
#define RTLD_LOCAL 2

