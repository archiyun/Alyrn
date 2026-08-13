// SPDX-License-Identifier: MIT
#pragma once

/*
 * BSD <sys/event.h> declarations, provided so the kqueue backend can be
 * compiled and exercised on a Linux developer machine against the in-memory
 * shim in fake_kqueue.cc.
 *
 * This directory is only ever placed on the include path of the shim test
 * target. On a real kqueue host the system header is used instead, and this
 * file takes no part in the build.
 */

#include <stdint.h>
#include <sys/types.h>
#include <time.h>

struct kevent {
  uintptr_t ident;
  int16_t filter;
  uint16_t flags;
  uint32_t fflags;
  intptr_t data;
  void *udata;
};

#define EV_SET(kevp, a, b, c, d, e, f) \
  do {                                 \
    struct kevent *__kevp = (kevp);    \
    __kevp->ident = (uintptr_t)(a);    \
    __kevp->filter = (int16_t)(b);     \
    __kevp->flags = (uint16_t)(c);     \
    __kevp->fflags = (uint32_t)(d);    \
    __kevp->data = (intptr_t)(e);      \
    __kevp->udata = (void *)(f);       \
  } while (0)

#define EVFILT_READ (-1)
#define EVFILT_WRITE (-2)

#define EV_ADD 0x0001
#define EV_DELETE 0x0002
#define EV_ENABLE 0x0004
#define EV_DISABLE 0x0008
#define EV_ONESHOT 0x0010
#define EV_CLEAR 0x0020
#define EV_EOF 0x8000
#define EV_ERROR 0x4000

#ifdef __cplusplus
extern "C" {
#endif

int kqueue(void);
int kevent(int kq, const struct kevent *changelist, int nchanges, struct kevent *eventlist,
           int nevents, const struct timespec *timeout);

#ifdef __cplusplus
}
#endif
