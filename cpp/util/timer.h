// THIS FILE IS NO LONGER BEING USED
// future profiling will be done with external tools including callgrind
// this file is kept for the `DEBUG_ONLY` macro which I hope to replace with
// something more robust

#ifndef TIMER_H_
#define TIMER_H_

#if DEBUG
#   define DEBUG_ONLY(msg) msg
#else
#   define DEBUG_ONLY(msg)
#endif

#endif
