// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright (c) 2006, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#if (defined(_WIN32) || defined(__MINGW32__)) && !defined(__CYGWIN__) && !defined(__CYGWIN32)
# define PLATFORM_WINDOWS 1
#endif

#include <ctype.h>    // for isspace()
#include <stdlib.h>   // for getenv()
#include <stdio.h>    // for snprintf(), sscanf()
#include <string.h>   // for memmove(), memchr(), etc.
#include <fcntl.h>    // for open()
#include <errno.h>    // for errno
#ifdef HAVE_UNISTD_H
#include <unistd.h>   // for read()
#endif
#if defined __MACH__          // Mac OS X, almost certainly
#include <mach-o/dyld.h>      // for iterating over dll's in ProcMapsIter
#include <mach-o/loader.h>    // for iterating over dll's in ProcMapsIter
#include <sys/types.h>
#include <sys/sysctl.h>       // how we figure out numcpu's on OS X
#elif defined __FreeBSD__
#include <sys/sysctl.h>
#elif defined __sun__         // Solaris
#include <procfs.h>           // for, e.g., prmap_t
#elif defined(PLATFORM_WINDOWS)
#include <process.h>          // for getpid() (actually, _getpid())
#include <shlwapi.h>          // for SHGetValueA()
#include <tlhelp32.h>         // for Module32First()
#endif
#include "gutil/sysinfo.h"
#include "gutil/dynamic_annotations.h"   // for RunningOnValgrind
#include "gutil/walltime.h"
#include <glog/logging.h>

// This isn't in the 'base' namespace in tcmallc. But, tcmalloc
// exports these functions, so we need to namespace them to avoid
// the conflict.
namespace base {

// ----------------------------------------------------------------------
// CyclesPerSecond()
// NumCPUs()
//    It's important this not call malloc! -- they may be called at
//    global-construct time, before we've set up all our proper malloc
//    hooks and such.
// ----------------------------------------------------------------------

static double cpuinfo_cycles_per_second = 1.0;  // 0.0 might be dangerous
static int cpuinfo_num_cpus = 1;  // Conservative guess

void SleepForMilliseconds(int milliseconds) {
#ifdef PLATFORM_WINDOWS
  _sleep(milliseconds);   // Windows's _sleep takes milliseconds argument
#else
  // Sleep for a few milliseconds
  struct timespec sleep_time;
  sleep_time.tv_sec = milliseconds / 1000;
  sleep_time.tv_nsec = (milliseconds % 1000) * 1000000;
  while (nanosleep(&sleep_time, &sleep_time) != 0 && errno == EINTR)
    ;  // Ignore signals and wait for the full interval to elapse.
#endif
}

// Helper function estimates cycles/sec by observing cycles elapsed during
// sleep(). Using small sleep time decreases accuracy significantly.
static int64 EstimateCyclesPerSecond(const int estimate_time_ms) {
  CHECK(estimate_time_ms > 0);
  if (estimate_time_ms <= 0)
    return 1;
  double multiplier = 1000.0 / (double)estimate_time_ms;  // scale by this much

  const int64 start_ticks = CycleClock::Now();
  SleepForMilliseconds(estimate_time_ms);
  const int64 guess = int64(multiplier * (CycleClock::Now() - start_ticks));
  return guess;
}

// ReadIntFromFile is only called on linux and cygwin platforms.
#if defined(__linux__) || defined(__CYGWIN__) || defined(__CYGWIN32__)
// Helper function for reading an int from a file. Returns true if successful
// and the memory location pointed to by value is set to the value read.
static bool ReadIntFromFile(const char *file, int *value) {
  bool ret = false;
  int fd = open(file, O_RDONLY);
  if (fd == -1) return ret;
  char line[1024];
  char* err;
  memset(line, '\0', sizeof(line));
  if (read(fd, line, sizeof(line) - 1) != -1) {
    const int temp_value = strtol(line, &err, 10);
    if (line[0] != '\0' && (*err == '\n' || *err == '\0')) {
      *value = temp_value;
      ret = true;
    }
  }
  close(fd);
  return ret;
}
#endif

// WARNING: logging calls back to InitializeSystemInfo() so it must
// not invoke any logging code.  Also, InitializeSystemInfo() can be
// called before main() -- in fact it *must* be since already_called
// isn't protected -- before malloc hooks are properly set up, so
// we make an effort not to call any routines which might allocate
// memory.

static void InitializeSystemInfo() {
  static bool already_called = false;   // safe if we run before threads
  if (already_called)  return;
  already_called = true;

  bool saw_mhz = false;

  if (RunningOnValgrind()) {
    // Valgrind may slow the progress of time artificially (--scale-time=N
    // option). We thus can't rely on CPU Mhz info stored in /sys or /proc
    // files. Thus, actually measure the cps.
    cpuinfo_cycles_per_second = EstimateCyclesPerSecond(100);
    saw_mhz = true;
  }

#if defined(__linux__) || defined(__CYGWIN__) || defined(__CYGWIN32__)
  char line[1024];
  char* err;
  int freq;

  // If the kernel is exporting the tsc frequency use that. There are issues
  // where cpuinfo_max_freq cannot be relied on because the BIOS may be
  // exporintg an invalid p-state (on x86) or p-states may be used to put the
  // processor in a new mode (turbo mode). Essentially, those frequencies
  // cannot always be relied upon. The same reasons apply to /proc/cpuinfo as
  // well.
  if (!saw_mhz &&
      ReadIntFromFile("/sys/devices/system/cpu/cpu0/tsc_freq_khz", &freq)) {
      // The value is in kHz (as the file name suggests).  For example, on a
      // 2GHz warpstation, the file contains the value "2000000".
      cpuinfo_cycles_per_second = freq * 1000.0;
      saw_mhz = true;
  }

  // If CPU scaling is in effect, we want to use the *maximum* frequency,
  // not whatever CPU speed some random processor happens to be using now.
  if (!saw_mhz &&
      ReadIntFromFile("/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq",
                      &freq)) {
    // The value is in kHz.  For example, on a 2GHz machine, the file
    // contains the value "2000000".
    cpuinfo_cycles_per_second = freq * 1000.0;
    saw_mhz = true;
  }

  // Read /proc/cpuinfo for other values, and if there is no cpuinfo_max_freq.
  const char* pname = "/proc/cpuinfo";
  int fd = open(pname, O_RDONLY);
  if (fd == -1) {
    perror(pname);
    if (!saw_mhz) {
      cpuinfo_cycles_per_second = EstimateCyclesPerSecond(1000);
    }
    return;          // TODO: use generic tester instead?
  }

  double bogo_clock = 1.0;
  bool saw_bogo = false;
  int num_cpus = 0;
  line[0] = line[1] = '\0';
  int chars_read = 0;
  do {   // we'll exit when the last read didn't read anything
    // Move the next line to the beginning of the buffer
    const int oldlinelen = strlen(line);
    if (sizeof(line) == oldlinelen + 1)    // oldlinelen took up entire line
      line[0] = '\0';
    else                                   // still other lines left to save
      memmove(line, line + oldlinelen+1, sizeof(line) - (oldlinelen+1));
    // Terminate the new line, reading more if we can't find the newline
    char* newline = strchr(line, '\n');
    if (newline == NULL) {
      const int linelen = strlen(line);
      const int bytes_to_read = sizeof(line)-1 - linelen;
      CHECK(bytes_to_read > 0);  // because the memmove recovered >=1 bytes
      chars_read = read(fd, line + linelen, bytes_to_read);
      line[linelen + chars_read] = '\0';
      newline = strchr(line, '\n');
    }
    if (newline != NULL)
      *newline = '\0';

#if defined(__powerpc__) || defined(__ppc__)
    // PowerPC cpus report the frequency in "clock" line
    if (strncasecmp(line, "clock", sizeof("clock")-1) == 0) {
      const char* freqstr = strchr(line, ':');
      if (freqstr) {
        // PowerPC frequencies are only reported as MHz (check 'show_cpuinfo'
        // function at arch/powerpc/kernel/setup-common.c)
        char *endp = strstr(line, "MHz");
        if (endp) {
          *endp = 0;
          cpuinfo_cycles_per_second = strtod(freqstr+1, &err) * 1000000.0;
          if (freqstr[1] != '\0' && *err == '\0' && cpuinfo_cycles_per_second > 0)
            saw_mhz = true;
        }
      }
#else
    // When parsing the "cpu MHz" and "bogomips" (fallback) entries, we only
    // accept postive values. Some environments (virtual machines) report zero,
    // which would cause infinite looping in WallTime_Init.
    if (!saw_mhz && strncasecmp(line, "cpu MHz", sizeof("cpu MHz")-1) == 0) {
      const char* freqstr = strchr(line, ':');
      if (freqstr) {
        cpuinfo_cycles_per_second = strtod(freqstr+1, &err) * 1000000.0;
        if (freqstr[1] != '\0' && *err == '\0' && cpuinfo_cycles_per_second > 0)
          saw_mhz = true;
      }
    } else if (strncasecmp(line, "bogomips", sizeof("bogomips")-1) == 0) {
      const char* freqstr = strchr(line, ':');
      if (freqstr) {
        bogo_clock = strtod(freqstr+1, &err) * 1000000.0;
        if (freqstr[1] != '\0' && *err == '\0' && bogo_clock > 0)
          saw_bogo = true;
      }
#endif
    } else if (strncasecmp(line, "processor", sizeof("processor")-1) == 0) {
      num_cpus++;  // count up every time we see an "processor :" entry
    }
  } while (chars_read > 0);
  close(fd);

  if (!saw_mhz) {
    if (saw_bogo) {
      // If we didn't find anything better, we'll use bogomips, but
      // we're not happy about it.
      cpuinfo_cycles_per_second = bogo_clock;
    } else {
      // If we don't even have bogomips, we'll use the slow estimation.
      cpuinfo_cycles_per_second = EstimateCyclesPerSecond(1000);
    }
  }
  if (cpuinfo_cycles_per_second == 0.0) {
    cpuinfo_cycles_per_second = 1.0;   // maybe unnecessary, but safe
  }
  if (num_cpus > 0) {
    cpuinfo_num_cpus = num_cpus;
  }

#elif defined __FreeBSD__
  // For this sysctl to work, the machine must be configured without
  // SMP, APIC, or APM support.  hz should be 64-bit in freebsd 7.0
  // and later.  Before that, it's a 32-bit quantity (and gives the
  // wrong answer on machines faster than 2^32 Hz).  See
  //  http://lists.freebsd.org/pipermail/freebsd-i386/2004-November/001846.html
  // But also compare FreeBSD 7.0:
  //  http://fxr.watson.org/fxr/source/i386/i386/tsc.c?v=RELENG70#L223
  //  231         error = sysctl_handle_quad(oidp, &freq, 0, req);
  // To FreeBSD 6.3 (it's the same in 6-STABLE):
  //  http://fxr.watson.org/fxr/source/i386/i386/tsc.c?v=RELENG6#L131
  //  139         error = sysctl_handle_int(oidp, &freq, sizeof(freq), req);
#if __FreeBSD__ >= 7
  uint64_t hz = 0;
#else
  unsigned int hz = 0;
#endif
  size_t sz = sizeof(hz);
  const char *sysctl_path = "machdep.tsc_freq";
  if ( sysctlbyname(sysctl_path, &hz, &sz, NULL, 0) != 0 ) {
    fprintf(stderr, "Unable to determine clock rate from sysctl: %s: %s\n",
            sysctl_path, strerror(errno));
    cpuinfo_cycles_per_second = EstimateCyclesPerSecond(1000);
  } else {
    cpuinfo_cycles_per_second = hz;
  }
  // TODO(csilvers): also figure out cpuinfo_num_cpus

#elif defined(PLATFORM_WINDOWS)
# pragma comment(lib, "shlwapi.lib")  // for SHGetValue()
  // In NT, read MHz from the registry. If we fail to do so or we're in win9x
  // then make a crude estimate.
  OSVERSIONINFO os;
  os.dwOSVersionInfoSize = sizeof(os);
  DWORD data, data_size = sizeof(data);
  if (GetVersionEx(&os) &&
      os.dwPlatformId == VER_PLATFORM_WIN32_NT &&
      SUCCEEDED(SHGetValueA(HKEY_LOCAL_MACHINE,
                         "HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0",
                           "~MHz", NULL, &data, &data_size)))
    cpuinfo_cycles_per_second = (int64)data * (int64)(1000 * 1000); // was mhz
  else
    cpuinfo_cycles_per_second = EstimateCyclesPerSecond(500); // TODO <500?

  // Get the number of processors.
  SYSTEM_INFO info;
  GetSystemInfo(&info);
  cpuinfo_num_cpus = info.dwNumberOfProcessors;

#elif defined(__MACH__) && defined(__APPLE__)
  // returning "mach time units" per second. the current number of elapsed
  // mach time units can be found by calling uint64 mach_absolute_time();
  // while not as precise as actual CPU cycles, it is accurate in the face
  // of CPU frequency scaling and multi-cpu/core machines.
  // Our mac users have these types of machines, and accuracy
  // (i.e. correctness) trumps precision.
  // See cycleclock.h: CycleClock::Now(), which returns number of mach time
  // units on Mac OS X.
  mach_timebase_info_data_t timebase_info;
  mach_timebase_info(&timebase_info);
  double mach_time_units_per_nanosecond =
      static_cast<double>(timebase_info.denom) /
      static_cast<double>(timebase_info.numer);
  cpuinfo_cycles_per_second = mach_time_units_per_nanosecond * 1e9;

  int num_cpus = 0;
  size_t size = sizeof(num_cpus);
  int numcpus_name[] = { CTL_HW, HW_NCPU };
  if (::sysctl(numcpus_name, arraysize(numcpus_name), &num_cpus, &size, 0, 0)
      == 0
      && (size == sizeof(num_cpus)))
    cpuinfo_num_cpus = num_cpus;

#else
  // Generic cycles per second counter
  cpuinfo_cycles_per_second = EstimateCyclesPerSecond(1000);
#endif
}

double CyclesPerSecond(void) {
  InitializeSystemInfo();
  return cpuinfo_cycles_per_second;
}

int NumCPUs(void) {
  InitializeSystemInfo();
  return cpuinfo_num_cpus;
}

} // namespace base
