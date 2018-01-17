// Copyright (c) 2017 Alexey Tourbin
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#pragma once

#include <stdint.h>
#include <sys/types.h> // ssize_t

#ifdef __cplusplus
extern "C" {
#endif

struct fda; // reada.h
struct zstdreader;

// Returns 1 on success, 0 on EOF, -1 on error.  On success, the Reader
// handle is returned via zp.  Information about the error is returned via
// the err[2] parameter: the first string is typically the function name,
// and the second is the string which describes the error.  Both strings
// normally come from the read-only data section.
int zstdreader_open(struct zstdreader **zp, struct fda *fda, const char *err[2])
		    __attribute__((nonnull));

// The open/read functions process only one ZSTD frame, and do not read
// past the end of that frame.  Multiple frames can be concatenated,
// but then frame boundaries can be meaningful.  The implementation also
// rejects skippable frames, because they may need to be processed somehow.
// It is possible to reuse the Reader for reading another frame.
// When fda is NULL, this function will reuse the previous file descriptor;
// otherwise, the previous file descriptor will not be closed automatically.
int zstdreader_reopen(struct zstdreader *z, struct fda *fda, const char *err[2])
		      __attribute__((nonnull(1, 3)));

// Doesn't close its file descriptor.
void zstdreader_free(struct zstdreader *z);

// Returns the number of bytes read, 0 on EOF, -1 on error.  If the number
// of bytes read is less than the number of bytes requested, this indicates
// EOF (subsequent reads will return 0).
ssize_t zstdreader_read(struct zstdreader *z, void *buf, size_t size, const char *err[2])
			__attribute__((nonnull));

// Returns the uncompressed size, -1 if the size is unknown, 0 for an
// empty frame (the first read is to return 0).  No error is possible,
// since the size is actually determined at the open/reopen stage.
int64_t zstdreader_contentSize(struct zstdreader *z) __attribute__((nonnull));

#ifdef __cplusplus
}
#endif
