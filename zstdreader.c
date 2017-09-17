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

#include <stdio.h> // sys_errlist
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#define ZSTD_STATIC_LINKING_ONLY // ZSTD_FRAMEHEADERSIZE_MAX
#include <zstd.h>
#include "zstdreader.h"
#include "reada.h"

// A thread-safe strerror(3) replacement.
static const char *xstrerror(int errnum)
{
    // Some of the great minds say that sys_errlist is deprecated.
    // Well, at least it's thread-safe, and it does not deadlock.
    if (errnum > 0 && errnum < sys_nerr)
	return sys_errlist[errnum];
    return "Unknown error";
}

// Helpers to fill err[2] arg.
#define ERRNO(func) err[0] = func, err[1] = xstrerror(errno)
#define ERRZSTD(func, ret) err[0] = func, err[1] = ZSTD_getErrorName(ret)
#define ERRSTR(str) err[0] = __func__, err[1] = str

// Start decoding at the beginning of a frame.
static ssize_t zstdreader_begin(struct fda *fda, ZSTD_DStream *ds,
				int64_t *contentSizep, const char *err[2])
{
    size_t zret = ZSTD_initDStream(ds);
    assert(zret > 0); // no good reason for failure

#ifndef ZSTD_BLOCKHEADERSIZE
#define ZSTD_BLOCKHEADERSIZE 3
#endif
    char buf[(ZSTD_FRAMEHEADERSIZE_MAX + ZSTD_BLOCKHEADERSIZE + 7) & ~7];
    const unsigned char magic[4] = { 0x28, 0xb5, 0x2f, 0xfd };

    ssize_t n = peeka(fda, buf, sizeof buf);
    if (n < 0)
	return ERRNO("read"), -1;
    if (n == 0)
	return 0;
    if (n < 4)
	return ERRSTR("unexpected EOF"), -1;
    if (memcmp(buf, magic, 4))
	return ERRSTR("bad zstd magic"), -1;
    if (n < ZSTD_FRAMEHEADERSIZE_MIN)
	return ERRSTR("unexpected EOF"), -1;

    // Start decoding with a NULL output buffer.
    // All the input must be consumed while no output produced.
    ZSTD_inBuffer in = { buf, ZSTD_FRAMEHEADERSIZE_MIN, 0 };
    ZSTD_outBuffer out = { NULL, 0 , 0 };
    zret = ZSTD_decompressStream(ds, &out, &in);
    if (ZSTD_isError(zret))
	return ERRZSTD("ZSTD_decompressStream", zret), -1;
    assert(in.pos == ZSTD_FRAMEHEADERSIZE_MIN);

    // The second call should get us to the first block size.
    size_t nextSize = zret;
    assert(ZSTD_FRAMEHEADERSIZE_MIN + nextSize <= ZSTD_FRAMEHEADERSIZE_MAX + ZSTD_BLOCKHEADERSIZE);
    if (n < ZSTD_FRAMEHEADERSIZE_MIN + nextSize)
	return ERRSTR("unexpected EOF"), -1;
    in = (ZSTD_inBuffer) { buf + ZSTD_FRAMEHEADERSIZE_MIN, nextSize, 0 };
    out = (ZSTD_outBuffer) { NULL, 0 , 0 };
    zret = ZSTD_decompressStream(ds, &out, &in);
    if (ZSTD_isError(zret))
	return ERRZSTD("ZSTD_decompressStream", zret), -1;
    assert(in.pos == nextSize);

    // See how many bytes have been read.
    fda->cur += ZSTD_FRAMEHEADERSIZE_MIN + nextSize;
    if (fda->cur == fda->end)
	fda->cur = fda->end = NULL;

    unsigned long long csize = ZSTD_getFrameContentSize(buf,
	    ZSTD_FRAMEHEADERSIZE_MIN + nextSize - ZSTD_BLOCKHEADERSIZE);
    assert(csize != ZSTD_CONTENTSIZE_ERROR);
    *contentSizep = csize == ZSTD_CONTENTSIZE_UNKNOWN ? -1 : csize;

    nextSize = zret;
    return nextSize + 1;
}

struct zstdreader {
    struct fda *fda;
    ZSTD_DStream *ds;
    bool eof, err;
    size_t nextSize;
    int64_t contentSize;
    size_t zfill;
    size_t zpos;
    char zbuf[ZSTD_BLOCKSIZE_MAX + ZSTD_BLOCKHEADERSIZE];
};

int zstdreader_open(struct zstdreader **zp, struct fda *fda, const char *err[2])
{
    ZSTD_DStream *ds = ZSTD_createDStream();
    if (!ds)
	return ERRSTR("ZSTD_createDStream failed"), -1;

    int64_t contentSize;
    ssize_t nextSize = zstdreader_begin(fda, ds, &contentSize, err);
    if (nextSize < 0)
	return ZSTD_freeDStream(ds), -1;
    if (nextSize == 0)
	return ZSTD_freeDStream(ds), 0;
    nextSize--;

    struct zstdreader *z = malloc(sizeof *z);
    if (!z)
	return ERRNO("malloc"), ZSTD_freeDStream(ds), -1;

    z->fda = fda;
    z->ds = ds;
    z->eof = z->err = false;
    z->nextSize = nextSize;
    z->contentSize = contentSize;
    z->zfill = z->zpos = 0;

    *zp = z;
    return 1;
}

int zstdreader_reopen(struct zstdreader *z, struct fda *fda, const char *err[2])
{
    if (fda)
	z->fda = fda;

    z->eof = false;
    z->contentSize = -1;

    ssize_t nextSize = zstdreader_begin(z->fda, z->ds, &z->contentSize, err);
    if (nextSize < 0)
	return -(z->err = true);
    if (nextSize == 0)
	return z->eof = true, +(z->err = false);
    nextSize--;

    z->err = false;

    z->nextSize = nextSize;
    z->zfill = z->zpos = 0;

    return 1;
}

void zstdreader_free(struct zstdreader *z)
{
    if (!z)
	return;
    ZSTD_freeDStream(z->ds);
    free(z);
}

// ex:set ts=8 sts=4 sw=4 noet:
