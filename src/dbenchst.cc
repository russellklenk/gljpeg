/*/////////////////////////////////////////////////////////////////////////////
/// @summary Implement a benchmark application to test how quickly a single 
/// thread can decompress a target 512x512, lossy Q=80 single-channel JPEG 
/// image without any I/O or memory management traffic to give us a some idea
/// of what our maximum theoretical decode rate is.
///////////////////////////////////////////////////////////////////////////80*/

/*////////////////////
//   Preprocessor   //
////////////////////*/
/// @summary Define static/dynamic library import/export for the compiler.
#ifndef library_function
    #if   defined(BUILD_DYNAMIC)
        #define library_function                     __declspec(dllexport)
    #elif defined(BUILD_STATIC)
        #define library_function
    #else
        #define library_function                     __declspec(dllimport)
    #endif
#endif /* !defined(library_function) */

/// @summary Tag used to mark a function as available for use outside of the current translation unit (the default visibility).
#ifndef export_function
    #define export_function                          library_function
#endif

/// @summary Tag used to mark a function as available for public use, but not exported outside of the translation unit.
#ifndef public_function
    #define public_function                          static
#endif

/// @summary Tag used to mark a function internal to the translation unit.
#ifndef internal_function
    #define internal_function                        static
#endif

/// @summary Tag used to mark a variable as local to a function, and persistent across invocations of that function.
#ifndef local_persist
    #define local_persist                            static
#endif

/// @summary Tag used to mark a variable as global to the translation unit.
#ifndef global_variable
    #define global_variable                          static
#endif

/// @summary Define some useful macros for specifying common resource sizes.
#ifndef Kilobytes
    #define Kilobytes(x)                            (size_t((x)) * size_t(1024))
#endif
#ifndef Megabytes
    #define Megabytes(x)                            (size_t((x)) * size_t(1024) * size_t(1024))
#endif
#ifndef Gigabytes
    #define Gigabytes(x)                            (size_t((x)) * size_t(1024) * size_t(1024) * size_t(1024))
#endif

/// @summary Define macros for controlling compiler inlining.
#ifndef never_inline
    #define never_inline                            __declspec(noinline)
#endif
#ifndef force_inline
    #define force_inline                            __forceinline
#endif

/// @summary Helper macro to write a message to stdout.
#ifndef ConsoleOutput
    #ifndef NO_CONSOLE_OUTPUT
        #define ConsoleOutput(fmt_str, ...)         _ftprintf(stdout, _T(fmt_str), __VA_ARGS__)
    #else
        #define ConsoleOutput(fmt_str, ...)         
    #endif
#endif

/// @summary Helper macro to write a message to stderr.
#ifndef ConsoleError
    #ifndef NO_CONSOLE_OUTPUT
        #define ConsoleError(fmt_str, ...)          _ftprintf(stderr, _T(fmt_str), __VA_ARGS__)
    #else
        #define ConsoleError(fmt_str, ...)          
    #endif
#endif

/*////////////////
//   Includes   //
////////////////*/
#include <stdint.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdio.h>

#include <tchar.h>
#include <Windows.h>

#include "jpeglib.h"
#include "decompress.cc"

/*//////////////////
//   Data Types   //
//////////////////*/

/*///////////////
//   Globals   //
///////////////*/

/*//////////////////////////
//   Internal Functions   //
//////////////////////////*/
/// @summary Rounds a size up to the nearest even multiple of a given power-of-two.
/// @param size The size value to round up.
/// @param pow2 The power-of-two alignment.
/// @return The input size, rounded up to the nearest even multiple of pow2.
public_function size_t 
AlignUp
(
    size_t size, 
    size_t pow2
)
{
    return (size == 0) ? pow2 : ((size + (pow2-1)) & ~(pow2-1));
}

/// @summary Read an entire file into memory. The memory buffer is allocated using malloc.
/// @param input The JPEG_DECOMPRESS_INPUT structure to populate.
/// @param path The zero-terminated path of the file to load.
/// @return Zero if the file is loaded successfully, or -1 if an error occurred.
internal_function int
JpegIoReadFileData
(
    JPEG_DECOMPRESS_INPUT *input,
    char const             *path
)
{
    LARGE_INTEGER file_size = {};
    HANDLE     fd = INVALID_HANDLE_VALUE;
    void     *buf = NULL;
    size_t     nb = 0;
    int64_t    nr = 0;

    // initialize the fields of the JPEG_DECOMPRESS_INPUT structure.
    ZeroMemory(input, sizeof(JPEG_DECOMPRESS_INPUT));

    // open the requested input file, read-only, to be read from start to end.
    if ((fd = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_FLAG_SEQUENTIAL_SCAN, NULL)) == INVALID_HANDLE_VALUE)
    {
        ConsoleError("ERROR: %S(%u): Unable to open JPEG input file \"%S\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, GetLastError());
        goto cleanup_and_fail;
    }
    // retrieve the file size, and use that to allocate a buffer for the file data.
    if (!GetFileSizeEx(fd, &file_size))
    {
        ConsoleError("ERROR: %S(%u): Failed to retrieve file size for JPEG input file \"%S\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, GetLastError());
        goto cleanup_and_fail;
    }
    if ((nb = (size_t) file_size.QuadPart) == 0 || (buf = malloc(nb)) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Failed to allocate %Iu byte input buffer for JPEG file \"%S\".\n", __FUNCTION__, GetCurrentThreadId(), nb, path);
        goto cleanup_and_fail;
    }
    // read the entire file contents into the buffer, in 1MB chunks.
    while (nr < file_size.QuadPart)
    {
        uint8_t     *dst =(uint8_t*) buf + nr;
        int64_t   remain = file_size.QuadPart - nr;
        DWORD    to_read =(remain < Megabytes(1)) ? (DWORD) remain : (DWORD) Megabytes(1); 
        DWORD bytes_read = 0;
        if (!ReadFile(fd, dst, to_read, &bytes_read, NULL))
        {   // the read failed. treat this as a fatal error.
            ConsoleError("ERROR: %S(%u): ReadFile failed for input file \"%S\", offset %I64d (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, nr, GetLastError());
            goto cleanup_and_fail;
        }
        else
        {   // the read completed successfully.
            nr += bytes_read;
        }
    }
    // the file was successfully read, so clean up and set the fields on the JPEG_DECOMPRESS_INPUT.
    CloseHandle(fd); fd = INVALID_HANDLE_VALUE;
    return JpegInitDecompressInput(input, buf, nb);

cleanup_and_fail:
    ZeroMemory(input, sizeof(JPEG_DECOMPRESS_INPUT));
    if (fd != INVALID_HANDLE_VALUE) CloseHandle(fd);
    if (buf != NULL) free(buf);
    return -1;
}

/// @summary Free the file buffer previously allocated by a call to JpegIoReadFileData.
/// @param input The JPEG_DECOMPRESS_INPUT describing the buffer.
internal_function void
JpegIoFreeFileData
(
    JPEG_DECOMPRESS_INPUT *input
)
{
    if (input->Buffer != NULL)
    {
        free((void*) input->Buffer);
        input->Buffer = NULL;
        input->DataSize = 0;
    }
}

/// @summary Allocate memory for a decompress JPEG immage based on its image attributes.
/// @param output The JPEG_DECOMPRESS_OUTPUT describing the image.
/// @return Zero if the image buffer was successfully allocated, or -1 if an error occurred.
internal_function int
JpegAllocateImageData
(
    JPEG_DECOMPRESS_OUTPUT *output
)
{
    output->RowStride   = AlignUp(output->RowStride, 4);
    output->DataSize    = output->RowStride * output->ImageHeight;
    if ((output->Buffer =(uint8_t*) VirtualAlloc(NULL, output->DataSize, MEM_COMMIT, PAGE_READWRITE)) == NULL)
    {
        return -1;
    }
    return 0;
}

/// @summary Free the memory buffer previously allocated by a call to JpegAllocateImageData.
/// @param output The JPEG_DECOMPRESS_OUTPUT describing the buffer.
internal_function void
JpegFreeImageData
(
    JPEG_DECOMPRESS_OUTPUT *output
)
{
    if (output->Buffer != NULL)
    {
        VirtualFree(output->Buffer, 0, MEM_RELEASE);
        output->Buffer = NULL;
    }
}

/// @summary Retrieve a high-resolution timestamp value.
/// @return A high-resolution timestamp. The timestamp is specified in counts per-second.
internal_function uint64_t
TimestampInTicks
(
    void
)
{
    LARGE_INTEGER ticks;
    QueryPerformanceCounter(&ticks);
    return (uint64_t) ticks.QuadPart;
}

/// @summary Given two timestamp values, calculate the number of nanoseconds between them.
/// @param start_ticks The TimestampInTicks at the beginning of the measured interval.
/// @param end_ticks The TimestampInTicks at the end of the measured interval.
/// @return The elapsed time between the timestamps, specified in nanoseconds.
internal_function uint64_t
ElapsedNanoseconds
(
    uint64_t start_ticks, 
    uint64_t   end_ticks
)
{   
    LARGE_INTEGER freq;
    QueryPerformanceFrequency(&freq);
    // scale the tick value by the nanoseconds-per-second multiplier
    // before scaling back down by ticks-per-second to avoid loss of precision.
    return (1000000000ULL * (end_ticks - start_ticks)) / uint64_t(freq.QuadPart);
}

internal_function double
NanosecondsToSeconds
(
    uint64_t nanoseconds
)
{
    return ((double) nanoseconds / 1000000000.0);
}

/*////////////////////////
//   Public Functions   //
////////////////////////*/
/// @summary Implement the entry point of the application.
/// @param argc The number of arguments passed on the command line.
/// @param argv An array of @a argc zero-terminated strings specifying the command-line arguments.
/// @return Zero if the function completes successfully, or non-zero otherwise.
export_function int 
main
(
    int    argc, 
    char **argv
)
{
    JPEG_DECOMPRESS_INPUT   input = {};
    JPEG_DECOMPRESS_OUTPUT output = {};
    JPEG_DECOMPRESS_OUTPUT  dummy = {};
    JPEG_DECOMPRESS_STATE    jpeg = {};
    size_t const         NSAMPLES = 10;
    uint64_t     tsenter[NSAMPLES]= {};
    uint64_t     tsleave[NSAMPLES]= {};
    uint64_t                dtmin = UINT64_MAX;
    uint64_t                dtmax = 0;
    uint64_t                dtavg = 0;

    // warm-up - get everything into memory and warm the caches.
    if (JpegIoReadFileData(&input, "data\\004F46ON.1.JPG") < 0)
    {
        return -1;
    }
    if (JpegInitDecompressState(&jpeg, &input) < 0)
    {
        return -1;
    }
    if (JpegReadImageAttributes(&output, &jpeg) < 0)
    {
        return -1;
    }
    if (JpegAllocateImageData(&output) < 0)
    {
        return -1;
    }
    if (JpegDecompressImageData(&output, &jpeg) < 0)
    {
        return -1;
    }
    JpegDestroyDecompressState(&jpeg, NULL, NULL);

    // gather NSAMPLES counts of how long it takes to decompress 1000 images.
    for (size_t iteration  = 0; iteration < NSAMPLES; ++iteration)
    {
        tsenter[iteration] = TimestampInTicks();
        for (size_t image  = 0; image < 1000; ++image)
        {
            JpegInitDecompressState(&jpeg, &input);
            JpegReadImageAttributes(&dummy, &jpeg);
            JpegDecompressImageData(&output, &jpeg);
            JpegDestroyDecompressState(&jpeg, NULL, NULL);
        }
        tsleave[iteration] = TimestampInTicks();
    }
    JpegIoFreeFileData(&input);
    JpegFreeImageData(&output);

    // calculate the min/max/average timing values:
    for (size_t i = 0; i < NSAMPLES; ++i)
    {
        uint64_t dt = ElapsedNanoseconds(tsenter[i], tsleave[i]);
        if (dt < dtmin) dtmin = dt;
        if (dt > dtmax) dtmax = dt;
        dtavg += dt;
    }
    dtavg /= NSAMPLES;

    // print out the timing report:
    ConsoleOutput("MIN: %I64uns (%0.4fs)\n", dtmin, NanosecondsToSeconds(dtmin));
    ConsoleOutput("MAX: %I64uns (%0.4fs)\n", dtmax, NanosecondsToSeconds(dtmax));
    ConsoleOutput("AVG: %I64uns (%0.4fs)\n", dtavg, NanosecondsToSeconds(dtavg));
    for (size_t i = 0; i < NSAMPLES; ++i)
    {
        uint64_t dt = ElapsedNanoseconds(tsenter[i], tsleave[i]);
        ConsoleOutput("RS%02Iu: %I64uns (%0.4fs)\n" , i, dt, NanosecondsToSeconds(dt));
    }
    ConsoleOutput("\n");

    UNREFERENCED_PARAMETER(argc);
    UNREFERENCED_PARAMETER(argv);
    return 0;
}


