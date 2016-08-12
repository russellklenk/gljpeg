/*/////////////////////////////////////////////////////////////////////////////
/// @summary Test multi-threaded JPEG decoding using the thread pool. JPEG 
/// files are memory-mapped on the decoder thread.
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

/// @summary Helper macro to align a size value up to the next even multiple of a given power-of-two.
#ifndef align_up
    #define align_up(x, a)                          ((x) == 0) ? (a) : (((x) + ((a)-1)) & ~((a)-1))
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
#include <assert.h>
#include <stdint.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdio.h>

#include <process.h>

#include <tchar.h>
#include <Windows.h>

#include <GL/gl.h>

#include <vector>
#include <algorithm>

#include "jpeglib.h"
#include "cvmarkers.h"

#include "fileio.cc"
#include "threadpool.cc"
#include "decompress.cc"

/*//////////////////
//   Data Types   //
//////////////////*/
struct SHARED_DATA
{
    FILE_LIST        FileList;
    size_t           NextFile;
    HANDLE           AllDone;
    CRITICAL_SECTION SyncObj;
};

enum THREAD_SIGNAL : uintptr_t
{
    SIGNAL_DECOMPRESS_JPEG = 0,
};

/*///////////////
//   Globals   //
///////////////*/

/*//////////////////////////
//   Internal Functions   //
//////////////////////////*/
internal_function bool GetWorkItem
(
    SHARED_DATA  *shared, 
    FILE_INFO &file_info
)
{
    bool result = false;
    EnterCriticalSection(&shared->SyncObj);
    {
        if (shared->NextFile < shared->FileList.size())
        {
            file_info = shared->FileList[shared->NextFile++];
            result = true;
        }
    }
    LeaveCriticalSection(&shared->SyncObj);
    return result;
}

internal_function void
FinishWorkItem
(
    SHARED_DATA *shared
)
{
    bool all_done = false;
    EnterCriticalSection(&shared->SyncObj);
    {
        if (shared->NextFile == shared->FileList.size())
            all_done = true;
    }
    LeaveCriticalSection(&shared->SyncObj);
    if (all_done) SetEvent(shared->AllDone);
}

internal_function void
JpegInitDecompressInput
(
    JPEG_DECOMPRESS_INPUT *input, 
    FILE_DATA              *file
)
{
    input->Buffer   = file->Buffer;
    input->DataSize = file->DataSize;
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
    output->RowStride   = align_up(output->RowStride, 4);
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

/// @summary Define the signature for the callback invoked during worker thread initialization to allow the application to create any per-thread resources.
/// @param thread_args A WORKER_THREAD instance specifying worker thread data. The callback should set the ThreadContext field to its private data.
/// @return Zero if initialization was successful, or -1 to terminate the worker thread.
internal_function int WorkerInit
(
    WORKER_THREAD *thread_args
)
{
    UNREFERENCED_PARAMETER(thread_args);
    return WORKER_THREAD_INIT_SUCCESS;
}

/// @summary Define the signature for the callback representing the application entry point on a worker thread.
/// @param thread_args A WORKER_THREAD instance, valid until the WORKER_ENTRY function returns, specifying per-thread data.
/// @param signal_arg An application-defined value specified with the wake notification.
/// @param wake_reason One of WORKER_THREAD_WAKE_REASON indicating the reason the thread was woken.
internal_function void WorkerMain
(
    WORKER_THREAD *thread_args, 
    uintptr_t       signal_arg, 
    int            wake_reason
)
{
    TASK_PROFILER_SPAN span;
    THREAD_POOL       *pool = thread_args->ThreadPool;

    if (wake_reason != WORKER_THREAD_WAKE_FOR_RUN)
    {   // TODO(rlk): any thread-local cleanup.
        // ...
    }
    TpThreadSpanEnter(pool, span, "DECOMPRESS_JPEG");
    if (signal_arg == SIGNAL_DECOMPRESS_JPEG)
    {
        JPEG_DECOMPRESS_INPUT   input = {};
        JPEG_DECOMPRESS_OUTPUT output = {};
        JPEG_DECOMPRESS_STATE   state = {};
        SHARED_DATA *shared = (SHARED_DATA*) thread_args->PoolContext;
        FILE_INFO file_info = {};
        FILE_MAPPING   fmap = {};
        FILE_DATA      file = {};

        if (!GetWorkItem(shared, file_info))
        {   // spurious wakeup; there's no file to decompress.
            TpThreadSpanLeave(pool, span);
            return;
        }
        TpThreadEvent(pool, "Decompress %s (%I64d bytes)", file_info.Path, file_info.FileSize);
        if (IoOpenFileMapping(&fmap, file_info.Path) < 0)
        {
            TpThreadSpanLeave(pool, span);
            FinishWorkItem(shared);
            return;
        }
        if (IoMapFileRegion(&file, 0, fmap.FileSize, &fmap) < 0)
        {
            TpThreadSpanLeave(pool, span);
            IoCloseFileMapping(&fmap);
            FinishWorkItem(shared);
            return;
        }
        JpegInitDecompressInput(&input, &file);
        if (JpegInitDecompressState(&state, &input) < 0)
        {
            TpThreadSpanLeave(pool, span);
            FinishWorkItem(shared);
            return;
        }
        if (JpegReadImageAttributes(&output, &state) < 0)
        {
            TpThreadSpanLeave(pool, span);
            FinishWorkItem(shared);
            return;
        }
        if (JpegAllocateImageData(&output) < 0)
        {
            TpThreadSpanLeave(pool, span);
            FinishWorkItem(shared);
            return;
        }
        if (JpegDecompressImageData(&output, &state) < 0)
        {
            TpThreadSpanLeave(pool, span);
            FinishWorkItem(shared);
            return;
        }
        JpegDestroyDecompressState(&state, NULL, NULL);
        JpegFreeImageData(&output);
        IoFreeFileData(&file);
        IoCloseFileMapping(&fmap);
        FinishWorkItem(shared);
    }
    TpThreadSpanLeave(pool, span);
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
    SHARED_DATA         shared = {};
    THREAD_POOL_INIT pool_init = {};
    THREAD_POOL           pool = {};
    CPU_INFO               cpu = {};

    UNREFERENCED_PARAMETER(argc);
    UNREFERENCED_PARAMETER(argv);

    if (TpQueryHostCpuLayout(&cpu) < 0)
    {
        ConsoleError("ERROR: %S(%u): Failed to query the host CPU layout.\n", __FUNCTION__, GetCurrentThreadId());
        return -1;
    }
    if (IoEnumerateDirectoryFiles(shared.FileList, "data", true) < 0 || shared.FileList.size() == 0)
    {
        ConsoleError("ERROR: %S(%u): The data directory is empty.\n", __FUNCTION__, GetCurrentThreadId());
        return -1;
    }
    else
    {   // set up the shared data.
        InitializeCriticalSection(&shared.SyncObj);
        IoSortFileList(shared.FileList);
        shared.AllDone = CreateEvent(NULL, TRUE, FALSE, NULL);
        shared.NextFile = 0;
    }

    // set up the thread pool.
    pool_init.ThreadInit  = WorkerInit;
    pool_init.ThreadMain  = WorkerMain;
    pool_init.PoolContext = &shared;
    pool_init.ThreadCount = cpu.HardwareThreads;
    pool_init.StackSize   = WORKER_THREAD_STACK_DEFAULT;
    pool_init.NUMAGroup   = 0;
    if (TpCreateThreadPool(&pool, &pool_init, "Decompression Thread Pool") < 0)
    {
        ConsoleError("ERROR: %S(%u): Failed to create the image decompression thread pool.\n", __FUNCTION__, GetCurrentThreadId());
        return -1;
    }

    // publish the set of work. there's one signal for each file.
    TpSignalWorkerThreads(&pool, SIGNAL_DECOMPRESS_JPEG, shared.FileList.size());

    // start all threads running. they'll immediately enter a wait state.
    TpLaunchThreadPool(&pool);

    // wait until all files have been decompressed, and then shut down.
    WaitForSingleObject(shared.AllDone, INFINITE);
    TpDestroyThreadPool(&pool);
    CloseHandle(shared.AllDone);
    return 0;
}



