/*/////////////////////////////////////////////////////////////////////////////
/// @summary Test multi-threaded JPEG decoding using the thread pool and 
/// the asynchronous I/O system.
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
struct JPEG_FILE_DATA
{
    WCHAR            *Path;
    void             *Buffer;
    int64_t           DataSize;
};

struct SHARED_DATA
{
    THREAD_POOL      *WorkerPool;
    IO_REQUEST_QUEUE *IOQueue;
    HANDLE            IOSemaphore;
    HANDLE            AllDone;
    size_t            TotalFiles;
    JPEG_FILE_DATA   *FileList;
    size_t            FileCount;
    size_t            NextFile;
    CRITICAL_SECTION  SyncObj;
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
/// @summary Mark all file operations as being complete for a single file. This allows another file to be submitted.
/// @param shared_data The SHARED_DATA specifying the file I/O sempahore.
internal_function void
CompleteFile
(
    SHARED_DATA *shared_data
)
{
    ReleaseSemaphore(shared_data->IOSemaphore, 1, NULL);
}

/// @summary Callback executed when an asynchronous file read request has completed.
/// @param result An IO_RESULT used to return data to the caller.
/// @param queue_delay The number of nanoseconds elapsed between the application submitting the request and the background I/O thread executing the request.
/// @param execution_time The number of nanoseconds elapsed between the background I/O thread executing the request and the request completing.
/// @param was_successful Set to true if the operation completed successfully.
internal_function void
FileClose_Complete
(
    IO_RESULT        result, 
    uint64_t    queue_delay, 
    uint64_t execution_time, 
    bool     was_successful
)
{
    SHARED_DATA *shared = (SHARED_DATA*) result.UserContext;
    CompleteFile(shared);
    UNREFERENCED_PARAMETER(queue_delay);
    UNREFERENCED_PARAMETER(execution_time);
    UNREFERENCED_PARAMETER(was_successful);
}

/// @summary Callback executed when an asynchronous file read request has completed.
/// @param result An IO_RESULT used to return data to the caller.
/// @param queue_delay The number of nanoseconds elapsed between the application submitting the request and the background I/O thread executing the request.
/// @param execution_time The number of nanoseconds elapsed between the background I/O thread executing the request and the request completing.
/// @param was_successful Set to true if the operation completed successfully.
internal_function void
FileRead_Complete
(
    IO_RESULT        result, 
    uint64_t    queue_delay, 
    uint64_t execution_time, 
    bool     was_successful
)
{
    SHARED_DATA *shared = (SHARED_DATA*) result.UserContext;
    if (was_successful)
    {   // queue the file close operation.
        IO_REQUEST  req = {};
        req.RequestType = IO_REQUEST_CLOSE_FILE;
        req.IoHintFlags = IO_HINT_FLAGS_NONE;
        req.UserContext = result.UserContext;
        req.FileHandle  = result.FileHandle;
        req.SubmitTime  = GetIoTimestamp();
        req.IoComplete  = FileClose_Complete;
        IoEnqueueRequest(shared->IOQueue, req);
        // submit a job for the decompression pool.
        EnterCriticalSection(&shared->SyncObj);
        {
            JPEG_FILE_DATA &jpeg = shared->FileList[shared->FileCount];
            jpeg.Path     = result.PathBuffer;
            jpeg.Buffer   = result.DataBuffer;
            jpeg.DataSize = result.DataAmount;
            shared->FileCount++;
        }
        LeaveCriticalSection(&shared->SyncObj);
        TpSignalWorkerThreads(shared->WorkerPool, SIGNAL_DECOMPRESS_JPEG, 1);
    }
    else
    {   // terminate the call chain.
        CompleteFile(shared);
    }
    UNREFERENCED_PARAMETER(queue_delay);
    UNREFERENCED_PARAMETER(execution_time);
}

/// @summary Callback executed when an asynchronous file open request has completed.
/// @param result An IO_RESULT used to return data to the caller.
/// @param queue_delay The number of nanoseconds elapsed between the application submitting the request and the background I/O thread executing the request.
/// @param execution_time The number of nanoseconds elapsed between the background I/O thread executing the request and the request completing.
/// @param was_successful Set to true if the operation completed successfully.
internal_function void 
FileOpen_Complete
(
    IO_RESULT        result, 
    uint64_t    queue_delay, 
    uint64_t execution_time, 
    bool     was_successful
)
{
    SHARED_DATA *shared = (SHARED_DATA*) result.UserContext;
    if (was_successful)
    {   // allocate a buffer for the file contents. 
        void  *data_buf = NULL;
        if   ((data_buf = malloc(result.FileSize)) == NULL)
        {   // terminate the call chain; we're out of memory.
            ConsoleError("ERROR: %S(%u): Cannot allocate %I64d bytes for file \"%s\".\n", __FUNCTION__, GetCurrentThreadId(), result.FileSize, result.PathBuffer);
            CompleteFile(shared);
            return;
        }
        // submit the read request for the entire file contents.
        // this request should execute asynchronously.
        IO_REQUEST  req = {};
        req.RequestType = IO_REQUEST_READ_FILE;
        req.IoHintFlags = IO_HINT_FLAGS_NONE;
        req.UserContext = result.UserContext;
        req.FileHandle  = result.FileHandle;
        req.PathBuffer  = result.PathBuffer; // pass this through
        req.DataBuffer  = data_buf;
        req.DataAmount  = result.FileSize;
        req.BaseOffset  = 0;
        req.FileOffset  = 0;
        req.SubmitTime  = GetIoTimestamp();
        req.IoComplete  = FileRead_Complete;
        IoEnqueueRequest(shared->IOQueue, req);
    }
    else
    {   // terminate the call chain.
        CompleteFile(shared);
    }
    UNREFERENCED_PARAMETER(queue_delay);
    UNREFERENCED_PARAMETER(execution_time);
}

internal_function bool GetWorkItem
(
    SHARED_DATA       *shared, 
    JPEG_FILE_DATA &file_data
)
{
    bool result = false;
    EnterCriticalSection(&shared->SyncObj);
    {
        if (shared->NextFile < shared->FileCount)
        {
            file_data = shared->FileList[shared->NextFile++];
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
        if (shared->NextFile == shared->TotalFiles)
            all_done = true;
    }
    LeaveCriticalSection(&shared->SyncObj);
    if (all_done) SetEvent(shared->AllDone);
}

internal_function void
JpegInitDecompressInput
(
    JPEG_DECOMPRESS_INPUT *input, 
    JPEG_FILE_DATA         *file
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

internal_function void
JpegFreeFileData
(
    JPEG_FILE_DATA *data
)
{
    if (data->Buffer != NULL)
    {
        free(data->Buffer);
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
        JPEG_FILE_DATA data = {};

        if (!GetWorkItem(shared, data))
        {   // spurious wakeup; there's no file to decompress.
            TpThreadSpanLeave(pool, span);
            return;
        }
        TpThreadEvent(pool, "Decompress %s (%I64d bytes)", data.Path, data.DataSize);
        JpegInitDecompressInput(&input, &data);
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
        JpegFreeFileData(&data);
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
    SHARED_DATA                shared = {};
    THREAD_POOL_INIT        pool_init = {};
    THREAD_POOL                  pool = {};
    IO_BACKGROUND_THREAD    io_thread = {};
    IO_BACKGROUND_THREAD_INIT io_init = {};
    CPU_INFO                      cpu = {};
    FILE_LIST               file_list = {};
    JPEG_FILE_DATA         *jpeg_list = NULL;
    HANDLE                       qsem = NULL;

    UNREFERENCED_PARAMETER(argc);
    UNREFERENCED_PARAMETER(argv);

    if ((qsem = CreateSemaphore(NULL, 64 /* count */, 64 /* max */, NULL)) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Failed to create flow-rate semaphore (%08X).\n", __FUNCTION__, GetCurrentThreadId(), GetLastError());
        return -1;
    }
    if (TpQueryHostCpuLayout(&cpu) < 0)
    {
        ConsoleError("ERROR: %S(%u): Failed to query the host CPU layout.\n", __FUNCTION__, GetCurrentThreadId());
        return -1;
    }

    // set up the background I/O thread and launch it.
    // there can be 64 files in-flight at any given time, and up to two active requests per-file.
    io_init.MaxRequestsQueued = 1024;
    io_init.MaxRequestsActive = 128;
    io_init.EventDequeueCount = 128;
    io_init.DefaultHints      = IO_HINT_FLAG_READ | IO_HINT_FLAG_SEQUENTIAL;
    if (IoCreateBackgroundThread(&io_thread, &io_init) < 0)
    {
        ConsoleError("ERROR: %S(%u): Failed to create background I/O thread.\n", __FUNCTION__, GetCurrentThreadId());
        return -1;
    }
    if (IoEnumerateDirectoryFiles(file_list, "data", true) < 0 || file_list.size() == 0)
    {
        ConsoleError("ERROR: %S(%u): The data directory is empty.\n", __FUNCTION__, GetCurrentThreadId());
        IoTerminateBackgroundThread(&io_thread);
        return -1;
    }
    else
    {   // allocate the 'work queue' - pointers to loaded JPEG-compressed data.
        if ((jpeg_list = (JPEG_FILE_DATA*) malloc(file_list.size() * sizeof(JPEG_FILE_DATA))) == NULL)
        {
            ConsoleError("ERROR: %S(%u): Failed to allocate memory for JPEG list.\n", __FUNCTION__, GetCurrentThreadId());
            IoTerminateBackgroundThread(&io_thread);
            return -1;
        }
        IoSortFileList(file_list);
        InitializeCriticalSection(&shared.SyncObj);
        shared.WorkerPool  = &pool;
        shared.IOQueue     = IoGetBackgroundRequestQueue(&io_thread);
        shared.IOSemaphore = qsem;
        shared.AllDone     = CreateEvent(NULL, TRUE, FALSE, NULL);
        shared.TotalFiles  = file_list.size();
        shared.FileList    = jpeg_list;
        shared.FileCount   = 0;
        shared.NextFile    = 0;
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
        IoTerminateBackgroundThread(&io_thread);
        return -1;
    }

    // start all threads running. they'll immediately enter a wait state.
    TpLaunchThreadPool(&pool);

    // begin submitting asynchronous file open requests.
    // only so many files are allowed to be in flight at any given time.
    for (size_t i = 0, n = file_list.size(); i < n; ++i)
    {   // wait for an I/O slot to open up.
        if (WaitForSingleObject(qsem, INFINITE) != WAIT_OBJECT_0)
        {   // there's a serious error - bail out like we're finished.
            break;
        }
        IO_REQUEST req;
        size_t n_posted = 0;
        do
        {   // fill out the asynchronous file open request. this will execute on the background I/O thread.
            req.RequestType = IO_REQUEST_OPEN_FILE;
            req.IoHintFlags = IO_HINT_FLAG_READ | IO_HINT_FLAG_SEQUENTIAL;
            req.UserContext =(uintptr_t)   &shared;
            req.FileHandle  = INVALID_HANDLE_VALUE;
            req.PathBuffer  = file_list[i].Path;
            req.DataBuffer  = NULL;
            req.DataAmount  = 0;
            req.BaseOffset  = 0;
            req.FileOffset  = 0;
            req.SubmitTime  = GetIoTimestamp(); // TODO(rlk): make public
            req.IoComplete  = FileOpen_Complete;
        } while ((n_posted  = IoEnqueueRequest(IoGetBackgroundRequestQueue(&io_thread), req)) == 0);
    }

    // wait until all files have been decompressed, and then shut down.
    WaitForSingleObject(shared.AllDone, INFINITE);
    TpDestroyThreadPool(&pool);
    IoTerminateBackgroundThread(&io_thread);
    CloseHandle(shared.AllDone);
    return 0;
}

