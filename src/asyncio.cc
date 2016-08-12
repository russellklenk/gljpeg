/*/////////////////////////////////////////////////////////////////////////////
/// @summary Implement a thread pool based around Windows I/O completion ports 
/// intended specifically for asynchronous I/O operations on files and sockets.
///////////////////////////////////////////////////////////////////////////80*/

/*//////////////////
//   Data Types   //
//////////////////*/
struct  IO_THREAD;
struct  IO_THREAD_INIT;
struct  IO_THREAD_POOL;
struct  IO_REQUEST;
struct  IO_REQUEST_POOL;
struct  IO_RESULT;

/// @summary Define the signature for the function invoked when an I/O operation has completed.
/// @param result Data describing the result of the I/O operation that was executed.
/// @param request_pool The I/O request pool from which the request was allocated.
/// @param io_pool The I/O thread pool that executed the request.
/// @param was_successful Set to true if the request executed successfully.
/// @return A chained I/O request to execute immediately, or NULL.
typedef IO_REQUEST* (*IO_COMPLETE_CALLBACK)(IO_RESULT *result, IO_REQUEST_POOL *request_pool, IO_THREAD_POOL *io_pool, bool was_successful);

/// @summary Helper macros to emit task profiler events and span markers.
#define IoThreadEvent(pool, fmt, ...)               CvWriteAlertW((pool)->MarkerSeries, _T(fmt), __VA_ARGS__)
#define IoThreadSpanEnter(pool, span, fmt, ...)     CvEnterSpanW((pool)->MarkerSeries, &(span), _T(fmt), __VA_ARGS__)
#define IoThreadSpanLeave(pool, span)               CvLeaveSpan((span))

/// @summary Define the data used to execute a low-level I/O operation. Not all data is used by all operations.
struct IO_OPERATION
{
    WCHAR               *FilePath;                  /// Pointer to a zero-terminated string specifying the file path. Used for OPEN operations only.
    HANDLE               FileHandle;                /// The file handle specifying the target for the operation. Used for READ, WRITE, FLUSH and CLOSE operations.
    HANDLE               CompletionPort;            /// The I/O completion port to associate with the file, or NULL. Used for OPEN operations only.
    OVERLAPPED          *Overlapped;                /// The OVERLAPPED object to use for asynchronous I/O, or NULL to use synchronous I/O. Used for READ and WRITE operations.
    void                *DataBuffer;                /// The source buffer specifying data to write, or destination buffer for data being read. Used for READ and WRITE operations.
    int64_t              FileOffset;                /// The absolute byte offset within the file at which to perform the I/O operation. Used for asynchronous READ and WRITE only.
    int64_t              PreallocationSize;         /// The desired size of the file, if IO_HINT_FLAG_PREALLOCATE is specified. Used for OPEN operations only.
    uint32_t             TransferAmount;            /// The number of bytes to transfer. Used for READ and WRITE operations.
    uint32_t             IoHintFlags;               /// One or more of IO_HINT_FLAGS. Used for OPEN operations only.
};

/// @summary Define the data resulting from the execution of a low-level I/O operation.
struct IO_OPERATION_RESULT
{
    HANDLE               FileHandle;                /// The file handle specifying the target for the operaiton. For OPEN operations, this is the handle of the opened file.
    DWORD                ResultCode;                /// The operating system result code.
    uint32_t             TransferAmount;            /// The number of bytes transferred.
    bool                 CompletedSynchronously;    /// Set to true if the I/O operation completed synchronously, or false if the I/O operation completed asynchronously.
    bool                 WasSuccessful;             /// Set to true if the I/O operation completed successfully, or false if the I/O operation failed.
};

/// @summary Define the data representing an active request internally within the background I/O system.
struct IO_REQUEST
{
    IO_REQUEST          *NextRequest;               /// Pointer to the next node in the list, or NULL if this is the tail node.
    IO_REQUEST          *PrevRequest;               /// Pointer to the previous node in the list, or NULL if this is the head node.
    IO_REQUEST_POOL     *RequestPool;               /// The IO_REQUEST_POOL from which this request was allocated.
    int                  RequestType;               /// One of IO_REQUEST_TYPE specifying the type of operation being performed.
    int                  RequestState;              /// One of IO_REQUEST_STATE specifying the current state of the request.
    OVERLAPPED           Overlapped;                /// The OVERLAPPED instance associated with the asynchronous request.
    HANDLE               FileHandle;                /// The file handle associated with the request.
    WCHAR               *PathBuffer;                /// Pointer to a caller-managed buffer specifying the path of the file to LOAD or SAVE.
    void                *DataBuffer;                /// The caller-managed buffer from which to READ/LOAD or WRITE/SAVE data.
    int64_t              DataAmount;                /// The number of bytes to transfer to or from the caller-managed data buffer.
    int64_t              BaseOffset;                /// The byte offset of the start of the operation from the start of the physical file.
    int64_t              FileOffset;                /// The byte offset of the start of the operation from the start of the logical file.
    uintptr_t            UserContext;               /// Opaque data associated with the request to be passed through to the completion callback.
    IO_COMPLETE_CALLBACK CompletionCallback;        /// The callback to invoke when the operation has completed. May be NULL.
    CV_SPAN             *QueueSpan;                 /// The Concurrency Visualizer span defining the time the request was submitted and the time it was launched.
    CV_SPAN             *ExecuteSpan;               /// The Concurrency Visualizer span defining the time the request spent executing.
    uint32_t             IoHintFlags;               /// A combination of one or more IO_HINT_FLAGS. Generally only used for OPEN operations.
};

/// @summary Define an object managing a preallocated pool of I/O requests.
struct IO_REQUEST_POOL
{
    CRITICAL_SECTION     ListLock;                  /// The lock protecting the live and free lists.
    IO_REQUEST          *LiveRequest;               /// Pointer to the first live request, or NULL if no requests have been allocated from the pool.
    IO_REQUEST          *FreeRequest;               /// Pointer to the first free request, or NULL if no requests are available in the pool.
    IO_REQUEST          *NodePool;                  /// Pointer to the pool allocation.
};

/// @summary Define the data returned from a background I/O request submitted by the application.
struct IO_RESULT
{
    int                  RequestType;               /// One of IO_REQUEST_TYPE specifying the type of operation that completed.
    uint32_t             ResultCode;                /// ERROR_SUCCESS or another result code indicating whether the operation completed successfully.
    uintptr_t            UserContext;               /// Opaque data associated with the request by the application.
    HANDLE               FileHandle;                /// The handle of the file associated with the I/O request. This value may be INVALID_HANDLE_VALUE.
    WCHAR               *PathBuffer;                /// The path of the file associated with the I/O request. This value may be NULL.
    void                *DataBuffer;                /// The source or destination caller-managed buffer. This value may be NULL.
    union
    {
        int64_t          FileSize;                  /// For an OPEN operation, the file size is returned in this field.
        int64_t          DataAmount;                /// The number of bytes read from or written to the data buffer.
    };
    union
    {
        int64_t          BaseOffset;                /// The byte offset of the start of the operation from the start of the physical file.
        int64_t          PhysicalSectorSize;        /// For an OPEN operation, the physical device sector size in bytes is returned in this field.
    };
    int64_t              FileOffset;                /// The byte offset of the start of the operation from the start of the logical file.
};

/// @summary Define the data passed to an I/O worker thread on startup. The thread should copy the data into thread-local memory.
struct IO_THREAD_INIT
{
    IO_THREAD_POOL      *ThreadPool;                /// The IO_THREAD_POOL to which the worker belongs.
    HANDLE               ReadySignal;               /// A manual-reset event to be signaled by the worker when it has successfully completed initialization and is ready-to-run.
    HANDLE               ErrorSignal;               /// A manual-reset event to be signaled by the worker before it terminates when it encounters a fatal error.
    HANDLE               TerminateSignal;           /// A manual-reset event to be signaled by the application when the worker should terminate.
    HANDLE               CompletionPort;            /// The I/O completion port to be monitored by the thread for incoming events.
    uintptr_t            PoolContext;               /// Opaque data specified by the application.
};

/// @summary Define the data associated with an I/O thread pool.
struct IO_THREAD_POOL
{
    size_t               ActiveThreads;             /// The number of I/O worker threads in the pool.
    unsigned int        *OSThreadIds;               /// An array of operating system thread identifiers.
    HANDLE              *OSThreadHandle;            /// An array of operating system thread handles.
    HANDLE              *WorkerReady;               /// An array of manual-reset events used by each thread to signal successful initialization.
    HANDLE              *WorkerError;               /// An array of manual-reset events used by each thread to signal fatal errors prior to termination.
    HANDLE               CompletionPort;            /// The I/O completion port used to signal worker threads.
    HANDLE               TerminateSignal;           /// A manual-reset event used to signal all worker threads to terminate.
    CV_PROVIDER         *ProfileProvider;           /// The Concurrency Visualizer interface used to emit I/O profiling events.
    CV_MARKERSERIES     *MarkerSeries;              /// The Concurrency Visualizer marker series used for I/O profiling events.
};

/// @summary Define the data used by the application to configure the thread pool.
struct IO_THREAD_POOL_INIT
{
    size_t               ThreadCount;               /// The number of threads in the pool. 
    uintptr_t            PoolContext;               /// Opaque data associated with the pool.
};

/// @summary Define the supported types of asynchronous I/O requests.
enum IO_REQUEST_TYPE : int
{
    IO_REQUEST_NOOP                =  0,            /// Ignore the operation.
    IO_REQUEST_OPEN_FILE           =  1,            /// Asynchronously open a file, but do not issue any other I/O operations.
    IO_REQUEST_READ_FILE           =  2,            /// Issue an explicit asynchronous read request.
    IO_REQUEST_WRITE_FILE          =  3,            /// Issue an explicit asynchronous write request.
    IO_REQUEST_FLUSH_FILE          =  4,            /// Issue an explicit asynchronous flush request.
    IO_REQUEST_CLOSE_FILE          =  5,            /// Asynchronously close a file.
};

/// @summary Define the states that an I/O request can be in.
enum IO_REQUEST_STATE : int
{
    IO_REQUEST_STATE_CHAINED       =  0,            /// The I/O request has been submitted as a chained request.
    IO_REQUEST_STATE_SUBMITTED     =  1,            /// The I/O request has been submitted, but not yet launched.
    IO_REQUEST_STATE_LAUNCHED      =  2,            /// The I/O request has been picked up by an I/O thread and has begun executing.
    IO_REQUEST_STATE_COMPLETED     =  3,            /// The I/O request has been completed.
};

/// @summary Define flags used to optimize asynchronous I/O operations.
enum IO_HINT_FLAGS : uint32_t
{
    IO_HINT_FLAGS_NONE             = (0 << 0),      /// No I/O hints are specified, use the default behavior appropriate for the I/O request type.
    IO_HINT_FLAG_READ              = (1 << 0),      /// Read operations will be issued against the file.
    IO_HINT_FLAG_WRITE             = (1 << 1),      /// Write operations will be issues against the file.
    IO_HINT_FLAG_OVERWRITE         = (1 << 2),      /// The existing file contents should be discarded.
    IO_HINT_FLAG_PREALLOCATE       = (1 << 3),      /// Preallocate the file to the size specified in the IO_REQUEST::DataAmount field.
    IO_HINT_FLAG_SEQUENTIAL        = (1 << 4),      /// Optimize for sequential access when performing cached/buffered I/O (valid for OPEN, LOAD and SAVE.)
    IO_HINT_FLAG_UNCACHED          = (1 << 5),      /// Indicate that the I/O should bypass the OS page cache, and that the source or destination buffer meets sector alignment requirements (valid for OPEN, LOAD and SAVE).
    IO_HINT_FLAG_WRITE_THROUGH     = (1 << 6),      /// Indicate that writes should be immediately flushed to disk.
    IO_HINT_FLAG_TEMPORARY         = (1 << 7),      /// Indicate that the file is temporary, and will be deleted when the file handle is closed.
};

/*///////////////
//   Globals   //
///////////////*/
/// @summary The GUID of the Win32 OS Layer I/O profiler provider {B1456286-F2A5-4B9A-B7AD-3CDA15348C3C}.
global_variable GUID      const IoProfilerGUID = { 0xb1456286, 0xf2a5, 0x4b9a, { 0xb7, 0xad, 0x3c, 0xda, 0x15, 0x34, 0x8c, 0x3c } };

/// @summary OVERLAPPED_ENTRY::lpCompletionKey is set to IO_COMPLETION_KEY_SHUTDOWN to terminate the asynchronous I/O thread loop.
global_variable ULONG_PTR const IO_COMPLETION_KEY_SHUTDOWN = ~ULONG_PTR(0);

/*//////////////////////////
//   Internal Functions   //
//////////////////////////*/
/// @summary Execute a low-level file open operation.
/// @param args Information about the file to open. The FilePath and IoHintFlags fields must be set.
/// @param result Information returned from the operation specifying the file handle and whether the operation completed successfully.
/// @param file_info If this argument is not NULL, information about the file is stored here on return.
/// @param sector_size If this argument is not NULL, the physical device sector size is stored here on return.
internal_function void
IoExecuteOpen
(
    IO_OPERATION            &args, 
    IO_OPERATION_RESULT   &result, 
    FILE_STANDARD_INFO *file_info, 
    size_t           *sector_size
)
{
    HANDLE    fd = INVALID_HANDLE_VALUE;
    DWORD access = 0; // dwDesiredAccess
    DWORD share  = 0; // dwShareMode
    DWORD create = 0; // dwCreationDisposition
    DWORD flags  = 0; // dwFlagsAndAttributes

    if (args.IoHintFlags & IO_HINT_FLAG_OVERWRITE)
    {   // this implies write access.
        args.IoHintFlags |= IO_HINT_FLAG_WRITE;
    }
    if (args.IoHintFlags & IO_HINT_FLAG_READ)
    {
        access |= GENERIC_READ;
        share   = FILE_SHARE_READ;
        create  = OPEN_EXISTING;
        flags   = FILE_FLAG_OVERLAPPED;
    }
    if (args.IoHintFlags & IO_HINT_FLAG_WRITE)
    {
        access |= GENERIC_WRITE;
        flags   = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED;
        if (args.IoHintFlags & IO_HINT_FLAG_OVERWRITE)
        {   // opening the file will always succeed.
            create = CREATE_ALWAYS;
        }
        else
        {   // opening the file will always succeed, but existing contents are preserved.
            create = OPEN_ALWAYS;
        }
        if (args.IoHintFlags & IO_HINT_FLAG_TEMPORARY)
        {   // temporary files are deleted on close, and the cache manager will try to prevent writes to disk.
            flags |= FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE;
            share |= FILE_SHARE_DELETE;
        }
        else
        {   // standard persistent file, data will eventually end up on disk.
            flags |= FILE_ATTRIBUTE_NORMAL;
        }
    }
    if (args.IoHintFlags & IO_HINT_FLAG_SEQUENTIAL)
    {   // tell the cache manager to optimize for sequential access.
        flags |= FILE_FLAG_SEQUENTIAL_SCAN;
    }
    else
    {   // assume the file will be accessed randomly.
        flags |= FILE_FLAG_RANDOM_ACCESS;
    }
    if (args.IoHintFlags & IO_HINT_FLAG_UNCACHED)
    {   // use unbuffered I/O, reads must be performed in sector size multiples to 
        // a buffer whose address is also a multiple of the physical disk sector size.
        flags |= FILE_FLAG_NO_BUFFERING;
    }
    if (args.IoHintFlags & IO_HINT_FLAG_WRITE_THROUGH)
    {   // writes are immediately flushed to disk, if possible.
        flags |= FILE_FLAG_WRITE_THROUGH;
    }
    if ((fd = CreateFile(args.FilePath, access, share, NULL, create, flags, NULL)) == INVALID_HANDLE_VALUE)
    {
        ConsoleError("ERROR: %S(%u): Failed to open file \"%s\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), args.FilePath, GetLastError());
        result.FileHandle = INVALID_HANDLE_VALUE;
        result.ResultCode = GetLastError();
        result.TransferAmount = 0;
        result.CompletedSynchronously = true;
        result.WasSuccessful = false;
        return;
    }
    if (args.CompletionPort != NULL && CreateIoCompletionPort(fd, args.CompletionPort, 0, 0) != args.CompletionPort)
    {
        ConsoleError("ERROR: %S(%u): Unable to associate file \"%s\" with I/O completion port (%08X).\n", __FUNCTION__, GetCurrentThreadId(), args.FilePath, GetLastError());
        result.FileHandle = INVALID_HANDLE_VALUE;
        result.ResultCode = GetLastError();
        result.TransferAmount = 0;
        result.CompletedSynchronously = true;
        result.WasSuccessful = false;
        CloseHandle(fd);
        return;
    }
    if (args.CompletionPort != NULL)
    {   // immediately complete requests that execute synchronously; don't post completion port notification.
        // this reduces traffic on the completion port and improves performance for asynchronous requests.
        SetFileCompletionNotificationModes(fd, FILE_SKIP_COMPLETION_PORT_ON_SUCCESS);
    }
    if ((args.IoHintFlags & IO_HINT_FLAG_PREALLOCATE) && args.PreallocationSize > 0)
    {   // preallocate storage space for the file data, which can significantly improve performance when writing large files.
        // this can be slower for large files if the file is not written sequentially.
        LARGE_INTEGER new_ptr; new_ptr.QuadPart = args.PreallocationSize;
        if (SetFilePointerEx(fd, new_ptr, NULL, FILE_BEGIN))
        {   // set the end-of-file marker to the new location.
            if (SetEndOfFile(fd))
            {   // move the file pointer back to the start of the file.
                new_ptr.QuadPart = 0;
                SetFilePointerEx(fd, new_ptr, NULL, FILE_BEGIN);
            }
            else
            {   // this is a non-fatal error since preallocation is an optimization.
                ConsoleError("ERROR: %S(%u): Failed to preallocate file \"%s\" to %I64d bytes. SetEndOfFile failed (%08X).\n", __FUNCTION__, GetCurrentThreadId(), args.FilePath, args.PreallocationSize, GetLastError()); 
            }
        }
        else
        {   // this is a non-fatal error since preallocation is an optimization.
            ConsoleError("ERROR: %S(%u): Failed to preallocate file \"%s\" to %I64d bytes. SetFilePointerEx failed (%08X).\n", __FUNCTION__, GetCurrentThreadId(), args.FilePath, args.PreallocationSize, GetLastError()); 
        }
    }
    if (file_info != NULL)
    {   // retrieve the end-of-file (logical file size) and allocation size (physical size).
        ZeroMemory(file_info, sizeof(FILE_STANDARD_INFO));
        if (!GetFileInformationByHandleEx(fd, FileStandardInfo, file_info, sizeof(FILE_STANDARD_INFO)))
        {   // this is a non-fatal error since it's just an information request.
            ConsoleError("ERROR: %S(%u): Failed to retrieve attributes for file \"%s\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), args.FilePath, GetLastError());
        }
    }
    if (sector_size != NULL)
    {   // retrieve the physical device sector size. this value is necessary for unbuffered I/O operations, 
        // where the buffer address and transfer size must be a multiple of the physical device sector size.
        // TODO(rlk): for this to work, it needs the DEVICE path, ie. \\.\C:, and then you have to open a handle.
        // TODO(rlk): GetFileInformationByHandleEx could also be used, but that is Windows 8+ for the property we need.
        // TODO(rlk): for now, just hard-code to 4096.
        /*STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR desc;
        STORAGE_PROPERTY_QUERY query;
        DWORD bytes = 0;
        ZeroMemory(&desc , sizeof(STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR));
        ZeroMemory(&query, sizeof(STORAGE_PROPERTY_QUERY));
        query.QueryType  = PropertyStandardQuery;
        query.PropertyId = StorageAccessAlignmentProperty;
        if (DeviceIoControl(fd, IOCTL_STORAGE_QUERY_PROPERTY, &query, sizeof(query), &desc, sizeof(desc), &bytes, NULL))
        {   // save the physical device sector size.
           *sector_size  = desc.BytesPerPhysicalSector; 
        }
        else
        {   // this is a non-fatal error; return a default that will work for all common disks.
            ConsoleError("ERROR: %S(%u): Failed to retrieve physical device sector size for file \"%s\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), args.FilePath, GetLastError());
           *sector_size = 4096;
        }*/
        *sector_size = 4096;
    }
    result.FileHandle = fd;
    result.ResultCode = GetLastError();
    result.TransferAmount = 0;
    result.CompletedSynchronously = true;
    result.WasSuccessful = true;
}

/// @summary Execute a synchronous or asynchronous read operation.
/// @param args Information about the read operation. The FileHandle, DataBuffer and TransferAmount fields must be set.
/// @param result Information returned from the operation specifying the number of bytes transferred and whether the operation completed successfully.
internal_function void
IoExecuteRead
(
    IO_OPERATION          &args,
    IO_OPERATION_RESULT &result
)
{   
    DWORD transferred = 0;
    // the request is submitted to be performed asynchronously, but can complete
    // either synchronously or asynchronously. this routine hides that complexity.
    if (args.Overlapped)
    {   // populate the required fields of the OVERLAPPED strucure for an asynchronous request.
        args.Overlapped->Internal     = 0;
        args.Overlapped->InternalHigh = 0;
        args.Overlapped->Offset       =(DWORD) (args.FileOffset        & 0xFFFFFFFFUL);
        args.Overlapped->OffsetHigh   =(DWORD)((args.FileOffset >> 32) & 0xFFFFFFFFUL);
    }
    else if (args.FileOffset >= 0)
    {   // for synchronous I/O, support implicit seeking within the file.
        LARGE_INTEGER new_ptr; new_ptr.QuadPart = args.FileOffset;
        if (!SetFilePointerEx(args.FileHandle, new_ptr, NULL, FILE_BEGIN))
        {   // the seek operation failed - fail the read operation.
            result.FileHandle = args.FileHandle;
            result.ResultCode = GetLastError();
            result.CompletedSynchronously = true;
            result.WasSuccessful = false;
            return;
        }
    }
    if (ReadFile(args.FileHandle, args.DataBuffer, args.TransferAmount, &transferred, args.Overlapped))
    {   // the read operation completed synchronously (likely the data was in-cache.)
        result.FileHandle = args.FileHandle;
        result.ResultCode = GetLastError();
        result.TransferAmount = transferred;
        result.CompletedSynchronously = true;
        result.WasSuccessful = true;
    }
    else
    {   // the operation could have failed, or it could be completing asynchronously.
        // it could also be the case that end-of-file was reached.
        switch ((result.ResultCode = GetLastError()))
        {
            case ERROR_IO_PENDING:
                { // the request will complete asynchronously.
                  result.FileHandle = args.FileHandle;
                  result.TransferAmount = 0;
                  result.CompletedSynchronously = false;
                  result.WasSuccessful = true;
                } break;
            case ERROR_HANDLE_EOF:
                { // attempt to read past end-of-file; result.TransferAmount is set to the number of bytes available.
                  result.FileHandle = args.FileHandle;
                  result.TransferAmount = transferred;
                  result.CompletedSynchronously = true;
                  result.WasSuccessful = true;
                } break;
            default:
                { // an actual error occurred.
                  result.FileHandle = args.FileHandle;
                  result.TransferAmount = transferred;
                  result.CompletedSynchronously = true;
                  result.WasSuccessful = false;
                } break;
        }
    }
}

/// @summary Execute a synchronous or asynchronous write operation.
/// @param args Information about the write operation. The FileHandle, DataBuffer and TransferAmount fields must be set.
/// @param result Information returned from the operation specifying the number of bytes transferred and whether the operation completed successfully.
internal_function void
IoExecuteWrite
(
    IO_OPERATION          &args, 
    IO_OPERATION_RESULT &result
)
{   
    DWORD transferred = 0;
    // the request is submitted to be performed asynchronously, but can complete
    // either synchronously or asynchronously. this routine hides that complexity.
    if (args.Overlapped)
    {   // populate the required fields of the OVERLAPPED strucure for an asynchronous request.
        args.Overlapped->Internal     = 0;
        args.Overlapped->InternalHigh = 0;
        args.Overlapped->Offset       =(DWORD) (args.FileOffset        & 0xFFFFFFFFUL);
        args.Overlapped->OffsetHigh   =(DWORD)((args.FileOffset >> 32) & 0xFFFFFFFFUL);
    }
    else if (args.FileOffset >= 0)
    {   // for synchronous I/O, support implicit seeking within the file.
        LARGE_INTEGER new_ptr; new_ptr.QuadPart = args.FileOffset;
        if (!SetFilePointerEx(args.FileHandle, new_ptr, NULL, FILE_BEGIN))
        {   // the seek operation failed - fail the write operation.
            result.FileHandle = args.FileHandle;
            result.ResultCode = GetLastError();
            result.CompletedSynchronously = true;
            result.WasSuccessful = false;
            return;
        }
    }
    if (WriteFile(args.FileHandle, args.DataBuffer, args.TransferAmount, &transferred, args.Overlapped))
    {   // the write operation completed synchronously.
        result.FileHandle = args.FileHandle;
        result.ResultCode = GetLastError();
        result.TransferAmount = transferred;
        result.CompletedSynchronously = true;
        result.WasSuccessful = true;
    }
    else
    {   // the operation could have failed, or it could be completing asynchronously.
        // it could also be the case that end-of-file was reached.
        switch ((result.ResultCode = GetLastError()))
        {
            case ERROR_IO_PENDING:
                { // the request will complete asynchronously.
                  result.FileHandle = args.FileHandle;
                  result.TransferAmount = 0;
                  result.CompletedSynchronously = false;
                  result.WasSuccessful = true;
                } break;
            default:
                { // an actual error occurred.
                  result.FileHandle = args.FileHandle;
                  result.TransferAmount = transferred;
                  result.CompletedSynchronously = true;
                  result.WasSuccessful = false;
                } break;
        }
    }
}

/// @summary Execute a synchronous flush operation.
/// @param args Information about the flush operation. The FileHandle field must be set.
/// @param result Information returned from the operation specifying whether the operation completed successfully.
internal_function void
IoExecuteFlush
(
    IO_OPERATION          &args, 
    IO_OPERATION_RESULT &result
)
{
    if (FlushFileBuffers(args.FileHandle))
    {
        result.FileHandle = args.FileHandle;
        result.ResultCode = GetLastError();
        result.TransferAmount = 0;
        result.CompletedSynchronously = true;
        result.WasSuccessful = true;
    }
    else
    {
        result.FileHandle = args.FileHandle;
        result.ResultCode = GetLastError();
        result.TransferAmount = 0;
        result.CompletedSynchronously = true;
        result.WasSuccessful = false;
    }
}

/// @summary Execute a synchronous file close operation.
/// @param args Information about the close operation. The FileHandle field must be set.
/// @param result Information returned from the operation specifying whether the operation completed successfully.
internal_function void
IoExecuteClose
(
    IO_OPERATION          &args, 
    IO_OPERATION_RESULT &result
)
{
    if (CloseHandle(args.FileHandle))
    {
        result.FileHandle = args.FileHandle;
        result.ResultCode = GetLastError();
        result.TransferAmount = 0;
        result.CompletedSynchronously = true;
        result.WasSuccessful = true;
    }
    else
    {
        result.FileHandle = args.FileHandle;
        result.ResultCode = GetLastError();
        result.TransferAmount = 0;
        result.CompletedSynchronously = true;
        result.WasSuccessful = false;
    }
}

/// @summary Prepare an IO_RESULT to pass to the I/O completion callback after executing an I/O operation.
/// @param result The IO_RESULT structure to populate.
/// @param request The IO_REQUEST describing the I/O operation.
/// @param opres The IO_OPERATION_RESULT describing the completion of the I/O operation.
internal_function void
IoPrepareResult
(
    IO_RESULT          &result,
    IO_REQUEST        *request, 
    IO_OPERATION_RESULT &opres
)
{   // NOTE: this routine intentionally prepares the result to pass through as much
    // data as possible, even if that data might not have been needed for the request.
    result.RequestType = request->RequestType;
    result.ResultCode  = opres.ResultCode;
    result.UserContext = request->UserContext;
    result.FileHandle  = opres.FileHandle;
    result.PathBuffer  = request->PathBuffer;
    result.DataBuffer  = request->DataBuffer;
    result.DataAmount  = opres.TransferAmount;
    result.BaseOffset  = request->BaseOffset;
    result.FileOffset  = request->FileOffset;
}

/// @summary Execute an I/O request. The request may complete synchronously or asynchronously.
/// @param request The I/O request to execute.
/// @param completion_port The I/O completion port to associate with the request.
/// @param opres The structure to populate with the result of the I/O operation.
/// @param result The structure to populate with the result of the I/O request.
internal_function void
IoExecuteRequest
(
    IO_REQUEST         *request, 
    HANDLE      completion_port,
    IO_OPERATION_RESULT  &opres, 
    IO_RESULT           &result
)
{
    switch (request->RequestType)
    {
        case IO_REQUEST_NOOP:
            { // there's no actual I/O operation to perform.
              opres.FileHandle             = request->FileHandle;
              opres.ResultCode             = ERROR_SUCCESS;
              opres.TransferAmount         = 0;
              opres.CompletedSynchronously = true;
              opres.WasSuccessful          = true;
              IoPrepareResult(result, request, opres);
            } break;
        case IO_REQUEST_OPEN_FILE:
            { // populate the operation request for the file open operation.
              // open operations return more data than most requests, and are always performed synchronously.
              FILE_STANDARD_INFO  fsi = {};
              IO_OPERATION      opreq = {};
              size_t      sector_size = 0;
              opreq.FilePath          = request->PathBuffer;
              opreq.FileHandle        = INVALID_HANDLE_VALUE;
              opreq.CompletionPort    = completion_port;
              opreq.Overlapped        = NULL;
              opreq.DataBuffer        = NULL;
              opreq.FileOffset        = 0;
              opreq.PreallocationSize = request->DataAmount;
              opreq.TransferAmount    = 0;
              opreq.IoHintFlags       = request->IoHintFlags;
              IoExecuteOpen(opreq, opres, &fsi, &sector_size);
              IoPrepareResult(result, request, opres);
              result.FileSize = fsi.EndOfFile.QuadPart;
              result.PhysicalSectorSize = sector_size;
            } break;
        case IO_REQUEST_READ_FILE:
            { // read requests may complete asynchronously.
              IO_OPERATION      opreq = {};
              opreq.FilePath          = NULL;
              opreq.FileHandle        = request->FileHandle;
              opreq.CompletionPort    = NULL;
              opreq.Overlapped        =&request->Overlapped;
              opreq.DataBuffer        = request->DataBuffer;
              opreq.FileOffset        = request->BaseOffset + request->FileOffset;
              opreq.PreallocationSize = 0;
              opreq.TransferAmount    =(uint32_t) request->DataAmount;
              opreq.IoHintFlags       = IO_HINT_FLAGS_NONE;
              IoExecuteRead(opreq, opres);
              IoPrepareResult(result, request, opres);
            } break;
        case IO_REQUEST_WRITE_FILE:
            { // write requests may complete asynchronously.
              IO_OPERATION      opreq = {};
              opreq.FilePath          = NULL;
              opreq.FileHandle        = request->FileHandle;
              opreq.CompletionPort    = NULL;
              opreq.Overlapped        =&request->Overlapped;
              opreq.DataBuffer        = request->DataBuffer;
              opreq.FileOffset        = request->BaseOffset + request->FileOffset;
              opreq.PreallocationSize = 0;
              opreq.TransferAmount    =(uint32_t) request->DataAmount;
              opreq.IoHintFlags       = IO_HINT_FLAGS_NONE;
              IoExecuteWrite(opreq, opres);
              IoPrepareResult(result, request, opres);
            } break;
        case IO_REQUEST_FLUSH_FILE:
            { // populate the operation request for the flush operation.
              // flush operations are always performed synchronously.
              IO_OPERATION      opreq = {};
              opreq.FilePath          = NULL;
              opreq.FileHandle        = request->FileHandle;
              opreq.CompletionPort    = NULL;
              opreq.Overlapped        = NULL;
              opreq.DataBuffer        = NULL;
              opreq.FileOffset        = 0;
              opreq.PreallocationSize = 0;
              opreq.TransferAmount    = 0;
              opreq.IoHintFlags       = IO_HINT_FLAGS_NONE;
              IoExecuteFlush(opreq, opres);
              IoPrepareResult(result, request, opres);
            } break;
        case IO_REQUEST_CLOSE_FILE:
            { // populate the operation request for the close operation.
              // close operations are always performed synchronously.
              IO_OPERATION      opreq = {};
              opreq.FilePath          = NULL;
              opreq.FileHandle        = request->FileHandle;
              opreq.CompletionPort    = NULL;
              opreq.Overlapped        = NULL;
              opreq.DataBuffer        = NULL;
              opreq.FileOffset        = 0;
              opreq.PreallocationSize = 0;
              opreq.TransferAmount    = 0;
              opreq.IoHintFlags       = IO_HINT_FLAGS_NONE;
              IoExecuteClose(opreq, opres);
              IoPrepareResult(result, request, opres);
            } break;
        default:
            { // don't know what this is - just pass through data.
              opres.FileHandle             = request->FileHandle;
              opres.ResultCode             = ERROR_SUCCESS;
              opres.TransferAmount         = 0;
              opres.CompletedSynchronously = true;
              opres.WasSuccessful          = true;
              IoPrepareResult(result, request, opres);
            } break;
    }
}

/// @summary Return an I/O request to the pool it was allocated from.
/// @param request The I/O request to return.
internal_function void
IoReturnRequest
(
    IO_REQUEST *request
)
{
    EnterCriticalSection(&request->RequestPool->ListLock);
    {   // remove the node from the live list.
        if (request->NextRequest != NULL)
            request->NextRequest->PrevRequest = request->PrevRequest;
        if (request->PrevRequest != NULL)
            request->PrevRequest->NextRequest = request->NextRequest;
        // push the node into the front of the free list.
        request->NextRequest = request->RequestPool->FreeRequest;
        request->PrevRequest = NULL;
        request->RequestPool->FreeRequest = request;
    }
    LeaveCriticalSection(&request->RequestPool->ListLock);
}

/// @summary Retrieve the IO_REQUEST for an OVERLAPPED address associated with an active request slot.
/// @param overlapped The OVERLAPPED instance corresponding to a completed request.
/// @return The associated IO_REQUEST.
internal_function inline IO_REQUEST*
IoRequestForOVERLAPPED
(
    OVERLAPPED *overlapped
)
{
    return ((IO_REQUEST*)(((uint8_t*) overlapped) - offsetof(IO_REQUEST, Overlapped)));
}

/// @summary Implement the entry point of an I/O worker thread.
/// @param argp An IO_THREAD_INIT instance specifying data that should be copied to thread-local memory.
/// @return The function always returns 1.
internal_function unsigned int __cdecl
IoThreadMain
(
    void *argp
)
{
    IO_THREAD_INIT    init = {};
    OVERLAPPED         *ov = NULL;
    uintptr_t          key = 0;
    DWORD              tid = GetCurrentThreadId();
    DWORD           nbytes = 0;
    DWORD           termrc = 0;
    unsigned int exit_code = 1;

    // copy the initialization data into local stack memory.
    // argp may have been allocated on the stack of the caller 
    // and is only guaranteed to remain valid until ReadySignal is set.
    CopyMemory(&init, argp, sizeof(IO_THREAD_INIT));

    // spit out a message just prior to initialization:
    ConsoleOutput("START: %S(%u): I/O thread starting on pool 0x%p.\n", __FUNCTION__, tid, init.ThreadPool);
    IoThreadEvent(init.ThreadPool, "I/O thread %u starting", tid);

    // signal the main thread that this thread is ready to run.
    SetEvent(init.ReadySignal);

    __try
    {   // enter the main I/O loop. the thread waits for an event to be available on the completion port.
        for ( ; ; )
        {   // prior to possibly entering a wait state, check the termination signal.
            if ((termrc = WaitForSingleObject(init.TerminateSignal, 0)) != WAIT_TIMEOUT)
            {   // either termination was signaled, or an error occurred.
                // either way, exit prior to entering the wait state.
                if (termrc != WAIT_OBJECT_0)
                {   // an error occurred while checking the termination signal.
                    ConsoleError("ERROR: %S(%u): Checking termination signal failed with result 0x%08X (0x%08X).\n", __FUNCTION__, tid, termrc, GetLastError());
                }
                break;
            }
            // wait for an event on the completion port.
            if (GetQueuedCompletionStatus(init.CompletionPort, &nbytes, &key, &ov, INFINITE))
            {   // check the termination signal prior to processing the request.
                if ((termrc = WaitForSingleObject(init.TerminateSignal, 0)) != WAIT_TIMEOUT)
                {   // either termination was signaled, or an error occurred.
                    if (termrc != WAIT_OBJECT_0)
                    {   // an error occurred while checking the termination signal.
                        ConsoleError("ERROR: %S(%u): Checking termination signal failed with result 0x%08X (0x%08X).\n", __FUNCTION__, tid, termrc, GetLastError());
                    }
                    break;
                }
                if (key == IO_COMPLETION_KEY_SHUTDOWN)
                {
                    ConsoleOutput("EVENT: %S(%u): I/O worker received shutdown signal.\n", __FUNCTION__, tid);
                    break;
                }

                IO_REQUEST *request = IoRequestForOVERLAPPED(ov);
                while (request != NULL)
                {
                    switch (request->RequestState)
                    {
                        case IO_REQUEST_STATE_CHAINED:
                        case IO_REQUEST_STATE_SUBMITTED:
                            { // launch the I/O operation.
                              // the I/O request may complete synchronously or asynchronously.
                              IO_RESULT result;
                              IO_OPERATION_RESULT opres;
                              IO_REQUEST *chained = NULL;
                              IoThreadSpanLeave(init.ThreadPool, request->QueueSpan);
                              IoThreadSpanEnter(init.ThreadPool, request->ExecuteSpan, "Execute I/O request %p (type %d, %I64d bytes)", request, request->RequestType, request->DataAmount);
                              //ConsoleError("BGIO: %S(%u): Request %p(%d) (%I64d bytes) transitioned to launched.\n", __FUNCTION__, GetCurrentThreadId(), request, request->RequestType, request->DataAmount);
                              request->RequestState   = IO_REQUEST_STATE_LAUNCHED;
                              IoExecuteRequest(request, init.CompletionPort, opres, result);
                              if (opres.CompletedSynchronously)
                              {   // update the request state and execute the completion callback.
                                  // there should be no additional events reported for this request.
                                  //ConsoleError("BGIO: %S(%u): Request %p(%d) (%u bytes) completed synchronously.\n", __FUNCTION__, GetCurrentThreadId(), request, request->RequestType, opres.TransferAmount);
                                  request->RequestState = IO_REQUEST_STATE_COMPLETED;
                                  if (request->CompletionCallback != NULL)
                                  {   // the request may return a chained request from its completion callback.
                                      chained = request->CompletionCallback(&result, request->RequestPool, init.ThreadPool, opres.WasSuccessful);
                                  }
                                  IoThreadSpanLeave(init.ThreadPool, request->ExecuteSpan);
                                  IoReturnRequest(request);
                                  request = chained;
                              }
                              else
                              {   // don't return the request until it completes.
                                  request = NULL;
                              }
                            } break;
                        case IO_REQUEST_STATE_LAUNCHED:
                            { // the asynchronous read or write operation has completed.
                              IO_RESULT    result;
                              IO_REQUEST *chained = NULL;
                              DWORD   result_code = ERROR_SUCCESS;
                              bool was_successful = true;
                              if (!GetOverlappedResult(request->FileHandle, ov, &nbytes, FALSE))
                              {   // the I/O request may have completed unsuccessfully.
                                  if ((result_code   = GetLastError()) != ERROR_HANDLE_EOF)
                                      was_successful = false;
                              }
                              //ConsoleError("BGIO: %S(%u): Request %p(%d) (%u bytes) completed asynchronously.\n", __FUNCTION__, GetCurrentThreadId(), request, request->RequestType, nbytes);
                              request->RequestState  = IO_REQUEST_STATE_COMPLETED;
                              result.RequestType     = request->RequestType;
                              result.ResultCode      = result_code;
                              result.UserContext     = request->UserContext;
                              result.FileHandle      = request->FileHandle;
                              result.PathBuffer      = request->PathBuffer;
                              result.DataBuffer      = request->DataBuffer;
                              result.DataAmount      = nbytes;
                              result.BaseOffset      = request->BaseOffset;
                              result.FileOffset      = request->FileOffset;
                              if (request->CompletionCallback != NULL)
                              {   // the request may return a chained request from its completion callback.
                                  chained = request->CompletionCallback(&result, request->RequestPool, init.ThreadPool, was_successful);
                              }
                              IoThreadSpanLeave(init.ThreadPool, request->ExecuteSpan);
                              IoReturnRequest(request);
                              request = chained;
                            } break;
                        case IO_REQUEST_STATE_COMPLETED:
                            { ConsoleError("ERROR: %S(%u): I/O thread received already completed request %p from pool %p.\n", __FUNCTION__, GetCurrentThreadId(), request, request->RequestPool);
                              IoReturnRequest(request);
                            } break;
                        default:
                            { ConsoleError("ERROR: %S(%u): I/O thread received request %p from pool %p with unknown state %d.\n", __FUNCTION__, GetCurrentThreadId(), request, request->RequestPool, request->RequestState);
                            } break;
                    }
                }
            }
        }
    }
    __finally
    {   // the I/O thread is terminating - clean up thread-local resources.
        // ...
        // spit out a message just prior to termination.
        ConsoleError("DEATH: %S(%u): I/O thread terminating in pool 0x%p.\n", __FUNCTION__, tid, init.ThreadPool);
        return exit_code;
    }
}

/*////////////////////////
//   Public Functions   //
////////////////////////*/
/// @summary Create a thread pool. The calling thread is blocked until all worker threads have successfully started and initialized.
/// @param pool The IO_THREAD_POOL instance to initialize.
/// @param init An IO_THREAD_POOL_INIT object describing the thread pool configuration.
/// @param name A zero-terminated string constant specifying a human-readable name for the thread pool, or NULL. This name is used for task profiler display.
/// @return Zero if the thread pool is created successfully and worker threads are ready-to-run, or -1 if an error occurs.
public_function int
IoCreateThreadPool
(
    IO_THREAD_POOL      *pool, 
    IO_THREAD_POOL_INIT *init,
    char const          *name=NULL
)
{
    HANDLE                iocp = NULL;
    HANDLE       evt_terminate = NULL;
    DWORD                  tid = GetCurrentThreadId();
    CV_PROVIDER   *cv_provider = NULL;
    CV_MARKERSERIES *cv_series = NULL;
    HRESULT          cv_result = S_OK;
    char           cv_name[64] = {};

    // Zero the fields of the IO_THREAD_POOL instance to start from a known state.
    ZeroMemory(pool, sizeof(IO_THREAD_POOL));

    // create objects used to synchronize the threads in the pool.
    if ((evt_terminate = CreateEvent(NULL, TRUE, FALSE, NULL)) == NULL)
    {   // without the termination event, there's no way to synchronize worker shutdown.
        ConsoleError("ERROR: %S(%u): Unable to create I/O pool termination event (0x%08X).\n", __FUNCTION__, tid, GetLastError());
        goto cleanup_and_fail;
    }
    if ((iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, (DWORD) init->ThreadCount+1)) == NULL)
    {   // without the completion port, there's no way to synchronize worker execution.
        ConsoleError("ERROR: %S(%u): Unable to create I/O pool completion port (0x%08X).\n", __FUNCTION__, tid, GetLastError());
        goto cleanup_and_fail;
    }

    if (name == NULL)
    {   // Concurrency Visualizer SDK requires a non-NULL string.
        sprintf_s(cv_name, "Unnamed I/O pool 0x%p", pool);
    }
    if (!SUCCEEDED((cv_result = CvInitProvider(&IoProfilerGUID, &cv_provider))))
    {
        ConsoleError("ERROR: %S(%u): Unable to initialize I/O profiler provider (HRESULT 0x%08X).\n", __FUNCTION__, tid, cv_result);
        goto cleanup_and_fail;
    }
    if (!SUCCEEDED((cv_result = CvCreateMarkerSeriesA(cv_provider, name, &cv_series))))
    {
        ConsoleError("ERROR: %S(%u): Unable to create I/O profiler marker series (HRESULT 0x%08X).\n", __FUNCTION__, tid, cv_result);
        goto cleanup_and_fail;
    }

    // initialize the thread pool fields and allocate memory for per-thread arrays.
    pool->ActiveThreads   = 0;
    pool->OSThreadIds     = (unsigned int*) malloc(init->ThreadCount * sizeof(unsigned int));
    pool->OSThreadHandle  = (HANDLE      *) malloc(init->ThreadCount * sizeof(HANDLE));
    pool->WorkerReady     = (HANDLE      *) malloc(init->ThreadCount * sizeof(HANDLE));
    pool->WorkerError     = (HANDLE      *) malloc(init->ThreadCount * sizeof(HANDLE));
    if (pool->OSThreadIds == NULL || pool->OSThreadHandle == NULL || 
        pool->WorkerReady == NULL || pool->WorkerError == NULL)
    {
        ConsoleError("ERROR: %S(%u): Unable to allocate I/O pool memory.\n", __FUNCTION__, tid);
        goto cleanup_and_fail;
    }
    pool->CompletionPort  = iocp;
    pool->TerminateSignal = evt_terminate;
    pool->ProfileProvider = cv_provider;
    pool->MarkerSeries    = cv_series;
    ZeroMemory(pool->OSThreadIds    , init->ThreadCount * sizeof(unsigned int));
    ZeroMemory(pool->OSThreadHandle , init->ThreadCount * sizeof(HANDLE));
    ZeroMemory(pool->WorkerReady    , init->ThreadCount * sizeof(HANDLE));
    ZeroMemory(pool->WorkerError    , init->ThreadCount * sizeof(HANDLE));

    // set up the worker init structure and spawn all threads.
    for (size_t i = 0, n = init->ThreadCount; i < n; ++i)
    {
        IO_THREAD_INIT     winit  = {};
        HANDLE             whand  = NULL;
        HANDLE            wready  = NULL;
        HANDLE            werror  = NULL;
        unsigned int   thread_id  = 0;
        const DWORD THREAD_READY  = 0;
        const DWORD THREAD_ERROR  = 1;
        const DWORD   WAIT_COUNT  = 2;
        HANDLE   wset[WAIT_COUNT] = {};
        DWORD             waitrc  = 0;

        // create the manual-reset events signaled by the worker to indicate that it is ready.
        if ((wready = CreateEvent(NULL, TRUE, FALSE, NULL)) == NULL)
        {
            ConsoleError("ERROR: %S(%u): Unable to create ready signal for I/O worker %Iu of %Iu (0x%08X).\n", __FUNCTION__, tid, i, n, GetLastError());
            goto cleanup_and_fail;
        }
        if ((werror = CreateEvent(NULL, TRUE, FALSE, NULL)) == NULL)
        {
            ConsoleError("ERROR: %S(%u): Unable to create error signal for I/O worker %Iu of %Iu (0x%08X).\n", __FUNCTION__, tid, i, n, GetLastError());
            CloseHandle(wready);
            goto cleanup_and_fail;
        }

        // populate the IO_THREAD_INIT and then spawn the worker thread.
        // the worker thread will need to copy this structure if it wants to access it 
        // past the point where it signals the wready event.
        winit.ThreadPool      = pool;
        winit.ReadySignal     = wready;
        winit.ErrorSignal     = werror;
        winit.TerminateSignal = evt_terminate;
        winit.CompletionPort  = iocp;
        winit.PoolContext     = init->PoolContext;
        if ((whand = (HANDLE) _beginthreadex(NULL, (unsigned) 0/*stack size*/, IoThreadMain, &winit, 0, &thread_id)) == NULL)
        {
            ConsoleError("ERROR: %S(%u): Unable to spawn I/O worker %Iu of %Iu (errno = %d).\n", __FUNCTION__, tid, i, n, errno);
            CloseHandle(werror);
            CloseHandle(wready);
            goto cleanup_and_fail;
        }

        // save the various thread attributes in case 
        pool->OSThreadHandle[i] = whand;
        pool->OSThreadIds[i] = thread_id;
        pool->WorkerReady[i] = wready;
        pool->WorkerError[i] = werror;
        pool->ActiveThreads++;

        // wait for the thread to become ready.
        wset[THREAD_READY] = wready; 
        wset[THREAD_ERROR] = werror;
        if ((waitrc = WaitForMultipleObjects(WAIT_COUNT, wset, FALSE, INFINITE)) != (WAIT_OBJECT_0+THREAD_READY))
        {   // thread initialization failed, or the wait failed.
            // events are already in the IO_THREAD_POOL arrays, so don't clean up here.
            ConsoleError("ERROR: %S(%u): Failed to initialize I/O worker %Iu of %Iu (0x%08X).\n", __FUNCTION__, tid, i, n, waitrc);
            goto cleanup_and_fail;
        }
    }

    // everything has been successfully initialized. 
    // all worker threads are waiting on the completion port.
    return 0;

cleanup_and_fail:
    if (pool->ActiveThreads > 0)
    {   // signal all threads to terminate, and then wait until they all die.
        // all workers are blocked waiting on the launch event.
        SetEvent(evt_terminate);
        WaitForMultipleObjects((DWORD) pool->ActiveThreads, pool->OSThreadHandle, TRUE, INFINITE);
        // now that all threads have exited, close their handles.
        for (size_t i = 0, n = pool->ActiveThreads; i < n; ++i)
        {
            if (pool->OSThreadHandle != NULL) CloseHandle(pool->OSThreadHandle[i]);
            if (pool->WorkerReady    != NULL) CloseHandle(pool->WorkerReady[i]);
            if (pool->WorkerError    != NULL) CloseHandle(pool->WorkerError[i]);
        }
    }
    // free all of the thread-pool ID memory.
    if (pool->WorkerError    != NULL) free(pool->WorkerError);
    if (pool->WorkerReady    != NULL) free(pool->WorkerReady);
    if (pool->OSThreadHandle != NULL) free(pool->OSThreadHandle);
    if (pool->OSThreadIds    != NULL) free(pool->OSThreadIds);
    // clean up the task profiler objects.
    if (cv_series) CvReleaseMarkerSeries(cv_series);
    if (cv_provider) CvReleaseProvider(cv_provider);
    // clean up the I/O completion port and synchronization objects.
    if (evt_terminate) CloseHandle(evt_terminate);
    if (iocp) CloseHandle(iocp);
    // zero out the IO_THREAD_POOL prior to returning to the caller.
    ZeroMemory(pool, sizeof(IO_THREAD_POOL));
    return -1;
}

/// @summary Perform a fast shutdown of a thread pool. The calling thread does not wait for the worker threads to exit. No handles are closed.
/// @param pool The IO_THREAD_POOL to shut down.
public_function void
IoTerminateThreadPool
(
    IO_THREAD_POOL *pool
)
{
    if (pool->ActiveThreads > 0)
    {   // signal the termination event prior to waking any waiting threads.
        SetEvent(pool->TerminateSignal);
        // signal all worker threads in the pool. any active processing will complete before this signal is received.
        for (size_t i = 0; i < pool->ActiveThreads; ++i)
        {
            if (!PostQueuedCompletionStatus(pool->CompletionPort, 0, IO_COMPLETION_KEY_SHUTDOWN, NULL))
            {   // only report the first failure.
                ConsoleError("ERROR: %S(%u): Failed to post shutdown signal to I/O worker %Iu (%08X).\n", __FUNCTION__, GetCurrentThreadId(), i, GetLastError());
            }
        }
    }
}

/// @summary Perform a complete shutdown and cleanup of a thread pool. The calling thread is blocked until all threads exit.
/// @param pool The IO_THREAD_POOL to shut down and clean up.
public_function void
IoDestroyThreadPool
(
    IO_THREAD_POOL *pool
)
{
    if (pool->ActiveThreads > 0)
    {   // signal the termination event prior to waking any waiting threads.
        SetEvent(pool->TerminateSignal);
        // signal all worker threads in the pool. any active processing will complete before this signal is received.
        for (size_t i = 0; i < pool->ActiveThreads; ++i)
        {
            if (!PostQueuedCompletionStatus(pool->CompletionPort, 0, IO_COMPLETION_KEY_SHUTDOWN, NULL))
            {   // only report the first failure.
                ConsoleError("ERROR: %S(%u): Failed to post shutdown signal to I/O worker %Iu (%08X).\n", __FUNCTION__, GetCurrentThreadId(), i, GetLastError());
            }
        }
        // finally, wait for all threads to terminate gracefully.
        WaitForMultipleObjects((DWORD) pool->ActiveThreads, pool->OSThreadHandle, TRUE, INFINITE);
        // now that all threads have exited, close their handles.
        for (size_t i = 0, n = pool->ActiveThreads; i < n; ++i)
        {
            CloseHandle(pool->OSThreadHandle[i]);
            CloseHandle(pool->WorkerReady[i]);
            CloseHandle(pool->WorkerError[i]);
        }
        free(pool->WorkerError);    pool->WorkerError    = NULL;
        free(pool->WorkerReady);    pool->WorkerReady    = NULL;
        free(pool->OSThreadHandle); pool->OSThreadHandle = NULL;
        free(pool->OSThreadIds);    pool->OSThreadIds    = NULL;
        pool->ActiveThreads = 0;
    }
    // clean up the task provider objects from the Concurrency Visualizer SDK.
    if (pool->MarkerSeries != NULL)
    {
        CvReleaseMarkerSeries(pool->MarkerSeries);
        pool->MarkerSeries = NULL;
    }
    if (pool->ProfileProvider != NULL)
    {
        CvReleaseProvider(pool->ProfileProvider);
        pool->ProfileProvider = NULL;
    }
    if (pool->TerminateSignal != NULL)
    {
        CloseHandle(pool->TerminateSignal);
        pool->TerminateSignal = NULL;
    }
    if (pool->CompletionPort != NULL)
    {
        CloseHandle(pool->CompletionPort);
        pool->CompletionPort = NULL;
    }
}

/// @summary Create an I/O request pool.
/// @param pool The I/O request pool to initialize.
/// @param pool_capacity The maximum number of I/O requests that can be allocated from the pool at any one time.
/// @return Zero if the pool is successfully initialized, or -1 if an error occurred.
public_function int
IoCreateRequestPool
(
    IO_REQUEST_POOL *pool, 
    size_t  pool_capacity
)
{
    IO_REQUEST *node_pool = NULL;

    // initialize the fields of the IO_REQUEST_POOL instance.
    ZeroMemory(pool, sizeof(IO_REQUEST_POOL));

    // allocate and initialize the pool nodes and free list.
    if ((node_pool = (IO_REQUEST*) malloc(pool_capacity * sizeof(IO_REQUEST))) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Unable to allocate I/O request pool of %Iu items.\n", __FUNCTION__, GetCurrentThreadId(), pool_capacity);
        return -1;
    }
    ZeroMemory(node_pool,  pool_capacity * sizeof(IO_REQUEST));
    for (size_t i = 0; i < pool_capacity; ++i)
    {
        node_pool[i].NextRequest = pool->FreeRequest;
        pool->FreeRequest = &node_pool[i];
    }
    InitializeCriticalSectionAndSpinCount(&pool->ListLock, 0x1000);
    pool->NodePool = node_pool;
    return 0;
}

/// @summary Free resources allocated to an I/O request pool.
/// @param pool The I/O request pool to delete.
public_function void
IoDeleteRequestPool
(
    IO_REQUEST_POOL *pool
)
{
    if (pool->NodePool != NULL)
    {
        free(pool->NodePool);
        DeleteCriticalSection(&pool->ListLock);
        ZeroMemory(pool, sizeof(IO_REQUEST_POOL));
    }
}

/// @summary Allocate an I/O request from an I/O request pool.
/// @param pool The I/O request pool to allocate from.
/// @return The I/O request, or NULL if no requests are available in the pool.
public_function IO_REQUEST*
IoCreateRequest
(
    IO_REQUEST_POOL *pool
)
{
    IO_REQUEST *node = NULL;
    EnterCriticalSection(&pool->ListLock);
    {
        if ((node = pool->FreeRequest) != NULL)
        {   // pop a node from the head of the free list.
            pool->FreeRequest = pool->FreeRequest->NextRequest;
            // insert the node at the head of the live list.
            node->NextRequest = pool->LiveRequest;
            node->PrevRequest = NULL;
            node->RequestPool = pool;
            if (pool->LiveRequest != NULL)
                pool->LiveRequest->PrevRequest = node;
            pool->LiveRequest  = node;
            node->RequestState = IO_REQUEST_STATE_CHAINED;
            node->QueueSpan    = NULL;
            node->ExecuteSpan  = NULL;
        }
    }
    LeaveCriticalSection(&pool->ListLock);
    return node;
}

/// @summary Submit an I/O request for asynchronous execution.
/// @param io_pool The I/O thread pool that will execute the background /I/O request.
/// @param request The I/O request to submit.
/// @return true if the request is successfully submitted.
public_function bool
IoSubmitRequest
(
    IO_THREAD_POOL *io_pool, 
    IO_REQUEST     *request
)
{
    //ConsoleError("BGIO: %S(%u): Request %p(%d) transitioned to SUBMITTED.\n", __FUNCTION__, GetCurrentThreadId(), request, request->RequestType);
    request->RequestState = IO_REQUEST_STATE_SUBMITTED;
    IoThreadSpanEnter(io_pool, request->QueueSpan, "Submit request %p to pool %p (type = %d, %I64d bytes)", request, io_pool, request->RequestType, request->DataAmount);
    if (!PostQueuedCompletionStatus(io_pool->CompletionPort, 0, 0, &request->Overlapped))
    {
        ConsoleError("ERROR: %S(%u): Failed to submit request %p from pool %p (%08X).\n", __FUNCTION__, GetCurrentThreadId(), request, request->RequestPool, GetLastError());
        return false;
    }
    return true;
}

