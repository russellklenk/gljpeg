/*/////////////////////////////////////////////////////////////////////////////
/// @summary Implement various routines related to file I/O, including loading
/// a file from disk all at once, using memory-mapped I/O to access portions 
/// of a file, and enumerating all of the files in a directory.
///////////////////////////////////////////////////////////////////////////80*/

/*//////////////////
//   Data Types   //
//////////////////*/
/// @summary Define the data returned for each file during directory enumeration.
struct FILE_INFO
{
    WCHAR              *Path;                       /// The absolute path of the file.
    int64_t             FileSize;                   /// The size of the file, in bytes, at the time the directory enumeration was performed.
    FILETIME            LastWrite;                  /// The last write time of the file.
    DWORD               Attributes;                 /// The file attributes, as returned by GetFileAttributes().
    uint32_t            SortKey;                    /// The sort key, reconstructed from the file extension.
};

/// @summary Define the container type for a list of files returned during directory enumeration.
typedef std::vector<FILE_INFO> FILE_LIST;

/// @summary Define the data maintained with a memory-mapped file opened for read access.
struct FILE_MAPPING
{
    HANDLE              Filedes;                    /// A valid file handle, or INVALID_HANDLE_VALUE.
    HANDLE              Filemap;                    /// A valid file mapping handle, or NULL.
    int64_t             FileSize;                   /// The size of the file, in bytes, at the time it was opened.
    size_t              Granularity;                /// The system allocation granularity, in bytes.
};

/// @summary Define the data associated with a region of a file loaded or mapped into memory.
struct FILE_DATA
{
    uint8_t            *Buffer;                     /// The buffer containing the loaded file data.
    void               *MapPtr;                     /// The address returned by MapViewOfFile.
    int64_t             Offset;                     /// The offset of the data in this region from the start of the file, in bytes.
    int64_t             DataSize;                   /// The number of bytes in Buffer that are valid.
    uint32_t            Flags;                      /// One or more of FILE_DATA_FLAGS describing the allocation attributes of the FILE_DATA.
};

/// @summary Define the data returned from an asynchronous I/O request.
struct IO_RESULT
{
    int                 RequestType;                /// One of IO_REQUEST_TYPE specifying the type of operation that completed.
    uint32_t            ResultCode;                 /// ERROR_SUCCESS or another result code indicating whether the operation completed successfully.
    uintptr_t           UserContext;                /// Opaque data associated with the request by the application.
    HANDLE              FileHandle;                 /// The handle of the file associated with the I/O request. This value may be INVALID_HANDLE_VALUE.
    WCHAR              *PathBuffer;                 /// The path of the file associated with the I/O request. This value may be NULL.
    void               *DataBuffer;                 /// The source or destination caller-managed buffer. This value may be NULL.
    int64_t             DataAmount;                 /// The number of bytes read from or written to the data buffer.
    int64_t             BaseOffset;                 /// The byte offset of the start of the operation from the start of the physical file.
    int64_t             FileOffset;                 /// The byte offset of the start of the operation from the start of the logical file.
};

/// @summary Define the signature for the callback invoked when an asynchronous I/O operation has completed.
/// @param result An IO_RESULT used to return data to the caller.
/// @param was_successful Set to true if the operation completed successfully.
typedef void (*IO_REQUEST_COMPLETE)(IO_RESULT result, bool was_successful);

/// @summary Define the data associated with an asynchronous I/O request, as submitted by the application.
/// The background I/O system uses a different format to represent the data.
struct IO_REQUEST
{
    int                 RequestType;                /// One of IO_REQUEST_TYPE specifying the type of operation to perform.
    uint32_t            IoHintFlags;                /// One or more of IO_HINT_FLAGS, specifying hints that may be used to optimize the I/O operation.
    uintptr_t           UserContext;                /// Opaque data associated with the request to be passed through to the completion callback.
    HANDLE              FileHandle;                 /// The handle of the file associated with the READ, WRITE or FLUSH request.
    WCHAR              *PathBuffer;                 /// Pointer to a caller-managed buffer specifying the path of the file to OPEN, LOAD or SAVE.
    void               *DataBuffer;                 /// The caller-managed buffer from which to READ/LOAD or WRITE/SAVE data, or NULL for NOOP, OPEN and FLUSH requests.
    int64_t             DataAmount;                 /// The number of bytes to transfer to or from the caller-managed data buffer.
    int64_t             BaseOffset;                 /// The byte offset of the start of the operation from the start of the physical file.
    int64_t             FileOffset;                 /// The byte offset of the start of the operation from the start of the logical file.
    uint64_t            SubmitTime;                 /// The timestamp (in ticks) at which the request was submitted by the application.
    IO_REQUEST_COMPLETE IoComplete;                 /// The callback to invoke when the operation has completed.
};

/// @summary Define the data representing an active request within the background I/O system.
/// The IO_REQUEST_NODE is also associated with the file handle, and passed back as the completion key.
struct IO_REQUEST_NODE
{
    IO_REQUEST_NODE    *NextRequest;                /// Pointer to the next node in the list, or NULL if this is the tail node.
    IO_REQUEST_NODE    *PrevRequest;                /// Pointer to the previous node in the list, or NULL if this is the head node.
    int                 RequestType;                /// One of IO_REQUEST_TYPE specifying the type of operation being performed.
    int                 RequestState;               /// An integer value indicating the current state of the request.
    HANDLE              FileHandle;                 /// The file handle associated with the request.
    OVERLAPPED          Overlapped;                 /// The OVERLAPPED instance associated with the asynchronous request.
    WCHAR              *PathBuffer;                 /// Pointer to a caller-managed buffer specifying the path of the file to LOAD or SAVE.
    void               *DataBuffer;                 /// The caller-managed buffer from which to READ/LOAD or WRITE/SAVE data.
    int64_t             DataAmount;                 /// The number of bytes to transfer to or from the caller-managed data buffer.
    int64_t             BaseOffset;                 /// The byte offset of the start of the operation from the start of the physical file.
    int64_t             FileOffset;                 /// The byte offset of the start of the operation from the start of the logical file.
    uintptr_t           UserContext;                /// Opaque data associated with the request to be passed through to the completion callback.
    IO_REQUEST_COMPLETE CompletionCallback;         /// The callback to invoke when the operation has completed. May be NULL.
    uint64_t            SubmitTime;                 /// The timestamp (in ticks) at which the request was submitted by the application.
    uint64_t            LaunchTime;                 /// The timestamp (in ticks) at which the request was launched by the background I/O thread.
    uint64_t            FinishTime;                 /// The timestamp (in ticks) at which the request was completed.
};

/// @summary Define the data associated with a doubly-linked list of IO_REQUEST_NODE.
/// The list has fixed capacity; nodes are allocated from and returned to a free list.
struct IO_REQUEST_LIST
{
    size_t              Count;                      /// The number of requests in the list.
    size_t              Capacity;                   /// The list capacity, in nodes/requests.
    IO_REQUEST_NODE    *HeadNode;                   /// Pointer to the node at the front of the list, or NULL if the list is empty.
    IO_REQUEST_NODE    *FreeList;                   /// Pointer to the node at the front of the free list, or NULL if the list is empty.
    IO_REQUEST_NODE    *NodePool;                   /// The pool of requests. This array has size IO_REQUEST_LIST::Capacity.
};

/// @summary Define the data associated with an I/O request queue. The I/O request queue is used to capture asynchronous I/O requests from the application.
/// The 'queue' is double-buffered. The application threads write to one buffer while the background I/O thread reads from the other.
struct IO_REQUEST_QUEUE
{
    size_t              Count;                      /// The number of items in the active write buffer.
    size_t              Capacity;                   /// The maximum capacity of the active write buffer.
    HANDLE              CompletionPort;             /// The I/O completion port to signal when a request is written to the queue.
    size_t              WriteBuffer;                /// The zero-based index of the current write buffer (either 0 or 1).
    IO_REQUEST         *ContiguousBuffer;           /// The contiguous I/O request buffer from which all other buffers are sub-allocated.
    IO_REQUEST         *RequestBuffers[2];          /// Two buffers, each with allocated capacity Capacity requests.
    CRITICAL_SECTION    QueueWriteLock;             /// The critical section used to synchronize access to the write buffer.
    char                Padding[32];                /// Pad queue data out to a cacheline boundary.
};

/// @summary Define the data associated with active asynchronous I/O operations. I/O completion events and application event notifications are posted to an I/O completion port.
/// Aside from the completion port, all data is intended to be accessed from a single thread only (the I/O thread.)
/// Only I/O requests that can actually be executed asynchronously by the kernel are tracked in the ActiveRequests list.
struct IO_ASYNC_STATE
{
    size_t              MaxCompletions;             /// The maximum number of completion or notification events that can be received at any one time.
    HANDLE              CompletionPort;             /// The I/O completion port used to receive asynchronous I/O completion notifications.
    OVERLAPPED_ENTRY   *CompletionBuffer;           /// The array of OVERLAPPED_ENTRY values representing events posted to the I/O completion port.
    size_t              RequestCount;               /// The number of I/O requests waiting in the request buffer.
    size_t              RequestIndex;               /// The zero-based index of the next I/O request to process from the request buffer.
    IO_REQUEST         *RequestBuffer;              /// The buffered I/O requests to process. This is a pointer to a buffer from the IO_REQUEST_QUEUE.
    IO_REQUEST_LIST     ActiveRequests;             /// The list of active I/O requests. This list has a fixed capacity.
    uint32_t            IoStateFlags;               /// One or more of IO_ASYNC_STATE_FLAGS.
};

/// @summary Define the data passed to the background I/O thread.
struct IO_BACKGROUND_THREAD_ARGS
{
    IO_ASYNC_STATE     *AIOState;                   /// The asynchronous I/O state data used to track in-flight asynchronous I/O requests submitted to the kernel.
    IO_REQUEST_QUEUE   *AIOQueue;                   /// The I/O request queue used to submit requests to the I/O thread from the application.
};

/// @summary Define the data maintained by a background I/O thread.
struct IO_BACKGROUND_THREAD
{
    HANDLE              ThreadHandle;               /// The operating system thread handle, which can be used to wait for the thread to exit.
    unsigned int        ThreadId;                   /// The operating system thread identifier.
    uint32_t            DefaultHints_Open;          /// The application's default IO_HINT_FLAGS to be applied for OPEN requests.
    uint32_t            DefaultHints_Load;          /// The application's default IO_HINT_FLAGS to be applied for LOAD requests.
    uint32_t            DefaultHints_Save;          /// The application's default IO_HINT_FLAGS to be applied for SAVE requests.
    IO_ASYNC_STATE      AIOState;                   /// The data used to track in-flight asynchronous I/O requests.
    IO_REQUEST_QUEUE    AIOQueue;                   /// The queue used to submit I/O requests to the background I/O thread.
};

/// @summary Define the parameters that can be set by the application to configure background I/O behavior.
struct IO_BACKGROUND_THREAD_INIT
{
    size_t              MaxRequestsQueued;          /// The maximum number of I/O requests that can be queued by the application.
    size_t              MaxRequestsActive;          /// The maximum number of I/O operations that can be submitted to the kernel, but not completed, at any given time.
    size_t              EventDequeueCount;          /// The maximum number of I/O completion events to receive at once.
    uint32_t            DefaultHints_Open;          /// The application's default IO_HINT_FLAGS to be applied for OPEN requests.
    uint32_t            DefaultHints_Load;          /// The application's default IO_HINT_FLAGS to be applied for LOAD requests.
    uint32_t            DefaultHints_Save;          /// The application's default IO_HINT_FLAGS to be applied for SAVE requests.
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
    IO_REQUEST_LOAD_FILE           =  6,            /// Asynchronously open a file, read its entire contents into a caller-managed buffer, and close the file.
    IO_REQUEST_SAVE_FILE           =  7,            /// Asynchronously open a file, write the contents of a caller-managed buffer to the file, and close the file.
};

/// @summary Define the states that an asynchronous I/O request may have.
enum IO_REQUEST_STATE : int
{
    IO_REQUEST_STATE_RECEIVED      =  0,            /// The request has been received, but not yet submitted to the kernel.
    IO_REQUEST_STATE_IN_PROGRESS   =  1,            /// The request is a simple request (READ, WRITE) and has been submitted to the kernel.
    IO_REQUEST_STATE_COMPLETED     =  2,            /// The request has completed successfully.
    IO_REQUEST_STATE_ERROR         =  3,            /// The request has completed, and encountered an error.
    // IO_REQUEST_STATE_LOAD_xxx
};

/// @summary Define a name for the default number of threads that can submit asynchronous I/O requests.
enum IO_SUBMIT_THREAD_COUNT : size_t
{
    IO_SUBMIT_THREAD_COUNT_DEFAULT =  0,            /// Asynchronous I/O operations may be submitted from as many threads as there are logical processors in the system.
};

/// @summary Define the status flags for an asynchronous I/O state object. These flags define pending operations.
enum IO_ASYNC_STATE_FLAGS : uint32_t
{
    IO_ASYNC_STATE_FLAGS_NONE      = (0 << 0),      /// There are no pending operations.
    IO_ASYNC_STATE_FLAG_SWAP       = (1 << 0),      /// Indicates that IO_REQUEST_QUEUE buffers should be swapped when all events from the current buffer have been consumed.
    IO_ASYNC_STATE_FLAG_SHUTDOWN   = (1 << 1),      /// Indicates that the I/O thread should stop processing I/O requests.
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

/// @summary Define various allocation attributes of a file region.
enum FILE_DATA_FLAGS : uint32_t
{
    FILE_DATA_FLAGS_NONE           = (0 << 0),      /// The FILE_DATA is invalid.
    FILE_DATA_FLAG_COMMITTED       = (1 << 0),      /// The FILE_DATA buffer is an explicitly allocated region of memory.
    FILE_DATA_FLAG_MAPPED_REGION   = (1 << 1),      /// The FILE_DATA represents a mapped region of a file.
};

/*///////////////
//   Globals   //
///////////////*/
/// @summary OVERLAPPED_ENTRY::lpCompletionKey is set to AIO_COMPLETION_KEY_WAKEUP0 to indicate that one or more requests are waiting in request buffer 0.
global_variable ULONG_PTR const AIO_COMPLETION_KEY_WAKEUP0  = 0;

/// @summary OVERLAPPED_ENTRY::lpCompletionKey is set to AIO_COMPLETION_KEY_WAKEUP1 to indicate that one or more requests are waiting in request buffer 1.
global_variable ULONG_PTR const AIO_COMPLETION_KEY_WAKEUP1  = 1;

/// @summary OVERLAPPED_ENTRY::lpCompletionKey is set to AIO_COMPLETION_KEY_SHUTDOWN to terminate the asynchronous I/O thread loop.
global_variable ULONG_PTR const AIO_COMPLETION_KEY_SHUTDOWN = 2;

/// @summary OVERLAPPED_ENTRY::lpCompletionKey that is guaranteed to be an 'invalid' completion key.
global_variable ULONG_PTR const AIO_COMPLETION_KEY_UNUSED   =~ULONG_PTR(0);

/*//////////////////////////
//   Internal Functions   //
//////////////////////////*/
/// @summary Helper function to convert a UTF-8 encoded string to the system native WCHAR. Free the returned buffer using the standard C library free() call.
/// @param str The NULL-terminated UTF-8 string to convert.
/// @param size_chars On return, stores the length of the string in characters, not including NULL-terminator.
/// @param size_bytes On return, stores the length of the string in bytes, including the NULL-terminator.
/// @return The WCHAR string buffer, or NULL if the string could not be converted.
internal_function WCHAR*
Utf8toUtf16
(
    char const    *str, 
    size_t &size_chars, 
    size_t &size_bytes
)
{   // figure out how much memory needs to be allocated, including NULL terminator.
    int nchars = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, str, -1, NULL, 0);
    if (nchars == 0)
    {   // the path cannot be converted from UTF-8 to UTF-16.
        size_chars = 0;
        size_bytes = 0;
        return NULL;
    }
    // store output values for the caller.
    size_chars = nchars - 1;
    size_bytes = nchars * sizeof(WCHAR);
    // allocate buffer space for the wide character string.
    WCHAR *pathbuf = NULL;
    if   ((pathbuf = (WCHAR*) malloc(size_bytes)) == NULL)
    {   // unable to allocate temporary memory for UTF-16 path.
        ConsoleError("ERROR: %S(%u): Memory allocation for %Iu bytes failed.\n", __FUNCTION__, GetCurrentThreadId(), size_bytes);
        return NULL;
    }
    if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, str, -1, pathbuf, nchars) == 0)
    {   // the path cannot be converted from UTF-8 to UTF-16.
        ConsoleError("ERROR: %S(%u): Cannot convert from UTF8 to UTF16 (%08X).\n", __FUNCTION__, GetCurrentThreadId(), GetLastError());
        free(pathbuf);
        return NULL;
    }
    return pathbuf;
}

/// @summary Retrieve the fully-resolved absolute path for a file or directory. Free the returned buffer using the standard C library free() call.
/// @param handle The handle of the opened file or directory.
/// @param size_chars On return, stores the length of the string in characters, not including NULL-terminator.
/// @param size_bytes On return, stores the length of the string in bytes, including the NULL-terminator.
/// @return The WCHAR string buffer, or NULL if the string could not be converted.
internal_function WCHAR*
ResolvePathForHandle
(
    HANDLE      handle, 
    size_t &size_chars, 
    size_t &size_bytes
)
{
    WCHAR *pathbuf = NULL;
    DWORD    flags = VOLUME_NAME_DOS | FILE_NAME_NORMALIZED;
    DWORD  pathlen = GetFinalPathNameByHandleW(handle, NULL, 0, flags);
    // GetFinalPathNameByHandle returns the buffer length, in TCHARs, including zero terminator.
    if (pathlen == 0)
    {
        ConsoleError("ERROR: %S(%u): Cannot retrieve path for handle %p (%08X).\n", __FUNCTION__, GetCurrentThreadId(), handle, GetLastError());
        size_chars = 0;
        size_bytes = 0;
        return NULL;
    }
    if ((pathbuf = (WCHAR*) malloc(pathlen * sizeof(WCHAR))) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Memory allocation for %Iu bytes failed.\n", __FUNCTION__, GetCurrentThreadId(), pathlen * sizeof(WCHAR));
        size_chars = pathlen - 1;
        size_bytes = pathlen * sizeof(WCHAR);
        return NULL;
    }
    // GetFinalPathNameByHandle returns the number of TCHARs written, not including zero terminator.
    if (GetFinalPathNameByHandleW(handle, pathbuf, pathlen-1, flags) != (pathlen-1))
    {
        ConsoleError("ERROR: %S(%u): Cannot retrieve path for handle %p (%08X).\n", __FUNCTION__, GetCurrentThreadId(), handle, GetLastError());
        size_chars = pathlen - 1;
        size_bytes = pathlen * sizeof(WCHAR);
        free(pathbuf);
        return NULL;
    }
    pathbuf[pathlen - 1] = 0;
    size_chars = pathlen - 1;
    size_bytes = pathlen * sizeof(WCHAR);
    return pathbuf;
}

/// @summary Append a path fragment (subdirectory or filename) to the end of a path string.
/// @param pathend Pointer to the zero-terminator character of the existing path string.
/// @param append The zero-terminated string to append.
/// @return A pointer to the zero terminator of the path string, after appending the fragment.
internal_function WCHAR*
AppendPathFragment
(
    WCHAR      *pathend, 
    WCHAR const *append
)
{   // ASSUME: the buffer into which pathend points contains a valid, normalized path.
    // ASSUME: the buffer into which pathend points is large enough.
    if (append != NULL && *append != 0)
    {   // ensure that the path string has a trailing slash.
        if (*(pathend - 1) != L'\\')
        {   // overwrite the zero-terminator with a trailing slash.
            *pathend++ = L'\\';
        }
        // append the new bit to the end of the path buffer.
        while (*append)
        {
            *pathend++ = *append++;
        }
        // apply the new zero-terminator character.
        *pathend = 0;
    }
    return pathend;
}

/// @summary Extract a sort key from a filename. The filename is assumed to have a numeric component, such as FILENAME.###.
/// @param filename Pointer to the start of a zero-terminated filename string.
/// @return An integer sort key corresponding to the decimal integer equivalent of the numeric component of the file extension.
internal_function uint32_t
ExtractSortKey
(
    WCHAR const *filename
)
{
    uint32_t     key = 0;
    WCHAR const *ext = filename;
    do
    {   // search for the first occurrence of a '.' extension separator.
        if (*ext == L'.')
        {   // ext will point at the first character of the extension component.
            ext++;
            break;
        }
    } while (*ext++);

    for ( ; ; )
    {   // convert the first part of the extension to a decimal integer.
        WCHAR ch = *ext++;
        if ((ch < L'0') || (ch > L'9'))
            break;
        key = (key * 10) + (ch - L'0');
    }
    return key;
}

/// @summary Called recursively to enumerate all files in a directory and its subdirectories.
/// @param files The list of files to populate.
/// @param pathbuf Pointer to the start of a zero-terminated string specifying the absolute path of the directory to search.
/// @param pathend Pointer to the zero-terminator character of the pathbuf.
/// @param recurse Specify true to recurse into subdirectories.
/// @return Zero if enumeration is successful or -1 if an error occurred.
internal_function int
EnumerateDirectoryFiles
(
    FILE_LIST &files, 
    WCHAR   *pathbuf, 
    WCHAR   *pathend, 
    bool     recurse
)
{
    HANDLE        find = INVALID_HANDLE_VALUE;
    WCHAR    *base_end = pathend;
    WIN32_FIND_DATA fd;

    if (recurse)
    {   // look at directories only with a filter string of '*'.
        pathend = AppendPathFragment(pathend, L"*");
        // start loading directory metadata.
        if ((find = FindFirstFileEx(pathbuf, FindExInfoBasic, &fd, FindExSearchNameMatch, NULL, FIND_FIRST_EX_LARGE_FETCH)) == INVALID_HANDLE_VALUE)
        {
            ConsoleError("ERROR: %S(%u): Unable to enumerate subdirectories for \"%s\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), pathbuf, GetLastError());
            return -1;
        }
        do
        {
            if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
            {
                if ((fd.cFileName[0] == L'.' && fd.cFileName[1] == 0) || (fd.cFileName[0] == L'.' && fd.cFileName[1] == L'.' && fd.cFileName[2] == 0))
                {   // skip current and parent directory links.
                    continue;
                }
                else
                {   // build the path string for the subdirectory and recurse into it.
                    pathend = AppendPathFragment(base_end, fd.cFileName);
                    if (EnumerateDirectoryFiles(files, pathbuf, pathend, recurse) < 0)
                    {
                        FindClose(find);
                        return -1;
                    }
                }
            }
            // else, skip this entry - it's not a directory.
        } while (FindNextFile(find, &fd));
        FindClose(find); find = INVALID_HANDLE_VALUE;
    }

    // search in this directory only, considering files only.
    pathend = AppendPathFragment(base_end, L"*");
    if ((find = FindFirstFileEx(pathbuf, FindExInfoBasic, &fd, FindExSearchNameMatch, NULL, FIND_FIRST_EX_LARGE_FETCH)) == INVALID_HANDLE_VALUE)
    {
        ConsoleError("ERROR: %S(%u): Unable to enumerate files for \"%s\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), pathbuf, GetLastError());
        return -1;
    }
    do
    {   // skip over anything that's not a proper file or symlink.
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
            continue;
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_DEVICE)
            continue;
        if (fd.dwFileAttributes & FILE_ATTRIBUTE_VIRTUAL)
            continue;

        // build the absolute path of the file.
        pathend = AppendPathFragment(base_end, fd.cFileName);
        FILE_INFO info = {};
        if ((info.Path =(WCHAR*) malloc(((pathend - pathbuf) + 1) * sizeof(WCHAR))) == NULL)
        {
            ConsoleError("ERROR: %S(%u): Unable to allocate path memory for file.\n", __FUNCTION__, GetCurrentThreadId());
            FindClose(find);
            return -1;
        }
        CopyMemory(info.Path, pathbuf, (base_end - pathbuf) * sizeof(WCHAR));
        AppendPathFragment(info.Path + (base_end - pathbuf), fd.cFileName);
        info.FileSize   = (((int64_t) fd.nFileSizeHigh) << 32) | ((int64_t) fd.nFileSizeLow);
        info.LastWrite  = fd.ftLastWriteTime;
        info.Attributes = fd.dwFileAttributes;
        info.SortKey    = ExtractSortKey(fd.cFileName);
        files.push_back(info);
    } while (FindNextFile(find, &fd));
    FindClose(find);
    return 0;
}

/// @summary Retrieve a timestamp value from the high-resolution clock.
/// @return A 64-bit integer specifying the timestamp value, in ticks.
internal_function inline uint64_t
GetIoTimestamp
(
    void
)
{
    LARGE_INTEGER ticks;
    QueryPerformanceCounter(&ticks);
    return (uint64_t) ticks.QuadPart;
}

/// @summary Classify a GetLastError() code returned from an I/O operation as successful or not successful.
/// @param err The code returned by GetLastError(), or returned with the completed OVERLAPPED_ENTRY.
/// @return true if the I/O operation completed successfully, or false otherwise.
internal_function bool
ClassifyIoResult
(
    DWORD err
)
{
    if (err == ERROR_SUCCESS || 
        err == ERROR_HANDLE_EOF)
    {   // obviously the operation was successful.
        return true;
    }
    else
    {   // unknown result code - assume failure.
        return false;
    }
}

/// @summary Complete an asynchronous I/O request.
/// @param request The request being completed.
/// @param file_handle The handle of the file associated with the request.
/// @param bytes_transferred The number of bytes transferred to or from the request data buffer.
/// @param result_code ERROR_SUCCESS or another operating system result code indicating the result of the operation.
/// @param was_successful true if the I/O request completed successfully.
internal_function void
CompleteIoRequest
(
    IO_REQUEST_NODE const *request, 
    int64_t      bytes_transferred, 
    uint32_t           result_code
)
{
    if (request->CompletionCallback != NULL)
    {
        IO_RESULT  res;
        res.RequestType = request->RequestType;
        res.ResultCode  = result_code;
        res.UserContext = request->UserContext;
        res.FileHandle  = request->FileHandle;
        res.PathBuffer  = request->PathBuffer;
        res.DataBuffer  = request->DataBuffer;
        res.DataAmount  = bytes_transferred;
        res.BaseOffset  = request->BaseOffset;
        res.FileOffset  = request->FileOffset;
        request->CompletionCallback(res, ClassifyIoResult(result_code));
    }
}

/// @summary Complete a no-op or pass-through I/O request.
/// @param request The request being completed.
internal_function inline void
CompleteIoRequest_Noop
(
    IO_REQUEST const *request
)
{
    if (request->CompletionCallback != NULL)
    {
        IO_RESULT  res;
        res.RequestType = IO_REQUEST_NOOP;
        res.ResultCode  = ERROR_SUCCESS;
        res.UserContext = request->UserContext;
        res.FileHandle  = INVALID_HANDLE_VALUE;
        res.PathBuffer  = request->PathBuffer;
        res.DataBuffer  = request->DataBuffer;
        res.DataAmount  = 0;
        res.BaseOffset  = request->BaseOffset;
        res.FileOffset  = request->FileOffset;
        request->CompletionCallback(res, true);
    }
}

/// @summary Allocate and initialize an I/O request queue. The queue is safe for multiple writers (though contention will cause a writer to spin or possibly block.)
/// @param ioq The IO_REQUEST_QUEUE to initialize.
/// @param max_requests The maximum number of I/O requests that can be queued by the application.
/// @param completion_port The I/O completion port to signal when one or more requests are available in the write queue.
/// @param spin_count The spin count to use for the queue writer critical section.
/// @return Zero if the request queue is successfully initialized, or -1 if an error occurred.
internal_function int
CreateIoRequestQueue
(
    IO_REQUEST_QUEUE  *ioq, 
    size_t    max_requests,
    HANDLE completion_port, 
    uint32_t    spin_count=4096
)
{
    IO_REQUEST *contiguous_io_buffer = NULL;
    size_t            io_buffer_size = max_requests * sizeof(IO_REQUEST) * 2;

    // initialize the fields of the IO_REQUEST_QUEUE structure.
    ZeroMemory(ioq, sizeof(IO_REQUEST_QUEUE));

    // allocate a large buffer for I/O requests. this buffer will be split into 
    // IOBUFS_COUNT equally-sized pieces for various uses and simplifies cleanup code.
    if ((contiguous_io_buffer = (IO_REQUEST*) malloc(io_buffer_size)) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Failed to allocate %Iu bytes for I/O request data.\n", __FUNCTION__, GetCurrentThreadId(), io_buffer_size);
        return -1;
    }

    // initialize the fields of the request queue structure now that all storage is allocated.
    ioq->Count             = 0;
    ioq->Capacity          = max_requests;
    ioq->CompletionPort    = completion_port;
    ioq->WriteBuffer       = 0;
    ioq->ContiguousBuffer  = contiguous_io_buffer;
    ioq->RequestBuffers[0] =&contiguous_io_buffer[max_requests * 0];
    ioq->RequestBuffers[1] =&contiguous_io_buffer[max_requests * 1];
    InitializeCriticalSectionAndSpinCount(&ioq->QueueWriteLock , spin_count);
    return 0;
}

/// @summary Attempt to enqueue a single I/O request to the asynchronous I/O thread.
/// @param ioq The IO_REQUEST_QUEUE to which the request will be posted.
/// @param req The I/O request to submit.
/// @return The number of I/O requests posted to the queue. This value is zero if the queue is full.
internal_function size_t
EnqueueIoRequest
(
    IO_REQUEST_QUEUE *ioq, 
    IO_REQUEST const &req
)
{
    ULONG_PTR completion_key = 0;
    size_t    enqueue_count  = 0;
    EnterCriticalSection(&ioq->QueueWriteLock);
    {
        if (ioq->Count != ioq->Capacity)
        {   // there's sufficient space in the buffer; enqueue the item.
            ioq->RequestBuffers[ioq->WriteBuffer][ioq->Count++] = req;
            completion_key = AIO_COMPLETION_KEY_WAKEUP0 + ioq->WriteBuffer;
            enqueue_count  = 1;
        }
    }
    LeaveCriticalSection(&ioq->QueueWriteLock);
    
    if (enqueue_count > 0)
    {   // notify any waiter that at least one request is available.
        if (!PostQueuedCompletionStatus(ioq->CompletionPort, 0, completion_key, NULL))
        {   // this isn't a fatal error - hopefully a subsequent notification will succeed.
            ConsoleError("ERROR: %S(%u): Failed to notify async I/O thread about waiting request (%08X).\n", __FUNCTION__, GetCurrentThreadId(), GetLastError());
        }
    }
    return enqueue_count;
}

/// @summary Attempt to enqueue one or more I/O requests to the asynchronous I/O thread.
/// @param ioq The IO_REQUEST_QUEUE to which the request will be posted.
/// @param request_list The I/O request(s) to submit.
/// @param request_count The number of I/O request(s) to submit.
/// @return The number of I/O requests posted to the queue. This value is zero if the queue is full.
internal_function size_t
EnqueueIoRequest
(
    IO_REQUEST_QUEUE          *ioq, 
    IO_REQUEST const *request_list, 
    size_t     const  request_count
)
{
    ULONG_PTR completion_key = 0;
    size_t     enqueue_avail = 0;
    size_t     enqueue_count = 0;
    EnterCriticalSection(&ioq->QueueWriteLock);
    {   // figure out how many items can be enqueued and copy them into the write buffer.
        enqueue_avail  = ioq->Capacity -  ioq->Count;
        enqueue_count  = request_count <= enqueue_avail ? request_count : enqueue_avail;
        completion_key = AIO_COMPLETION_KEY_WAKEUP0 + ioq->WriteBuffer;
        CopyMemory(&ioq->RequestBuffers[ioq->WriteBuffer][ioq->Count], request_list, enqueue_count * sizeof(IO_REQUEST));
        ioq->Count    += enqueue_count;
    }
    LeaveCriticalSection(&ioq->QueueWriteLock);

    if (enqueue_count > 0)
    {   // notify any waiter that at least one request is available.
        if (!PostQueuedCompletionStatus(ioq->CompletionPort, 0, completion_key, NULL))
        {   // this isn't a fatal error - hopefully a subsequent notification will succeed.
            ConsoleError("ERROR: %S(%u): Failed to notify async I/O thread about waiting request(s) (%08X).\n", __FUNCTION__, GetCurrentThreadId(), GetLastError());
        }
    }
    return enqueue_count;
}

/// @summary Retrieve waiting I/O requests and swap the read and write buffers. This function should be called from the I/O thread only.
/// @param ioq The IO_REQUEST_QUEUE to poll.
/// @param waiting_count On return, the number of requests waiting in the returned buffer is stored here.
/// @return The buffer containing I/O requests to read.
internal_function IO_REQUEST*
RetrieveWaitingIoRequests
(
    IO_REQUEST_QUEUE *ioq, 
    size_t &waiting_count
)
{
    IO_REQUEST  *rdbuf = NULL;
    EnterCriticalSection(&ioq->QueueWriteLock);
    {   // retrieve the number of waiting items, and swap buffers.
        rdbuf = ioq->RequestBuffers[ioq->WriteBuffer];
        ioq->WriteBuffer = 1 - ioq->WriteBuffer;
        waiting_count = ioq->Count;
        ioq->Count = 0;
    }
    LeaveCriticalSection(&ioq->QueueWriteLock);
    return rdbuf;
}

/// @summary Allocate storage and initialize an I/O request list used to track active asynchronous I/O requests.
/// @param request_list The IO_REQUEST_LIST to initialize.
/// @param max_requests The maximum number of asynchronous I/O requests active at any one time.
/// @return Zero if the I/O request list is successfully initialized, or -1 if an error occurred.
internal_function int
CreateIoRequestList
(
    IO_REQUEST_LIST *request_list, 
    size_t const     max_requests
)
{
    IO_REQUEST_NODE *pool = NULL;

    // initialize the fields of the IO_REQUEST_LIST.
    ZeroMemory(request_list, sizeof(IO_REQUEST_LIST));

    // allocate the pool of I/O request nodes.
    if ((pool = (IO_REQUEST_NODE*) malloc(max_requests * sizeof(IO_REQUEST_NODE))) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Unable to allocate memory for I/O request list. Consider reducing max_requests (%Iu).\n", __FUNCTION__, GetCurrentThreadId(), max_requests);
        return -1;
    }
    ZeroMemory(pool, max_requests * sizeof(IO_REQUEST_NODE));

    // initialize the fields of the IO_REQUEST_LIST.
    request_list->Capacity = max_requests;
    request_list->HeadNode = NULL;
    request_list->NodePool = pool;

    // push all nodes onto the free list.
    for (size_t i = 0; i < max_requests; ++i)
    {   // the free list is maintained as a singly-linked list.
        pool[i].NextRequest = request_list->FreeList;
        request_list->FreeList = &pool[i];
    }
    return 0;
}

/// @summary Determine the initial state for an I/O request.
/// @param request_type One of IO_REQUEST_TYPE.
/// @return One of IO_REQUEST_STATE.
internal_function int
InitialIoRequestState
(
    int request_type
)
{   // TODO(rlk): something more complex can be done here.
    UNREFERENCED_PARAMETER(request_type);
    return IO_REQUEST_STATE_RECEIVED;
}

/// @summary Allocate and initialize a slot for an asynchronous I/O request. This should not be called for requests that do not have an asynchronous component.
/// @param io The application I/O request parameters.
/// @param request_list The I/O request list used to track active requests.
/// @return The initialize request slot, or NULL if no slots are available.
internal_function IO_REQUEST_NODE*
InitActiveIoRequest
(
    IO_REQUEST const &io,
    IO_REQUEST_LIST *request_list
)
{
    IO_REQUEST_NODE   *node  = request_list->FreeList;
    if (request_list->Count != request_list->Capacity)
    {   // pop a node from the head of the free list; insert at the head of the active list.
        request_list->FreeList = n->NextRequest;
        node->NextRequest = request_list->HeadNode;
        if (request_list->HeadNode != NULL)
        {
            request_list->HeadNode->PrevRequest = node;
        }
        request_list->HeadNode = node;
        request_list->Count++;

        // initialize the node with data from the user request.
        node->PrevRequest  = NULL;
        node->RequestType  = io.RequestType;
        node->RequestState = InitialIoRequestState(io.RequestType);
        node->FileHandle   = io.FileHandle;
        ZeroMemory(&node->Overlapped, sizeof(OVERLAPPED));

        node->PathBuffer  = io.PathBuffer;
        node->DataBuffer  = io.DataBuffer;
        node->BaseOffset  = io.BaseOffset;
        node->FileOffset  = io.FileOffset;
        node->UserContext = io.UserContext;
        node->CompletionCallback = io.IoComplete;
        node->SubmitTime  = io.SubmitTime;
        node->LaunchTime  = GetIoTimestamp();
        node->FinishTime  = node->LaunchTime;
    }
    return node;
}

/// @summary Retrieve the IO_REQUEST_NODE for an OVERLAPPED address associated with an active request slot.
/// @param overlapped The OVERLAPPED instance corresponding to a completed request.
/// @return The associated IO_REQUEST_NODE.
internal_function inline IO_REQUEST_NODE*
ActiveIoRequestForOVERLAPPED
(
    OVERLAPPED *overlapped
)
{
    return ((IO_REQUEST_NODE*)(((uint8_t*) overlapped) - offsetof(IO_REQUEST_NODE, Overlapped)));
}

/// @summary Complete and retire an active asynchronous I/O request.
/// @param node The IO_REQUEST_NODE corresponding to the completed request.
/// @param request_list The IO_REQUEST_LIST from which the node was allocated.
/// @param bytes_transferred The number of bytes transferred (read or written.)
/// @param result_code The system code indicating the result of the operation.
internal_function void
RetireActiveIoRequest
(
    IO_REQUEST_NODE         *node, 
    IO_REQUEST_LIST *request_list,
    DWORD       bytes_transferred, 
    DWORD             result_code
)
{   // complete the I/O request by invoking the user callback.
    if (node->CompletionCallback != NULL)
    {
        IO_RESULT  res;
        res.RequestType = request->RequestType;
        res.ResultCode  = result_code;
        res.UserContext = request->UserContext;
        res.FileHandle  = request->FileHandle;
        res.PathBuffer  = request->PathBuffer;
        res.DataBuffer  = request->DataBuffer;
        res.DataAmount  = bytes_transferred;
        res.BaseOffset  = request->BaseOffset;
        res.FileOffset  = request->FileOffset;
        request->CompletionCallback(res, ClassifyIoResult(result_code));
    }
    // remove the node from the active list and insert it at the head of the free list.
    if (node->NextRequest != NULL)
        node->NextRequest->PrevRequest = node->PrevRequest;
    if (node->PrevRequest != NULL)
        node->PrevRequest->NextRequest = node->NextRequest;
    node->NextRequest      = request_list->FreeList;
    request_list->FreeList = node;
}

/// @summary Initialize an IO_ASYNC_STATE object.
/// @param aio The IO_ASYNC_STATE object to initialize.
/// @param max_requests The maximum number of requests that may be submitted to the kernel, but uncompleted, at any given time.
/// @param max_completions The maximum number of I/O completions and event notifications to retrive during a single call to GetOverlappedResultEx.
/// @param submit_threads The maximum number of threads that can submit asynchronous I/O requests concurrently.
/// @return Zero if the object is successfully initialized, or -1 if an error occurred.
internal_function int
CreateIoAsyncState
(
    IO_ASYNC_STATE    *aio, 
    size_t    max_requests, 
    size_t max_completions,
    size_t  submit_threads=IO_SUBMIT_THREAD_COUNT_DEFAULT
)
{
    IO_REQUEST_LIST   actreq = {};
    OVERLAPPED_ENTRY *evtbuf = NULL;
    HANDLE              iocp = NULL;

    // initialize the fields of the IO_ASYNC_STATE structure.
    ZeroMemory(aio, sizeof(IO_ASYNC_STATE));

    // create an I/O completion port used to receive asynchronous I/O completion notifications 
    // and signals when I/O requests are posted to the associated IO_REQUEST_QUEUE.
    if ((iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, AIO_COMPLETION_KEY_UNUSED, (DWORD) submit_threads)) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Unable to create asynchronous I/O completion port (%08X).\n", __FUNCTION__, GetCurrentThreadId(), GetLastError());
        goto cleanup_and_fail;
    }
    if ((evtbuf = (OVERLAPPED_ENTRY*) malloc(max_completions * sizeof(OVERLAPPED_ENTRY))) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Unable to allocate I/O completion buffer memory. Consider reducing max_completions (%Iu).\n", __FUNCTION__, GetCurrentThreadId(), max_completions);
        goto cleanup_and_fail;
    }
    if (CreateIoRequestList(&actreq, max_requests) < 0)
    {
        ConsoleError("ERROR: %S(%u): Unable to initialize I/O request list. Consider reducing max_requests (%Iu).\n", __FUNCTION__, GetCurrentThreadId(), max_requests);
        goto cleanup_and_fail;
    }

    // initialize the fields of the IO_ASYNC_STATE structure.
    aio->MaxCompletions     = max_completions;
    aio->CompletionPort     = iocp;
    aio->CompletionBuffer   = evtbuf;
    aio->RequestCount       = 0;
    aio->RequestIndex       = 0;
    aio->RequestBuffer      = NULL;
    aio->ActiveRequests     = actreq;
    aio->IoStateFlags       = IO_ASYNC_STATE_FLAGS_NONE;
    return 0;

cleanup_and_fail:
    // TODO(rlk): clean up actreq.
    if (evtbuf != NULL) free(evtbuf);
    if (iocp   != NULL) CloseHandle(iocp);
    return -1;
}

/*////////////////////////
//   Public Functions   //
////////////////////////*/
/// @summary Enumerate all files in a directory (and possibly its subdirectories.)
/// @param files The file list to populate. New items are appended to the list.
/// @param path The zero-terminated, UTF-8 string specifying the path to search.
/// @param recurse Specify true to also search the subdirectories of the specified path.
/// @return Zero if enumeration is successful or -1 if an error occurred.
public_function int
IoEnumerateDirectoryFiles
(
    FILE_LIST &files, 
    char const *path, 
    bool     recurse
)
{
    size_t const MAXP = 32768;
    WCHAR    *pathbuf = NULL;
    size_t    pathlen = strlen(path) + 1; // +1 include zero terminator
    size_t     nchars = 0;
    HANDLE        dir = INVALID_HANDLE_VALUE;
    DWORD       share = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
    int            nc = 0;

    // sanity check - make sure that the input path does not exceed the maximum long-form path length.
    if (pathlen >= MAXP)
    {
        ConsoleError("ERROR: %S(%u): Input path exceeds maximum path length.\n", __FUNCTION__, GetCurrentThreadId());
        goto cleanup_and_fail;
    }
    // allocate some working memory for the path buffer.
    // the maximum long-form (\\?\-prefixed) path is 32767 characters (+1 for the zero terminator.)
    if ((pathbuf = (WCHAR*) malloc(MAXP * sizeof(WCHAR))) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Unable to allocate working memory for path buffer.\n", __FUNCTION__, GetCurrentThreadId());
        goto cleanup_and_fail;
    }
    // convert the input path from UTF-8 to UTF-16.
    if ((nc = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, (int) pathlen, pathbuf, (int) MAXP)) == 0)
    {
        ConsoleError("ERROR: %S(%u): Unable to convert input path \"%S\" to UTF-16 (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, GetLastError());
        goto cleanup_and_fail;
    }
    // open a handle to the directory so that the absolute path can be retrieved.
    if ((dir = CreateFile(pathbuf, 0, share, NULL, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, NULL)) == INVALID_HANDLE_VALUE)
    {   // this probably happens because the input directory doesn't exist.
        ConsoleError("ERROR: %S(%u): Unable to open directory \"%S\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, GetLastError());
        goto cleanup_and_fail;
    }
    // retrieve the absolute path of the starting directory.
    if ((nchars = GetFinalPathNameByHandleW(dir, pathbuf, MAXP-1, VOLUME_NAME_DOS | FILE_NAME_NORMALIZED)) == 0)
    {
        ConsoleError("ERROR: %S(%u): Unable to retrieve absolute path for directory \"%S\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, GetLastError());
        goto cleanup_and_fail;
    }
    // enumerate all files in the root directory.
    if (EnumerateDirectoryFiles(files, pathbuf, pathbuf + nchars, recurse) < 0)
    {
        ConsoleError("ERROR: %S(%u): File search failed for directory \"%S\".\n", __FUNCTION__, GetCurrentThreadId(), path);
        goto cleanup_and_fail;
    }
    CloseHandle(dir);
    free(pathbuf);
    return 0;

cleanup_and_fail:
    if (dir != INVALID_HANDLE_VALUE) CloseHandle(dir);
    if (pathbuf != NULL) free(pathbuf);
    return -1;
}

/// @summary Free resources allocated for a file list.
/// @param files The file list to free.
public_function void
IoFreeFileList
(
    FILE_LIST &files
)
{
    for (size_t i = 0, n = files.size(); i < n; ++i)
    {
        if (files[i].Path != NULL)
        {
            free(files[i].Path);
            files[i].Path = NULL;
        }
    }
    files.clear();
}

/// @summary Sort a FILE_LIST in ascending order by numeric sort key (derived from the file extension.)
/// @param files The FILE_LIST to sort.
public_function void
IoSortFileList
(
    FILE_LIST &files
)
{
    struct { bool operator()(FILE_INFO const &a, FILE_INFO const &b) const { 
        return (a.SortKey < b.SortKey); 
    } } KEYCMP;
    std::sort(files.begin(), files.end(), KEYCMP);
}

/// @summary Load the entire contents of a file into memory.
/// @param data The FILE_DATA instance to populate.
/// @param path The zero-terminated UTF-16 path of the file to load.
/// @return Zero if the file is loaded successfully, or -1 if an error occurred.
public_function int
IoLoadFileData
(
    FILE_DATA   *data,
    WCHAR const *path
)
{
    LARGE_INTEGER file_size = {};
    HANDLE     fd = INVALID_HANDLE_VALUE;
    void     *buf = NULL;
    size_t     nb = 0;
    int64_t    nr = 0;

    // initialize the fields of the FILE_DATA structure.
    ZeroMemory(data, sizeof(FILE_DATA));

    // open the requested input file, read-only, to be read from start to end.
    if ((fd = CreateFileW(path, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_FLAG_SEQUENTIAL_SCAN, NULL)) == INVALID_HANDLE_VALUE)
    {
        ConsoleError("ERROR: %S(%u): Unable to open input file \"%s\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, GetLastError());
        goto cleanup_and_fail;
    }
    // retrieve the file size, and use that to allocate a buffer for the file data.
    if (!GetFileSizeEx(fd, &file_size))
    {
        ConsoleError("ERROR: %S(%u): Failed to retrieve file size for input file \"%s\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, GetLastError());
        goto cleanup_and_fail;
    }
    if ((nb = (size_t) file_size.QuadPart) == 0 || (buf = malloc(nb)) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Failed to allocate %Iu byte input buffer for file \"%s\".\n", __FUNCTION__, GetCurrentThreadId(), nb, path);
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
            ConsoleError("ERROR: %S(%u): ReadFile failed for input file \"%s\", offset %I64d (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, nr, GetLastError());
            goto cleanup_and_fail;
        }
        else
        {   // the read completed successfully.
            nr += bytes_read;
        }
    }
    // the file was successfully read, so clean up and set the fields on the FILE_DATA.
    CloseHandle(fd);
    data->Buffer   =(uint8_t*) buf;
    data->MapPtr   = NULL;
    data->Offset   = 0;
    data->DataSize = file_size.QuadPart;
    data->Flags    = FILE_DATA_FLAG_COMMITTED;
    return 0;

cleanup_and_fail:
    ZeroMemory(data, sizeof(FILE_DATA));
    if (fd != INVALID_HANDLE_VALUE) CloseHandle(fd);
    if (buf != NULL) free(buf);
    return -1;
}

/// @summary Load the entire contents of a file into memory.
/// @param data The FILE_DATA instance to populate.
/// @param path The zero-terminated UTF-8 path of the file to load.
/// @return Zero if the file is loaded successfully, or -1 if an error occurred.
public_function int
IoLoadFileData
(
    FILE_DATA  *data,
    char const *path
)
{
    WCHAR  *wpath = NULL;
    size_t  pchar = 0;
    size_t  pbyte = 0;
    int       res = -1;

    // convert the path from UTF-8 input to the native system encoding.
    if ((wpath = Utf8toUtf16(path, pchar, pbyte)) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Unable to convert input path \"%S\" to UTF-16.\n", __FUNCTION__, GetCurrentThreadId(), path);
        ZeroMemory(data, sizeof(FILE_DATA));
        return -1;
    }
    res = IoLoadFileData(data, wpath);
    free(wpath);
    return res;
}

/// @summary Open a file for memory-mapped I/O optimized for sequential reads.
/// @param file The FILE_MAPPING object to initialize.
/// @param path The zero-terminated UTF-8 path of the file to open.
/// @return Zero if the file mapping was opened successfully, or -1 if an error occurred.
public_function int
IoOpenFileMapping
(
    FILE_MAPPING *file, 
    char const   *path
)
{
    SYSTEM_INFO    sys_info = {};
    LARGE_INTEGER file_size = {};
    HANDLE     fd = INVALID_HANDLE_VALUE;
    HANDLE    map = NULL;
    WCHAR  *wpath = NULL;
    size_t  pchar = 0;
    size_t  pbyte = 0;

    // initialize the fields of the FILE_MAPPING structure.
    ZeroMemory(file, sizeof(FILE_MAPPING));

    // convert the path from UTF-8 input to the native system encoding.
    if ((wpath = Utf8toUtf16(path, pchar, pbyte)) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Unable to convert input path \"%S\" to UTF-16.\n", __FUNCTION__, GetCurrentThreadId(), path);
        goto cleanup_and_fail;
    }
    // open the requested input file, read-only, to be read from start to end.
    if ((fd = CreateFileW(wpath, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_FLAG_SEQUENTIAL_SCAN, NULL)) == INVALID_HANDLE_VALUE)
    {
        ConsoleError("ERROR: %S(%u): Unable to open input file \"%S\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, GetLastError());
        goto cleanup_and_fail;
    }
    // retrieve the current size of the file, in bytes.
    if (!GetFileSizeEx(fd, &file_size))
    {
        ConsoleError("ERROR: %S(%u): Failed to retrieve file size for input file \"%S\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, GetLastError());
        goto cleanup_and_fail;
    }
    // map the entire file (but don't map a view of the file.)
    if ((map = CreateFileMapping(fd, NULL, PAGE_READONLY, 0, 0, NULL)) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Failed to create the file mapping for input file \"%S\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, GetLastError());
        goto cleanup_and_fail;
    }
    // retrieve system information to get the allocation granularity.
    GetNativeSystemInfo(&sys_info);

    // all finished. the user should call IoMapFileRegion next.
    file->Filedes     = fd;
    file->Filemap     = map;
    file->FileSize    = file_size.QuadPart;
    file->Granularity = sys_info.dwAllocationGranularity;
    free(wpath);
    return 0;

cleanup_and_fail:
    if (map != NULL) CloseHandle(map);
    if (fd != INVALID_HANDLE_VALUE) CloseHandle(fd);
    if (wpath != NULL) free(wpath);
    return -1;
}

/// @summary Close a file mapping opened with IoOpenFileMapping. All views should have been unmapped already.
/// @param file The FILE_MAPPING to close.
public_function void
IoCloseFileMapping
(
    FILE_MAPPING *file
)
{
    if (file->Filemap != NULL)
    {
        CloseHandle(file->Filemap);
        file->Filemap = NULL;
    }
    if (file->Filedes != INVALID_HANDLE_VALUE)
    {
        CloseHandle(file->Filedes);
        file->Filedes = INVALID_HANDLE_VALUE;
    }
}

/// @summary Map a region of a file into the process address space. The file is mapped read-only.
/// @param data The FILE_DATA to populate with the mapped region.
/// @param offset The zero-based offset of the first byte within the file to map.
/// @param size The number of bytes to map into the process address space.
/// @param file The FILE_MAPPING returned by a previous call to IoOpenFileMapping.
/// @return Zero if the region is successfully mapped into the process address space, or -1 if an error occurred.
public_function int
IoMapFileRegion
(
    FILE_DATA    *data, 
    int64_t     offset, 
    int64_t       size, 
    FILE_MAPPING *file
)
{   // the mapping offset must be an integer multiple of the system allocation granularity.
    // sys_offset is the starting offset of the view, adhering to this requirement.
    // sys_nbytes is the actual size of the view, in bytes, adjusted for the granularity requirement.
    // adjust is the byte adjustment between the start of the mapped region and what the user asked to see.
    int64_t sys_offset = (offset / file->Granularity) * file->Granularity;
    ptrdiff_t   adjust =  offset - sys_offset;
    int64_t sys_nbytes =  size   + adjust;
    DWORD         hofs = (DWORD)  (sys_offset >> 32);
    DWORD         lofs = (DWORD)  (sys_offset & 0xFFFFFFFFUL);
    DWORD        wsize = (DWORD) ((sys_offset + sys_nbytes > file->FileSize) ? 0 : sys_nbytes);
    void         *base =  MapViewOfFile(file->Filemap, FILE_MAP_READ, hofs, lofs, wsize);
    if (base == NULL)
    {
        ConsoleError("ERROR: %S(%u): Unable to map region [%I64d, %I64d) (%08X).\n", __FUNCTION__, GetCurrentThreadId(), sys_offset, sys_offset+sys_nbytes, GetLastError());
        ZeroMemory(data, sizeof(FILE_DATA));
        return -1;
    }
    data->Buffer   =((uint8_t*) base) + adjust;
    data->MapPtr   = base;
    data->Offset   = offset;
    data->DataSize = size;
    data->Flags    = FILE_DATA_FLAG_MAPPED_REGION;
    return 0;
}

/// @summary Free resources associated with a loaded FILE_DATA object.
/// @param data The FILE_DATA to free.
public_function void
IoFreeFileData
(
    FILE_DATA *data
)
{
    if ((data->MapPtr != NULL) && (data->Flags & FILE_DATA_FLAG_MAPPED_REGION))
    {   // unmap the region, which will drop the pages.
        UnmapViewOfFile(data->MapPtr);
    }
    else if ((data->Buffer != NULL) && (data->Flags & FILE_DATA_FLAG_COMMITTED))
    {   // free the buffer, which was allocated with malloc.
        free(data->Buffer);
    }
    else
    {   // nothing to do in this case.
        // ...
    }
    ZeroMemory(data, sizeof(FILE_DATA));
}

// TODO(rlk): need to refactor this.
// when swapping buffers, track total number of events in buffer, and number consumed.
// have list (doubly-linked list!) of active requests, each with state. pointer to list node can be supplied as completion key (associated with file handle when file is opened.)
// - this works because there's no circumstance where we need to iterate over the active requests.
// - pool of request (intrinsic list) nodes allocated at init time.
// - can keep overlapped, etc. all together in one place.
// - can nicely keep timing information for profiling.
// - large-ish nodes, ie. 128 bytes.
// - requests have state, to support complex requests (like LOAD) - has OPEN, READ and CLOSE.
// events from IOCP are either:
// - I/O completion
//   - copied into separate completion buffer
// - buffer swap notification (ie. I/O requests are waiting)
//   - if buffer ID != current buffer ID, set flag indicating swap required and flip current buffer ID
// - shutdown notification
//   - set flag, and then drop
// limit on IOCP events retrieved at any given time
// - say 512
// process completion buffer first
// then pop one pending request at a time from the request buffer
// - if it's a non-trivial synchronous request (OPEN or FLUSH) add to a deferrment list
// - if it's a CLOSE request, process it immediately
// - if it's a READ, WRITE, LOAD or SAVE request:
//   - if you can get a list node, an active request slot is available
//   - if not, stop processing immediately - need to wait for some completions
// - if there are no more requests in the request buffer:
//   - if the swap flag is set, swap the buffers

public_function unsigned int __cdecl
IoBackgroundThreadMain
(
    void *argp
)
{
    IO_BACKGROUND_THREAD_ARGS args = {};
    IO_REQUEST_QUEUE          *ioq = NULL;
    IO_REQUEST_LIST           *rql = NULL;
    IO_ASYNC_STATE            *aio = NULL;
    OVERLAPPED_ENTRY       *evtbuf = NULL;
    ULONG_PTR       completion_key = AIO_COMPLETION_KEY_UNUSED;
    HANDLE                    iocp = NULL;
    DWORD                thread_id = GetCurrentThreadId();
    DWORD               max_events = 0;
    bool             more_requests = false;

    // copy argument data into a thread-local instance.
    CopyMemory(&args, argp, sizeof(IO_BACKGROUND_THREAD_ARGS));
    max_events = (DWORD) args.AIOState->MaxCompletions;
    evtbuf = args.AIOState->CompletionBuffer;
    aio = args.AIOState; ioq = args.AIOQueue;
    rql =&args.AIOState->ActiveRequests;
    iocp= args.AIOState->CompletionPort;

    for ( ; ; )
    {   // wait until events are available on the completion port, indicating:
        // 1. one or more I/O requests have been completed by the kernel
        // 2. one or more I/O requests have been submitted by the application
        // 3. the application has requested the thread to shut down
        ULONG num_events = 0; // the number of completion port events returned
        if (!GetQueuedCompletionStatusEx(iocp, evtbuf, max_events, &num_events, INFINITE, FALSE))
        {
            ConsoleError("ERROR: %S(%u): Background I/O thread failed waiting on completion port (%08X).\n", __FUNCTION__, GetCurrentThreadId(), GetLastError());
            goto terminate_thread;
        }
        // process the received event notifications and complete active requests.
        for (ULONG evi = 0; evi < num_events; ++evi)
        {
            if (evtbuf[i].lpOverlapped != NULL)
            {   // this is a completed I/O request.
                IO_REQUEST_NODE  *node  = ActiveIoRequestForOVERLAPPED(evtbuf[i].lpOverlapped);
                int64_t    transferred  = evtbuf[i].dwNumberOfBytesTransferred;
                DWORD       error_code  = HRESULT_FROM_NT(evtbuf[i].Internal);
                // TODO(rlk): need to update the request state here.
                // which may retire the request.
            }
            else
            {   // this is some kind of thread notification.
                switch (evtbuf[i].lpCompletionKey)
                {
                    case AIO_COMPLETION_KEY_WAKEUP0:
                    case AIO_COMPLETION_KEY_WAKEUP1:
                        { // if the completion key doesn't match the current request_buffer, queue a buffer swap.
                          // this can happen at most once per-iteration of the outermost loop.
                          // if the completion key matches the current request buffer, ignore the event
                          // as it has already been retrieved with the most recent buffer swap.
                          if (evtbuf[i].lpCompletionKey != completion_key)
                          {   // queue a buffer swap when the current buffer is exhausted.
                              aio->IoStateFlags |= IO_ASYNC_STATE_FLAG_SWAP;
                              completion_key     = evtbuf[i].lpCompletionKey;
                          }
                        } break;
                    case AIO_COMPLETION_KEY_SHUTDOWN:
                        { ConsoleOutput("DEATH: %S(%u): Background I/O thread received shutdown signal.\n", __FUNCTION__, GetCurrentThreadId());
                          aio->IoStateFlags |= IO_ASYNC_STATE_FLAG_SHUTDOWN;
                        } goto terminate_thread;
                    default:
                        { ConsoleError("ERROR: %S(%u): Background I/O thread received unknown signal %p.\n", __FUNCTION__, GetCurrentThreadId(), evt.lpCompletionKey);
                        } break;
                }
            }
        }
        do
        {   // process requests received from the application.
            if (aio->RequestIndex == aio->RequestCount)
            {   // there are no additional requests in the current buffer.
                if (aio->IoStateFlags & IO_ASYNC_STATE_FLAG_SWAP)
                {   // there are one or more I/O requests waiting, so retrieve them all in one go.
                    // this updates aio->RequestCount with the number of requests retrieved.
                    aio->RequestBuffer = RetrieveWaitingIoRequests(ioq, aio->RequestCount);
                    aio->IoStateFlags &=~IO_ASYNC_STATE_FLAG_SWAP;
                    aio->RequestIndex  = 0;
                }
                if (aio->RequestIndex == aio->RequestCount)
                {   // there are no more requests pending.
                    // immediately terminate this loop and go back to sleep.
                    break;
                }
            }

            IO_REQUEST app_request = aio->RequestBuffer[aio->RequestIndex];
            switch (app_request.RequestType)
            {
                case IO_REQUEST_NOOP:
                    { CompleteIoRequest_Noop(&app_request);
                      aio->RequestIndex++;
                    } break;
                case IO_REQUEST_READ:
                    { // asynchronous, simple.
                    } break;
                case IO_REQUEST_WRITE:
                    { // asynchronous, simple.
                    } break;
                case IO_REQUEST_FLUSH:
                    { // flush any pending writes - always synchronous.
                      DWORD result_code = ERROR_SUCCESS;
                      if (!FlushFileBuffers(app_request.FileHandle))
                      {   // the buffered data could not be flushed, save the error code.
                          result_code = GetLastError();
                      }
                      CompleteIoRequest(&app_request, 0, result_code);
                      aio->RequestIndex++;
                    } break;
                case IO_REQUEST_CLOSE:
                    { // close handle - always synchronous.
                      DWORD result_code = ERROR_SUCCESS;
                      if (!CloseHandle(app_request.FileHandle))
                      {   // the handle could not be closed, save the error code.
                          result_code = GetLastError();
                      }
                      CompleteIoRequest(&app_request, 0, result_code);
                      aio->RequestIndex++;
                    } break;
                case IO_REQUEST_LOAD:
                    { // asynchronous, multi-state.
                    } break;
                case IO_REQUEST_SAVE:
                    { // asynchronous, multi-state.
                    } break;
            }
        } while (more_requests);
    }

terminate_thread:
    return 0;
}

struct AIO_INPUT
{
    HANDLE      FileHandle;
    OVERLAPPED *Overlapped;
    void       *DataBuffer;
    int64_t     FileOffset;
    uint32_t    TransferAmount;
};

struct AIO_OUTPUT
{
    HANDLE      FileHandle;
    DWORD       ResultCode;
    uint32_t    TransferAmount;
    bool        CompletedSynchronously;
    bool        WasSuccessful;
};

/// @summary Calculate the absolute filesystem offset for an I/O operation.
/// @param slot The asynchronous I/O request slot associated with the operation.
/// @return The absolute filesystem byte offset.
internal_function inline int64_t
AioAbsoluteFileOffset
(
    IO_REQUEST_NODE const *slot
)
{
    return slot->BaseOffset + slot->FileOffset;
}

/// @summary Calculate the absolute filesystem offset for an I/O operation.
/// @param io The asynchronous I/O request associated with the operation.
/// @return The absolute filesystem byte offset.
internal_function inline int64_t
AioAbsoluteFileOffset
(
    IO_REQUEST const *io
)
{
    return io->BaseOffset + io->FileOffset;
}

internal_function void
AioExecuteOpen
(
    IO_REQUEST   &args, 
    HANDLE        iocp,
    AIO_OUTPUT &result
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
        share   = FILE_SHARE_NONE;
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
    if ((fd = CreateFile(args.PathBuffer, access, share, NULL, create, flags, NULL)) == INVALID_HANDLE_VALUE)
    {
        ConsoleError("ERROR: %S(%u): Failed to open file \"%s\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), args.PathBuffer, GetLastError());
        args.FileHandle = INVALID_HANDLE_VALUE;
        args.ResultCode = GetLastError();
        args.TransferAmount = 0;
        args.CompletedSynchronously = true;
        args.WasSuccessful = false;
        return;
    }
    if (CreateIoCompletionPort(fd, iocp, AIO_COMPLETION_KEY_UNUSED, 0) != iocp)
    {
        ConsoleError("ERROR: %S(%u): Unable to associate file handle with I/O completion port (%08X).\n", __FUNCTION__, GetCurrentThreadId(), GetLastError());
        args.FileHandle = INVALID_HANDLE_VALUE;
        args.ResultCode = GetLastError();
        args.TransferAmount = 0;
        args.CompletedSynchronously = true;
        args.WasSuccessful = false;
        CloseHandle(fd);
        return;
    }
    {   // immediately complete requests that execute synchronously; don't post completion port notification.
        SetFileCompletionNotificationModes(fd, FILE_SKIP_COMPLETION_PORT_ON_SUCCESS);
    }
    if (args.IoHintFlags & IO_HINT_FLAG_PREALLOCATE)
    {   // preallocate storage space for the file data, which can significantly improve performance when writing large files.
        // TODO(rlk): SetFilePointerEx
        // TODO(rlk): SetEndOfFile
        // TODO(rlk): warn about performance if not writing sequentially.
        // see https://blogs.msdn.microsoft.com/oldnewthing/20110922-00/?p=9573/
    }
}
/*struct IO_REQUEST
{
    int                 RequestType;                /// One of IO_REQUEST_TYPE specifying the type of operation to perform.
    uint32_t            IoHintFlags;                /// One or more of IO_HINT_FLAGS, specifying hints that may be used to optimize the I/O operation.
    uintptr_t           UserContext;                /// Opaque data associated with the request to be passed through to the completion callback.
    HANDLE              FileHandle;                 /// The handle of the file associated with the READ, WRITE or FLUSH request.
    WCHAR              *PathBuffer;                 /// Pointer to a caller-managed buffer specifying the path of the file to OPEN, LOAD or SAVE.
    void               *DataBuffer;                 /// The caller-managed buffer from which to READ/LOAD or WRITE/SAVE data, or NULL for NOOP, OPEN and FLUSH requests.
    int64_t             DataAmount;                 /// The number of bytes to transfer to or from the caller-managed data buffer.
    int64_t             BaseOffset;                 /// The byte offset of the start of the operation from the start of the physical file.
    int64_t             FileOffset;                 /// The byte offset of the start of the operation from the start of the logical file.
    uint64_t            SubmitTime;                 /// The timestamp (in ticks) at which the request was submitted by the application.
    IO_REQUEST_COMPLETE IoComplete;                 /// The callback to invoke when the operation has completed.
};*/

internal_function void
AioExecuteRead
(
    AIO_INPUT    &args,
    AIO_OUTPUT &result
)
{   // the request is submitted to be performed asynchronously, but can complete
    // either synchronously or asynchronously. this routine hides that complexity.
    args.Overlapped->Internal     = 0;
    args.Overlapped->InternalHigh = 0;
    args.Overlapped->Offset       =(DWORD) (args.FileOffset        & 0xFFFFFFFFUL);
    args.Overlapped->OffsetHigh   =(DWORD)((args.FileOffset >> 32) & 0xFFFFFFFFUL);
    if (ReadFile(args.FileHandle, args.DataBuffer, args.TransferAmount, &result.TransferAmount, args.Overlapped))
    {   // the read operation completed synchronously (likely the data was in-cache.)
        result.FileHandle = args.FileHandle;
        result.ResultCode = GetLastError();
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
                  result.CompletedSynchronously = true;
                  result.WasSuccessful = true;
                } break;
            default:
                { // an actual error occurred.
                  result.FileHandle = args.FileHandle;
                  result.CompletedSynchronously = true;
                  result.WasSuccessful = false;
                } break;
        }
    IO_HINT_FLAGS_NONE             = (0 << 0),      /// No I/O hints are specified, use the default behavior appropriate for the I/O request type.
    IO_HINT_FLAG_READ              = (1 << 0),      /// Read operations will be issued against the file.
    IO_HINT_FLAG_WRITE             = (1 << 1),      /// Write operations will be issues against the file.
    IO_HINT_FLAG_OVERWRITE         = (1 << 2),      /// The existing file contents should be discarded.
    IO_HINT_FLAG_PREALLOCATE       = (1 << 3),      /// Preallocate the file to the size specified in the IO_REQUEST::DataAmount field.
    IO_HINT_FLAG_SEQUENTIAL        = (1 << 4),      /// Optimize for sequential access when performing cached/buffered I/O (valid for OPEN, LOAD and SAVE.)
    IO_HINT_FLAG_UNCACHED          = (1 << 5),      /// Indicate that the I/O should bypass the OS page cache, and that the source or destination buffer meets sector alignment requirements (valid for OPEN, LOAD and SAVE).
    IO_HINT_FLAG_WRITE_THROUGH
    }
}

internal_function void
AioExecuteWrite
(
    AIO_INPUT    &args, 
    AIO_OUTPUT &result
)
{   // the request is submitted to be performed asynchronously, but can complete
    // either synchronously or asynchronously. this routine hides that complexity.
    args.Overlapped->Internal     = 0;
    args.Overlapped->InternalHigh = 0;
    args.Overlapped->Offset       =(DWORD) (args.FileOffset        & 0xFFFFFFFFUL);
    args.Overlapped->OffsetHigh   =(DWORD)((args.FileOffset >> 32) & 0xFFFFFFFFUL);
    if (WriteFile(args.FileHandle, args.DataBuffer, args.TransferAmount, &result.TransferAmount, args.Overlapped))
    {   // the write operation completed synchronously.
        result.FileHandle = args.FileHandle;
        result.ResultCode = GetLastError();
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
                  result.CompletedSynchronously = true;
                  result.WasSuccessful = false;
                } break;
        }
    }
}


