/*/////////////////////////////////////////////////////////////////////////////
/// @summary Implement a thread pool based around Windows I/O completion ports.
///////////////////////////////////////////////////////////////////////////80*/

/*//////////////////
//   Data Types   //
//////////////////*/
struct CPU_INFO;
struct THREAD_POOL;
struct THREAD_POOL_INIT;
struct WORKER_THREAD;
struct TASK_PROFILER;
struct TASK_PROFILER_SPAN;

/// @summary Helper macros to emit task profiler events and span markers.
#define TpThreadEvent(pool, fmt, ...)                CvWriteAlertW((pool)->TaskProfiler.MarkerSeries, _T(fmt), __VA_ARGS__)
#define TpThreadSpanEnter(pool, span, fmt, ...)      CvEnterSpanW((pool)->TaskProfiler.MarkerSeries, &(span).CvSpan, _T(fmt), __VA_ARGS__)
#define TpThreadSpanLeave(pool, span)                CvLeaveSpan((span).CvSpan)

/// @summary Define the CPU topology information for the local system.
struct CPU_INFO
{
    size_t              NumaNodes;                   /// The number of NUMA nodes in the system.
    size_t              PhysicalCPUs;                /// The number of physical CPUs installed in the system.
    size_t              PhysicalCores;               /// The total number of physical cores in all CPUs.
    size_t              HardwareThreads;             /// The total number of hardware threads in all CPUs.
    size_t              ThreadsPerCore;              /// The number of hardware threads per physical core.
    char                VendorName[13];              /// The CPUID vendor string.
    char                PreferAMD;                   /// Set to 1 if AMD OpenCL implementations are preferred.
    char                PreferIntel;                 /// Set to 1 if Intel OpenCL implementations are preferred.
    char                IsVirtualMachine;            /// Set to 1 if the process is running in a virtual machine.
};

/// @summary Define the data associated with the system task profiler. The task profiler can be used to emit:
/// Events: Used for point-in-time events such as worker thread startup and shutdown or task submission.
/// Spans : Used to represent a range of time such as the time between task submission and execution, or task start and finish.
struct TASK_PROFILER
{
    CV_PROVIDER        *Provider;                    /// The Win32 OS Layer provider.
    CV_MARKERSERIES    *MarkerSeries;                /// The marker series. Each profiler instance has a separate marker series.
};

/// @summary Defines the data associated with a task profiler object used to track a time range.
struct TASK_PROFILER_SPAN
{
    CV_SPAN            *CvSpan;                      /// The Concurrency Visualizer SDK object representing the time span.
};

/// @summary Define the data available to an application callback executing on a worker thread.
struct WORKER_THREAD
{
    THREAD_POOL        *ThreadPool;                  /// The thread pool that manages the worker.
    HANDLE              CompletionPort;              /// The I/O completion port used to wait and wake the thread.
    void               *PoolContext;                 /// The opaque, application-specific data passed through to the thread.
    void               *ThreadContext;               /// The opaque, application-specific data created by the OS_WORKER_INIT callback for the thread.
    unsigned int        ThreadId;                    /// The operating system identifier for the thread.
};

/// @summary Define the signature for the callback invoked during worker thread initialization to allow the application to create any per-thread resources.
/// @param thread_args A WORKER_THREAD instance specifying worker thread data. The callback should set the ThreadContext field to its private data.
/// @return Zero if initialization was successful, or -1 to terminate the worker thread.
typedef int           (*WORKER_INIT_FUNC)(WORKER_THREAD *thread_args);

/// @summary Define the signature for the callback representing the application entry point on a worker thread.
/// @param thread_args A WORKER_THREAD instance, valid until the WORKER_ENTRY function returns, specifying per-thread data.
/// @param signal_arg An application-defined value specified with the wake notification.
/// @param wake_reason One of WORKER_THREAD_WAKE_REASON indicating the reason the thread was woken.
typedef void          (*WORKER_ENTRY_FUNC)(WORKER_THREAD *thread_args, uintptr_t signal_arg, int wake_reason);

/// @summary Define the data package passed to WorkerThreadMain during thread pool creation.
struct WORKER_THREAD_INIT
{
    THREAD_POOL        *ThreadPool;                  /// The thread pool that manages the worker thread.
    HANDLE              ReadySignal;                 /// The manual-reset event to be signaled by the worker when it has successfully completed initialization and is ready-to-run.
    HANDLE              ErrorSignal;                 /// The manual-reset event to be signaled by the worker if it encounters a fatal error during initialization.
    HANDLE              LaunchSignal;                /// The manual-reset event to be signaled by the pool when all worker threads should begin running.
    HANDLE              TerminateSignal;             /// The manual-reset event to be signaled by the pool when all worker threads should terminate.
    HANDLE              CompletionPort;              /// The I/O completion port signaled when the worker thread should wake up.
    WORKER_INIT_FUNC    ThreadInit;                  /// The callback function to run during thread launch to allow the application to allocate or initialize any per-thread data.
    WORKER_ENTRY_FUNC   ThreadMain;                  /// The callback function to run when a message is received by the worker thread.
    void               *PoolContext;                 /// Opaque application-supplied data to pass through to AppThreadMain.
    size_t              StackSize;                   /// The stack size of the worker thread, in bytes, or OS_WORKER_THREAD_STACK_DEFAULT.
    uint32_t            NUMAGroup;                   /// The zero-based index of the NUMA processor group on which the worker thread will be scheduled.
};

/// @summary Define the data maintained by a fixed pool of worker threads.
struct THREAD_POOL
{
    size_t              ActiveThreads;               /// The number of currently active threads in the pool.
    unsigned int       *OSThreadIds;                 /// The operating system thread identifier for each active worker thread.
    HANDLE             *OSThreadHandle;              /// The operating system thread handle for each active worker thread.
    HANDLE             *WorkerReady;                 /// The manual-reset event signaled by each active worker to indicate that it is ready to run.
    HANDLE             *WorkerError;                 /// The manual-reset event signaled by each active worker to indicate a fatal error has occurred.
    HANDLE              CompletionPort;              /// The I/O completion port used to wait and wake worker threads in the pool.
    HANDLE              LaunchSignal;                /// The manual-reset event used to launch all threads in the pool.
    HANDLE              TerminateSignal;             /// The manual-reset event used to notify all threads that they should terminate.
    TASK_PROFILER       TaskProfiler;                /// The task profiler associated with the thread pool.
};

/// @summary Define the parameters used to configure a thread pool.
struct THREAD_POOL_INIT
{
    WORKER_INIT_FUNC    ThreadInit;                  /// The callback function to run during launch for each worker thread to initialize thread-local resources.
    WORKER_ENTRY_FUNC   ThreadMain;                  /// The callback function to run on the worker thread(s) when a signal is received.
    void               *PoolContext;                 /// Opaque application-supplied data to be passed to OS_WORKER_INIT for each worker thread.
    size_t              ThreadCount;                 /// The number of worker threads to create.
    size_t              StackSize;                   /// The stack size for each worker thread, in bytes, or OS_WORKER_THREAD_STACK_DEFAULT.
    uint32_t            NUMAGroup;                   /// The zero-based index of the NUMA processor group on which the worker threads will be scheduled. Set to 0.
};

/// @summary Define constants for specifying worker thread stack sizes.
enum WORKER_THREAD_STACK_SIZE     : size_t
{
    WORKER_THREAD_STACK_DEFAULT   = 0,               /// Use the default stack size for each worker thread.
};

/// @summary Define the set of return codes expected from the WORKER_INIT_FUNC callback.
enum WORKER_THREAD_INIT_RESULT    : int
{
    WORKER_THREAD_INIT_SUCCESS    = 0,               /// The worker thread initialized successfully.
    WORKER_THREAD_INIT_FAILED     =-1,               /// The worker thread failed to initialize and the thread should be terminated.
};

/// @summary Define the set of reasons for waking a sleeping worker thread and invoking the worker callback.
enum WORKER_THREAD_WAKE_REASON    : int
{
    WORKER_THREAD_WAKE_FOR_EXIT   = 0,               /// The thread was woken because the thread pool is being terminated.
    WORKER_THREAD_WAKE_FOR_SIGNAL = 1,               /// The thread was woken because of a general signal.
    WORKER_THREAD_WAKE_FOR_RUN    = 2,               /// The thread was woken because an explicit work wakeup signal was sent.
    WORKER_THREAD_WAKE_FOR_ERROR  = 3,               /// The thread was woken because of an error in GetQueuedCompletionStatus.
};

/*///////////////
//   Globals   //
///////////////*/
/// @summary The GUID of the Win32 OS Layer task profiler provider {349CE0E9-6DF5-4C25-AC5B-C84F529BC0CE}.
global_variable GUID const TaskProfilerGUID = { 0x349ce0e9, 0x6df5, 0x4c25, { 0xac, 0x5b, 0xc8, 0x4f, 0x52, 0x9b, 0xc0, 0xce } };

/*//////////////////////////
//   Internal Functions   //
//////////////////////////*/
/// @summary Send an application-defined signal from one worker thread to one or more other worker threads in the same pool.
/// @param iocp The I/O completion port handle for the thread pool.
/// @param signal_arg The application-defined data to send as the signal.
/// @param thread_count The number of waiting threads to signal.
/// @param last_error If the function returns false, the system error code is stored in this location.
/// @return true if the specified number of signal notifications were successfully posted to the thread pool.
internal_function bool
SignalWorkerThreads
(
    HANDLE          iocp, 
    uintptr_t signal_arg, 
    size_t  thread_count, 
    DWORD    &last_error
)
{
    for (size_t i = 0; i < thread_count; ++i)
    {
        if (!PostQueuedCompletionStatus(iocp, 0, signal_arg, NULL))
        {   // only report the first failure.
            if (last_error == ERROR_SUCCESS)
            {   // save the error code.
                last_error  = GetLastError();
            } return false;
        }
    }
    return true;
}

/// @summary Call from a thread pool worker only. Puts the worker thread to sleep until a signal is received on the pool, or the pool is shutting down.
/// @param iocp The handle of the I/O completion port used to signal the thread pool.
/// @param term The handle of the manual-reset event used to signal worker threads to terminate.
/// @param tid The operating system thread identifier of the calling worker thread.
/// @param key The location to receive the pointer value sent with the wake signal.
/// @return One of WORKER_THREAD_WAKE_REASON.
internal_function int
WorkerThreadWaitForWakeup
(
    HANDLE    iocp,
    HANDLE    term, 
    uint32_t   tid,
    uintptr_t &key
)
{
    OVERLAPPED *ov_addr = NULL;
    DWORD        nbytes = 0;
    DWORD        termrc = 0;

    // prior to entering the wait state, check the termination signal.
    if ((termrc = WaitForSingleObject(term, 0)) != WAIT_TIMEOUT)
    {   // either termination was signaled, or an error occurred.
        // either way, exit prior to entering the wait state.
        if (termrc != WAIT_OBJECT_0)
        {   // an error occurred while checking the termination signal.
            ConsoleError("ERROR: %S(%u): Checking termination signal failed with result 0x%08X (0x%08X).\n", __FUNCTION__, tid, termrc, GetLastError());
        }
        return WORKER_THREAD_WAKE_FOR_EXIT;
    }
    if (GetQueuedCompletionStatus(iocp, &nbytes, &key, &ov_addr, INFINITE))
    {   // check the termination signal before possibly dispatching work.
        int      rc = key != 0 ? WORKER_THREAD_WAKE_FOR_RUN : WORKER_THREAD_WAKE_FOR_SIGNAL;
        if ((termrc = WaitForSingleObject(term, 0)) != WAIT_TIMEOUT)
        {
            if (termrc != WAIT_OBJECT_0)
            {   // an error occurred while checking the termination signal.
                ConsoleError("ERROR: %S(%u): Checking termination signal failed with result 0x%08X (0x%08X).\n", __FUNCTION__, tid, termrc, GetLastError());
            }
            rc = WORKER_THREAD_WAKE_FOR_EXIT;
        }
        return rc;
    }
    else
    {   // the call to GetQueuedCompletionStatus failed.
        ConsoleError("ERROR: %S(%u): GetQueuedCompletionStatus failed with result 0x%08X.\n", __FUNCTION__, tid, GetLastError());
        return WORKER_THREAD_WAKE_FOR_ERROR;
    }
}

/// @summary Implement the internal entry point of a worker thread.
/// @param argp Pointer to an OS_WORKER_THREAD_INIT instance specific to this thread.
/// @return Zero if the thread terminated normally, or non-zero for abnormal termination.
internal_function unsigned int __cdecl
WorkerThreadMain
(
    void *argp
)
{
    WORKER_THREAD_INIT     init = {};
    WORKER_THREAD          args = {};
    uintptr_t        signal_arg = 0;
    const DWORD          LAUNCH = 0;
    const DWORD       TERMINATE = 1;
    const DWORD      WAIT_COUNT = 2;
    HANDLE   waitset[WAIT_COUNT]= {};
    DWORD                   tid = GetCurrentThreadId();
    DWORD                waitrc = 0;
    bool           keep_running = true;
    int             wake_reason = WORKER_THREAD_WAKE_FOR_EXIT;
    unsigned int      exit_code = 1;

    // copy the initialization data into local stack memory.
    // argp may have been allocated on the stack of the caller 
    // and is only guaranteed to remain valid until the ReadySignal is set.
    CopyMemory(&init, argp, sizeof(WORKER_THREAD_INIT));

    // spit out a message just prior to initialization:
    ConsoleOutput("START: %S(%u): Worker thread starting on pool 0x%p.\n", __FUNCTION__, tid, init.ThreadPool);
    TpThreadEvent(init.ThreadPool, "Worker thread %u starting", tid);

    // set up the data to be passed through to the application executing on the worker.
    args.ThreadPool     = init.ThreadPool;
    args.CompletionPort = init.CompletionPort;
    args.PoolContext    = init.PoolContext;
    args.ThreadContext  = NULL;
    args.ThreadId       = tid;

    // allow the application to perform per-thread setup.
    if (init.ThreadInit(&args) < 0)
    {
        ConsoleError("ERROR: %S(%u): Application thread initialization failed on pool 0x%p.\n", __FUNCTION__, tid, init.ThreadPool);
        ConsoleError("DEATH: %S(%u): Worker terminating in pool 0x%p.\n", __FUNCTION__, tid, init.ThreadPool);
        SetEvent(init.ErrorSignal);
        return 2;
    }

    // signal the main thread that this thread is ready to run.
    SetEvent(init.ReadySignal);

    // enter a wait state until the thread pool launches all of the workers.
    waitset[LAUNCH]    = init.LaunchSignal;
    waitset[TERMINATE] = init.TerminateSignal;
    switch ((waitrc = WaitForMultipleObjects(WAIT_COUNT, waitset, FALSE, INFINITE)))
    {
        case WAIT_OBJECT_0 + LAUNCH:
            {   // enter the main wait loop.
                keep_running = true;
            } break;
        case WAIT_OBJECT_0 + TERMINATE:
            {   // do not enter the main wait loop.
                keep_running = false;
                exit_code = 0;
            } break; 
        default: 
            {   // do not enter the main wait loop.
                ConsoleError("ERROR: %S(%u): Unexpected result 0x%08X while waiting for launch signal.\n", __FUNCTION__, tid, waitrc);
                keep_running = false;
                exit_code = 1;
            } break;
    }

    __try
    {
        while (keep_running)
        {   // once launched by the thread pool, enter the main wait loop.
            switch ((wake_reason = WorkerThreadWaitForWakeup(init.CompletionPort, init.TerminateSignal, tid, signal_arg)))
            {
                case WORKER_THREAD_WAKE_FOR_EXIT:
                    {   // allow the application to clean up any thread-local resources.
                        init.ThreadMain(&args, signal_arg, wake_reason);
                        keep_running = false;
                        exit_code = 0;
                    } break;
                case WORKER_THREAD_WAKE_FOR_SIGNAL:
                case WORKER_THREAD_WAKE_FOR_RUN:
                    {   // allow the application to execute the signal handler or work item.
                        init.ThreadMain(&args, signal_arg, wake_reason);
                    } break;
                case WORKER_THREAD_WAKE_FOR_ERROR:
                    {   // OsWorkerThreadWaitForWakeup output error information already.
                        ConsoleError("ERROR: %S(%u): Worker terminating due to previous error(s).\n", __FUNCTION__, tid);
                        keep_running = false;
                        exit_code = 1;
                    } break;
                default:
                    {   // spit out an error if we get an unexpected return value.
                        assert(false && __FUNCTION__ ": Unexpected wake_reason.");
                        ConsoleError("ERROR: %S(%u): Worker terminating due to unexpected wake reason.\n", __FUNCTION__, tid);
                        keep_running = false;
                        exit_code = 1;
                    } break;
            }
            // reset the wake signal to 0/NULL for the next iteration.
            signal_arg = 0;
        }
    }
    __finally
    {   // the worker is terminating - clean up thread-local resources.
        // the AppThreadMain will not be called again by this thread.
        // ...
        // spit out a message just prior to termination.
        ConsoleError("DEATH: %S(%u): Worker terminating in pool 0x%p.\n", __FUNCTION__, tid, init.ThreadPool);
        return exit_code;
    }
}

/*////////////////////////
//   Public Functions   //
////////////////////////*/
/// @summary Enumerate all CPU resources of the host system.
/// @param cpu_info The structure to populate with information about host CPU resources.
/// @param arena The memory arena used to allocate temporary memory. The temporary memory is freed before the function returns.
/// @return Zero if the host CPU information was successfully retrieved, or -1 if an error occurred.
public_function int
TpQueryHostCpuLayout
(
    CPU_INFO  *cpu_info 
)
{
    SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX *lpibuf = NULL;
    SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX *info   = NULL;
    size_t     smt_count = 0;
    uint8_t     *bufferp = NULL;
    uint8_t     *buffere = NULL;
    DWORD    buffer_size = 0;
    int          regs[4] ={0, 0, 0, 0};

    // zero out the CPU information returned to the caller.
    ZeroMemory(cpu_info, sizeof(CPU_INFO));
    
    // retrieve the CPU vendor string using the __cpuid intrinsic.
    __cpuid(regs  , 0); // CPUID function 0
    *((int*)&cpu_info->VendorName[0]) = regs[1]; // EBX
    *((int*)&cpu_info->VendorName[4]) = regs[3]; // ECX
    *((int*)&cpu_info->VendorName[8]) = regs[2]; // EDX
         if (!strcmp(cpu_info->VendorName, "AuthenticAMD")) cpu_info->PreferAMD        = true;
    else if (!strcmp(cpu_info->VendorName, "GenuineIntel")) cpu_info->PreferIntel      = true;
    else if (!strcmp(cpu_info->VendorName, "KVMKVMKVMKVM")) cpu_info->IsVirtualMachine = true;
    else if (!strcmp(cpu_info->VendorName, "Microsoft Hv")) cpu_info->IsVirtualMachine = true;
    else if (!strcmp(cpu_info->VendorName, "VMwareVMware")) cpu_info->IsVirtualMachine = true;
    else if (!strcmp(cpu_info->VendorName, "XenVMMXenVMM")) cpu_info->IsVirtualMachine = true;

    // figure out the amount of space required, and allocate a temporary buffer:
    GetLogicalProcessorInformationEx(RelationAll, NULL, &buffer_size);
    if ((lpibuf = (SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*) malloc(buffer_size)) == NULL)
    {
        ConsoleError("ERROR: %S: Insufficient memory to query host CPU layout.\n", __FUNCTION__);
        cpu_info->NumaNodes       = 1;
        cpu_info->PhysicalCPUs    = 1;
        cpu_info->PhysicalCores   = 1;
        cpu_info->HardwareThreads = 1;
        cpu_info->ThreadsPerCore  = 1;
        return -1;
    }
    GetLogicalProcessorInformationEx(RelationAll, lpibuf, &buffer_size);

    // step through the buffer and update counts:
    bufferp = (uint8_t*) lpibuf;
    buffere =((uint8_t*) lpibuf) + size_t(buffer_size);
    while (bufferp < buffere)
    {
        info = (SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*) bufferp;
        switch (info->Relationship)
        {
            case RelationNumaNode:
                { cpu_info->NumaNodes++;
                } break;

            case RelationProcessorPackage:
                { cpu_info->PhysicalCPUs++;
                } break;

            case RelationProcessorCore:
                { cpu_info->PhysicalCores++;
                  if (info->Processor.Flags == LTP_PC_SMT)
                      smt_count++;
                } break;

            default:
                {   // RelationGroup, RelationCache - don't care.
                } break;
        }
        bufferp += size_t(info->Size);
    }
    // free the temporary buffer:
    free(lpibuf);

    // determine the total number of logical processors in the system.
    // use this value to figure out the number of threads per-core.
    if (smt_count > 0)
    {   // determine the number of logical processors in the system and
        // use this value to figure out the number of threads per-core.
        SYSTEM_INFO sysinfo;
        GetNativeSystemInfo(&sysinfo);
        cpu_info->ThreadsPerCore = size_t(sysinfo.dwNumberOfProcessors) / smt_count;
    }
    else
    {   // there are no SMT-enabled CPUs in the system, so 1 thread per-core.
        cpu_info->ThreadsPerCore = 1;
    }

    // calculate the total number of available hardware threads.
    cpu_info->HardwareThreads = (smt_count * cpu_info->ThreadsPerCore) + (cpu_info->PhysicalCores - smt_count);
    return 0;
}

/// @summary Create a thread pool. The calling thread is blocked until all worker threads have successfully started and initialized.
/// @param pool The OS_THREAD_POOL instance to initialize.
/// @param init An OS_THREAD_POOL_INIT object describing the thread pool configuration.
/// @param arena The memory arena from which thread pool storage will be allocated.
/// @param name A zero-terminated string constant specifying a human-readable name for the thread pool, or NULL. This name is used for task profiler display.
/// @return Zero if the thread pool is created successfully and worker threads are ready-to-run, or -1 if an error occurs.
public_function int
TpCreateThreadPool
(
    THREAD_POOL      *pool, 
    THREAD_POOL_INIT *init,
    char const       *name=NULL
)
{
    HANDLE                iocp = NULL;
    HANDLE          evt_launch = NULL;
    HANDLE       evt_terminate = NULL;
    DWORD                  tid = GetCurrentThreadId();
    CV_PROVIDER   *cv_provider = NULL;
    CV_MARKERSERIES *cv_series = NULL;
    HRESULT          cv_result = S_OK;
    char           cv_name[64] = {};

    // Zero the fields of the THREAD_POOL instance to start from a known state.
    ZeroMemory(pool, sizeof(THREAD_POOL));

    // create objects used to synchronize the threads in the pool.
    if ((evt_launch = CreateEvent(NULL, TRUE, FALSE, NULL)) == NULL)
    {   // without the launch event, there's no way to synchronize worker launch.
        ConsoleError("ERROR: %S(%u): Unable to create pool launch event (0x%08X).\n", __FUNCTION__, tid, GetLastError());
        goto cleanup_and_fail;
    }
    if ((evt_terminate = CreateEvent(NULL, TRUE, FALSE, NULL)) == NULL)
    {   // without the termination event, there's no way to synchronize worker shutdown.
        ConsoleError("ERROR: %S(%u): Unable to create pool termination event (0x%08X).\n", __FUNCTION__, tid, GetLastError());
        goto cleanup_and_fail;
    }
    if ((iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, (DWORD) init->ThreadCount+1)) == NULL)
    {   // without the completion port, there's no way to synchronize worker execution.
        ConsoleError("ERROR: %S(%u): Unable to create pool I/O completion port (0x%08X).\n", __FUNCTION__, tid, GetLastError());
        goto cleanup_and_fail;
    }

    if (name == NULL)
    {   // Concurrency Visualizer SDK requires a non-NULL string.
        sprintf_s(cv_name, "Unnamed pool 0x%p", pool);
    }
    if (!SUCCEEDED((cv_result = CvInitProvider(&TaskProfilerGUID, &cv_provider))))
    {
        ConsoleError("ERROR: %S(%u): Unable to initialize task profiler provider (HRESULT 0x%08X).\n", __FUNCTION__, tid, cv_result);
        goto cleanup_and_fail;
    }
    if (!SUCCEEDED((cv_result = CvCreateMarkerSeriesA(cv_provider, name, &cv_series))))
    {
        ConsoleError("ERROR: %S(%u): Unable to create task profiler marker series (HRESULT 0x%08X).\n", __FUNCTION__, tid, cv_result);
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
        ConsoleError("ERROR: %S(%u): Unable to allocate thread pool memory.\n", __FUNCTION__, GetCurrentThreadId());
        goto cleanup_and_fail;
    }
    pool->CompletionPort  = iocp;
    pool->LaunchSignal    = evt_launch;
    pool->TerminateSignal = evt_terminate;
    pool->TaskProfiler.Provider     = cv_provider;
    pool->TaskProfiler.MarkerSeries = cv_series;
    ZeroMemory(pool->OSThreadIds    , init->ThreadCount * sizeof(unsigned int));
    ZeroMemory(pool->OSThreadHandle , init->ThreadCount * sizeof(HANDLE));
    ZeroMemory(pool->WorkerReady    , init->ThreadCount * sizeof(HANDLE));
    ZeroMemory(pool->WorkerError    , init->ThreadCount * sizeof(HANDLE));

    // set up the worker init structure and spawn all threads.
    for (size_t i = 0, n = init->ThreadCount; i < n; ++i)
    {
        WORKER_THREAD_INIT winit  = {};
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
            ConsoleError("ERROR: %S(%u): Unable to create ready signal for worker %Iu of %Iu (0x%08X).\n", __FUNCTION__, tid, i, n, GetLastError());
            goto cleanup_and_fail;
        }
        if ((werror = CreateEvent(NULL, TRUE, FALSE, NULL)) == NULL)
        {
            ConsoleError("ERROR: %S(%u): Unable to create error signal for worker %Iu of %Iu (0x%08X).\n", __FUNCTION__, tid, i, n, GetLastError());
            CloseHandle(wready);
            goto cleanup_and_fail;
        }

        // populate the OS_WORKER_THREAD_INIT and then spawn the worker thread.
        // the worker thread will need to copy this structure if it wants to access it 
        // past the point where it signals the wready event.
        winit.ThreadPool      = pool;
        winit.ReadySignal     = wready;
        winit.ErrorSignal     = werror;
        winit.LaunchSignal    = evt_launch;
        winit.TerminateSignal = evt_terminate;
        winit.CompletionPort  = iocp;
        winit.ThreadInit      = init->ThreadInit;
        winit.ThreadMain      = init->ThreadMain;
        winit.PoolContext     = init->PoolContext;
        winit.StackSize       = init->StackSize;
        winit.NUMAGroup       = init->NUMAGroup;
        if ((whand = (HANDLE) _beginthreadex(NULL, (unsigned) init->StackSize, WorkerThreadMain, &winit, 0, &thread_id)) == NULL)
        {
            ConsoleError("ERROR: %S(%u): Unable to spawn worker %Iu of %Iu (errno = %d).\n", __FUNCTION__, tid, i, n, errno);
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
            // events are already in the OS_THREAD_POOL arrays, so don't clean up here.
            ConsoleError("ERROR: %S(%u): Failed to initialize worker %Iu of %Iu (0x%08X).\n", __FUNCTION__, tid, i, n, waitrc);
            goto cleanup_and_fail;
        }
        // SetThreadGroupAffinity, eventually.
    }

    // everything has been successfully initialized. all worker threads are waiting on the launch signal.
    return 0;

cleanup_and_fail:
    if (pool->ActiveThreads > 0)
    {   // signal all threads to terminate, and then wait until they all die.
        // all workers are blocked waiting on the launch event.
        SetEvent(evt_terminate);
        SetEvent(evt_launch);
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
    if (evt_launch) CloseHandle(evt_launch);
    if (iocp) CloseHandle(iocp);
    // zero out the THREAD_POOL prior to returning to the caller.
    ZeroMemory(pool, sizeof(THREAD_POOL));
    return -1;
}

/// @summary Launch all worker threads in a thread pool, allowing them to execute tasks.
/// @param pool The THREAD_POOL managing the worker threads to launch.
public_function void
TpLaunchThreadPool
(
    THREAD_POOL *pool
)
{
    if (pool->LaunchSignal != NULL)
    {
        SetEvent(pool->LaunchSignal);
    }
}

/// @summary Perform a fast shutdown of a thread pool. The calling thread does not wait for the worker threads to exit. No handles are closed.
/// @param pool The OS_THREAD_POOL to shut down.
public_function void
TpTerminateThreadPool
(
    THREAD_POOL *pool
)
{
    if (pool->ActiveThreads > 0)
    {
        DWORD last_error = ERROR_SUCCESS;
        // signal the termination event prior to waking any waiting threads.
        SetEvent(pool->TerminateSignal);
        // signal all worker threads in the pool. any active processing will complete before this signal is received.
        SignalWorkerThreads(pool->CompletionPort, 0, pool->ActiveThreads, last_error);
        // signal the launch event, in case no threads have been launched yet.
        SetEvent(pool->LaunchSignal);
    }
}

/// @summary Perform a complete shutdown and cleanup of a thread pool. The calling thread is blocked until all threads exit.
/// @param pool The OS_THREAD_POOL to shut down and clean up.
public_function void
TpDestroyThreadPool
(
    THREAD_POOL *pool
)
{
    if (pool->ActiveThreads > 0)
    {   
        DWORD last_error = ERROR_SUCCESS;
        // signal the termination event prior to waking any waiting threads.
        SetEvent(pool->TerminateSignal);
        // signal all worker threads in the pool. any active processing will complete before this signal is received.
        SignalWorkerThreads(pool->CompletionPort, 0, pool->ActiveThreads, last_error);
        // signal the launch event, in case no threads have been launched yet.
        SetEvent(pool->LaunchSignal);
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
    if (pool->TaskProfiler.MarkerSeries != NULL)
    {
        CvReleaseMarkerSeries(pool->TaskProfiler.MarkerSeries);
        pool->TaskProfiler.MarkerSeries = NULL;
    }
    if (pool->TaskProfiler.Provider != NULL)
    {
        CvReleaseProvider(pool->TaskProfiler.Provider);
        pool->TaskProfiler.Provider = NULL;
    }
    if (pool->LaunchSignal != NULL)
    {
        CloseHandle(pool->LaunchSignal);
        pool->LaunchSignal = NULL;
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

/// @summary Send an application-defined signal from one worker thread to one or more other worker threads in the same pool.
/// @param sender The WORKER_THREAD state for the thread sending the signal.
/// @param signal_arg The application-defined data to send as the signal.
/// @param thread_count The number of waiting threads to signal.
/// @return true if the specified number of signal notifications were successfully posted to the thread pool.
public_function bool
TpSignalWorkerThreads
(
    WORKER_THREAD *sender, 
    uintptr_t  signal_arg, 
    size_t   thread_count 
)
{
    DWORD last_error = ERROR_SUCCESS;
    if  (!SignalWorkerThreads(sender->CompletionPort, signal_arg, thread_count, last_error))
    {
        ConsoleError("ERROR: %S(%u): Signaling worker threads failed with result 0x%08X.\n", __FUNCTION__, sender->ThreadId, last_error);
        return false;
    }
    return true;
}

/// @summary Send an application-defined signal to wake one or more worker threads in a thread pool.
/// @param pool The thread pool to signal.
/// @param signal_arg The application-defined data to send as the signal.
/// @param thread_count The number of waiting threads to signal.
/// @return true if the specified number of signal notifications were successfully posted to the thread pool.
public_function bool
TpSignalWorkerThreads
(
    THREAD_POOL    *pool, 
    uintptr_t signal_arg, 
    size_t  thread_count
)
{
    DWORD last_error = ERROR_SUCCESS;
    if  (!SignalWorkerThreads(pool->CompletionPort, signal_arg, thread_count, last_error))
    {
        ConsoleError("ERROR: %S(%u): Signaling worker pool failed with result 0x%08X.\n", __FUNCTION__, GetCurrentThreadId(), last_error);
        return false;
    }
    return true;
}

