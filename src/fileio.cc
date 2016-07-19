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
    WCHAR        *Path;                       /// The absolute path of the file.
    int64_t       FileSize;                   /// The size of the file, in bytes, at the time the directory enumeration was performed.
    FILETIME      LastWrite;                  /// The last write time of the file.
    DWORD         Attributes;                 /// The file attributes, as returned by GetFileAttributes().
    uint32_t      SortKey;                    /// The sort key, reconstructed from the file extension.
};

/// @summary Define the container type for a list of files returned during directory enumeration.
typedef std::vector<FILE_INFO> FILE_LIST;

/// @summary Define the data maintained with a memory-mapped file opened for read access.
struct FILE_MAPPING
{
    HANDLE        Filedes;                    /// A valid file handle, or INVALID_HANDLE_VALUE.
    HANDLE        Filemap;                    /// A valid file mapping handle, or NULL.
    int64_t       FileSize;                   /// The size of the file, in bytes, at the time it was opened.
    size_t        Granularity;                /// The system allocation granularity, in bytes.
};

/// @summary Define the data associated with a region of a file loaded or mapped into memory.
struct FILE_DATA
{
    uint8_t      *Buffer;                     /// The buffer containing the loaded file data.
    void         *MapPtr;                     /// The address returned by MapViewOfFile.
    int64_t       Offset;                     /// The offset of the data in this region from the start of the file, in bytes.
    int64_t       DataSize;                   /// The number of bytes in Buffer that are valid.
    uint32_t      Flags;                      /// One or more of FILE_DATA_FLAGS describing the allocation attributes of the FILE_DATA.
};

/// @summary Define various allocation attributes of a file region.
enum FILE_DATA_FLAGS : uint32_t
{
    FILE_DATA_FLAGS_NONE         = (0 << 0),  /// The FILE_DATA is invalid.
    FILE_DATA_FLAG_COMMITTED     = (1 << 0),  /// The FILE_DATA buffer is an explicitly allocated region of memory.
    FILE_DATA_FLAG_MAPPED_REGION = (1 << 1),  /// The FILE_DATA represents a mapped region of a file.
};

/*///////////////
//   Globals   //
///////////////*/

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
        if ((ch <= L'0') || (ch >= L'9'))
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
/// @param path The zero-terminated UTF-8 path of the file to load.
/// @return Zero if the file is loaded successfully, or -1 if an error occurred.
public_function int
IoLoadFileData
(
    FILE_DATA  *data,
    char const *path
)
{
    LARGE_INTEGER file_size = {};
    HANDLE     fd = INVALID_HANDLE_VALUE;
    WCHAR  *wpath = NULL;
    void     *buf = NULL;
    size_t  pchar = 0;
    size_t  pbyte = 0;
    size_t     nb = 0;
    int64_t    nr = 0;

    // initialize the fields of the FILE_DATA structure.
    ZeroMemory(data, sizeof(FILE_DATA));

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
    // retrieve the file size, and use that to allocate a buffer for the file data.
    if (!GetFileSizeEx(fd, &file_size))
    {
        ConsoleError("ERROR: %S(%u): Failed to retrieve file size for input file \"%S\" (%08X).\n", __FUNCTION__, GetCurrentThreadId(), path, GetLastError());
        goto cleanup_and_fail;
    }
    if ((nb = (size_t) file_size.QuadPart) == 0 || (buf = malloc(nb)) == NULL)
    {
        ConsoleError("ERROR: %S(%u): Failed to allocate %Iu byte input buffer for file \"%S\".\n", __FUNCTION__, GetCurrentThreadId(), nb, path);
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
    // the file was successfully read, so clean up and set the fields on the FILE_DATA.
    CloseHandle(fd);
    free(wpath);
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
    if (wpath != NULL) free(wpath);
    return -1;
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

