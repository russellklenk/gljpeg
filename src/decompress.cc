/*/////////////////////////////////////////////////////////////////////////////
/// @summary Decompress a JPEG file from memory into a memory buffer.
///////////////////////////////////////////////////////////////////////////80*/

/*//////////////////
//   Data Types   //
//////////////////*/
/// @summary Forward-declare the publicly-visible types.
struct JPEG_DECOMPRESS_STATE;
struct JPEG_DECOMPRESS_ERROR;
struct JPEG_DECOMPRESS_INPUT;
struct JPEG_DECOMPRESS_OUTPUT;

/// @summary Define the data associated with a custom libjpeg decompressor state.
struct IJG_DECOMPRESS_STATE
{
    jpeg_decompress_struct Config;       /// The base libjpeg decompressor state. Must be the first field in the structure.
    jpeg_error_mgr         ErrorMgr;     /// The custom libjpeg error manager used to handle errors reported by libjpeg.
    jmp_buf                ErrorJmp;     /// The setjmp state data defining the error handler location.
    JPEG_DECOMPRESS_STATE *State;        /// A pointer back to the main application-visible state.
};

/// @summary Define the data returned by JpegGetDecompressError.
struct JPEG_DECOMPRESS_ERROR
{   static size_t const    ERRBUF          = 200;
    size_t                 WarningCount; /// The number of warnings reported by the decompressor.
    int                    LastError;    /// An integer identifier of the most recent error.
    char                   Error[ERRBUF];/// The formatted error string for the most recent error.
};

/// @summary Define the data representing a compressed input stream.
struct JPEG_DECOMPRESS_INPUT
{
    void const            *Buffer;       /// A pointer to the first byte of JPEG input data.
    size_t                 DataSize;     /// The maximum number of bytes to read from the input buffer.
};

/// @summary Define the data describing a decompressed image.
struct JPEG_DECOMPRESS_OUTPUT
{
    uint8_t               *Buffer;       /// A pointer to the first byte of decompressed output data. Set by the application.
    size_t                 DataSize;     /// The maximum number of bytes to write to the output buffer. Set by the application.
    size_t                 RowStride;    /// The number of bytes between rows in the decompressed image. Set by the decompressor; may be modified by the application.
    size_t                 ImageWidth;   /// The width of the image, in pixels. Set by the decompressor.
    size_t                 ImageHeight;  /// The height of the image, in pixels. Set by the decompressor.
    size_t                 ChannelSize;  /// The number of bytes in a data channel sample value. Set by the decompressor.
    size_t                 ChannelCount; /// The number of data channels comprising a single pixel. Set by the decompressor.
};

/// @summary Define the data associated with a JPEG decompressor instance.
struct JPEG_DECOMPRESS_STATE
{
    IJG_DECOMPRESS_STATE   Codec;        /// The libjpeg decompressor configuration.
    JPEG_DECOMPRESS_INPUT  Input;        /// The input buffer containing the compressed JPEG data.
    JPEG_DECOMPRESS_OUTPUT Output;       /// Information about the image being decompressed.
};

/*///////////////
//   Globals   //
///////////////*/

/*//////////////////////////
//   Internal Functions   //
//////////////////////////*/
/// @summary Callback function used to return control to application code, instead of exiting, when an error is reported by libjpeg.
/// @param cinfo A pointer to an instance of IJG_DECOMPRESS_STATE.
internal_function void
JpegDecompress_ErrorExit
(
    j_common_ptr cinfo
)
{
    IJG_DECOMPRESS_STATE *argp = (IJG_DECOMPRESS_STATE*) cinfo;
    longjmp(argp->ErrorJmp, 1);
}

/// @summary Callback used to report an error or warning message. These messages are suppressed.
/// @param cinfo A pointer to an instance of IJG_DECOMPRESS_STATE.
internal_function void
JpegDecompress_OutputMessage
(
    j_common_ptr cinfo
)
{
    UNREFERENCED_PARAMETER(cinfo);
}

/*////////////////////////
//   Public Functions   //
////////////////////////*/
/// @summary Initialize a JPEG_DECOMPRESS_INPUT structure.
/// @param input The JPEG_DECOMPRESS_INPUT structure to initialize.
/// @param buffer A pointer to the first byte of the compressed JPEG data.
/// @param buffer_size The maximum number of bytes to read from input buffer.
/// @return Zero if successful, or -1 if an error occurred.
public_function int
JpegInitDecompressInput
(
    JPEG_DECOMPRESS_INPUT *input,
    void const           *buffer, 
    size_t           buffer_size
)
{
    if (buffer == NULL)
    {
        ConsoleError("ERROR: %S(%u): Invalid input buffer specified.\n", __FUNCTION__, GetCurrentThreadId());
        return -1;
    }
    if (buffer_size < 107)
    {
        ConsoleError("ERROR: %S(%u): Input buffer size of %Iu bytes is too small.\n", __FUNCTION__, GetCurrentThreadId(), buffer_size);
        return -1;
    }
    input->Buffer   = buffer;
    input->DataSize = buffer_size;
    return 0;
}

/// @summary Initialize a JPEG_DECOMPRESS_OUTPUT structure before decompressing any data.
/// @param output The JPEG_DECOMPRESS_OUTPUT structure to initialize.
/// @return Zero if successful, or -1 if an error occurred.
public_function int
JpegInitDecompressOutput
(
    JPEG_DECOMPRESS_OUTPUT *output
)
{
    ZeroMemory(output, sizeof(JPEG_DECOMPRESS_OUTPUT));
    return 0;
}

/// @summary Initialize a JPEG decompressor. This function must be called for each image to be decompressed.
/// @param state The JPEG_DECOMPRESS_STATE to initialize.
/// @param input A JPEG_DECOMPRESS_INPUT, initialized with JpegInitDecompressInput, specifying the compressed JPEG data.
/// @return Zero if successful, or -1 if an error occurred.
public_function int
JpegInitDecompressState
(
    JPEG_DECOMPRESS_STATE       *state,
    JPEG_DECOMPRESS_INPUT const *input
)
{   // initialize all fields of the decompressor.
    ZeroMemory( state, sizeof(JPEG_DECOMPRESS_STATE));
    CopyMemory(&state->Input, input, sizeof(JPEG_DECOMPRESS_INPUT));
    state->Codec.State = state;

    // set up libjpeg error handling.
    state->Codec.Config.err = jpeg_std_error(&state->Codec.ErrorMgr);
    state->Codec.ErrorMgr.output_message = JpegDecompress_OutputMessage;
    state->Codec.ErrorMgr.error_exit = JpegDecompress_ErrorExit;
    if (setjmp(state->Codec.ErrorJmp))
    {   // the libjpeg code signaled an error during one of the routines below.
        ConsoleError("ERROR: %S(%u): libjpeg signaled an error: %d.\n", __FUNCTION__, GetCurrentThreadId(), state->Codec.ErrorMgr.msg_code);
        return -1;
    }

    // initialize the libjpeg decompressor state.
    jpeg_create_decompress(&state->Codec.Config);
    jpeg_mem_src(&state->Codec.Config, (unsigned char const *) input->Buffer, (unsigned long) input->DataSize);
    return 0;
}

/// @summary Read data from the current input buffer until the image attributes are available or an error occurs.
/// @param output On return, the image attributes are written to this location, including default values for DataSize and RowStride. The application may then set the Buffer, DataSize and RowStride fields.
/// @param state The JPEG_DECOMPRESS_STATE to update.
/// @return Zero if the image attributes were read successfully, or -1 if an error occurred.
public_function int
JpegReadImageAttributes
(
    JPEG_DECOMPRESS_OUTPUT *output, 
    JPEG_DECOMPRESS_STATE   *state
)
{   // set up libjpeg error handling.
    if (setjmp(state->Codec.ErrorJmp))
    {   // the libjpeg code signaled an error during one of the routines below.
        ConsoleError("ERROR: %S(%u): libjpeg signaled an error: %d.\n", __FUNCTION__, GetCurrentThreadId(), state->Codec.ErrorMgr.msg_code);
        ZeroMemory(&state->Output, sizeof(JPEG_DECOMPRESS_OUTPUT));
        ZeroMemory(output, sizeof(JPEG_DECOMPRESS_OUTPUT));
        return -1;
    }

    // read data from the input buffer up to the start of the compressed image data.
    switch (jpeg_read_header(&state->Codec.Config, TRUE))
    {
        case JPEG_SUSPENDED:
            { // insufficient data in the input buffer.
              ConsoleError("ERROR: %S(%u): Insufficient data in JPEG data buffer %p (%Iu bytes.)\n", __FUNCTION__, GetCurrentThreadId(), state->Input.Buffer, state->Input.DataSize);
              ZeroMemory(&state->Output, sizeof(JPEG_DECOMPRESS_OUTPUT));
              ZeroMemory(output, sizeof(JPEG_DECOMPRESS_OUTPUT));
            } return -1;
        case JPEG_HEADER_OK:
            { // header information and image dimensions are known.
              // set up decompression parameters here, such as dct_method.
              jpeg_calc_output_dimensions  (&state->Codec.Config);
              output->DataSize     = (size_t)state->Codec.Config.output_width * (size_t)state->Codec.Config.output_components * sizeof(JSAMPLE) * (size_t)state->Codec.Config.output_height;
              output->RowStride    = (size_t)state->Codec.Config.output_width * (size_t)state->Codec.Config.output_components * sizeof(JSAMPLE);
              output->ImageWidth   = (size_t)state->Codec.Config.output_width;
              output->ImageHeight  = (size_t)state->Codec.Config.output_height;
              output->ChannelSize  =  sizeof(JSAMPLE);
              output->ChannelCount = (size_t)state->Codec.Config.output_components;
              // save data to an internal JPEG_DECOMPRESS_OUTPUT.
              CopyMemory(&state->Output, output, sizeof(JPEG_DECOMPRESS_OUTPUT));
              state->Output.Buffer = NULL;
            } return  0;
        case JPEG_HEADER_TABLES_ONLY:
            { // tables-only data streams are not supported.
              ConsoleError("ERROR: %S(%u): JPEG data buffer %p (%Iu bytes) contains no image data.\n", __FUNCTION__, GetCurrentThreadId(), state->Input.Buffer, state->Input.DataSize);
              ZeroMemory(&state->Output, sizeof(JPEG_DECOMPRESS_OUTPUT));
              ZeroMemory(output, sizeof(JPEG_DECOMPRESS_OUTPUT));
            } return -1;
        default:
            { // unknown return value.
              ConsoleError("ERROR: %S(%u): Unknown return value from jpeg_read_header.\n", __FUNCTION__, GetCurrentThreadId());
              ZeroMemory(&state->Output, sizeof(JPEG_DECOMPRESS_OUTPUT));
              ZeroMemory(output, sizeof(JPEG_DECOMPRESS_OUTPUT));
            } return -1;
    }
}

/// @summary Retrieve information about the current input buffer.
/// @param input On return, information about the current input buffer is copied to this location.
/// @param state The JPEG_DECOMPRESS_STATE to query.
public_function void
JpegGetDecompressInput
(
    JPEG_DECOMPRESS_INPUT *input, 
    JPEG_DECOMPRESS_STATE *state
)
{
    CopyMemory(input, &state->Input, sizeof(JPEG_DECOMPRESS_INPUT));
}

/// @summary Retrieve information about the current image and output buffer.
/// @param output On return, information about the image being decompressed and the output buffer is copied to this location.
/// @param state The JPEG_DECOMPRESS_STATE to query.
public_function void
JpegGetDecompressOutput
(
    JPEG_DECOMPRESS_OUTPUT *output, 
    JPEG_DECOMPRESS_STATE   *state
)
{
    CopyMemory(output, &state->Output, sizeof(JPEG_DECOMPRESS_OUTPUT));
}

/// @summary Retrieve the most recent error generated by the decompressor.
/// @param error The JPEG_DECOMPRESS_ERROR to populate.
/// @param state The JPEG_DECOMPRESS_STATE to query.
/// @return Zero if no error occurred, or 1 if an error occurred during decompression.
public_function int
JpegGetDecompressError
(
    JPEG_DECOMPRESS_ERROR *error, 
    JPEG_DECOMPRESS_STATE *state
)
{
    if (state->Codec.ErrorMgr.msg_code == 0)
    {   // there's no error to return.
        error->WarningCount =(size_t) state->Codec.ErrorMgr.num_warnings;
        error->LastError    = state->Codec.ErrorMgr.msg_code;
        error->Error[0]     = 0;
        return 0;
    }
    else
    {   // format the error message string.
        state->Codec.ErrorMgr.format_message((j_common_ptr) &state->Codec.Config, error->Error);
        error->WarningCount =(size_t) state->Codec.ErrorMgr.num_warnings;
        error->LastError    = state->Codec.ErrorMgr.msg_code;
        return 1;
    }
}

/// @summary Decompress JPEG-compressed image data once image attributes have been read.
/// @param output The JPEG_DECOMPRESS_OUTPUT populated by JpegReadImageAttributes, with the Buffer (and optionally DataSize and RowStride) fields set.
/// @param state The JPEG_DECOMPRESS_STATE object to use for decompression.
/// @return Zero if the image data was successfully decompressed, or -1 if an error occurred.
public_function int
JpegDecompressImageData
(
    JPEG_DECOMPRESS_OUTPUT *output, 
    JPEG_DECOMPRESS_STATE   *state
)
{   
    JDIMENSION const MAX_SCANS  = 16;
    JSAMPLE   *scans[MAX_SCANS] = {};
    JDIMENSION      scan_count  = 0;

    // ensure that the output buffer has been specified, and that it is of at least the minimum required size.
    if (state->Output.DataSize == 0)
    {
        ConsoleError("ERROR: %S(%u): Output data size not set. Call JpegReadImageAttributes first.\n", __FUNCTION__, GetCurrentThreadId());
        return -1;
    }
    if (output->Buffer == NULL)
    {
        ConsoleError("ERROR: %S(%u): Invalid output buffer for image %p (%Iu bytes.)\n", __FUNCTION__, GetCurrentThreadId(), state->Input.Buffer, state->Input.DataSize);
        return -1;
    }
    if (output->DataSize < state->Output.DataSize)
    {
        ConsoleError("ERROR: %S(%u): Output buffer too small, %Iu bytes required.\n", __FUNCTION__, GetCurrentThreadId(), state->Output.DataSize);
        return -1;
    }
    if (output->RowStride < state->Output.RowStride)
    {
        ConsoleError("ERROR: %S(%u): Output row stride too small, expected %Iu bytes or greater.\n", __FUNCTION__, GetCurrentThreadId(), state->Output.RowStride);
        return -1;
    }
    // set up libjpeg error handling.
    if (setjmp(state->Codec.ErrorJmp))
    {   // the libjpeg code signaled an error during one of the routines below.
        ConsoleError("ERROR: %S(%u): libjpeg signaled an error: %d.\n", __FUNCTION__, GetCurrentThreadId(), state->Codec.ErrorMgr.msg_code);
        return -1;
    }

    // initialize the output and set up the decompression context.
    if (!jpeg_start_decompress(&state->Codec.Config))
    {
        ConsoleError("ERROR: %S(%u): Failed to start decompression. Check decompression parameters.\n", __FUNCTION__, GetCurrentThreadId());
        return -1;
    }
    // initialize the scan_count and scans array according to the recommended output buffer height.
    if (state->Codec.Config.rec_outbuf_height <= MAX_SCANS)
    {   // use the recommended value; typically 1, 2 or 4 scanlines decompressed per call to jpeg_read_scanlines.
        scan_count = state->Codec.Config.rec_outbuf_height;
    }
    else
    {   // limit the number of scanlines decompressed per call to jpeg_read_scanlines.
        scan_count = MAX_SCANS;
    }
    for (JDIMENSION i = 0; i < scan_count; ++i)
    {
        scans[i] = ((uint8_t*) output->Buffer) + (i * output->RowStride);
    }

    // decompress the image data one or more scanlines at a time.
    while (state->Codec.Config.output_scanline < state->Codec.Config.output_height)
    {
        JDIMENSION    n = jpeg_read_scanlines(&state->Codec.Config, scans, scan_count);
        for (scan_count = 0; scan_count < n; ++scan_count)
        {   // compute the next destination scanlines. if these are off the end 
            // of the image, that's fine; the pointers won't actually be dereferenced.
            scans[scan_count] = ((uint8_t*)output->Buffer) + ((state->Codec.Config.output_scanline + scan_count) * output->RowStride);
        }
    }

    // all image data has been decompressed.
    if (jpeg_finish_decompress(&state->Codec.Config))
    {   // copy output buffer related information to internal state.
        state->Output.Buffer    = output->Buffer;
        state->Output.DataSize  = output->DataSize;
        state->Output.RowStride = output->RowStride;
        return 0;
    }
    else
    {
        ConsoleError("ERROR: %S(%u): JPEG decompression failed. Check the last reported error.\n", __FUNCTION__, GetCurrentThreadId());
        return -1;
    }
}

/// @summary Free any memory allocated internally for a JPEG decompression state, not including any input or output buffers.
/// @param state The JPEG_DECOMPRESS_STATE to free. Use JpegInitDecompressState to re-initialize the decompressor for a new image.
/// @param input On return, the input buffer and size are copied to this location.
/// @param output On return, the output buffer and image attributes are copied to this location.
public_function void
JpegDestroyDecompressState
(
    JPEG_DECOMPRESS_STATE   *state, 
    JPEG_DECOMPRESS_INPUT   *input, 
    JPEG_DECOMPRESS_OUTPUT *output
)
{
    if (input != NULL)
    {   // the caller requests a copy of the input buffer attributes.
        CopyMemory(input, &state->Input, sizeof(JPEG_DECOMPRESS_INPUT));
    }
    if (output != NULL)
    {   // the caller requests a copy of the output buffer attributes.
        CopyMemory(output, &state->Output, sizeof(JPEG_DECOMPRESS_OUTPUT));
    }
    jpeg_destroy_decompress(&state->Codec.Config);
    ZeroMemory(&state->Codec.Config, sizeof(jpeg_decompress_struct));
}

