// Copyright (c) 2014, Robert Escriva
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of this project nor the names of its contributors may
//       be used to endorse or promote products derived from this software
//       without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <e/guard.h>
#include <treadstone.h>
#include <ygor.h>
#include "conversion.h"

int
treadstone_convert(ygor_data_logger* dl, const char* line)
{
    unsigned char* binary = NULL;
    size_t binary_sz = 0;
    ygor_data_record dr;

    // benchmark json->binary
    dr.series = SERIES_JSON_TO_BINARY;
    ygor_data_logger_start(dl, &dr);

    if (treadstone_json_to_binary(line, &binary, &binary_sz) < 0)
    {
        std::cerr << "failure on json->binary conversion" << std::endl;
        return -1;
    }

    ygor_data_logger_finish(dl, &dr);

    if (ygor_data_logger_record(dl, &dr) < 0)
    {
        std::cerr << "data logger failed: " << strerror(errno) << std::endl;
        return -1;
    }

    // benchmark binary->json
    dr.series = SERIES_BINARY_TO_JSON;
    ygor_data_logger_start(dl, &dr);

    char* json = NULL;

    if (treadstone_binary_to_json(binary, binary_sz, &json) < 0)
    {
        std::cerr << "failure on binary->json conversion" << std::endl;
        return -1;
    }

    ygor_data_logger_finish(dl, &dr);

    if (ygor_data_logger_record(dl, &dr) < 0)
    {
        std::cerr << "data logger failed: " << strerror(errno) << std::endl;
        return -1;
    }

    // benchmark size of resulting binary
    dr.series = SERIES_BINARY_SIZE;
    ygor_data_logger_start(dl, &dr);
    dr.data = binary_sz;

    if (ygor_data_logger_record(dl, &dr) < 0)
    {
        std::cerr << "data logger failed: " << strerror(errno) << std::endl;
        return -1;
    }

    // cleanup
    free(binary);
    free(json);
    return 0;
}

int
main(int argc, const char* argv[])
{
    return common_convert(argc, argv, treadstone_convert);
}
