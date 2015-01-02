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
common_convert(int argc, const char* argv[], int(*func)(ygor_data_logger* dl, const char* line))
{
    const char* input = "json";
    const char* output = "benchmark.dat.bz2";

    e::argparser ap;
    ap.autohelp();
    ap.arg().name('i', "input")
            .description("json to read for conversion (default: json)")
            .as_string(&input);
    ap.arg().name('o', "output")
            .description("data file for benchmark results (default: benchmark.dat.bz2)")
            .as_string(&output);

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    FILE* inputf = fopen(input, "r");

    if (!inputf)
    {
        std::cerr << "could not open input file: " << strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }

    // record start times at ms-level; latency at us-level
    ygor_data_logger* dl = ygor_data_logger_create(output, 1000*1000, 1000);

    if (!dl)
    {
        std::cerr << "could not open data file: " << strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }

    while (true)
    {
        char* line = NULL;
        size_t line_sz = 0;
        ssize_t amt = getline(&line, &line_sz, inputf);

        if (amt < 0)
        {
            if (feof(inputf) != 0)
            {
                break;
            }

            std::cerr << "could not read from input: " << strerror(ferror(inputf)) << std::endl;
            return EXIT_FAILURE;
        }

        if (!line)
        {
            continue;
        }

        e::guard line_guard = e::makeguard(free, line);
        (void) line_guard;

        if (amt < 1)
        {
            continue;
        }

        line[amt - 1] = '\0';

        if (func(dl, line) < 0)
        {
            std::cerr << "aborting because conversion failed" << std::endl;
            return EXIT_FAILURE;
        }
    }

    if (ygor_data_logger_flush_and_destroy(dl) < 0)
    {
        std::cerr << "could not close data file: " << strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
