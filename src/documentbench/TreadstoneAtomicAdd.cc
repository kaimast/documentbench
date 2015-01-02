// Copyright (c) 2015, Robert Escriva
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
#include "atomic_add.h"

int
treadstone_atomic_add(ygor_data_logger* dl, const char* line, const char* field)
{
    unsigned char* binary = NULL;
    size_t binary_sz = 0;

    if (treadstone_json_to_binary(line, &binary, &binary_sz) < 0)
    {
        std::cerr << "failure on json->binary conversion" << std::endl;
        return -1;
    }

    e::guard g1 = e::makeguard(free, binary);
    (void) g1;

    // benchmark the atomic add, from a byte string of binary json, to another
    // byte string of binary json
    ygor_data_record dr;
    dr.series = SERIES_ATOMIC_ADD;
    ygor_data_logger_start(dl, &dr);

    struct treadstone_transformer* trans = NULL;
    trans = treadstone_transformer_create(binary, binary_sz);
    e::guard g2 = e::makeguard(treadstone_transformer_destroy, trans);
    (void) g2;

    if (!trans)
    {
        std::cerr << "could not create treadstone transformer" << std::endl;
        return -1;
    }

    unsigned char* value;
    size_t value_sz;

    if (treadstone_transformer_extract_value(trans, field, &value, &value_sz) < 0)
    {
        // doesn't have the field; skip it without error
        return 0;
    }

    if (treadstone_binary_is_integer(value, value_sz) < 0)
    {
        // field is not integer
        free(value);
        return 0;
    }

    int64_t num = treadstone_binary_to_integer(value, value_sz);
    free(value);
    ++num;

    if (treadstone_integer_to_binary(num, &value, &value_sz) < 0)
    {
        std::cerr << "could not convert integer to binary" << std::endl;
        return -1;
    }

    if (treadstone_transformer_set_value(trans, field, value, value_sz) < 0)
    {
        std::cerr << "could not overwrite integer" << std::endl;
        return -1;
    }

    free(value);
    unsigned char* output = NULL;
    size_t output_sz = 0;

    if (treadstone_transformer_output(trans, &output, &output_sz) < 0)
    {
        std::cerr << "could not output binary" << std::endl;
        return -1;
    }

    treadstone_transformer_destroy(trans);
    g2.dismiss();
    free(output);
    ygor_data_logger_finish(dl, &dr);

    if (ygor_data_logger_record(dl, &dr) < 0)
    {
        std::cerr << "data logger failed: " << strerror(errno) << std::endl;
        return -1;
    }

    return 0;
}

int
main(int argc, const char* argv[])
{
    return common_atomic_add(argc, argv, treadstone_atomic_add);
}
