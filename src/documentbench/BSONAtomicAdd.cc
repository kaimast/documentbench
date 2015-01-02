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
#include <bson.h>
#include <ygor.h>
#include "atomic_add.h"

int
bson_atomic_add(ygor_data_logger* dl, const char* line, const char* field)
{
    bson_error_t err;
    bson_t* tmp = bson_new_from_json(reinterpret_cast<const uint8_t*>(line), strlen(line), &err);

    if (!tmp)
    {
        std::cerr << "could not convert json to binary" << std::endl;
        return -1;
    }

    const unsigned char* binary = bson_get_data(tmp);
    size_t binary_sz = tmp->len;

    // benchmark the atomic add, from a byte string of binary json, to another
    // byte string of binary json
    ygor_data_record dr;
    dr.series = SERIES_ATOMIC_ADD;
    ygor_data_logger_start(dl, &dr);

    bson_t* bson = bson_new_from_data(binary, binary_sz);

    if (!bson)
    {
        std::cerr << "could not construct bson from data" << std::endl;
        return -1;
    }

    bson_iter_t iter;

    if (!bson_iter_init(&iter, bson))
    {
        std::cerr << "could not initialize integer" << std::endl;
        return -1;
    }

    bson_iter_t needle;

    if (!bson_iter_find_descendant(&iter, field, &needle))
    {
        // doesn't have the field; skip it without error
        return 0;
    }

    int64_t num = 0;

    if (BSON_ITER_HOLDS_INT64(&needle))
    {
        num = bson_iter_int64_unsafe(&needle) + 1;
    }
    else if (BSON_ITER_HOLDS_INT32(&needle))
    {
        num = bson_iter_int32_unsafe(&needle) + 1;
    }
    else
    {
        // field is not integer
        return 0;
    }

    bson_iter_overwrite_int64(&needle, num);
    bson_destroy(bson);
    ygor_data_logger_finish(dl, &dr);

    if (ygor_data_logger_record(dl, &dr) < 0)
    {
        std::cerr << "data logger failed: " << strerror(errno) << std::endl;
        return -1;
    }

    // cleanup
    bson_destroy(tmp);
    return 0;
}

int
main(int argc, const char* argv[])
{
    return common_atomic_add(argc, argv, bson_atomic_add);
}
