/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/noncopyable_function.hh>
#include "seastarx.hh"

/// \brief Creates an input_stream to read from a supplied buffer
///
/// \param buf Buffer to return from the stream while reading
/// \return resulting input stream
input_stream<char> make_buffer_input_stream(temporary_buffer<char>&& buf);

/// \brief Creates an input_stream to read from a supplied buffer
///
/// \param buf Buffer to return from the stream while reading
/// \param limit_generator Generates limits of chunk sizes
/// \return resulting input stream
input_stream<char> make_buffer_input_stream(temporary_buffer<char>&& buf,
                                            seastar::noncopyable_function<size_t()>&& limit_generator);
