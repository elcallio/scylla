/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include "gms/inet_address.hh"
#include "streaming/stream_summary.hh"
#include "streaming/stream_session_state.hh"
#include "streaming/progress_info.hh"
#include <vector>
#include <map>

namespace streaming {

/**
 * Stream session info.
 */
class session_info {
public:
    using inet_address = gms::inet_address;
    inet_address peer;
    /** Immutable collection of receiving summaries */
    std::vector<stream_summary> receiving_summaries;
    /** Immutable collection of sending summaries*/
    std::vector<stream_summary> sending_summaries;
    /** Current session state */
    stream_session_state state;

    std::map<sstring, progress_info> receiving_files;
    std::map<sstring, progress_info> sending_files;

    session_info() = default;
    session_info(inet_address peer_,
                 std::vector<stream_summary> receiving_summaries_,
                 std::vector<stream_summary> sending_summaries_,
                 stream_session_state state_)
        : peer(peer_)
        , receiving_summaries(std::move(receiving_summaries_))
        , sending_summaries(std::move(sending_summaries_))
        , state(state_) {
    }

    bool is_failed() const {
        return state == stream_session_state::FAILED;
    }

    /**
     * Update progress of receiving/sending file.
     *
     * @param newProgress new progress info
     */
    void update_progress(progress_info new_progress);

    std::vector<progress_info> get_receiving_files() const;

    std::vector<progress_info> get_sending_files() const;

    /**
     * @return total number of files already received.
     */
    long get_total_files_received() const {
        return get_total_files_completed(get_receiving_files());
    }

    /**
     * @return total number of files already sent.
     */
    long get_total_files_sent() const {
        return get_total_files_completed(get_sending_files());
    }

    /**
     * @return total size(in bytes) already received.
     */
    long get_total_size_received() const {
        return get_total_size_in_progress(get_receiving_files());
    }

    /**
     * @return total size(in bytes) already sent.
     */
    long get_total_size_sent() const {
        return get_total_size_in_progress(get_sending_files());
    }

    /**
     * @return total number of files to receive in the session
     */
    long get_total_files_to_receive() const {
        return get_total_files(receiving_summaries);
    }

    /**
     * @return total number of files to send in the session
     */
    long get_total_files_to_send() const {
        return get_total_files(sending_summaries);
    }

    /**
     * @return total size(in bytes) to receive in the session
     */
    long get_total_size_to_receive() const {
        return get_total_sizes(receiving_summaries);
    }

    /**
     * @return total size(in bytes) to send in the session
     */
    long get_total_size_to_send() const {
        return get_total_sizes(sending_summaries);
    }

private:
    long get_total_size_in_progress(std::vector<progress_info> files) const;

    long get_total_files(std::vector<stream_summary> const& summaries) const;

    long get_total_sizes(std::vector<stream_summary> const& summaries) const;

    long get_total_files_completed(std::vector<progress_info> files) const;
};

} // namespace streaming
