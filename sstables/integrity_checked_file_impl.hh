/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

#include <algorithm>
#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include "bytes.hh"
#include "log.hh"

namespace sstables {

extern logging::logger sstlog;

class integrity_checked_file_impl : public file_impl {
public:
    integrity_checked_file_impl(sstring fname, file f);

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override;

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override;

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return get_file_impl(_file)->read_dma(pos, buffer, len, pc);
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return get_file_impl(_file)->read_dma(pos, iov, pc);
    }

    virtual future<> flush(void) override {
        return get_file_impl(_file)->flush();
    }

    virtual future<struct stat> stat(void) override {
        return get_file_impl(_file)->stat();
    }

    virtual future<> truncate(uint64_t length) override {
        return get_file_impl(_file)->truncate(length);
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return get_file_impl(_file)->discard(offset, length);
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return get_file_impl(_file)->allocate(position, length);
    }

    virtual future<uint64_t> size(void) override {
        return get_file_impl(_file)->size();
    }

    virtual future<> close() override {
        return get_file_impl(_file)->close();
    }

    // returns a handle for plain file, so make_checked_file() should be called
    // on file returned by handle.
    virtual std::unique_ptr<seastar::file_handle_impl> dup() override {
        return get_file_impl(_file)->dup();
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return get_file_impl(_file)->list_directory(next);
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        return get_file_impl(_file)->dma_read_bulk(offset, range_size, pc);
    }
private:
    sstring _fname;
    file _file;
};

inline file make_integrity_checked_file(sstring name, file f);

inline open_flags adjust_flags_for_integrity_checked_file(open_flags flags);

future<file>
open_integrity_checked_file_dma(sstring name, open_flags flags, file_open_options options);

future<file>
open_integrity_checked_file_dma(sstring name, open_flags flags);

}
