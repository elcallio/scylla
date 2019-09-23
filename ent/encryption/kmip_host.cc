/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */
#include <deque>
#include <unordered_map>
#include <regex>
#include <algorithm>

#include <seastar/net/dns.hh>
#include <seastar/net/api.hh>
#include <seastar/net/tls.hh>
#include <seastar/core/thread.hh>

// workaround cryptsoft sdk issue:
#define strcasestr  kmip_strcasestr
#include <kmip_os.h>
#include <kmip.h>
#undef strcasestr

#include "kmip_host.hh"
#include "encryption.hh"
#include "symmetric_key.hh"
#include "utils/hash.hh"
#include "utils/loading_cache.hh"
#include "utils/UUID.hh"
#include "utils/UUID_gen.hh"
#include "db/config.hh"

using namespace std::chrono_literals;

static logger kmip_log("kmip");
static constexpr int kmip_port = 5696;
// default for command execution/failover retry.
static constexpr int default_num_cmd_retry = 3;

std::ostream& operator<<(std::ostream& os, KMIP* kmip) {
    auto* s = KMIP_dump_str(kmip, KMIP_DUMP_FORMAT_DEFAULT);
    os << s;
    free(s);
    return os;
}

namespace encryption {

bool operator==(const kmip_host::key_options& l, const kmip_host::key_options& r) {
    return std::tie(l.template_name, l.key_namespace) == std::tie(r.template_name, r.key_namespace);
}

class kmip_error_category : public std::error_category {
public:
    constexpr kmip_error_category() noexcept : std::error_category{} {}
    const char * name() const noexcept {
        return "KMIP";
    }
    std::string message(int error) const {
        return KMIP_error2string(error);
    }
};

static const kmip_error_category kmip_errorc;

class kmip_error : public std::system_error {
public:
    kmip_error(int res)
        : system_error(res, kmip_errorc)
    {}
    kmip_error(int res, const std::string& msg)
        : system_error(res, kmip_errorc, msg)
    {}
};

// Checks a gnutls return value.
// < 0 -> error.
static void kmip_chk(int res, KMIP_CMD * cmd = nullptr) {
    if (res != KMIP_ERROR_NONE) {
        int status=0, reason=0;
        char* message = nullptr;

        if (KMIP_CMD_get_result(cmd, &status, &reason, &message) == KMIP_ERROR_NONE) {
            auto* ctxt = cmd != nullptr ? KMIP_CMD_get_ctx(cmd) : "(unknown cmd)";
            auto s = sprint("%s: status=%s, reason=%s, message=%s",
                            ctxt,
                            KMIP_RESULT_STATUS_to_string(status, 0, nullptr),
                            KMIP_RESULT_REASON_to_string(reason, 0, nullptr),
                            message ? message : "<none>"
                            );
            throw kmip_error(res, s);
        }
        throw kmip_error(res);
    }
}


class kmip_host::impl {
public:
    struct kmip_key_info {
        key_info info;
        key_options options;
        bool operator==(const kmip_key_info& i) const {
            return info == i.info && options == i.options;
        }
        friend std::ostream& operator<<(std::ostream& os, const kmip_key_info& info) {
            return os << info.info << ":" << info.options;
        }
    };
    struct kmip_key_info_hash {
        size_t operator()(const kmip_key_info& i) const {
            return utils::tuple_hash()(
                            std::tie(i.info.alg, i.info.len,
                                            i.options.template_name,
                                            i.options.key_namespace));
        }
    };

    using key_and_id_type = std::tuple<shared_ptr<symmetric_key>, id_type>;

    inline static constexpr std::chrono::milliseconds default_expiry = 30s;
    inline static constexpr std::chrono::milliseconds default_refresh = 100s;
    inline static constexpr uintptr_t max_hosts = 1<<8;

    inline static constexpr size_t def_max_pooled_connections_per_host = 8;

    impl(encryption_context& ctxt, const sstring& name, const host_options& options)
                    : _ctxt(ctxt), _name(name), _options(options), _attr_cache(
                                    std::numeric_limits<size_t>::max(),
                                    options.key_cache_expiry.value_or(
                                                    default_expiry),
                                    default_refresh, kmip_log,
                                    std::bind(&impl::create_key, this,
                                                    std::placeholders::_1)), _id_cache(
                                    std::numeric_limits<size_t>::max(),
                                    options.key_cache_expiry.value_or(
                                                    default_expiry),
                                    default_refresh, kmip_log,
                                    std::bind(&impl::find_key, this,
                                                    std::placeholders::_1)),
                                    _max_retry(std::max(size_t(1), options.max_command_retries.value_or(default_num_cmd_retry)))

    {
        if (_options.hosts.size() > max_hosts) {
            throw std::invalid_argument("Too many hosts");
        }
    }

    future<> connect();
    future<shared_ptr<symmetric_key>, id_type> get_or_create_key(const key_info&, const key_options& = {});
    future<shared_ptr<symmetric_key>> get_key_by_id(const id_type&, const std::optional<key_info>& = {});

private:
    future<key_and_id_type> create_key(const kmip_key_info&);
    future<shared_ptr<symmetric_key>> find_key(const id_type&);
    future<std::vector<id_type>> find_matching_keys(const kmip_key_info&, std::optional<int> max = {});

    static shared_ptr<symmetric_key> ensure_compatible_key(shared_ptr<symmetric_key>, const key_info&);

    template<typename T, int(*)(T *)>
    class kmip_handle;
    class kmip_cmd;
    class kmip_data_list;
    class connection;

    std::tuple<kmip_data_list, unsigned int> make_attributes(const kmip_key_info&, bool include_template = true) const;

    union userdata {
        void * ptr;
        const char* host;
    };

    friend std::ostream& operator<<(std::ostream& os, const impl& me) {
        return os << me._name;
    }

    using con_ptr = ::shared_ptr<connection>;
    using opt_int = std::optional<int>;

    template<typename Func>
    future<kmip_cmd> do_cmd(kmip_cmd, Func &&);
    template<typename Func>
    future<int> do_cmd(KMIP_CMD*, con_ptr, Func&);

    future<con_ptr> get_connection(KMIP_CMD*);
    future<con_ptr> get_connection(const sstring&);
    void release(KMIP_CMD*, con_ptr);

    size_t max_pooled_connections_per_host() const {
        return _options.max_pooled_connections_per_host.value_or(def_max_pooled_connections_per_host);
    }
    bool is_current_host(const sstring& host) {
        return host == _options.hosts.at(_index);
    }

    encryption_context& _ctxt;
    sstring _name;
    host_options _options;
    utils::loading_cache<kmip_key_info, key_and_id_type,
                    utils::loading_cache_reload_enabled::yes,
                    utils::simple_entry_size<key_and_id_type>,
                    kmip_key_info_hash> _attr_cache;

    utils::loading_cache<id_type, ::shared_ptr<symmetric_key>,
                    utils::loading_cache_reload_enabled::yes,
                    utils::simple_entry_size<::shared_ptr<symmetric_key>>> _id_cache;

    using connections = std::deque<con_ptr>;
    using host_to_connections = std::unordered_map<sstring, connections>;

    host_to_connections _host_connections;
    // current default host. If a host fails, incremented and
    // we try another in the host ip list.
    size_t _index = 0;
    size_t _max_retry = default_num_cmd_retry;
};

class kmip_host::impl::connection {
public:
    connection(const sstring& host, host_options& options)
        : _host(host)
        , _options(options)
    {}
    ~connection()
    {}

    const sstring& host() const {
        return _host;
    }

    void attach(KMIP_CMD*);

    future<> connect();
    future<> wait_for_io();
    future<> close();
private:
    static int io_callback(KMIP*, void*, int, void*, unsigned int, unsigned int*);

    int send(void*, unsigned int, unsigned int*);
    int recv(void*, unsigned int, unsigned int*);

    friend std::ostream& operator<<(std::ostream& os, const connection& me) {
        return os << me._host;
    }

    sstring _host;
    host_options& _options;
    output_stream<char> _output;
    input_stream<char> _input;
    std::optional<temporary_buffer<char>> _in_buffer;
    std::optional<future<>> _pending;
};

future<> kmip_host::impl::connection::connect() {
    auto cred = ::make_shared<seastar::tls::certificate_credentials>();
    auto f = make_ready_future();

    kmip_log.debug("connecting {}", _host);

    if (!_options.priority_string.empty()) {
        cred->set_priority_string(_options.priority_string);
    } else {
        cred->set_priority_string(db::config::default_tls_priority);
    }

    if (!_options.certfile.empty()) {
        f = f.then([this, cred] {
            return cred->set_x509_key_file(_options.certfile, _options.keyfile, seastar::tls::x509_crt_format::PEM);
        });
    }
    if (!_options.truststore.empty()) {
        f = f.then([this, cred] {
            return cred->set_x509_trust_file(_options.truststore, seastar::tls::x509_crt_format::PEM);
        });
    }
    return f.then([this, cred] {
        // TODO, find if we should do hostname verification
        // TODO: connect all failovers already?
        return seastar::net::dns::resolve_name(_host).then([this, cred](seastar::net::inet_address addr) {
            return seastar::tls::connect(cred, seastar::ipv4_addr{addr, kmip_port}).then([this](seastar::connected_socket s) {
                kmip_log.debug("Successfully connected {} ({})", _host);
                // #998 Set keepalive to try avoiding connection going stale inbetween commands. 
                s.set_keepalive_parameters(net::tcp_keepalive_params{60s, 60s, 10});
                s.set_keepalive(true);
                _input = s.input();
                _output = s.output();
            });
        });
    });
}

future<> kmip_host::impl::connection::wait_for_io() {
    kmip_log.trace("{}: Waiting...", *this);
    auto o = std::exchange(_pending, std::nullopt);
    return o ? std::move(*o) : make_ready_future();
}

int kmip_host::impl::connection::send(void* data, unsigned int len, unsigned int*) {
    if (_pending) {
        kmip_log.trace("{}: operation pending...", *this);
        return KMIP_ERROR_RETRY;
    }
    kmip_log.trace("{}: Sending {} bytes", *this, len);

    auto f = _output.write(reinterpret_cast<char *>(data), len).then([this] {
        kmip_log.trace("{}: send done. flushing...", *this);
        return _output.flush();
    });
    // if the call failed already, we still want to
    // drop back to "wait_for_io()", because we cannot throw
    // exceptions through the kmipc code frames.
    if (!f.available() || f.failed()) {
        _pending.emplace(std::move(f));
    }
    return KMIP_ERROR_NONE;
}

int kmip_host::impl::connection::recv(void* data, unsigned int len, unsigned int* outlen) {
    kmip_log.trace("{}: Waiting for data ({})", *this, len);
    for (;;) {
        if (_in_buffer) {
            auto n = std::min(unsigned(_in_buffer->size()), len);
            *outlen = n;
            kmip_log.trace("{}: returning {} ({}) bytes", *this, n, _in_buffer->size());
            std::copy(_in_buffer->begin(), _in_buffer->begin() + n, reinterpret_cast<char *>(data));
            _in_buffer->trim_front(n);
            if (_in_buffer->empty()) {
                _in_buffer = std::nullopt;
            }
            // #998 cryptsoft example returns error on EOF. 
            if (n == 0) {
                return KMIP_ERROR_IO;
            }
            break;
        }

        if (_pending) {
            kmip_log.trace("{}: operation pending...", *this);
            return KMIP_ERROR_RETRY;
        }

        kmip_log.trace("{}: issue read", *this);
        auto f = _input.read().then([this](temporary_buffer<char> buf) {
            kmip_log.trace("{}: got {} bytes", *this, buf.size());
           _in_buffer = std::move(buf);
        });

        // if the call failed already, we still want to
        // drop back to "wait_for_io()", because we cannot throw
        // exceptions through the kmipc code frames.
        if (!f.available() || f.failed()) {
            _pending.emplace(std::move(f));
        }
    }
    return KMIP_ERROR_NONE;
}

int kmip_host::impl::connection::io_callback(KMIP *kmip, void *cb_arg, int op, void *data, unsigned int len, unsigned int *outlen) {
    auto* conn = reinterpret_cast<kmip_host::impl::connection *>(cb_arg);
    try {
        switch(op) {
        default:
            return KMIP_ERROR_NOT_SUPPORTED;
        case KMIP_IO_CMD_SEND:
            return conn->send(data, len, outlen);
        case KMIP_IO_CMD_RECV:
            return conn->recv(data, len, outlen);
        }
    } catch (...) {
        kmip_log.warn("Error in KMIP IO: {}", std::current_exception());
        return KMIP_ERROR_IO;
    }
}

void kmip_host::impl::connection::attach(KMIP_CMD* cmd) {
    kmip_log.trace("{} Attach: {}", *this, cmd);
    if (cmd == nullptr) {
        return;
    }

    if (!_options.username.empty()) {
        kmip_chk(
                        KMIP_CMD_set_credential_username(cmd,
                                        const_cast<char *>(_options.username.c_str()),
                                        const_cast<char *>(_options.password.c_str())));
    }

    /* because we haven't passed in anything to the KMIP_CMD layer
     * that would provide it with the protocol version details we
     * have to separately indicate that here
     */
    kmip_chk(KMIP_CMD_set_lib_protocol(cmd, KMIP_LIB_PROTOCOL_KMIP1));
    /* handle all IO via the callback */
    kmip_chk(
                    KMIP_CMD_set_io_cb(cmd, &connection::io_callback,
                                    reinterpret_cast<void *>(this)));
}

future<> kmip_host::impl::connection::close() {
    return _output.close().finally([this] {
        return _input.close();
    });
}

template<typename T, int(*FreeFunc)(T *)>
class kmip_host::impl::kmip_handle {
public:
    kmip_handle(T * ptr)
        : _ptr(ptr, FreeFunc)
    {}
    kmip_handle(kmip_handle&&) = default;
    kmip_handle& operator=(kmip_handle&&) = default;

    T* get() const {
        return _ptr.get();
    }
    operator T*() const {
        return _ptr.get();
    }
    explicit operator bool() const {
        return _ptr != nullptr;
    }
private:
    using ptr_type = std::unique_ptr<T, int(*)(T *)>;
    ptr_type _ptr;
};

class kmip_host::impl::kmip_cmd : public kmip_handle<KMIP_CMD, &KMIP_CMD_free> {
public:
    kmip_cmd(int flags = KMIP_CMD_FLAGS_DEFAULT|KMIP_CMD_FLAGS_LOG|KMIP_CMD_FLAGS_LOG_XML)
        : kmip_handle([flags] {
        KMIP_CMD* cmd;
        kmip_chk(KMIP_CMD_new_ex(flags, nullptr, &cmd));
        return cmd;
    }())
    {}
    kmip_cmd(kmip_cmd&&) = default;
    kmip_cmd& operator=(kmip_cmd&&) = default;

    friend std::ostream& operator<<(std::ostream& os, const kmip_cmd& cmd) {
        return os << KMIP_CMD_get_request(cmd);
    }
};

class kmip_host::impl::kmip_data_list : public kmip_handle<KMIP_DATA_LIST, &KMIP_DATA_LIST_free> {
public:
    kmip_data_list(int flags = KMIP_DATA_LIST_FLAGS_DEFAULT)
        : kmip_handle([flags] {
        KMIP_DATA_LIST* kdl;
        kmip_chk(KMIP_DATA_LIST_new(flags, &kdl));
        return kdl;
    }())
    {}
    kmip_data_list(kmip_data_list&&) = default;
    kmip_data_list& operator=(kmip_data_list&&) = default;
};

void kmip_host::impl::release(KMIP_CMD* cmd, con_ptr cp) {
    auto i = _host_connections.find(cp->host());
    userdata u;
    u.host = i->first.c_str();
    if (cmd) {
        KMIP_CMD_set_userdata(cmd, u.ptr);
    }
    if (is_current_host(i->first) && max_pooled_connections_per_host() > i->second.size()) {
        i->second.emplace_back(std::move(cp));
    }
}

template<typename Func>
future<int> kmip_host::impl::do_cmd(KMIP_CMD* cmd, con_ptr cp, Func& f) {
    cp->attach(cmd);

    return repeat_until_value([this, cmd, &f, cp] {
        int res = f(cmd);
        switch (res) {
        case KMIP_ERROR_RETRY:
            return cp->wait_for_io().then([] {
                return opt_int();
            }).handle_exception([cp](auto ep) {
                // get here if we had any wire exceptions below.
                // make sure to force flush and stuff here as well.
                return cp->close().then_wrapped([ep = std::move(ep)](auto f) mutable {
                    try {
                        f.get();
                    } catch (...) {
                    }
                    return make_exception_future<opt_int>(std::move(ep));
                });
            });
        case 0:
            release(cmd, cp);
            return make_ready_future<opt_int>(res);
        default:
            // error. connection is dicarded. close it.
            return cp->close().then_wrapped([cp, res](auto f) {
                // ignore any exception thrown from the close.
                // ensure we provide the kmip error instead.
                try {
                    f.get();
                } catch (...) {
                }
                return make_ready_future<opt_int>(res);
            });
        }
    }).finally([cp] {});
}

template<typename Func>
future<kmip_host::impl::kmip_cmd> kmip_host::impl::do_cmd(kmip_cmd cmd_in, Func && f) {
    kmip_log.trace("{}: begin do_cmd", *this, cmd_in);
    KMIP_CMD* cmd = cmd_in;

    // #998 Need to do retry loop, because we can have either timed out connection,
    // lost it (connected server went down) or some other network error.
    return do_with(std::move(f), [this, cmd](Func& f) {
        return repeat_until_value([this, cmd, &f, retry = _max_retry]() mutable {
            --retry;
            return get_connection(cmd).then([this, cmd, &f, retry](con_ptr cp) mutable {
                auto res = do_cmd(cmd, std::move(cp), f);
                kmip_log.trace("{}: request {}", *this, KMIP_CMD_get_request(cmd));
                return res.then([this, retry](int res) mutable {
                    if (res == KMIP_ERROR_IO) {
                        kmip_log.debug("{}: request error {}", *this, kmip_errorc.message(res));
                        if (retry) {
                            kmip_log.debug("{}: retrying...", *this);
                            return opt_int{};
                        }
                    }
                    return opt_int(res);
                });
            });
        });
    }).then([this, cmd = std::move(cmd_in)](int res) mutable {
        kmip_chk(res, cmd);
        kmip_log.trace("{}: result {}", *this, KMIP_CMD_get_response(cmd));
        return std::move(cmd);
    });
}

future<kmip_host::impl::con_ptr> kmip_host::impl::get_connection(const sstring& host) {
    // TODO: if a pooled connection is stale, the command run will fail,
    // and the connection will be discarded. Would be good if we could detect this case
    // and re-run command with a new connection. Maybe always verify connection, even if
    // it is old?
    auto& q = _host_connections[host];
    if (q.empty()) {
        auto cp = ::make_shared<connection>(host, _options);
        kmip_log.trace("{}: connecting to {}", *this, host);
        return cp->connect().then([this, cp, host] {
            kmip_log.trace("{}: verifying {}", *this, host);
            kmip_cmd cmd;
            static auto connection_query = [](KMIP_CMD* cmd) {
                static const std::array<int, 2> query_options = {
                  KMIP_QUERY_FUNCTION_QUERY_OPERATIONS,
                  KMIP_QUERY_FUNCTION_QUERY_OBJECTS,
                };
                return KMIP_CMD_query(cmd, const_cast<int *>(query_options.data()), unsigned(query_options.size()));
            };
            auto f = do_cmd(cmd, cp, connection_query);
            return f.then([this, cp, host, cmd = std::move(cmd)](auto) {
                kmip_log.trace("{}: connected {}", *this, host);
                return cp;
            });
        });
    }

    auto cp = q.front();
    q.pop_front();
    return make_ready_future<::shared_ptr<connection>>(cp);
}


future<kmip_host::impl::con_ptr> kmip_host::impl::get_connection(KMIP_CMD* cmd) {
    userdata u{ KMIP_CMD_get_userdata(cmd) };
    if (u.host != nullptr) {
        return get_connection(u.host).then([this, cmd](con_ptr cp) {
            cp->attach(cmd);
            return cp;
        });
    }

    using con_ptr = ::shared_ptr<kmip_host::impl::connection>;
    using con_opt = std::optional<con_ptr>;

    return repeat_until_value([this, cmd, i = size_t(0)]() mutable {
        if (i++ == _options.hosts.size()) {
            throw std::runtime_error("Could not connect to any server");
        }
        auto& host = _options.hosts[_index % _options.hosts.size()];
        return get_connection(host).then([this, cmd](con_ptr cp) {
            cp->attach(cmd);
            return con_opt(std::move(cp));
        }).handle_exception([this](auto) {
            ++_index;
            return con_opt();
        });
    });
}

future<> kmip_host::impl::connect() {
    return do_for_each(_options.hosts, [this](const sstring& host) {
        return get_connection(host).then([this, &host](auto cp) {
            release(nullptr, cp);
        });
    });
}

static unsigned from_str(unsigned (*f)(char*, int, int*), const sstring& s) {
    int found = 0;
    auto res = f(const_cast<char *>(s.c_str()), CODE2STR_FLAG_STR_CASE, &found);
    if (!found) {
        throw std::invalid_argument(s);
    }
    return res;
}

std::tuple<kmip_host::impl::kmip_data_list, unsigned int> kmip_host::impl::make_attributes(const kmip_key_info& info, bool include_template) const {
    kmip_data_list kdl_attrs;

    if (!info.options.template_name.empty()) {
        kmip_chk(KMIP_DATA_LIST_add_attr_str_by_tag(kdl_attrs,
                        KMIP_TAG_TEMPLATE,
                        const_cast<char*>(info.options.template_name.c_str()))
                        );
    }
    if (!info.options.key_namespace.empty()) {
        kmip_chk(KMIP_DATA_LIST_add_attr_str(kdl_attrs,
                        const_cast<char *>("x-namespace"),
                        const_cast<char*>(info.options.key_namespace.c_str()))
                        );
    }

    sstring type, mode, padd;
    std::tie(type, mode, padd) = parse_key_spec(info.info.alg);

    auto crypt_alg = from_str(&KMIP_string_to_CRYPTOGRAPHIC_ALGORITHM, type);

    KMIP_CRYPTOGRAPHIC_PARAMETERS params;
    kmip_chk(KMIP_CRYPTOGRAPHIC_PARAMETERS_clear(&params));

    KMIP_CRYPTOGRAPHIC_PARAMETERS_set_cryptographic_algorithm(&params, crypt_alg);
    KMIP_CRYPTOGRAPHIC_PARAMETERS_set_block_cipher_mode(&params, from_str(&KMIP_string_to_BLOCK_CIPHER_MODE, mode));
    if (!padd.empty()) {
        KMIP_CRYPTOGRAPHIC_PARAMETERS_set_padding_method(&params, from_str(&KMIP_string_to_PADDING_METHOD, padd));
    }

    kmip_chk(KMIP_CRYPTOGRAPHIC_PARAMETERS_to_kdl(&params, kdl_attrs));

    return std::make_tuple(std::move(kdl_attrs), crypt_alg);
}

future<kmip_host::impl::key_and_id_type> kmip_host::impl::create_key(const kmip_key_info& info) {
    if (engine().cpu_id() == 0) {
        // #1039 First try looking for existing keys on server
        return find_matching_keys(info, 1).then([this, info](std::vector<id_type> ids) {
            if (!ids.empty()) {
                // got it
                return get_key_by_id(ids.front(), info.info).then([id = ids.front()](shared_ptr<symmetric_key> k) {
                    return key_and_id_type(std::move(k), id);
                });
            }

            kmip_log.debug("{}: Creating key {}", _name, info);

            auto [kdl_attrs, crypt_alg] = make_attributes(info);

            // TODO: this is inefficient. We can probably put this in a single batch.
            kmip_cmd cmd;
            KMIP_CMD_set_ctx(cmd, const_cast<char *>("Create key"));

            return do_cmd(std::move(cmd), [info, kdl_attrs = std::move(kdl_attrs), crypt_alg](KMIP_CMD* cmd) {
                return KMIP_CMD_create_smpl(cmd, KMIP_OBJECT_TYPE_SYMMETRIC_KEY,
                    crypt_alg,
                    KMIP_CRYPTOGRAPHIC_USAGE_ENCRYPT|KMIP_CRYPTOGRAPHIC_USAGE_DECRYPT,
                    int(info.info.len),
                    KMIP_DATA_LIST_attrs(kdl_attrs), KMIP_DATA_LIST_n_attrs(kdl_attrs)
                );
            }).then([this, info](kmip_cmd cmd) {
                /* now get the details (the value of the key) */
                char* new_id;
                kmip_chk(KMIP_CMD_get_uuid(cmd, 0, &new_id), cmd);

                utils::UUID uuid(new_id);
                kmip_log.debug("{}: Created {}:{}", _name, info, uuid);

                KMIP_CMD_set_ctx(cmd, const_cast<char *>("activate"));

                return do_cmd(std::move(cmd), [new_id](KMIP_CMD* cmd) {
                    return KMIP_CMD_activate(cmd, new_id);
                }).then([this, uuid, info](kmip_cmd cmd) {
                    bytes id = uuid.serialize();
                    kmip_log.debug("{}: Activated {}", _name, uuid);
                    return get_key_by_id(id, info.info).then([id](auto k) {
                        return key_and_id_type(k, id);
                    });
                });
            });
        });
    }

    return smp::submit_to(0, [this, info] {
        return _ctxt.get_kmip_host(_name)->get_or_create_key(info.info, info.options).then([](shared_ptr<symmetric_key> k, id_type id) {
            return make_ready_future<key_info, bytes, id_type>(k->info(), k->key(), id);
        });
    }).then([](key_info info, bytes b, id_type id) {
       return make_ready_future<key_and_id_type>(key_and_id_type(make_shared<symmetric_key>(info, b), id));
    });
}

future<std::vector<kmip_host::id_type>> kmip_host::impl::find_matching_keys(const kmip_key_info& info, std::optional<int> max) {
    kmip_log.debug("{}: Finding matching key {}", _name, info);

    auto [kdl_attrs, crypt_alg] = make_attributes(info, false);

    // #1079. Query mask apparently ignores things like cryptographic 
    // attribute set of options, instead we must specify the query 
    // as a list of attributes. 
    kmip_chk(KMIP_DATA_LIST_add_attr_enum_by_tag(kdl_attrs,
                    KMIP_TAG_OBJECT_TYPE,
                    KMIP_OBJECT_TYPE_SYMMETRIC_KEY)
                    );
    kmip_chk(KMIP_DATA_LIST_add_attr_enum_by_tag(kdl_attrs,
                    KMIP_TAG_CRYPTOGRAPHIC_ALGORITHM,
                    int(crypt_alg))
                    );
    kmip_chk(KMIP_DATA_LIST_add_attr_enum_by_tag(kdl_attrs,
                    KMIP_TAG_CRYPTOGRAPHIC_LENGTH,
                    int(info.info.len))
                    );
    kmip_chk(KMIP_DATA_LIST_add_attr_enum_by_tag(kdl_attrs,
                    KMIP_TAG_STATE,
                    KMIP_STATE_ACTIVE)
                    );
    kmip_chk(KMIP_DATA_LIST_add_attr_enum_by_tag(kdl_attrs,
                    KMIP_TAG_CRYPTOGRAPHIC_USAGE_MASK,
                    KMIP_CRYPTOGRAPHIC_USAGE_ENCRYPT|KMIP_CRYPTOGRAPHIC_USAGE_DECRYPT)
                    );

    kmip_cmd cmd;
    KMIP_CMD_set_ctx(cmd, const_cast<char *>("Find matching key"));

    std::unique_ptr<int> mp;
    int* maxp = nullptr;
    if (max) {
        mp = std::make_unique<int>(*max);
        maxp = mp.get();
    }

    return do_cmd(std::move(cmd), [kdl_attrs = std::move(kdl_attrs), maxp](KMIP_CMD* cmd) {
        return KMIP_CMD_locate(cmd, maxp, nullptr, KMIP_DATA_LIST_attrs(kdl_attrs), KMIP_DATA_LIST_n_attrs(kdl_attrs));
    }).then([this, info, mp = std::move(mp)](kmip_cmd cmd) {
        std::vector<id_type> result;

        for (int i = 0; ; ++i) {
            char* new_id;
            auto err = KMIP_CMD_get_uuid(cmd, i, &new_id);
            if (err == KMIP_ERROR_NOT_FOUND) {
                break;
            }
            kmip_chk(err, cmd);            
            utils::UUID uuid(new_id);
            result.emplace_back(uuid.serialize());
        }

        kmip_log.debug("{}: Found {} matching keys {}", _name, result.size(), info);

        return result;
    });
}

future<shared_ptr<symmetric_key>> kmip_host::impl::find_key(const id_type& id) {
    if (engine().cpu_id() == 0) {
        kmip_cmd cmd;
        KMIP_CMD_set_ctx(cmd, const_cast<char *>("Find key"));

        auto uuid = utils::UUID_gen::get_UUID(id);
        sstring tmp = uuid.to_sstring();

        kmip_log.debug("{}: Finding {}", _name, uuid);

        // Batch operation. Nothing is sent/received until xmit below
        kmip_chk(KMIP_CMD_batch_start(cmd));
        kmip_chk(KMIP_CMD_set_batch_order(cmd, 1));
        {
            int key_format_type = KMIP_KEY_FORMAT_TYPE_RAW;
            kmip_chk(KMIP_CMD_get(cmd, const_cast<char *>(tmp.c_str()), &key_format_type, nullptr, nullptr));
        }
        kmip_chk(KMIP_CMD_get_attributes(cmd, const_cast<char *>(tmp.c_str()), nullptr, 0));

        return do_cmd(std::move(cmd), [](KMIP_CMD* cmd) {
            return KMIP_CMD_batch_xmit(cmd);
        }).then([this, uuid](kmip_cmd cmd) {
            auto nb = KMIP_CMD_get_batch_count(cmd);
            if (nb != 2) {
                throw std::runtime_error("Invalid batch count in response: " + std::to_string(nb));
            }

            sstring alg;
            sstring mode;
            sstring padd;

            // "Get" result
            auto kdl_res = KMIP_CMD_get_batch(cmd, 0);

            /* get a reference to the key material (the actual key value) */
            unsigned char* key;
            unsigned int keylen;
            kmip_chk(KMIP_DATA_LIST_get_data(kdl_res, KMIP_TAG_KEY_MATERIAL, 0, &key, &keylen));

            auto tag_to_string = [](auto f, auto val) {
                int found;
                auto p = f(val, CODE2STR_FLAG_STR_CASE, &found);
                if (!found) {
                    throw std::invalid_argument("Invalid tag: " + std::to_string(val));
                }
                return sstring(p);
            };

            int crypto_alg;
            kmip_chk(KMIP_DATA_LIST_get_32(kdl_res, KMIP_TAG_CRYPTOGRAPHIC_ALGORITHM, 0, &crypto_alg));
            alg = tag_to_string(&KMIP_CRYPTOGRAPHIC_ALGORITHM_to_string, crypto_alg);

            // "Attribute list" result
            // This will apparently most of the time _not_ contain the info we want,
            // depending on server, but we record as much as we can anyway.
            // The actual resulting keys used will be based on external config. Only
            // key data and verifying that it is compatible with said info is
            // important for us.
            auto kdl_attr = KMIP_CMD_get_batch(cmd, 1);

            unsigned int attr_count = 0;
            kmip_chk(KMIP_DATA_LIST_get_count(kdl_attr, KMIP_TAG_ATTRIBUTE, &attr_count));

            for (unsigned int i = 0; i < attr_count; i++) {
                KMIP_DATA *attr = nullptr;
                int n_attr = 0;

                kmip_chk(KMIP_DATA_LIST_get_struct(kdl_attr, KMIP_TAG_ATTRIBUTE, i, &attr, &n_attr, NULL));


                KMIP_DATA *attr_val = nullptr;
                kmip_chk(KMIP_DATA_get(attr, n_attr,KMIP_TAG_ATTRIBUTE_VALUE, 0, &attr_val));

                switch (attr_val->tag) {
                case KMIP_TAG_BLOCK_CIPHER_MODE:
                    mode = tag_to_string(&KMIP_BLOCK_CIPHER_MODE_to_string, attr_val->data32);
                    break;
                case KMIP_TAG_PADDING_METHOD:
                    padd = tag_to_string(&KMIP_PADDING_METHOD_to_string, attr_val->data32);
                    break;
                default:
                    break;
                }
            }

            if (alg.empty()) {
                throw std::invalid_argument("Could not find algorithm");
            }
            if (mode.empty() != padd.empty()) {
                throw std::invalid_argument("Invalid block mode/padding");
            }

            auto str = mode.empty() || padd.empty() ? alg : alg + "/" + mode + "/" + padd;
            key_info derived_info{ str, keylen*8};

            kmip_log.trace("{}: Found {}:{} {}", _name, uuid, derived_info.alg, derived_info.len);

            return make_shared<symmetric_key>(derived_info, bytes(key, key + keylen));
        });
    }

    return smp::submit_to(0, [this, id] {
        return _ctxt.get_kmip_host(_name)->get_key_by_id(id).then([](shared_ptr<symmetric_key> k) {
            return make_ready_future<key_info, bytes>(k->info(), k->key());
        });
    }).then([](key_info info, bytes b) {
        return make_shared<symmetric_key>(info, b);
    });
}

shared_ptr<symmetric_key> kmip_host::impl::ensure_compatible_key(shared_ptr<symmetric_key> k, const key_info& info) {
    // keys we get back are typically void
    // of block mode/padding info (because this is meaningless
    // from the standpoint of the kmip server).
    // Check and re-init the actual key used based
    // on what the user wants so we adhere to block mode etc.
    if (!info.compatible(k->info())) {
        throw std::invalid_argument(sprint("Incompatible key: %s", k->info()));
    }
    if (k->info() != info) {
        k = ::make_shared<symmetric_key>(info, k->key());
    }
    return k;
}

future<shared_ptr<symmetric_key>, kmip_host::id_type> kmip_host::impl::get_or_create_key(const key_info& info, const key_options& opts) {
    kmip_log.debug("{}: Lookup key {}:{}", _name, info, opts);
    return _attr_cache.get(kmip_key_info{info, opts}).then([info](key_and_id_type kinfo) {
        return make_ready_future<shared_ptr<symmetric_key>, id_type>(ensure_compatible_key(std::get<0>(kinfo), info), std::get<1>(kinfo));
    });
}

future<shared_ptr<symmetric_key>> kmip_host::impl::get_key_by_id(const id_type& id, const std::optional<key_info>& info) {
    auto f = _id_cache.get(id);
    if (info) {
        f = f.then([info](shared_ptr<symmetric_key> k) {
           return ensure_compatible_key(k, *info);
        });
    }
    return f;
}

kmip_host::kmip_host(encryption_context& ctxt, const sstring& name, const std::unordered_map<sstring, sstring>& map)
    : kmip_host(ctxt, name, [&ctxt, &map] {
        host_options opts;
        map_wrapper<std::unordered_map<sstring, sstring>> m(map);

        try {
            static const std::regex wsc("\\s*,\\s*"); // comma+whitespace

            std::string hosts = m("hosts").value();

            auto i = std::sregex_token_iterator(hosts.begin(), hosts.end(), wsc, -1);
            auto e = std::sregex_token_iterator();

            std::for_each(i, e, [&](const std::string & s) {
                opts.hosts.emplace_back(s);
            });
        } catch (std::bad_optional_access&) {
            throw std::invalid_argument("No KMIP host names provided");
        }

        opts.certfile = m("certificate").value_or("");
        opts.keyfile = m("keyfile").value_or("");
        opts.truststore = m("truststore").value_or("");
        opts.priority_string = m("priority_string").value_or("");

        opts.username = m("username").value_or("");
        opts.password = ctxt.maybe_decrypt_config_value(m("password").value_or(""));

        if (m("max_command_retries")) {
            opts.max_command_retries = std::stoul(*m("max_command_retries"));
        }

        return opts;
    }())
{}

kmip_host::kmip_host(encryption_context& ctxt, const sstring& name, const host_options& opts)
    : _impl(std::make_unique<impl>(ctxt, name, opts))
{}

kmip_host::~kmip_host() = default;

future<> kmip_host::connect() {
    return _impl->connect();
}

future<shared_ptr<symmetric_key>, kmip_host::id_type> kmip_host::get_or_create_key(const key_info& info, const key_options& opts) {
    return _impl->get_or_create_key(info, opts);
}

future<shared_ptr<symmetric_key>> kmip_host::get_key_by_id(const id_type& id, std::optional<key_info> info) {
    return _impl->get_key_by_id(id, info);
}


std::ostream& operator<<(std::ostream& os, const kmip_host::key_options& opts) {
    return os << opts.template_name << ":" << opts.key_namespace;
}

}