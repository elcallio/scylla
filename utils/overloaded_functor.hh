/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#pragma once

/**
 * A helper class to encompass multiple lambdas accepting different input types
 * into a single object.
 */

template<typename... Ts> struct overloaded_functor : Ts... { using Ts::operator()...; };
template<typename... Ts> overloaded_functor(Ts...) -> overloaded_functor<Ts...>;

