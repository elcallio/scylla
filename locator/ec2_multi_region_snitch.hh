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

#include "locator/ec2_snitch.hh"

namespace locator {
class ec2_multi_region_snitch : public ec2_snitch {
public:
    ec2_multi_region_snitch(const sstring& fname = "", unsigned io_cpu_id = 0);
    virtual future<> gossiper_starting() override;
    virtual future<> start() override;
    virtual void set_local_private_addr(const sstring& addr_str) override;
    virtual sstring get_name() const override {
        return "org.apache.cassandra.locator.Ec2MultiRegionSnitch";
    }
private:
    sstring _local_private_address;
};
} // namespace locator
