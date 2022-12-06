/*
 * Copyright 2014-2023 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef AERON_COMMAND_CONNECTION_MESSAGE_FLYWEIGHT_H
#define AERON_COMMAND_CONNECTION_MESSAGE_FLYWEIGHT_H

#include <cstdint>
#include <cstddef>
#include "Flyweight.h"

namespace aeron { namespace command
{

/**
* Control message flyweight for any message that needs to represent an image
*
* <p>
*  0                   1                   2                   3
*  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
*  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*  |                        Correlation ID                         |
*  |                                                               |
*  +---------------------------------------------------------------+
*  |                 Subscription Registration ID                  |
*  |                                                               |
*  +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct ImageMessageDefn
{
    std::int64_t correlationId;
    std::int64_t subscriptionRegistrationId;
};
#pragma pack(pop)

static const util::index_t IMAGE_MESSAGE_FLYWEIGHT_LENGTH = sizeof(struct ImageMessageDefn);

class ImageMessageFlyweight : public Flyweight<ImageMessageDefn>
{
public:
    typedef ImageMessageFlyweight this_t;

    inline ImageMessageFlyweight(concurrent::AtomicBuffer &buffer, util::index_t offset) :
        Flyweight<ImageMessageDefn>(buffer, offset)
    {
    }

    inline std::int64_t correlationId() const
    {
        return m_struct.correlationId;
    }

    inline this_t &correlationId(std::int64_t value)
    {
        m_struct.correlationId = value;
        return *this;
    }

    inline std::int64_t subscriptionRegistrationId() const
    {
        return m_struct.subscriptionRegistrationId;
    }

    inline this_t &subscriptionRegistrationId(std::int64_t value)
    {
        m_struct.subscriptionRegistrationId = value;
        return *this;
    }
};

}}
#endif
