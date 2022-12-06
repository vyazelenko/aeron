/*
 * Copyright 2014-2022 Real Logic Limited.
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
package io.aeron;

/**
 * Provides details of why an {@link Image} became unavailable.
 */
public final class UnavailableImageReason
{
    private final String reason;

    /**
     * Create new instance with given arguments.
     *
     * @param reason the image goes unavailable.
     */
    public UnavailableImageReason(final String reason)
    {
        this.reason = reason;
    }

    /**
     * Returns the reason that describes why the {@link Image} went unavailable.
     *
     * @return the reason the {@link Image} went unavailable.
     */
    public String reason()
    {
        return reason;
    }
}
