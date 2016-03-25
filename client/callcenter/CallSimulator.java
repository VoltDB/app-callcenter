/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package callcenter;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import org.voltdb.types.TimestampType;

public class CallSimulator {

    CallCenterApp.CallCenterConfig config;

    // random number generator with constant seed
    final Random rand = new Random(0);

    final NavigableMap<Long, CallEvent> delayedEvents = new TreeMap<>();

    long currentSystemMilliTimestamp = 0;
    double targetEventsPerMillisecond;
    long targetEventsThisMillisecond;
    long eventsSoFarThisMillisecond;

    int lastAgentIdUsed = 0;
    long lastCallIdUsed = 0;

    CallSimulator(CallCenterApp.CallCenterConfig config) {
        this.config = config;
        targetEventsPerMillisecond = 5;
    }

    CallEvent[] makeRandomEvent() {
        long callId = ++lastCallIdUsed;
        // next one handles integer overflow
        int agentId = (lastAgentIdUsed == Integer.MAX_VALUE) ? 0 : lastAgentIdUsed + 1;
        lastAgentIdUsed = agentId;
        String phoneNo = String.format("+1 (%d) %d-%d",
                rand.nextInt(1000), rand.nextInt(1000), rand.nextInt(10000));
        // voltdb timestamp type uses micros from epoch
        TimestampType startTS = new TimestampType(currentSystemMilliTimestamp * 1000);
        long durationms = -1;
        while (durationms < 0) {
            durationms = (long) (rand.nextGaussian() * config.meancalldurationseconds * 1000.0 / 2.0) + (config.meancalldurationseconds * 1000);
        }
        TimestampType endTS = new TimestampType(((startTS.getTime() / 1000) + durationms) * 1000);

        CallEvent[] event = new CallEvent[2];
        event[0] = new CallEvent(callId, agentId, phoneNo, startTS, null);
        event[1] = new CallEvent(callId, agentId, phoneNo, null, endTS);

        //System.out.println("Creating event with range:");
        //System.out.println(new Date(startTS.getTime() / 1000));
        //System.out.println(new Date(endTS.getTime() / 1000));

        return event;
    }

    CallEvent next(long systemCurrentTimeMillis) {
        // check for time passing
        if (systemCurrentTimeMillis > currentSystemMilliTimestamp) {
            // build a target for this 1ms window
            long eventBacklog = targetEventsThisMillisecond - eventsSoFarThisMillisecond;
            targetEventsThisMillisecond = (long) Math.floor(targetEventsPerMillisecond);
            double targetFraction = targetEventsPerMillisecond - targetEventsThisMillisecond;
            targetEventsThisMillisecond += (rand.nextDouble() <= targetFraction) ? 1 : 0;
            targetEventsThisMillisecond += eventBacklog;
            // reset counter for this 1ms window
            eventsSoFarThisMillisecond = 0;
            currentSystemMilliTimestamp = systemCurrentTimeMillis;
        }

        // check if we made all the target events for this 1ms window
        if (targetEventsThisMillisecond == eventsSoFarThisMillisecond) {
            return null;
        }

        // drain scheduled events first
        if ((delayedEvents.size() > 0) && (delayedEvents.firstKey() < systemCurrentTimeMillis)) {
            Entry<Long, CallEvent> eventEntry = delayedEvents.pollFirstEntry();
            return eventEntry.getValue();
        }

        // generate rando event (begin/end pair)
        CallEvent[] event = makeRandomEvent();
        // schedule the end event
        delayedEvents.put(event[1].endTS.getTime() / 1000, event[1]);

        eventsSoFarThisMillisecond++;

        return event[0];
    }
}
