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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Date;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;

public class CallSimulator {

    CallCenterApp.CallCenterConfig config;

    // random number generator with constant seed
    final Random rand = new Random(0);

    final NavigableMap<Long, CallEvent[]> delayedEvents = new TreeMap<>();

    long currentSystemMilliTimestamp = 0;
    double targetEventsPerMillisecond;
    long targetEventsThisMillisecond;
    long eventsSoFarThisMillisecond;

    long drainedEvents = 0;
    long createdEvents = 0;

    long lastCallIdUsed = 0;

    Queue<Integer> agentsAvailable = new ArrayDeque<>();
    Queue<Long> phoneNumbersAvailable = new ArrayDeque<>();

    CallSimulator(CallCenterApp.CallCenterConfig config) {
        this.config = config;
        targetEventsPerMillisecond = 5;

        // generate agents
        for (int i = 0; i < config.agents; i++) {
            agentsAvailable.add(i);
        }

        // generate phone numbers
        for (int i = 0; i < config.numbers; i++) {
            // random area code between 200 and 799
            long areaCode = rand.nextInt(600) + 200;
            // random exchange between 200 and 999
            long exhange = rand.nextInt(800) + 200;
            // full random number
            long phoneNo = areaCode * 10000000 + exhange * 10000 + rand.nextInt(9999);
            phoneNumbersAvailable.add(phoneNo);
        }
    }

    CallEvent[] makeRandomEvent() {
        long callId = ++lastCallIdUsed;

        // get agentid
        Integer agentId = agentsAvailable.poll();
        if (agentId == null) {
            return null;
        }

        // get phone number
        Long phoneNo = phoneNumbersAvailable.poll();
        assert(phoneNo != null);

        // voltdb timestamp type uses micros from epoch
        Date startTS = new Date(currentSystemMilliTimestamp);

        long durationms = -1;
        long meancalldurationms = config.meancalldurationseconds * 1000;
        long maxcalldurationms = config.maxcalldurationseconds * 1000;
        double stddev = meancalldurationms / 2.0;

        // repeat until in the range (0..maxcalldurationms]
        while ((durationms <= 0) || (durationms > maxcalldurationms)) {
            durationms = (long) (rand.nextGaussian() * stddev) + meancalldurationms;
        }
        Date endTS = new Date(startTS.getTime() + durationms);

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

        // drain scheduled events first
        if ((delayedEvents.size() > 0) && (delayedEvents.firstKey() < systemCurrentTimeMillis)) {
            Entry<Long, CallEvent[]> eventEntry = delayedEvents.pollFirstEntry();
            CallEvent[] callEvents = eventEntry.getValue();

            CallEvent callEvent = callEvents[0];
            if (callEvents.length > 1) {
                int prevLength = callEvents.length;

                callEvents = Arrays.copyOfRange(callEvents, 1, callEvents.length);
                assert(callEvents.length == prevLength - 1);

                delayedEvents.put(eventEntry.getKey(), callEvents);
            }

            // double check this is an end event
            assert(callEvent.startTS == null);
            assert(callEvent.endTS != null);

            // return the agent/phone for this event to the available lists
            agentsAvailable.add(callEvent.agentId);
            phoneNumbersAvailable.add(callEvent.phoneNo);

            validate();
            drainedEvents++;
            return callEvent;
        }

        // check if we made all the target events for this 1ms window
        if (targetEventsThisMillisecond == eventsSoFarThisMillisecond) {
            validate();
            return null;
        }

        // generate rando event (begin/end pair)
        CallEvent[] event = makeRandomEvent();
        // this means all agents are busy
        if (event == null) {
            validate();
            return null;
        }

        long endTimeKey = event[1].endTS.getTime();
        assert((endTimeKey - systemCurrentTimeMillis) < (config.maxcalldurationseconds * 1000));

        // schedule the end event
        CallEvent[] callEvents = delayedEvents.get(endTimeKey);
        if (callEvents != null) {
            CallEvent[] callEvents2 = new CallEvent[callEvents.length + 1];
            callEvents2[0] = event[1];
            for (int i = 0; i < callEvents.length; i++) {
                callEvents2[i + 1] = callEvents[i];
            }
            callEvents = callEvents2;
        }
        else {
            callEvents = new CallEvent[] { event[1] };
        }

        delayedEvents.put(endTimeKey, callEvents);

        eventsSoFarThisMillisecond++;

        validate();
        createdEvents++;
        return event[0];
    }

    CallEvent drain() {
        Entry<Long, CallEvent[]> eventEntry = delayedEvents.pollFirstEntry();
        if (eventEntry == null) {
            validate();
            return null;
        }

        CallEvent[] callEvents = eventEntry.getValue();

        CallEvent callEvent = callEvents[0];
        if (callEvents.length > 1) {
            int prevLength = callEvents.length;

            callEvents = Arrays.copyOfRange(callEvents, 1, callEvents.length);
            assert(callEvents.length == prevLength - 1);

            delayedEvents.put(eventEntry.getKey(), callEvents);
        }

        // double check this is an end event
        assert(callEvent.startTS == null);
        assert(callEvent.endTS != null);

        // return the agent/phone for this event to the available lists
        agentsAvailable.add(callEvent.agentId);
        phoneNumbersAvailable.add(callEvent.phoneNo);

        validate();
        return callEvent;
    }

    private void validate() {
        long delayedEventCount = 0;
        for (Entry<Long, CallEvent[]> entry : delayedEvents.entrySet()) {
            delayedEventCount += entry.getValue().length;
        }

        long outstandingAgents = config.agents - agentsAvailable.size();
        long outstandingPhones = config.numbers - phoneNumbersAvailable.size();

        if (outstandingAgents != outstandingPhones) {
            throw new RuntimeException(
                    String.format("outstandingAgents (%d) != outstandingPhones (%d)",
                            outstandingAgents, outstandingPhones));
        }
        if (outstandingAgents != delayedEventCount) {
            throw new RuntimeException(
                    String.format("outstandingAgents (%d) != delayedEventCount (%d)",
                            outstandingAgents, delayedEventCount));
        }
    }

    void printSummary() {
        System.out.printf("There are %d agents outstanding and %d phones. %d entries waiting to go.\n",
                agentsAvailable.size(), phoneNumbersAvailable.size(), delayedEvents.size());
    }
}
