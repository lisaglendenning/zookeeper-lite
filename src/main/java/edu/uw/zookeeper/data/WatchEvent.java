/**
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
 */
package edu.uw.zookeeper.data;

import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.google.common.base.Objects;

import edu.uw.zookeeper.Event;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;

public class WatchEvent implements Event, Operation.RecordHolder<IWatcherEvent> {
    
    public static WatchEvent of(WatcherEvent message) {
        return new WatchEvent(EventType.fromInt(message.getType()), 
                KeeperState.fromInt(message.getState()),
                ZNodeLabel.Path.of(message.getPath()));
    }
    
    public static WatchEvent of(EventType eventType, KeeperState keeperState, ZNodeLabel.Path path) {
        return new WatchEvent(eventType, keeperState, path);
    }
    
    private final ZNodeLabel.Path path;
    private final KeeperState keeperState;
    private final EventType eventType;
    
    protected WatchEvent(EventType eventType, KeeperState keeperState, ZNodeLabel.Path path) {
        this.keeperState = keeperState;
        this.eventType = eventType;
        this.path = path;
    }
    
    public KeeperState state() {
        return keeperState;
    }
    
    public EventType type() {
        return eventType;
    }
    
    public ZNodeLabel.Path path() {
        return path;
    }

    @Override
    public IWatcherEvent asRecord() {
        return new IWatcherEvent(type().getIntValue(), 
                state().getIntValue(), 
                path().toString());
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("path", path).add("type", type()).add("state", state()).toString();
    }
}
