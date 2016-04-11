//
//  BTCloudKitSyncConstants.h
//  BTCloudKitSync
//
//  Created by Boyd Timothy on 3/19/16.
//  Copyright © 2016 Boyd Timothy.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

#ifndef BTCloudKitSyncConstants_h
#define BTCloudKitSyncConstants_h

#pragma mark - App Settings (NSUserDefaults)

#define kBTCloudKitSyncSettingSyncEnabledKey	@"SyncEnabled"
#define kBTCloudKitSyncSettingLastSyncDateKey	@"LastSyncDate"

#pragma mark - Sync Constants

#define kBTCloudKitSyncServerChangeTokenKey			@"BTCloudKitSyncServerChangeToken"

/**
 Whenever a user makes a change, we want to push the changes to the CloudKit
 server, but not immediately after a change. BTCloudKitSync uses a timer to wait
 for the number of seconds defined here before performing a sync. This way, we
 can potentially pick up rapid changes in a single sync.
 */
#define kBTCloudKitSyncChangeTimeoutInSeconds	3

/**
 Whenever BTCloudKitSync detects record changes that are within this time period
 of each other (number of seconds), it will prefer the server change. This aids
 in conflict resolution.
 */
#define kBTCloudKitSyncPreferServerChangeIfWithinSeconds 5

/**
 CloudKit calls to fetch data may happen at any time, and sometimes happen right
 after each other, so instead of indicating that a sync has completed, wait for
 the number of seconds specified here before notifying BTCloudKitSyncDatabase.
 */
#define kBTCloudKitSyncCompletedNotificationDelay 3

#endif /* BTCloudKitSyncConstants_h */
