//
//  BTConstants.h
//  BTCloudKitSyncDemo
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

#ifndef BTConstants_h
#define BTConstants_h

#pragma mark - App Notifications

/**
 Posted via NSNotificationCenter when a new contact is added to the database.
 
 The userInfo will contain the contact identifier with kBTContactIdentifierKey.
 */
#define kBTContactAddedNotification		@"BTContactAddedNotification"

/**
 Posted via NSNotificationCenter when a new contact is updated in the database.
 
 The userInfo will contain the contact identifier with kBTContactIdentifierKey.
 */
#define kBTContactUpdatedNotification	@"BTContactUpdatedNotification"

/**
 Posted via NSNotificationCenter when a new contact is deleted from the database.
 
 The userInfo will contain the contact identifier with kBTContactIdentifierKey.
 */
#define kBTContactDeletedNotification	@"BTContactDeletedNotification"

#define kBTContactIdentifierKey			@"BTContactIdentifierKey"

#endif /* BTConstants_h */
