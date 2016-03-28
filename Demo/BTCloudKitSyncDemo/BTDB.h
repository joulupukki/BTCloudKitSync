//
//  BTDB.h
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

#import <Foundation/Foundation.h>

#import "BTCloudKitSync.h"

@class BTContact;
@class BTContactChange;

@interface BTDB : NSObject <BTCloudKitSyncDatabase>

+ (BTDB *)sharedInstance;

/**
 Reads ALL contact objects currently in the local database.
 */
- (NSArray<BTContact *> *)readAllContacts;

/**
 Reads one contact object from the local database.
 */
- (BTContact *)readContactWithIdentifier:(NSString *)identifier;

/**
 Add a new contact to the database. If the contact does not yet have an
 identifier, one will be assigned (and the contact object will be updated with
 the new identifier). Using this convenience method, the contact will be added
 to the contact change log.
 
 @param contact The contact to add to the database.
 @param error Specifies an error or nil when successful.
 */
- (BOOL)addContact:(BTContact *)contact error:(NSError **)error;

/**
 Add a new contact to the database with the option of not logging it as a local
 change. This is used by sync code when receiving new records from a server.
 
 @param contact The contact to add to the database.
 @param logChanges When YES, log the changes made to the contact. Sync code should set this to NO.
 @param error Specifies an error or nil when successful.
 */
- (BOOL)addContact:(BTContact *)contact shouldLogChanges:(BOOL)logChanges error:(NSError **)error;

- (BOOL)updateContact:(BTContact *)contact error:(NSError **)error;
- (BOOL)updateContact:(BTContact *)contact shouldLogChanges:(BOOL)logChanges error:(NSError **)error;

- (BOOL)deleteContactWithIdentifier:(NSString *)identifier error:(NSError **)error;
- (BOOL)deleteContactWithIdentifier:(NSString *)identifier shouldLogChanges:(BOOL)logChanges error:(NSError **)error;

#pragma mark - Change Log

- (NSArray<BTContactChange *> *)contactChangesBeforeDate:(NSDate *)date limit:(NSUInteger)limit;
- (BTContactChange *)contactChangeForIdentifier:(NSString *)identifier beforeDate:(NSDate *)date;

/**
 Purge (delete) all records in the contacts_changelog table before the specified
 date. During a CloudKit sync, this us used to remove old changes after changes
 have successfully been uploaded to the CloudKit server. Doing this keeps the
 amount of extra space saved on the device down.
 
 This method is also used if the user disabled CloudKit sync. There's no reason
 to consume extra space keeping track of chnages if the user has never enabled
 CloudKit sync or disables sync.
 
 @param date Records entered before this date will be deleted from the table.
 @param error If the purge fails, this will contain an error with the failure details.
 
 @return YES when the purge is successful, otherwise NO. When NO, the details of
 the failure will be passed back in the error parameter.
 */
- (BOOL)purgeChangesBeforeDate:(NSDate *)date error:(NSError **)error;

/**
 When sync is enabled for the first time, this method adds every object in the
 contacts table and populates the contacts_changelog table so it's ready to
 upload local changes to CloudKit.
 
 @param error If there are any errors this will describe the error.
 
 @return YES if successful, otherwise NO and error will contain the details of
 the error.
 */
- (BOOL)populateContactChangeLogReturningError:(NSError **)error;

#pragma mark - CKRecord System Field Caching

- (NSData *)syncDataForRecordWithIdentifier:(NSString *)identifier error:(NSError **)error;
- (BOOL)saveSyncData:(NSData *)data withIdentifier:(NSString *)identifier error:(NSError **)error;
- (BOOL)deleteSyncDataForRecordWithIdentifier:(NSString *)identifier error:(NSError **)error;

/**
 Delete all sync data (CKRecord system fields) from the local database. This
 should be called if CloudKit sync is disabled so the database doesn't take up
 any extra space on the device.
 
 @param error If this fails, error will contain the reason for the failure.
 
 @return YES if successful or NO on failure.
 */
- (BOOL)purgeSyncDataWithError:(NSError **)error;

@end
