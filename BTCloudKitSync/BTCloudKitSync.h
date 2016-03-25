//
//  BTCloudKitSync.h
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

#import <Foundation/Foundation.h>

#import "BTCloudKitSyncConstants.h"

// The following constants are used in the NSDictionary returned by
// BTCloudKitSyncDatabase's recordChangesOfType:beforeDate:error:.
#define BTCloudKitSyncChangeTypeKey				@"ChangeType"
#define BTCloudKitSyncChangeTypeInsert			@"Insert"
#define BTCloudKitSyncChangeTypeUpdate			@"Update"
#define BTCloudKitSyncChangeTypeDelete			@"Delete"
#define BTCloudKitSyncChangeRecordIdentifier	@"RecordIdentifier"
#define BTCloudKitSyncChangeRecordInfoKey		@"RecordInfo"

#define BTCloudKitSyncRecordTypeKey				@"RecordType"

#define BTCloudKitSyncErrorDomain				@"BTCloudKitSync"

typedef enum : NSInteger {
	BTCloudKitSyncErrorInternalFailure			= 1, // Internal failure. Non-recoverable.
	BTCloudKitSyncErrorMissingDatabase			= 2, // The localDatabase property is missing (BTCloudKitSyncDatabase)
	BTCloudKitSyncErrorNoRecordZoneName			= 3, // The localDatabase did not provide a recordZoneName
	BTCloudKitSyncErrorNoRecordTypes			= 4, // The localDatabase did not provide recordTypes
	BTCloudKitSyncErrorNoChangeNotifications	= 5, // The localDatabase did not provide databaseChangeNotificationNames
	BTCloudKitSyncErrorConfigureChangeTracking	= 6, // The localDatabase could not configure change tracking. The userInfo dictionary will contain the record type in BTCloudKitSyncRecordTypeKey.
	BTCloudKitSyncErrorPurgeChanges				= 7, // The localDatabase could not purge changes. The userInfo dictionary will contain the record type in BTCloudKitSyncRecordTypeKey.
	BTCloudKitSyncErrorPurgeSystemFields		= 8, // The localDatabase could not purge system fields.
} BTCloudKitSyncErrorCode;

@protocol BTCloudKitSyncDatabase;

#pragma mark - BTCloudKitSync

/**
 This class coordinates synchronization of locally-cached data with CloudKit.
 You must set the database property with a class that implements the
 BTCloudKitSyncDatabase protocol. The object that implements this protocol is
 responsible for providing all the locally-cached objects to BTCloudKitSync.
 */
@interface BTCloudKitSync : NSObject

/**
 Determines whether sync is currently enabled.
 */
@property (nonatomic, readonly) BOOL syncEnabled;
@property (nonatomic, readonly) BOOL isConfigured;
@property (nonatomic, readonly) NSDate *lastSyncDate;
@property (nonatomic, weak, readonly) id<BTCloudKitSyncDatabase> localDatabase;

+ (BTCloudKitSync *)sharedInstance;

- (void)configureWithDatabase:(id<BTCloudKitSyncDatabase>)database;

/**
 Enable or disable sync with CloudKit.
 
 @return Returns YES on success or NO on error. On error, the error will explain
 what went wrong.
 */
- (void)enableSync:(BOOL)enableSync withCompletionBlock:(void (^)(BOOL success, NSError *error))completion;


- (void)performSyncWithCompletion:(void(^)(BOOL success, NSError *error))completion;
@end

#pragma mark - BTCloudKitSyncDatabase Protocol

/**
 This protocol allows BTCloudKitSync to know next to nothing about the local
 database that powers the app. BTCloudKitSync uses the methods defined in this
 protocol to interface with the cached data.
 */
@protocol BTCloudKitSyncDatabase <NSObject>

@optional

/**
 This isn't shown to the end user, but gives the implementation class an
 opportunity to identify itself. This may be used for debug logging.
 */
- (NSString *)databaseDescription;

@required

#pragma mark - Synchronization Enablement Methods

/**
 Allow the database to specify the CloudKit Record Zone name to use for all
 objects.
 */
- (NSString *)recordZoneName;

/**
 Supply all the record types being provided by the database. BTCloudKitSync uses
 this to know which kind of CKRecord objects to create and synchronize. The
 order of the record types returned is the same order that will be used to sync
 the data to CloudKit.
 
 @return An array of record types provided by the database.
 */
- (NSArray<NSString *> *)recordTypes;

/**
 Specify the notifications posted by the database, if any, that BTCloudKitSync
 can observe to know when changes occur. BTCloudKitSync will observe these using
 NSNotificationCenter and will automatically perform a synchronization.
 */
- (NSArray<NSString *> *)databaseChangeNotificationNames;

#pragma mark - Record Methods

/**
 Provide the full record information for the specified record type.
 
 @param recordType The type of record to retrieve from the database.
 @param identifier The record identifier.
 @param error If the method fails, the error should contain details for the failure.
 
 @return A dictionary representation of the record.
 */
- (NSDictionary<NSString *, NSObject *> *)infoForRecordType:(NSString *)recordType withIdentifier:(NSString *)identifier error:(NSError **)error;

/**
 Adds a record in the database.
 
 @param recordInfo A dictionary representation of the record.
 @param recordType The type of record to retrieve from the database.
 @param identifier The record identifier.
 @param error If the method fails, the error should contain details for the failure.
 
 @return YES on success, NO on failure.
 */
- (BOOL)addRecordInfo:(NSDictionary<NSString *, NSObject *> *)recordInfo withRecordType:(NSString *)recordType withIdentifier:(NSString *)identifier error:(NSError **)error;

/**
 Updates a record in the database. It's important to note that the record may
 not have all of the possible record fields specified in recordInfo and may only
 be the recently-changed fields.
 
 @param recordInfo A dictionary representation of the record.
 @param recordType The type of record to retrieve from the database.
 @param identifier The record identifier.
 @param error If the method fails, the error should contain details for the failure.
 
 @return YES on success, NO on failure.
 */
- (BOOL)updateRecordInfo:(NSDictionary<NSString *, NSObject *> *)recordInfo withRecordType:(NSString *)recordType withIdentifier:(NSString *)identifier error:(NSError **)error;

/**
 Deletes a record from the database.
 
 @param identifier The record identifier.
 @param error If the method fails, the error should contain details for the failure.
 
 @return YES on success, NO on failure.
 */
- (BOOL)deleteRecordWithIdentifier:(NSString *)identifer error:(NSError **)error;


#pragma mark - Record Change Methods

/**
 Builds an array of changes in the database of the specified record type that
 have occurred before the specified date. The recordInfo for each change should
 only include the properties that have changed.
 
 @param recordType The type of record to get changes for.
 @param identifier The record identifier.
 @param error If the method fails, the error should contain details for the failure.
 
 @return An array of NSDictionary objects representing the changes found for the
 specified recordType. On error, nil should be returned and error should contain
 the details of the error. If no changes were found, do not return error, but
 instead, return an empty array.
 */
- (NSArray<NSDictionary *> *)recordChangesOfType:(NSString *)recordType beforeDate:(NSDate *)date error:(NSError **)error;

/**
 Purge (delete) records of the specified type in the database before the
 specified date. During a CloudKit sync, this us used to let the database know
 that change information about the record types is no longer needed. Doing this
 can keep the space needed on the device low.
 
 This method is also used if the user disables CloudKit sync. There's no reason
 to consume extra space keeping track of chnages if the user has never enabled
 CloudKit sync or disables sync.
 
 @param recordType The type of record to purge.
 @param date Records entered before this date will be deleted from the table.
 @param error If the purge fails, this should contain an error with the failure details.
 
 @return YES when the purge is successful, otherwise NO. When NO, the details of
 the failure will be passed back in the error parameter.
 */
- (BOOL)purgeRecordChangesOfType:(NSString *)recordType beforeDate:(NSDate *)date error:(NSError **)error;

/**
 When sync is enabled, this method is called for every record type to give the
 database an opportunity to configure change tracking.
 
 @param recordType The type of record to configure change tracking on.
 @param error If the confiure fails, this should contain an error with the
 failure details.
 
 @return YES on success or NO on failure.
 
 */
- (BOOL)configureChangeTrackingForRecordType:(NSString *)recordType error:(NSError **)error;


#pragma mark - CKRecord System Fields Methods

/**
 If a previous communication with CloudKit resulted in a successful CKRecord
 stored on the CloudKit server, subsequent communication with CloudKit requires
 the system fields of the CKRecord. This method returns the system fields for
 the specified identifier. This NSData can then be used to reconstruct a
 CKRecord used to make further changes on the CKRecord.
 
 @param identifier The record identifier.
 @param error If this method fails, error will contain the details of the failure.
 
 @return The CKRecord system fields as received by the CloudKit service from the
 last interaction with CloudKit for the given record identifier.
 */
- (NSData *)systemFieldsDataForRecordWithIdentifier:(NSString *)identifier error:(NSError **)error;

/**
 This method should persist the CKRecord system fields so they can be used later
 to interface with CloudKit in the future.
 
 @param data The CKRecord system fields encoded as an NSData object.
 @param identifier The record identifier.
 @param error If the method fails, this will contain the details of the failure.
 
 @return YES if the purge succeeded or NO on a failure.
 */
- (BOOL)saveSystemFieldsData:(NSData *)data withIdentifier:(NSString *)identifier error:(NSError **)error;

/**
 If a CKRecord is ever deleted (permanently), this will be called to remove the
 CKRecord system fields from the local database.
 
 @param identifier The record identifier.
 @param error If the method fails, this will contain the details of the failure.
 
 @return YES if the purge succeeded or NO on a failure.
 */
- (BOOL)deleteSystemFieldsForRecordWithIdentifier:(NSString *)identifier error:(NSError **)error;

/**
 When sync is disabled for the app, this should purge all saved CKRecord system
 fields so that extra space is not consumed on the user's device.
 
 @param error If the method fails, this will contain the details of the failure.
 
 @return YES if the purge succeeded or NO on a failure.
 */
- (BOOL)purgeAllSystemFieldsWithError:(NSError **)error;


@end