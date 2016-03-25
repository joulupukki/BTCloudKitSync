//
//  BTCloudKitSync.m
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

#import "BTCloudKitSync.h"

@import CloudKit;

@interface BTCloudKitSync ()

@property (nonatomic, strong) CKDatabase *privateDB;
@property (nonatomic, strong) NSTimer *syncTimer;
@property (nonatomic, assign) CKAccountStatus currentCloudKitStatus;
@property (nonatomic, assign) BOOL isSynchronizing;

@end

@implementation BTCloudKitSync

+ (BTCloudKitSync *)sharedInstance
{
	static dispatch_once_t onceToken = 0;
	static BTCloudKitSync *_sharedInstance = nil;
	
	dispatch_once(&onceToken, ^{
		_sharedInstance = [[BTCloudKitSync alloc] init];
	});
	
	return _sharedInstance;
}

- (instancetype)init
{
	if (self = [super init]) {
		
		[[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(_iCloudAccountChangedNotification:) name:CKAccountChangedNotification object:nil];
//		[[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(_iCloudAccountChangedNotification:) name:NSUbiquityIdentityDidChangeNotification object:nil];
	}
	return self;
}


- (void)configureWithDatabase:(id<BTCloudKitSyncDatabase>)database
{
	_localDatabase = database;
	[self _configureSyncWithCompletionHandler:^(BOOL success, NSError *error) {
		if (success == NO) {
			NSLog(@"Could not configure BTCloudKitSync: %@", error ? error : @"Unknown error");
		} else {
			_isConfigured = YES;
		}
	}];
}


- (void)enableSync:(BOOL)enableSync withCompletionBlock:(void (^)(BOOL success, NSError *error))completion
{
	NSError *localDBError = nil;
	if ([self _isDatabaseValidWithError:&localDBError] == NO) {
		completion(NO, localDBError);
		return;
	}
	
	dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
		if (enableSync == self.syncEnabled) {
			// Nothing to do because the sync is already set up as the user wishes.
			dispatch_async(dispatch_get_main_queue(), ^{
				completion(YES, nil);
			});
			return;
		}
		
		if (enableSync && _currentCloudKitStatus != CKAccountStatusAvailable) {
			dispatch_async(dispatch_get_main_queue(), ^{
				// User is attempting to enable sync when there is no CloudKit account
				if (_currentCloudKitStatus == CKAccountStatusNoAccount) {
					completion(NO, [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"No iCloud account is logged in on this device."}]);
				} else if (_currentCloudKitStatus == CKAccountStatusRestricted) {
					completion(NO, [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"Parental Controls / Device Management has denied access to iCloud account credentials."}]);
				} else if (_currentCloudKitStatus == CKAccountStatusCouldNotDetermine) {
					completion(NO, [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"Could not determine the status of the device's iCloud account."}]);
				}
			});
			return;
		}
		
		if (enableSync) {
			// Enable sync
			//	1.	Fetch record zones and make sure that the Contacts Zone is created
			//	2.	Create the contacts zone if it's not already created
			//	3.	Register for silent notifications to receive server changes (CKSubscription)
			//	4.  Add change observers
			//	5.	Tell our local database to insert all existing records into the
			//		changelog table(s) so local changes will be picked up during
			//		syncs
			NSOperationQueue *queue = [NSOperationQueue new];
			queue.name = @"enableSyncQueue";
			queue.qualityOfService = NSQualityOfServiceUtility;
			
			CKFetchRecordZonesOperation *fetchRecordZonesOp = [CKFetchRecordZonesOperation fetchAllRecordZonesOperation];
			CKModifyRecordZonesOperation *modifyRecordZonesOp = [[CKModifyRecordZonesOperation alloc] init];
			CKFetchSubscriptionsOperation *fetchSubOp = [CKFetchSubscriptionsOperation fetchAllSubscriptionsOperation];
			CKModifySubscriptionsOperation *modifySubsOp = [[CKModifySubscriptionsOperation alloc] init];
			
			fetchRecordZonesOp.database = _privateDB;
			modifyRecordZonesOp.database = _privateDB;
			fetchSubOp.database = _privateDB;
			modifySubsOp.database = _privateDB;
			
			[modifyRecordZonesOp addDependency:fetchRecordZonesOp];
			[fetchSubOp addDependency:fetchRecordZonesOp];
			[fetchSubOp addDependency:modifyRecordZonesOp];
			[modifySubsOp addDependency:fetchSubOp];
			
			__block BOOL hasCreatedRecordZone = NO;
			NSString *recordZoneName = [_localDatabase recordZoneName];
			CKRecordZoneID *zoneID = [[CKRecordZoneID alloc] initWithZoneName:recordZoneName ownerName:CKOwnerDefaultName];
			fetchRecordZonesOp.fetchRecordZonesCompletionBlock = ^(NSDictionary <CKRecordZoneID *, CKRecordZone *> *recordZonesByZoneID, NSError *operationError) {
				if (fetchRecordZonesOp.isCancelled == NO) {
					if (operationError) {
						NSLog(@"Error fetching CKRecordZones: %@", operationError);
						[queue cancelAllOperations];
						dispatch_async(dispatch_get_main_queue(), ^{
							NSError *error = [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"Error checking for CKRecordZones.",
																															NSUnderlyingErrorKey:operationError}];
							completion(NO, error);
						});
					} else {
						if (recordZonesByZoneID[zoneID]) {
							hasCreatedRecordZone = YES;
							[modifyRecordZonesOp cancel];
						}
					}
				}
			};
			
			CKRecordZone *recordZone = [[CKRecordZone alloc] initWithZoneID:zoneID];
			modifyRecordZonesOp.recordZonesToSave = @[recordZone];
			modifyRecordZonesOp.modifyRecordZonesCompletionBlock = ^(NSArray *savedRecordZones, NSArray *deletedRecordZoneIDs, NSError *operationError) {
				if (modifyRecordZonesOp.cancelled == NO) {
					if (operationError) {
						NSLog(@"Error creating CKRecordZone: %@", operationError);
						[queue cancelAllOperations];
						dispatch_async(dispatch_get_main_queue(), ^{
							NSError *error = [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"Error saving the CKRecordZone.",
																															NSUnderlyingErrorKey:operationError}];
							completion(NO, error);
						});
					} else {
						if (savedRecordZones.count > 0) {
							hasCreatedRecordZone = YES;
						}
					}
				}
			};
			
			// 1. Register for CKSubscription
			
			NSString *subscriptionID = [recordZoneName stringByAppendingString:@"Subscription"];
			
			__block BOOL hasSubscribedToChanges = NO;
			fetchSubOp.fetchSubscriptionCompletionBlock = ^( NSDictionary <NSString *, CKSubscription *> *subscriptionsBySubscriptionID, NSError *operationError) {
				if (fetchSubOp.cancelled == NO) {
					if (operationError) {
						NSLog(@"Error retrieving existing subscriptions: %@", operationError);
						[queue cancelAllOperations];
						dispatch_async(dispatch_get_main_queue(), ^{
							NSError *error = [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"Error checking for existing CKSubscriptions.",
																															NSUnderlyingErrorKey:operationError}];
							completion(NO, error);
						});
					} else {
						if (subscriptionsBySubscriptionID[subscriptionID]) {
							hasSubscribedToChanges = YES;
							[modifySubsOp cancel];
						}
					}
				}
			};
			
			
			// This must be the first time sync has been enabled for this user. Create
			// a new zone subscription.
			CKSubscription *subscription = [[CKSubscription alloc] initWithZoneID:zoneID subscriptionID:subscriptionID options:0];
			
			// Configure the subscription to receive silent notifications
			CKNotificationInfo *info = [CKNotificationInfo new];
			info.shouldSendContentAvailable = YES;
			subscription.notificationInfo = info;
			
			modifySubsOp.subscriptionsToSave = @[subscription];
			modifySubsOp.modifySubscriptionsCompletionBlock = ^( NSArray <CKSubscription *> *savedSubscriptions, NSArray <NSString *> *deletedSubscriptionIDs, NSError *operationError) {
				if (modifySubsOp.cancelled == NO) {
					if (operationError) {
						NSLog(@"Error creating the subscription for contact changes: %@", operationError);
						if (operationError) {
							dispatch_async(dispatch_get_main_queue(), ^{
								completion(NO, [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"Error creating CKSubscription for contact changes.",
																															  NSUnderlyingErrorKey:operationError.localizedDescription}]);
							});
						}
					} else {
						hasSubscribedToChanges = YES;
					}
				}
			};
			
			[queue addOperations:@[fetchRecordZonesOp,modifyRecordZonesOp,fetchSubOp,modifySubsOp] waitUntilFinished:YES];
			
			if (hasSubscribedToChanges == NO) {
				return; // Could not subscribe to changes
			}
			
			// Set sync enabled so that when the changes are populated into the
			// database, they will automatically be synchronized.
			[[NSUserDefaults standardUserDefaults] setBool:enableSync forKey:kBTCloudKitSyncSettingSyncEnabledKey];
			[[NSUserDefaults standardUserDefaults] synchronize];
			
			// 2. Register observers
			[self _observeLocalDatabaseChanges:YES];
			
			// 3. Prep to track changes
			__block BOOL shouldReturn = NO;
			[[_localDatabase recordTypes] enumerateObjectsUsingBlock:^(NSString * _Nonnull recordType, NSUInteger idx, BOOL * _Nonnull stop) {
				NSError *localDBError = nil;
				if ([_localDatabase configureChangeTrackingForRecordType:recordType error:&localDBError] == NO) {
					NSError *error = [NSError errorWithDomain:BTCloudKitSyncErrorDomain
														 code:BTCloudKitSyncErrorConfigureChangeTracking
													 userInfo:@{NSLocalizedDescriptionKey:@"Could not configure change tracking in the local database.",
																BTCloudKitSyncRecordTypeKey:recordType,
																NSUnderlyingErrorKey:localDBError}];
					dispatch_async(dispatch_get_main_queue(), ^{
						completion(NO, error);
					});
					shouldReturn = YES;
					*stop = YES;
				}
			}];
			
			if (shouldReturn) {
				return;
			}
			
			// Kick off a sync by faking a change notification. This will go through the
			// timer system and delay by X seconds (which will allow startup of the app
			// to run smoothly).
			NSString *notificationName = [[_localDatabase databaseChangeNotificationNames] firstObject];
			[[NSNotificationCenter defaultCenter] postNotificationName:notificationName object:self];
		} else {
			// Disable sync
			//	1.	Stop observing changelog table changes
			//	2.	Delete all records from changelog database table(s) so that the
			//		app is not taking extra space
			//	3.	Update the app preference so that we don't try to sync on
			//		changes.
			//	4.	Remove the saved server token if one exists
			
			// 1. Remove observers
			[self _observeLocalDatabaseChanges:NO];
			
			// 2. Delete all records from the changelog db table
			__block BOOL shouldReturn = NO;
			[[_localDatabase recordTypes] enumerateObjectsUsingBlock:^(NSString * _Nonnull recordType, NSUInteger idx, BOOL * _Nonnull stop) {
				NSError *dbError = nil;
				if ([_localDatabase purgeRecordChangesOfType:recordType beforeDate:[NSDate date] error:&dbError] == NO) {
					NSError *error = [NSError errorWithDomain:BTCloudKitSyncErrorDomain
														 code:BTCloudKitSyncErrorPurgeChanges
													 userInfo:@{NSLocalizedDescriptionKey:@"Could not purge record changes from the database.",
																BTCloudKitSyncRecordTypeKey:recordType,
																NSUnderlyingErrorKey:dbError}];
					dispatch_async(dispatch_get_main_queue(), ^{
						completion(NO, error);
					});
					shouldReturn = YES;
					*stop = YES;
				}
			}];
			
			if (shouldReturn) {
				return;
			}
			
			// Purge the sync data (CKRecord system fields)
			NSError *dbError = nil;
			if ([_localDatabase purgeAllSystemFieldsWithError:&dbError] == NO) {
				NSError *error = [NSError errorWithDomain:BTCloudKitSyncErrorDomain
													 code:BTCloudKitSyncErrorPurgeSystemFields
												 userInfo:@{NSLocalizedDescriptionKey:@"Could not purge CKRecord system fields from the database.",
															NSUnderlyingErrorKey:dbError}];
				dispatch_async(dispatch_get_main_queue(), ^{
					completion(NO, error);
				});
				return;
			}
			
			[[NSUserDefaults standardUserDefaults] setBool:enableSync forKey:kBTCloudKitSyncSettingSyncEnabledKey];
			[[NSUserDefaults standardUserDefaults] removeObjectForKey:kBTCloudKitSyncSettingLastSyncDateKey];
			[[NSUserDefaults standardUserDefaults] removeObjectForKey:kBTCloudKitSyncServerChangeTokenKey];
			[[NSUserDefaults standardUserDefaults] synchronize];
		}
		
		dispatch_async(dispatch_get_main_queue(), ^{
			completion(YES, nil);
		});
	});
}


- (void)performSyncWithCompletion:(void(^)(BOOL success, NSError *error))completion
{
	if (self.syncEnabled == NO) {
		completion(NO, [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"Attempted to sync with sync currently disabled."}]);
		return;
	}
	
	@synchronized (self) {
		if (_isSynchronizing) {
			completion(NO, [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"Cannot perform a sync when a sync is already running."}]);
			return;
		}
		_isSynchronizing = YES;
	}
	
	dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
		
		dispatch_async(dispatch_get_main_queue(), ^{
			NSLog(@"Sync Began");
			[[NSNotificationCenter defaultCenter] postNotificationName:kBTCloudKitSyncBeganNotification object:nil];
		});
		
		__block BOOL syncSuccessful = YES;
		__block NSError *syncError = nil;
		
		NSString *recordZoneName = [_localDatabase recordZoneName];
		CKRecordZoneID *zoneID = [[CKRecordZoneID alloc] initWithZoneName:recordZoneName ownerName:CKOwnerDefaultName];
		
		NSDate *syncDate = [NSDate date];
		
		NSMutableArray<CKRecord *> *recordsToSaveA = [NSMutableArray new];
		NSMutableArray<CKRecordID *> *recordsToDeleteA = [NSMutableArray new];
		
		__block BOOL shouldReturn = NO;
		[[_localDatabase recordTypes] enumerateObjectsUsingBlock:^(NSString * _Nonnull recordType, NSUInteger idx, BOOL * _Nonnull stop) {
			NSError *dbError = nil;
			NSArray<NSDictionary *> *recordsA = [_localDatabase recordChangesOfType:recordType beforeDate:syncDate error:&dbError];
			if (recordsA == nil) {
				@synchronized (self) {
					_isSynchronizing = NO;
				}
				completion(NO, dbError);
				shouldReturn = YES;
				*stop = YES;
			} else {
				[recordsA enumerateObjectsUsingBlock:^(NSDictionary * _Nonnull changeInfo, NSUInteger idx, BOOL * _Nonnull stop) {
					NSString *recordIdentifier = changeInfo[BTCloudKitSyncChangeRecordIdentifier];
					if (recordIdentifier) {
						CKRecord *ckRecord = nil;
						CKRecordID *ckRecordID = nil;
						
						NSData *systemFields = [_localDatabase systemFieldsDataForRecordWithIdentifier:recordIdentifier error:nil];
						if (systemFields) {
							NSKeyedUnarchiver *archiver = [[NSKeyedUnarchiver alloc] initForReadingWithData:systemFields];
							archiver.requiresSecureCoding = YES;
							ckRecord = [[CKRecord alloc] initWithCoder:archiver];
							ckRecordID = ckRecord.recordID;
						} else {
							ckRecordID = [[CKRecordID alloc] initWithRecordName:recordIdentifier zoneID:zoneID];
						}
						
						NSString *changeType = changeInfo[BTCloudKitSyncChangeTypeKey];
						if ([changeType isEqualToString:BTCloudKitSyncChangeTypeDelete]) {
							// Only add records to delete that are known to be in the
							// CloudKit system. It's possible that a record showing up
							// for deletion was just added before every syncing to
							// CloudKit, so there's no reason to add it for deletion.
							if (systemFields) {
								[recordsToDeleteA addObject:ckRecordID];
							}
						} else {
							if (ckRecord == nil) {
								// This must be the very first time we've sent this
								// to the server (first insert).
								ckRecord = [[CKRecord alloc] initWithRecordType:recordType recordID:ckRecordID];
							}
							
							NSDictionary *recordInfo = changeInfo[BTCloudKitSyncChangeRecordInfoKey];
							if (recordInfo) {
								[recordInfo enumerateKeysAndObjectsUsingBlock:^(id  _Nonnull key, id  _Nonnull obj, BOOL * _Nonnull stop) {
									[ckRecord setObject:obj forKey:key];
								}];
								[recordsToSaveA addObject:ckRecord];
							}
						}
					}
				}];
			}
		}];
		
		if (shouldReturn) {
			return;
		}
		
		NSOperationQueue *queue = [NSOperationQueue new];
		queue.name = @"SyncQueue";
		queue.qualityOfService = NSQualityOfServiceUtility;
		NSMutableArray *operationsA = [NSMutableArray new];
		
		CKModifyRecordsOperation *modifyRecordsOp = nil;
		if (recordsToSaveA.count > 0 || recordsToDeleteA.count > 0) {
			modifyRecordsOp = [[CKModifyRecordsOperation alloc] initWithRecordsToSave:recordsToSaveA recordIDsToDelete:recordsToDeleteA];
			modifyRecordsOp.database = _privateDB;
			modifyRecordsOp.savePolicy = CKRecordSaveIfServerRecordUnchanged;
			modifyRecordsOp.atomic = YES;
			[operationsA addObject:modifyRecordsOp];
		}
		modifyRecordsOp.modifyRecordsCompletionBlock = ^(NSArray<CKRecord *> *savedRecords, NSArray<CKRecordID *> *deletedRecordIDs, NSError *operationError) {
			if (modifyRecordsOp.isCancelled == NO) {
				if (operationError == nil) {
					// Save off the system fields of the CKRecord so that future
					// updates of the CKRecords will work.
					if (savedRecords) {
						[savedRecords enumerateObjectsUsingBlock:^(CKRecord * _Nonnull savedRecord, NSUInteger idx, BOOL * _Nonnull stop) {
							NSMutableData *archivedData = [NSMutableData new];
							NSKeyedArchiver *archiver = [[NSKeyedArchiver alloc] initForWritingWithMutableData:archivedData];
							archiver.requiresSecureCoding = YES;
							[savedRecord encodeSystemFieldsWithCoder:archiver];
							[archiver finishEncoding];
							
							NSString *identifier = savedRecord.recordID.recordName;
							if (identifier) {
								NSError *dbError = nil;
								if ([_localDatabase saveSystemFieldsData:archivedData withIdentifier:identifier error:&dbError] == NO) {
									// TO-DO: Figure out what to do about this error (shouldn't ever happen)
									NSLog(@"Unable to save archived system fields: %@", dbError);
								}
							}
						}];
					}
					
					if (deletedRecordIDs) {
						[deletedRecordIDs enumerateObjectsUsingBlock:^(CKRecordID * _Nonnull recordID, NSUInteger idx, BOOL * _Nonnull stop) {
							NSString *identifier = recordID.recordName;
							if (identifier) {
								NSError *dbError = nil;
								if ([_localDatabase deleteSystemFieldsForRecordWithIdentifier:identifier error:&dbError] == NO) {
									NSLog(@"Unable to delete the archived system fields for record id (%@): %@", identifier, dbError);
								}
							}
						}];
					}
					
					[[_localDatabase recordTypes] enumerateObjectsUsingBlock:^(NSString * _Nonnull recordType, NSUInteger idx, BOOL * _Nonnull stop) {
						NSError *purgeError = nil;
						if ([_localDatabase purgeRecordChangesOfType:recordType beforeDate:syncDate error:&purgeError] == NO) {
							[queue cancelAllOperations];
							syncSuccessful = NO;
							syncError = [NSError errorWithDomain:BTCloudKitSyncErrorDomain
															code:BTCloudKitSyncErrorPurgeChanges
														userInfo:@{NSLocalizedDescriptionKey:@"Could not purge changes after successful sync.",
																   BTCloudKitSyncRecordTypeKey:recordType,
																   NSUnderlyingErrorKey: purgeError}];
							@synchronized (self) {
								_isSynchronizing = NO;
							}
							
							dispatch_async(dispatch_get_main_queue(), ^{
								completion(NO, syncError);
							});
						}
					}];
				} else {
					// TO-DO: Figure out how to handle errors
					switch (operationError.code) {
						case CKErrorRequestRateLimited:
						{
							// Client is being rate limited
							// Check userInfo's CKErrorRetryAfterKey to get NSTimeInterval when it's safe again to try
							break;
						}
						case CKErrorChangeTokenExpired:
						{
							// The previousServerChangeToken value is too old and the client must re-sync from scratch
							break;
						}
						case CKErrorBatchRequestFailed:
						{
							// One of the items in this batch operation failed in a zone with atomic updates, so the entire batch was rejected.
							
							// TO-DO: Go through each record and see which ones
							// we need to fix the problems on.
							break;
						}
						case CKErrorZoneBusy:
						{
							// The server is too busy to handle this zone operation. Try the operation again in a few seconds.
							break;
						}
						case CKErrorLimitExceeded:
						{
							// The request to the server was too large. Retry this request as a smaller batch.
							break;
						}
						case CKErrorQuotaExceeded:
						{
							// Saving a record would exceed quota
							break;
						}
						case CKErrorServerRecordChanged:
						{
							// TO-DO: Make modifications on the server record
							// and push these back to the server.
							CKRecord *serverRecord = operationError.userInfo[CKRecordChangedErrorServerRecordKey];
							break;
						}
						default:
						{
							NSLog(@"Unhandled CKModifyRecordsOperation error (%ld): %@", operationError.code, operationError);
							break;
						}

					}
				}
			}
		};

		CKServerChangeToken *serverChangeToken = nil;
		NSData *data = [[NSUserDefaults standardUserDefaults] objectForKey:kBTCloudKitSyncServerChangeTokenKey];
		if (data) {
			serverChangeToken = [NSKeyedUnarchiver unarchiveObjectWithData:data];
		}
		
		CKFetchRecordChangesOperation *fetchChangesOp = [[CKFetchRecordChangesOperation alloc] initWithRecordZoneID:zoneID previousServerChangeToken:serverChangeToken];
		fetchChangesOp.database = _privateDB;
		[operationsA addObject:fetchChangesOp];
		if (modifyRecordsOp) {
			[fetchChangesOp addDependency:modifyRecordsOp];
		}
		fetchChangesOp.fetchRecordChangesCompletionBlock = ^(CKServerChangeToken *serverChangeToken, NSData *clientChangeTokenData, NSError *operationError) {
			if (fetchChangesOp.cancelled == NO) {
				if (operationError != nil) {
					[queue cancelAllOperations];
					syncSuccessful = NO;
					syncError = [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"Could not fetch server changes.",
																											   NSUnderlyingErrorKey: operationError}];
					@synchronized (self) {
						_isSynchronizing = NO;
					}
					dispatch_async(dispatch_get_main_queue(), ^{
						completion(NO, syncError);
					});
				} else {
					NSData *encodedServerChangeToken = [NSKeyedArchiver archivedDataWithRootObject:serverChangeToken];
					[[NSUserDefaults standardUserDefaults] setObject:encodedServerChangeToken forKey:kBTCloudKitSyncServerChangeTokenKey];
					[[NSUserDefaults standardUserDefaults] setObject:[NSDate date] forKey:kBTCloudKitSyncSettingLastSyncDateKey];
					[[NSUserDefaults standardUserDefaults] synchronize];
					
					if (fetchChangesOp.moreComing == YES) {
						// TO-DO: Figure out how to issue another fetch operation
//						fetchChangesOp = [self _configureFetchChangesOp];
//						[queue addOperation:fetchChangesOp];
					}
				}
			}
		};
		fetchChangesOp.recordChangedBlock = ^(CKRecord *record) {
			
			// First check to see if the local record already matches and if
			// it does, do nothing.
			NSData *systemFields = [_localDatabase systemFieldsDataForRecordWithIdentifier:record.recordID.recordName error:nil];
			if (systemFields) {
				NSKeyedUnarchiver *archiver = [[NSKeyedUnarchiver alloc] initForReadingWithData:systemFields];
				archiver.requiresSecureCoding = YES;
				CKRecord *localRecord = [[CKRecord alloc] initWithCoder:archiver];
				if ([localRecord.recordChangeTag isEqualToString:record.recordChangeTag]) {
					// The record in the database is already the same and does
					// not need to be changed or updated.
					return;
				}
			}
			
			BOOL addContact = NO;
			NSMutableDictionary *mutableRecordInfo = nil;
			NSDictionary *recordInfo = [_localDatabase infoForRecordType:record.recordType withIdentifier:record.recordID.recordName error:nil];
			if (recordInfo == nil) {
				// This is a new contact
				addContact = YES;
				
				mutableRecordInfo = [NSMutableDictionary new];
//				contact.identifier = record.recordID.recordName;
			} else {
				mutableRecordInfo = [NSMutableDictionary dictionaryWithDictionary:recordInfo];
			}
			
			[[record allKeys] enumerateObjectsUsingBlock:^(NSString * _Nonnull key, NSUInteger idx, BOOL * _Nonnull stop) {
				mutableRecordInfo[key] = record[key];
			}];
			
			NSError *error = nil;
			if (addContact) {
				if ([_localDatabase addRecordInfo:mutableRecordInfo withRecordType:record.recordType withIdentifier:record.recordID.recordName error:&error] == NO) {
					// TO-DO: Figure out how to properly handle this error
					NSLog(@"Error adding a record fetched from the server: %@", error);
				}
			} else {
				if ([_localDatabase updateRecordInfo:mutableRecordInfo withRecordType:record.recordType withIdentifier:record.recordID.recordName error:&error] == NO) {
					// TO-DO: Figure out how to properly handle this error
					NSLog(@"Error updating a record fetched from the server: %@", error);
				}
			}
			
			// Save off the system fields of the CKRecord so that future updates
			// will work properly.
			NSMutableData *archivedData = [NSMutableData new];
			NSKeyedArchiver *archiver = [[NSKeyedArchiver alloc] initForWritingWithMutableData:archivedData];
			archiver.requiresSecureCoding = YES;
			[record encodeSystemFieldsWithCoder:archiver];
			[archiver finishEncoding];
			
			NSString *identifier = record.recordID.recordName;
			NSError *dbError = nil;
			if ([_localDatabase saveSystemFieldsData:archivedData withIdentifier:identifier error:&dbError] == NO) {
				// TO-DO: Figure out what to do about this error (shouldn't ever happen)
				NSLog(@"Unable to save archived system fields: %@", dbError);
			}
			
		};
		fetchChangesOp.recordWithIDWasDeletedBlock = ^(CKRecordID *recordID) {
			NSError *error = nil;
			[_localDatabase deleteRecordWithIdentifier:recordID.recordName error:&error];
			if (error) {
				// If there was an error deleting, not sure what to do, but it
				// may not be crucial.
				NSLog(@"Error deleting a contact while fetching server changes: %@", recordID.recordName);
			}
		};
		
		[queue addOperations:operationsA waitUntilFinished:YES];
		
		if (syncSuccessful) {
			[[NSUserDefaults standardUserDefaults] setObject:[NSDate date] forKey:kBTCloudKitSyncSettingLastSyncDateKey];
			[[NSUserDefaults standardUserDefaults] synchronize];
			
			@synchronized (self) {
				_isSynchronizing = NO;
			}
			dispatch_async(dispatch_get_main_queue(), ^{
				NSLog(@"Sync Ended with Success");
				[[NSNotificationCenter defaultCenter] postNotificationName:kBTCloudKitSyncSuccessNotification object:nil];
				completion(YES, nil);
			});
		} else {
			@synchronized (self) {
				_isSynchronizing = NO;
			}
			dispatch_async(dispatch_get_main_queue(), ^{
				NSLog(@"Sync Ended with Error");
				[[NSNotificationCenter defaultCenter] postNotificationName:kBTCloudKitSyncErrorNotification object:nil userInfo:@{kBTCloudKitSyncErrorKey:syncError}];
				completion(NO, syncError);
			});
		}
	});
}

#pragma mark - Custom Properties

- (BOOL)syncEnabled
{
	return [[NSUserDefaults standardUserDefaults] boolForKey:kBTCloudKitSyncSettingSyncEnabledKey];
}


- (NSDate *)lastSyncDate
{
	return [[NSUserDefaults standardUserDefaults] objectForKey:kBTCloudKitSyncSettingLastSyncDateKey];
}

#pragma mark - Private Methods

- (void)_localDatabaseChangedNotification:(NSNotification *)notification
{
	if (_syncTimer && _syncTimer.isValid) {
		[_syncTimer invalidate];
		_syncTimer = nil;
	}
	
	NSDate *fireDate = [NSDate dateWithTimeIntervalSinceNow:kBTCloudKitSyncChangeTimeoutInSeconds];
	_syncTimer = [[NSTimer alloc] initWithFireDate:fireDate interval:0 target:self selector:@selector(_syncTimerFired:) userInfo:nil repeats:NO];
	[[NSRunLoop mainRunLoop] addTimer:_syncTimer forMode:NSDefaultRunLoopMode];
}

- (void)_syncTimerFired:(NSTimer *)timer
{
	if (_syncTimer && _syncTimer.isValid) {
		[_syncTimer invalidate];
	}
	_syncTimer = nil;
	
	// Kick off a sync
	[self performSyncWithCompletion:^(BOOL success, NSError *error) {
		if (success) {
			NSLog(@"A successful sync occurred from the sync timer.");
		} else {
			NSLog(@"A sync from the sync timer failed: %@", error);
		}
	}];
}

- (void)_observeLocalDatabaseChanges:(BOOL)observeChanges
{
	NSNotificationCenter *notificationCenter = [NSNotificationCenter defaultCenter];
	NSArray *dbNotificationNames = [_localDatabase databaseChangeNotificationNames];
	if (dbNotificationNames) {
		if (observeChanges) {
			// Remove all observers first just in case we're already observing
			[self _observeLocalDatabaseChanges:NO];
			
			[dbNotificationNames enumerateObjectsUsingBlock:^(NSString * _Nonnull notificationName, NSUInteger idx, BOOL * _Nonnull stop) {
				[notificationCenter addObserver:self selector:@selector(_localDatabaseChangedNotification:) name:notificationName object:nil];
			}];
		} else {
			[dbNotificationNames enumerateObjectsUsingBlock:^(NSString * _Nonnull notificationName, NSUInteger idx, BOOL * _Nonnull stop) {
				[notificationCenter removeObserver:self name:notificationName object:nil];
			}];
		}
	}
}


- (void)_configureSyncWithCompletionHandler:(void (^)(BOOL success, NSError *error))completionHandler
{
	NSError *localDBError = nil;
	if ([self _isDatabaseValidWithError:&localDBError] == NO) {
		completionHandler(NO, localDBError);
		return;
	}
	
	// Check CloudKit account status even if sync is not enabled.
	CKContainer *container = [CKContainer defaultContainer];
	[container accountStatusWithCompletionHandler:^(CKAccountStatus accountStatus, NSError * _Nullable error) {
		_currentCloudKitStatus = accountStatus;
		if (accountStatus != CKAccountStatusAvailable) {
			[self enableSync:NO withCompletionBlock:^(BOOL success, NSError *error) {
				if (success) {
					NSLog(@"Disabled sync because the CloudKit account status is not available.");
				} else {
					NSLog(@"Could not disable sync in _configureSyncWithCompletionHandler:: %@", error);
				}
			}];
			dispatch_async(dispatch_get_main_queue(), ^{
				completionHandler(NO, error ? error : [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"A CloudKit account is not available"}]);
			});
		} else {
			_privateDB = container.privateCloudDatabase;
			
			// During launch of the app, we need to make sure that we have everything
			// set up to sync if sync is enabled.
			if (self.syncEnabled == NO) {
				// It's not really an error if sync is not enabled, but we don't
				// need to do any more processing here.
				completionHandler(YES, nil);
				return;
			}
			
			
			[self _observeLocalDatabaseChanges:YES];
			
			// Kick off a sync by faking a change notification. This will go through the
			// timer system and delay by X seconds (which will allow startup of the app
			// to run smoothly).
			NSString *notificationName = [[_localDatabase databaseChangeNotificationNames] firstObject];
			[[NSNotificationCenter defaultCenter] postNotificationName:notificationName object:self];
			
			dispatch_async(dispatch_get_main_queue(), ^{
				completionHandler(YES, nil);
			});
		}
	}];
}

- (void)_iCloudAccountChangedNotification:(NSNotification *)notification
{
	// Reconfigure sync (check on the CloudKit account status)
	[self _configureSyncWithCompletionHandler:^(BOOL success, NSError *error) {
		if (success == NO) {
			NSLog(@"Could not configure sync after receiving CKAccountChangedNotification: %@", error ? error : @"Unknown error");
		}
	}];
}


/**
 This is a convenience method that allows BTCloudKitSync to verify that all the
 necessary information from localDatabase are available.
 */
- (BOOL)_isDatabaseValidWithError:(NSError **)error
{
	if (_localDatabase == nil) {
		if (error) {
			*error = [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:BTCloudKitSyncErrorMissingDatabase userInfo:@{NSLocalizedDescriptionKey:@"Missing localDatabase."}];
		}
		return NO;
	}
	
	NSString *recordZoneName = [_localDatabase recordZoneName];
	if (recordZoneName == nil || [recordZoneName stringByTrimmingCharactersInSet:[NSCharacterSet whitespaceAndNewlineCharacterSet]].length == 0) {
		if (error) {
			*error = [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:BTCloudKitSyncErrorNoRecordZoneName userInfo:@{NSLocalizedDescriptionKey:@"Database is not providing a valid recordZoneName."}];
		}
		return NO;
	}
	
	NSArray *recordTypes = [_localDatabase recordTypes];
	if (recordTypes == nil || recordTypes.count == 0) {
		if (error) {
			*error = [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:BTCloudKitSyncErrorNoRecordTypes userInfo:@{NSLocalizedDescriptionKey:@"Database is not providing any recordTypes."}];
		}
		return NO;
	}
	
	NSArray *notificationNames = [_localDatabase databaseChangeNotificationNames];
	if (notificationNames == nil || notificationNames.count == 0) {
		if (error) {
			*error = [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:BTCloudKitSyncErrorNoChangeNotifications userInfo:@{NSLocalizedDescriptionKey:@"Database is not providing databaseChangeNotificationNames."}];
		}
		return NO;
	}
	
	return YES;
}

@end
