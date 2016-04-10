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

@property (nonatomic, strong) NSOperationQueue *syncQueue;
@property (nonatomic, strong) NSOperationQueue *fetchQueue;

/**
 If set to 0, we will ask for ALL changes from the local database. If set to
 something other than 0, BTCloudKitSync will only ask for this number of local
 changes to send to CloudKit at a time. This is a way for the implementation of
 BTCloudKitSyncDatabase to make an intelligent decision about how many records
 to allow at a time. Theoretically, if _ALL_ changes would be sent at once, it
 could consume too much memory. Depending on the data being sent and possibly
 during a first sync, this could be too much.
 */
@property (nonatomic, assign) NSUInteger maxLocalChangesToSend;

/**
 When startSync begins and we fetch local changes to send to the server, record
 the current time as the currentSyncDate. If CloudKit reports that we have too
 many records to send, we will use this date to grab half of the current records
 and restart the CKModifyRecordsOperation with a smaller batch.
 
 The currentBatchSizeToSend will be adjusted to half the current size and
 BTCloudKitSync will ask for modified records from BTCloudKitSyncDatabase with
 the limit set to currentBatchSizeToSend. At the successful completion of the
 current CKModifyRecordsOperation, BTCloudKitSyncDatabase should be asked to
 purge these changed records by specifying the same limit
 (currentBatchSizeToSend).
 */
@property (nonatomic, strong) NSDate *currentSyncDate;

/**
 During a CKModifyRecordsOperation, it's possible that CloudKit will report back
 that there were too many records sent. In this condition, BTCloudKitSync should
 set this to half the size and retry the CKModifyRecordsOperation with a reduced
 set of changes.
 */
@property (nonatomic, assign) NSUInteger currentBatchSizeToSend;

@property (nonatomic, strong) NSString *currentRecordType;


@property (nonatomic, strong) NSMutableDictionary *currentSyncDates;

@property (nonatomic, assign) BOOL isCurrentlyFetching;

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
		_syncQueue = [NSOperationQueue new];
		_syncQueue.name = @"SyncQueue";
		_syncQueue.qualityOfService = NSQualityOfServiceUtility;
		_syncQueue.maxConcurrentOperationCount = 1; // Keep some order to everything
		
		_fetchQueue = [NSOperationQueue new];
		_fetchQueue.name = @"FetchRecordChangesQueue";
		_fetchQueue.qualityOfService = NSQualityOfServiceUtility;
		_fetchQueue.maxConcurrentOperationCount = 1;
		
		_currentSyncDates = [NSMutableDictionary new];
		
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
			//	1.	Set up the CKRecordZone(s)
			//	2.	Set up the subscription(s), needed for silent notifications
			//		to be received.
			//	3.  Add change observers to receive notifications when the local
			//		database changes records.
			//	4.	Tell the local database to track local changes
			//	5.	Kick off a sync to push the local records to CloudKit
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
			
			//	1.	Set up the CKRecordZone(s)
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
			
			//	2.	Set up the subscription(s)
			
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
						NSLog(@"Error creating the subscription for record changes: %@", operationError);
						if (operationError) {
							dispatch_async(dispatch_get_main_queue(), ^{
								completion(NO, [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"Error creating CKSubscription for record changes.",
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
			
			//	3.  Add change observers to receive notifications when the local
			[self _observeLocalDatabaseChanges:YES];
			
			//	4.	Tell the local database to track local changes
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
			
			//	5.	Kick off a sync to push the local records to CloudKit
			// Kick off a sync by faking a change notification. This will go through the
			// timer system and delay by X seconds (which will allow startup of the app
			// to run smoothly).
			[self _localDatabaseChangedNotification:nil];
		} else {
			// Disable sync
			//	1.	Stop observing local database changes
			//	2.	Tell local database to purge local changes so it doesn't
			//		consume extra space
			//		app is not taking extra space
			//	3.	Delete CKRecord system fields
			//	4.	Reset sync keys saved in NSUserDefaults
			
			//	1.	Stop observing local database changes
			[self _observeLocalDatabaseChanges:NO];
			
			//	2.	Purge local changes
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
			
			//	3.	Delete CKRecord system fields
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
			
			//	4.	Reset sync keys saved in NSUserDefaults
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


- (void)startSync
{
	if ([_localDatabase shouldAllowSync] == NO) {
		// Prevent a sync from happening and also disable sync if it is enabled.
		NSLog(@"Attempted to sync, but a sync is not currently allowed. Disabling sync...");
		if ([self syncEnabled]) {
			[self enableSync:NO withCompletionBlock:^(BOOL success, NSError *error) {
				if (success) {
					NSLog(@"Sync has been disabled");
				} else {
					NSLog(@"Failed to disable sync: %@", error);
				}
			}];
		}
		
		return;
	}
	
	if (self.syncEnabled == NO) {
		NSLog(@"Attempted to sync with sync currently disabled.");
		return;
	}
	
	NSError *localDBError = nil;
	if ([self _isDatabaseValidWithError:&localDBError] == NO) {
		return;
	}
	
	// If _currentSyncDate is not nil, that means we are already performing a
	// synchronization and should not start another one.
	@synchronized (self) {
		if (_currentSyncDate != nil) {
			NSLog(@"startSync called while a sync is already in progress.");
			return;
		}
		
		_currentSyncDate = [NSDate date];
	}
	
	dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
		
		// If there is currently a fetch records operation occurring, wait until
		// it is finished before continuing.
		if (_isCurrentlyFetching) {
			[_fetchQueue waitUntilAllOperationsAreFinished];
		}
		
		// For now, each record type will be synchronized individually to make
		// it easier to control limits/etc.
		__block BOOL modifyRecordsSucceeded = YES;
		NSArray<NSString *> *recordTypes = [_localDatabase recordTypes];
		if (recordTypes && recordTypes.count > 0) {
			[recordTypes enumerateObjectsUsingBlock:^(NSString * _Nonnull recordType, NSUInteger idx, BOOL * _Nonnull stop) {
				
				_currentRecordType = recordType;
				
				_currentBatchSizeToSend = _maxLocalChangesToSend;
				_currentSyncDates[recordType] = _currentSyncDate;
				
				NSError *dbError = nil;
				NSArray<NSDictionary *> *recordsA = [_localDatabase recordChangesOfType:recordType beforeDate:_currentSyncDate limit:_currentBatchSizeToSend error:&dbError];
				if (recordsA && recordsA.count > 0) {
					[self _modifyRecords:recordsA ofRecordType:recordType completionHandler:^(BOOL success) {
						NSLog(@"Modify records for record type (%@): %@", recordType, success ? @"succeeded" : @"failed");
						if (success == NO) {
							modifyRecordsSucceeded = NO;
						}
					}];
					
					// If the modify records operation has to run multiple times
					// because of size limits or resolving server conflicts, we
					// don't want to complicate things by allowing the fetch changes
					// operation (below) to run, so go ahead and wait until all
					// modify operations are completed first before continuing.
					[_syncQueue waitUntilAllOperationsAreFinished];
				}
			}];
		}
		
		if (modifyRecordsSucceeded == YES) {
			// TO-DO: Perhaps check for server changes here?
			
//			CKServerChangeToken *serverChangeToken = nil;
//			NSData *data = [[NSUserDefaults standardUserDefaults] objectForKey:kBTCloudKitSyncServerChangeTokenKey];
//			if (data) {
//				serverChangeToken = [NSKeyedUnarchiver unarchiveObjectWithData:data];
//			}
//			
//			[self _fetchRecordChangesWithServerChangeToken:serverChangeToken
//										 completionHandler:^(BTFetchResult result, BOOL moreComing) {
//											 // Delay setting this by just a tiny bit because in some cases,
//											 // a fetch will actually not quite have finished a local save,
//											 // BTCloudKitSync will receive a local change notification, and
//											 // kick off another sync. Theoretically, there's a slight chance
//											 // a user could make a change during this period and the app
//											 // wouldn't pick up the change until a later sync.
//											 // TO-DO: Figure out if there's a better way to handle wrapping up fetchChanges local notification kicking off a sync
//											 dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)(0.5 * NSEC_PER_SEC)), dispatch_get_main_queue(), ^{
//												 @synchronized (self) {
//													 _currentSyncDate = nil;
//													 _currentRecordType = nil;
//													 _currentBatchSizeToSend = 0;
//												 }
//											 });
//										 }];
			
			[self fetchRecordChangesWithCompletionHandler:^(BTFetchResult result, BOOL moreComing) {
				// Delay setting this by just a tiny bit because in some cases,
				// a fetch will actually not quite have finished a local save,
				// BTCloudKitSync will receive a local change notification, and
				// kick off another sync. Theoretically, there's a slight chance
				// a user could make a change during this period and the app
				// wouldn't pick up the change until a later sync.
				// TO-DO: Figure out if there's a better way to handle wrapping up fetchChanges local notification kicking off a sync
				dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)(0.5 * NSEC_PER_SEC)), dispatch_get_main_queue(), ^{
					@synchronized (self) {
						_currentSyncDate = nil;
						_currentRecordType = nil;
						_currentBatchSizeToSend = 0;
						[_currentSyncDates removeAllObjects];
					}
				});
			}];
		} else {
			@synchronized (self) {
				_currentSyncDate = nil;
				_currentRecordType = nil;
				_currentBatchSizeToSend = 0;
				[_currentSyncDates removeAllObjects];
			}
		}
	});
}


//- (void)performSyncWithCompletion:(void(^)(BOOL success, NSError *error))completion
//{
//	// The general recipe for this method is to:
//	//	1.	Push local changes
//	//	2.	Pull server changes
//	if (self.syncEnabled == NO) {
//		completion(NO, [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"Attempted to sync with sync currently disabled."}]);
//		return;
//	}
//	
//	@synchronized (self) {
//		if (_isSynchronizing) {
//			completion(NO, [NSError errorWithDomain:BTCloudKitSyncErrorDomain code:-1 userInfo:@{NSLocalizedDescriptionKey:@"Cannot perform a sync when a sync is already running."}]);
//			return;
//		}
//		_isSynchronizing = YES;
//	}
//	
//	dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
//		
//		dispatch_async(dispatch_get_main_queue(), ^{
//			NSLog(@"Sync Began");
//			[[NSNotificationCenter defaultCenter] postNotificationName:kBTCloudKitSyncBeganNotification object:nil];
//		});
//		
//		__block BOOL syncSuccessful = YES;
//		__block NSError *syncError = nil;
//		
//		NSString *recordZoneName = [_localDatabase recordZoneName];
//		CKRecordZoneID *zoneID = [[CKRecordZoneID alloc] initWithZoneName:recordZoneName ownerName:CKOwnerDefaultName];
//		
//		NSDate *syncDate = [NSDate date];
//		
//		NSMutableArray<CKRecord *> *recordsToSaveA = [NSMutableArray new];
//		NSMutableArray<CKRecordID *> *recordsToDeleteA = [NSMutableArray new];
//		
//		__block BOOL shouldReturn = NO;
//		[[_localDatabase recordTypes] enumerateObjectsUsingBlock:^(NSString * _Nonnull recordType, NSUInteger idx, BOOL * _Nonnull stop) {
//			NSError *dbError = nil;
//			NSArray<NSDictionary *> *recordsA = [_localDatabase recordChangesOfType:recordType beforeDate:syncDate error:&dbError];
//			if (recordsA == nil) {
//				@synchronized (self) {
//					_isSynchronizing = NO;
//				}
//				completion(NO, dbError);
//				shouldReturn = YES;
//				*stop = YES;
//			} else {
//				[recordsA enumerateObjectsUsingBlock:^(NSDictionary * _Nonnull changeInfo, NSUInteger idx, BOOL * _Nonnull stop) {
//					NSString *recordIdentifier = changeInfo[BTCloudKitSyncChangeRecordIdentifier];
//					if (recordIdentifier) {
//						CKRecord *ckRecord = nil;
//						CKRecordID *ckRecordID = nil;
//						
//						NSData *systemFields = [_localDatabase systemFieldsDataForRecordWithIdentifier:recordIdentifier error:nil];
//						if (systemFields) {
//							NSKeyedUnarchiver *archiver = [[NSKeyedUnarchiver alloc] initForReadingWithData:systemFields];
//							archiver.requiresSecureCoding = YES;
//							ckRecord = [[CKRecord alloc] initWithCoder:archiver];
//							ckRecordID = ckRecord.recordID;
//						} else {
//							ckRecordID = [[CKRecordID alloc] initWithRecordName:recordIdentifier zoneID:zoneID];
//						}
//						
//						NSString *changeType = changeInfo[BTCloudKitSyncChangeTypeKey];
//						if ([changeType isEqualToString:BTCloudKitSyncChangeTypeDelete]) {
//							// Only add records to delete that are known to be in the
//							// CloudKit system. It's possible that a record showing up
//							// for deletion was just added before every syncing to
//							// CloudKit, so there's no reason to add it for deletion.
//							if (systemFields) {
//								[recordsToDeleteA addObject:ckRecordID];
//							}
//						} else {
//							if (ckRecord == nil) {
//								// This must be the very first time we've sent this
//								// to the server (first insert).
//								ckRecord = [[CKRecord alloc] initWithRecordType:recordType recordID:ckRecordID];
//							}
//							
//							NSDictionary *recordInfo = changeInfo[BTCloudKitSyncChangeRecordInfoKey];
//							if (recordInfo) {
//								[recordInfo enumerateKeysAndObjectsUsingBlock:^(id  _Nonnull key, id  _Nonnull obj, BOOL * _Nonnull stop) {
//									[ckRecord setObject:obj forKey:key];
//								}];
//								[recordsToSaveA addObject:ckRecord];
//							}
//						}
//					}
//				}];
//			}
//		}];
//		
//		if (shouldReturn) {
//			return;
//		}
//		
//		NSOperationQueue *queue = [NSOperationQueue new];
//		queue.name = @"SyncQueue";
//		queue.qualityOfService = NSQualityOfServiceUtility;
//		NSMutableArray *operationsA = [NSMutableArray new];
//		
//		CKModifyRecordsOperation *modifyRecordsOp = nil;
//		if (recordsToSaveA.count > 0 || recordsToDeleteA.count > 0) {
//			modifyRecordsOp = [[CKModifyRecordsOperation alloc] initWithRecordsToSave:recordsToSaveA recordIDsToDelete:recordsToDeleteA];
//			modifyRecordsOp.database = _privateDB;
//			modifyRecordsOp.savePolicy = CKRecordSaveIfServerRecordUnchanged;
//			modifyRecordsOp.atomic = YES;
//			[operationsA addObject:modifyRecordsOp];
//		}
//		modifyRecordsOp.modifyRecordsCompletionBlock = ^(NSArray<CKRecord *> *savedRecords, NSArray<CKRecordID *> *deletedRecordIDs, NSError *operationError) {
//			if (modifyRecordsOp.isCancelled == NO) {
//				if (operationError == nil) {
//					// Save off the system fields of the CKRecord so that future
//					// updates of the CKRecords will work.
//					if (savedRecords) {
//						[savedRecords enumerateObjectsUsingBlock:^(CKRecord * _Nonnull savedRecord, NSUInteger idx, BOOL * _Nonnull stop) {
//							NSMutableData *archivedData = [NSMutableData new];
//							NSKeyedArchiver *archiver = [[NSKeyedArchiver alloc] initForWritingWithMutableData:archivedData];
//							archiver.requiresSecureCoding = YES;
//							[savedRecord encodeSystemFieldsWithCoder:archiver];
//							[archiver finishEncoding];
//							
//							NSString *identifier = savedRecord.recordID.recordName;
//							if (identifier) {
//								NSError *dbError = nil;
//								if ([_localDatabase saveSystemFieldsData:archivedData withIdentifier:identifier error:&dbError] == NO) {
//									// TO-DO: Figure out what to do about this error (shouldn't ever happen)
//									NSLog(@"Unable to save archived system fields: %@", dbError);
//								}
//							}
//						}];
//					}
//					
//					if (deletedRecordIDs) {
//						[deletedRecordIDs enumerateObjectsUsingBlock:^(CKRecordID * _Nonnull recordID, NSUInteger idx, BOOL * _Nonnull stop) {
//							NSString *identifier = recordID.recordName;
//							if (identifier) {
//								NSError *dbError = nil;
//								if ([_localDatabase deleteSystemFieldsForRecordWithIdentifier:identifier error:&dbError] == NO) {
//									NSLog(@"Unable to delete the archived system fields for record id (%@): %@", identifier, dbError);
//								}
//							}
//						}];
//					}
//					
//					[[_localDatabase recordTypes] enumerateObjectsUsingBlock:^(NSString * _Nonnull recordType, NSUInteger idx, BOOL * _Nonnull stop) {
//						NSError *purgeError = nil;
//						if ([_localDatabase purgeRecordChangesOfType:recordType beforeDate:syncDate error:&purgeError] == NO) {
//							[queue cancelAllOperations];
//							syncSuccessful = NO;
//							syncError = [NSError errorWithDomain:BTCloudKitSyncErrorDomain
//															code:BTCloudKitSyncErrorPurgeChanges
//														userInfo:@{NSLocalizedDescriptionKey:@"Could not purge changes after successful sync.",
//																   BTCloudKitSyncRecordTypeKey:recordType,
//																   NSUnderlyingErrorKey: purgeError}];
//							@synchronized (self) {
//								_isSynchronizing = NO;
//							}
//							
//							dispatch_async(dispatch_get_main_queue(), ^{
//								completion(NO, syncError);
//							});
//						}
//					}];
//				} else {
//					// TO-DO: Figure out how to handle errors
//					switch (operationError.code) {
//						case CKErrorRequestRateLimited:
//						{
//							// Client is being rate limited
//							// Check userInfo's CKErrorRetryAfterKey to get NSTimeInterval when it's safe again to try
//							break;
//						}
//						case CKErrorChangeTokenExpired:
//						{
//							// The previousServerChangeToken value is too old and the client must re-sync from scratch
//							break;
//						}
//						case CKErrorBatchRequestFailed:
//						{
//							// One of the items in this batch operation failed in a zone with atomic updates, so the entire batch was rejected.
//							
//							// TO-DO: Go through each record and see which ones
//							// we need to fix the problems on.
//							break;
//						}
//						case CKErrorZoneBusy:
//						{
//							// The server is too busy to handle this zone operation. Try the operation again in a few seconds.
//							break;
//						}
//						case CKErrorLimitExceeded:
//						{
//							// The request to the server was too large. Retry this request as a smaller batch.
//							break;
//						}
//						case CKErrorQuotaExceeded:
//						{
//							// Saving a record would exceed quota
//							break;
//						}
//						case CKErrorServerRecordChanged:
//						{
//							// TO-DO: Make modifications on the server record
//							// and push these back to the server.
//							CKRecord *serverRecord = operationError.userInfo[CKRecordChangedErrorServerRecordKey];
//							break;
//						}
//						default:
//						{
//							NSLog(@"Unhandled CKModifyRecordsOperation error (%ld): %@", operationError.code, operationError);
//							break;
//						}
//
//					}
//				}
//			}
//		};
//		
//		NSBlockOperation *fetchChangesOp = [NSBlockOperation blockOperationWithBlock:^{
//			[self fetchRecordChangesWithCompletionHandler:nil];
//		}];
//		if (modifyRecordsOp) {
//			[fetchChangesOp addDependency:modifyRecordsOp];
//		}
//		[operationsA addObject:fetchChangesOp];
//		
//		[queue addOperations:operationsA waitUntilFinished:YES];
//		
//		if (syncSuccessful) {
//			[[NSUserDefaults standardUserDefaults] setObject:[NSDate date] forKey:kBTCloudKitSyncSettingLastSyncDateKey];
//			[[NSUserDefaults standardUserDefaults] synchronize];
//			
//			@synchronized (self) {
//				_isSynchronizing = NO;
//			}
//			dispatch_async(dispatch_get_main_queue(), ^{
//				NSLog(@"Sync Ended with Success");
//				[[NSNotificationCenter defaultCenter] postNotificationName:kBTCloudKitSyncSuccessNotification object:nil];
//				completion(YES, nil);
//			});
//		} else {
//			@synchronized (self) {
//				_isSynchronizing = NO;
//			}
//			dispatch_async(dispatch_get_main_queue(), ^{
//				NSLog(@"Sync Ended with Error");
//				[[NSNotificationCenter defaultCenter] postNotificationName:kBTCloudKitSyncErrorNotification object:nil userInfo:@{kBTCloudKitSyncErrorKey:syncError}];
//				completion(NO, syncError);
//			});
//		}
//	});
//}


- (void)fetchRecordChangesWithCompletionHandler:(void (^)(BTFetchResult result, BOOL moreComing))completionHandler
{
	if ([_localDatabase shouldAllowSync] == NO) {
		// Prevent a sync from happening and also disable sync if it is enabled.
		NSLog(@"Attempted to fetch record changes, but a sync is not currently allowed. Disabling sync...");
		if ([self syncEnabled]) {
			[self enableSync:NO withCompletionBlock:^(BOOL success, NSError *error) {
				if (success) {
					NSLog(@"Sync has been disabled");
				} else {
					NSLog(@"Failed to disable sync: %@", error);
				}
			}];
		}
		
		return;
	}
	
	// It's possible that a CKModifyRecordsOperation is currently running. If
	// so, wait until it's done before continuing.
	dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
		if (_currentSyncDate) {
//NSLog(@"\n--------------------------\n--------------------------\n    FETCH WAITING FOR MODIFY TO FINISH\n--------------------------\n--------------------------");
			[_syncQueue waitUntilAllOperationsAreFinished];
//NSLog(@"\n--------------------------\n--------------------------\n    MODIFY FINISHED\n--------------------------\n--------------------------");
		}
		
		CKServerChangeToken *serverChangeToken = nil;
		NSData *data = [[NSUserDefaults standardUserDefaults] objectForKey:kBTCloudKitSyncServerChangeTokenKey];
		if (data) {
			serverChangeToken = [NSKeyedUnarchiver unarchiveObjectWithData:data];
		}
		
		[self _fetchRecordChangesWithServerChangeToken:serverChangeToken
									 completionHandler:^(BTFetchResult result, BOOL moreComing) {
//NSLog(@"\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n          FETCH RESULT: %lu, %d \n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~", result, moreComing);
										 completionHandler(result, moreComing);
									 }];
		
		//	[self _fetchRecordChangesWithServerChangeToken:serverChangeToken
		//								 completionHandler:completionHandler];
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
//	NSLog(@"%s", __PRETTY_FUNCTION__);
	if (_currentSyncDate) {
		// A sync is currently running, so ignore any local db changes. They
		// will be picked up at the end of a successful sync automatically.
//		NSLog(@"Ignoring a local change during concurrent sync.");
		return;
	}
	
	NSDate *fireDate = [NSDate dateWithTimeIntervalSinceNow:kBTCloudKitSyncChangeTimeoutInSeconds];
	if (_syncTimer) {
		[_syncTimer setFireDate:fireDate];
	} else {
		_syncTimer = [[NSTimer alloc] initWithFireDate:fireDate interval:0 target:self selector:@selector(_syncTimerFired:) userInfo:nil repeats:NO];
		[[NSRunLoop mainRunLoop] addTimer:_syncTimer forMode:NSDefaultRunLoopMode];
	}
}

- (void)_syncTimerFired:(NSTimer *)timer
{
	if (_syncTimer && _syncTimer.isValid) {
		[_syncTimer invalidate];
	}
	_syncTimer = nil;
	
	// Kick off a sync
	[self startSync];
//	[self performSyncWithCompletion:^(BOOL success, NSError *error) {
//		if (success) {
//			NSLog(@"A successful sync occurred from the sync timer.");
//		} else {
//			NSLog(@"A sync from the sync timer failed: %@", error);
//		}
//	}];
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
			[self _localDatabaseChangedNotification:nil];
			
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


- (void)_modifyRecords:(NSArray<NSDictionary *> *)records
		  ofRecordType:(NSString *)recordType
	 completionHandler:(void (^)(BOOL success))completionHandler
{
	if (records == nil || records.count == 0) {
		NSLog(@"_modifyRecords:ofRecordType:completionHandler: sent empty records");
		if (completionHandler) {
			completionHandler(NO);
		}
		return;
	}
	
	if (recordType == nil) {
		NSLog(@"_modifyRecords:ofRecordType:completionHandler: sent empty recordType.");
		if (completionHandler) {
			completionHandler(NO);
		}
		return;
	}
	
	//
	// First build up the arrays of records to save & delete
	//
	NSMutableArray *recordsToSaveA = [NSMutableArray new];
	NSMutableArray *recordsToDeleteA = [NSMutableArray new];
	
	NSString *recordZoneName = [_localDatabase recordZoneName];
	CKRecordZoneID *zoneID = [[CKRecordZoneID alloc] initWithZoneName:recordZoneName ownerName:CKOwnerDefaultName];
	
	[records enumerateObjectsUsingBlock:^(NSDictionary * _Nonnull changeInfo, NSUInteger idx, BOOL * _Nonnull stop) {
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
				} else {
					// Let's purge the changes from the local database so we
					// don't try to send this again.
					[_localDatabase purgeRecordChangeOfRecordType:recordType withIdentifier:recordIdentifier beforeDate:_currentSyncDates[recordType] error:nil];
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
	
	// Safety check to see that we have at least something to send
	if (recordsToSaveA.count == 0 && recordsToDeleteA.count == 0) {
		// This condition could be hit if we had local changes that were a
		// delete for records that were never sent to the server in the first
		// place.
		
		NSLog(@"After querying the local database for changes, no records were found to send to CloudKit.");
		completionHandler(YES);
		return;
	}
	
	//
	// Attempt to push the changes to CloudKit
	//
	
	CKModifyRecordsOperation *modifyRecordsOp = nil;
	modifyRecordsOp = [[CKModifyRecordsOperation alloc] initWithRecordsToSave:recordsToSaveA recordIDsToDelete:recordsToDeleteA];
	modifyRecordsOp.database = _privateDB;
	modifyRecordsOp.savePolicy = CKRecordSaveIfServerRecordUnchanged;
	modifyRecordsOp.atomic = YES;
	
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
						
						NSError *purgeError = nil;
						[_localDatabase purgeRecordChangeOfRecordType:recordType withIdentifier:identifier beforeDate:_currentSyncDates[recordType] error:&purgeError];
						if (purgeError) {
							NSLog(@"Error purging record change for modified record with identifier: %@", identifier);
						}
						
//NSLog(@"\n++++ Record Uploaded ++++\n%@\n+++++++++++++++++++++++", savedRecord);
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
						
						NSError *purgeError = nil;
						[_localDatabase purgeRecordChangeOfRecordType:recordType withIdentifier:identifier beforeDate:_currentSyncDates[recordType] error:&purgeError];
						if (purgeError) {
							NSLog(@"Error purging record change for deleted record with identifier: %@", identifier);
						}
						
//NSLog(@"\n++++ Record Deleted ++++\n%@\n+++++++++++++++++++++++", recordID.recordName);
					}];
				}
				
				// During sync, BTCloudKitSync doesn't pay attention to local
				// change notifications, so just to be sure we didn't actually
				// miss anything that was a user-initiated change, check for
				// record changes and kick off another sync if needed.
				
				// This is a new sync for this particular record type, so reset
				// the current sync date for this record type.
				_currentSyncDates[recordType] = [NSDate date];
				
				// Also reset the batch size so in case the previous sync had to
				// reduce the size for whatever reason, we won't punish this
				// modify records operation.
				_currentBatchSizeToSend = _maxLocalChangesToSend;
				
				NSError *dbError = nil;
				NSArray<NSDictionary *> *recordsA = [_localDatabase recordChangesOfType:_currentRecordType
																			 beforeDate:_currentSyncDates[recordType]
																				  limit:_currentBatchSizeToSend
																				  error:&dbError];
				if (dbError) {
					NSLog(@"Error getting local database changes when checking for any user-based changes at the end of a modify records operation: %@", dbError);
					completionHandler(NO);
				} else {
					if (recordsA == nil || recordsA.count == 0) {
						completionHandler(YES);
					} else {
						[self _modifyRecords:recordsA
								ofRecordType:_currentRecordType
						   completionHandler:completionHandler];
					}
				}
			} else {
				BOOL shouldRetryModifyRecords = NO;
//NSLog(@"\n++++ Operation Error: %ld ++++", operationError.code);

				switch (operationError.code) {
					case CKErrorPartialFailure:
					{
						// Some of the records sent had errors. There could be
						// conflicts to resolve and/or some records may have failed
						// to delete (if some other client deleted the same record
						// prior to this device).
						//
						// To keep things simplistic, server changes will win in a
						// conflict. Change the local record, save off the new
						// system fields for the record, and purge the local record
						// from its changelog so it no longer gets picked up as a
						// change during subsequent calls to _modifyRecords: with
						// the same sync date.
						
						NSDictionary *partialErrorResultsD = operationError.userInfo[CKPartialErrorsByItemIDKey];
						if (partialErrorResultsD) {
							[partialErrorResultsD enumerateKeysAndObjectsUsingBlock:^(CKRecordID * _Nonnull recordID, NSError * _Nonnull ckError, BOOL * _Nonnull stop) {
								switch (ckError.code) {
									case CKErrorServerRecordChanged:
									{
										// A conflict was found. Check the
										// server and client record objects.
										// Choose whichever one was modified
										// latest.
										
										
										CKRecord *serverRecord = ckError.userInfo[CKRecordChangedErrorServerRecordKey];
//										CKRecord *ancestorRecord = ckError.userInfo[CKRecordChangedErrorAncestorRecordKey];
//										CKRecord *clientRecord = ckError.userInfo[CKRecordChangedErrorClientRecordKey];
//NSLog(@"\n++++ CONFLICT RESOLUTION ++++\n%@\n---- CLIENT RECORD ----\n%@\n+++++++++++++++++++++++", serverRecord, clientRecord);
										
										NSUInteger changeIndex = [records indexOfObjectPassingTest:^BOOL(NSDictionary * _Nonnull obj, NSUInteger idx, BOOL * _Nonnull stop) {
											NSString *possibleMatchingIdentifier = obj[BTCloudKitSyncChangeRecordIdentifier];
											if (possibleMatchingIdentifier && [possibleMatchingIdentifier isEqualToString:recordID.recordName]) {
												return YES;
												*stop = YES;
											}
											return NO;
										}];
										NSDictionary *clientChanges = records[changeIndex];
										
										NSDate *serverModificationDate = serverRecord.modificationDate;
										NSDate *clientModificationDate = clientChanges[BTCloudKitSyncChangeLastModifiedKey];
										
										BOOL saveServerVersion = YES;
										NSTimeInterval difference = ABS([serverModificationDate timeIntervalSince1970] - [clientModificationDate timeIntervalSince1970]);
										if (difference > kBTCloudKitSyncPreferServerChangeIfWithinSeconds) {
											NSComparisonResult comparisonResult = [serverModificationDate compare:clientModificationDate];
											if (comparisonResult == NSOrderedAscending) {
												// The client record is newer
//NSLog(@"\n++++ CLIENT record is newer, saving system fields and retrying ++++");
												// Save the system fields of the server change so that when
												// this retries to send, the local changes will be saved.
												
												[self _saveSystemFieldsForRecord:serverRecord];
												saveServerVersion = NO;
											}
										}
										
										if (saveServerVersion) {
//NSLog(@"\n++++ SERVER record is newer, saving ++++");
											// The server record is newer or the same modification date.
											// Save over the local record.
											if ([self _saveRecordLocally:serverRecord] == YES) {
												// Remove the change info for this
												// record so it doesn't get sent again
												// in any subsequent modify operation
												// since we just dealt with getting it
												// in sync with the server.
												[_localDatabase purgeRecordChangeOfRecordType:recordType
																			   withIdentifier:recordID.recordName
																				   beforeDate:_currentSyncDates[recordType]
																						error:nil];
											}
										}
										
										break;
									}
									case CKErrorBatchRequestFailed:
									{
//NSLog(@"\n++++ CKErrorBatchRequestFailed ++++");
										// This is a record that was part of the
										// atomic batch and so the record was
										// automatically marked with this failure
										// and should be retried
										break;
									}
										
									default:
										break;
								}
							}];
						}
						
						shouldRetryModifyRecords = YES;
						break;
					}
					case CKErrorRequestRateLimited:
					{
//NSLog(@"\n++++ CKErrorRequestRateLimited ++++");
						
					}
					case CKErrorServiceUnavailable:
					{
//NSLog(@"\n++++ CKErrorServiceUnavailable ++++");
					}
					case CKErrorZoneBusy:
					{
						// Client is being rate limited
						// Check userInfo's CKErrorRetryAfterKey to get NSTimeInterval when it's safe again to try
						NSNumber *secondsToWaitUntilRetry = operationError.userInfo[CKErrorRetryAfterKey];
						if (secondsToWaitUntilRetry == nil) {
							// determine a reasonable amount of time to wait to retry
							secondsToWaitUntilRetry = @30; // TO-DO: Really figure out how to handle this
						}
//NSLog(@"\n++++ CKErrorZoneBusy ++++\n    Waiting for %ld seconds\n++++++++++++++++++++++++++++++", secondsToWaitUntilRetry.integerValue);
						
						// TO-DO: Determine how to restart a modify records operation after X seconds
						
						// Since this is not the main thread, it's conceivable
						// that one option would be to just sleep this thread
						// for the given amount of seconds and then continue.
						[NSThread sleepUntilDate:[NSDate dateWithTimeIntervalSinceNow:(NSTimeInterval)secondsToWaitUntilRetry.integerValue]];
						shouldRetryModifyRecords = YES;
						break;
					}
					case CKErrorLimitExceeded:
					{

						// The request to the server was too large. Retry this request as a smaller batch.
						
						// According to the WWDC 2015 CloudKit Tips & Tricks
						// session, the batch size should just be cut in half.
						if (_currentBatchSizeToSend == 0) {
							// We never set a batch size, so grab the actual
							// number of records we attempted to send and set
							// _currentBatchSizeToSend to half that.
							NSUInteger currentBatchSize = recordsToSaveA.count + recordsToDeleteA.count;
							_currentBatchSizeToSend = (NSUInteger)ceil(currentBatchSize / 2);
						} else {
							_currentBatchSizeToSend = (NSUInteger)ceil(_currentBatchSizeToSend / 2);
						}
//NSLog(@"\n++++ CKErrorLimitExceeded ++++\n    New Batch Size: %lu\n++++++++++++++++++++++++++++++", _currentBatchSizeToSend);
						shouldRetryModifyRecords = YES;
						
						break;
					}
					case CKErrorQuotaExceeded:
					{
						// TO-DO: Figure out how to deal with CKErrorQuotaExceeded during CKModifyRecordsOperation
						break;
					}
				}
				
				// Restart if this was a recoverable error
				if (shouldRetryModifyRecords) {
					
					NSError *dbError = nil;
					NSArray<NSDictionary *> *recordsA = [_localDatabase recordChangesOfType:_currentRecordType
																				 beforeDate:_currentSyncDates[recordType]
																					  limit:_currentBatchSizeToSend
																					  error:&dbError];
					if (dbError) {
						NSLog(@"Error getting local database changes when retrying modify records operation: %@", dbError);
						completionHandler(NO);
					} else {
						if (recordsA == nil || recordsA.count == 0) {
							completionHandler(YES);
						} else {
//NSLog(@"\n++++ Retrying Modify ++++");
							[self _modifyRecords:recordsA
									ofRecordType:_currentRecordType
							   completionHandler:completionHandler];
						}
					}
				}
			}
		} else {
			// The operation was cancelled
			completionHandler(NO);
		}
	};
	
	[_syncQueue addOperation:modifyRecordsOp];
}


- (void)_fetchRecordChangesWithServerChangeToken:(CKServerChangeToken *)serverChangeToken
							   completionHandler:(void (^)(BTFetchResult result, BOOL moreComing))completionHandler
{
	@synchronized (self) {
		_isCurrentlyFetching = YES;
	}
	
	__block BOOL recordsWereDeleted = NO;
	__block BOOL recordsWereModified = NO;
	
	NSString *recordZoneName = [_localDatabase recordZoneName];
	CKRecordZoneID *zoneID = [[CKRecordZoneID alloc] initWithZoneName:recordZoneName ownerName:CKOwnerDefaultName];
	
	CKFetchRecordChangesOperation *fetchChangesOp = [[CKFetchRecordChangesOperation alloc] initWithRecordZoneID:zoneID previousServerChangeToken:serverChangeToken];
	__weak CKFetchRecordChangesOperation *weakFetchChangesOp = fetchChangesOp;
	fetchChangesOp.database = _privateDB;
	fetchChangesOp.fetchRecordChangesCompletionBlock = ^(CKServerChangeToken *newServerChangeToken, NSData *clientChangeTokenData, NSError *operationError) {
		if (weakFetchChangesOp.cancelled == NO) {
			if (operationError) {
				switch (operationError.code) {
					case CKErrorChangeTokenExpired:
					{
						// The server change token we sent previously is too old and
						// we need to fetch everything fresh.
						[self _clearServerChangeToken];
						[self _fetchRecordChangesWithServerChangeToken:nil
													 completionHandler:completionHandler];
					}
					case CKErrorRequestRateLimited:
					case CKErrorServiceUnavailable:
					{
						NSNumber *secondsUntilRetry = operationError.userInfo[CKErrorRetryAfterKey];
						// TO-DO: Determine how to restart a _fetchRecordChangesWithServerChangeToken: after the specified number of seconds
						@synchronized (self) {
							_isCurrentlyFetching = NO;
						}
						completionHandler(BTFetchResultNoData, NO);
						
						break;
					}
				}
				
				
			} else if (!recordsWereDeleted && !recordsWereModified && !weakFetchChangesOp.moreComing) {
				[self _saveServerChangeToken:newServerChangeToken];
				if (completionHandler) {
					@synchronized (self) {
						_isCurrentlyFetching = NO;
					}
					completionHandler(BTFetchResultNoData, NO);
				}
			} else { // if (recordsWereDeleted || recordsWereModified || moreComing)
				[self _saveServerChangeToken:newServerChangeToken];
				
				if (completionHandler) {
					if (recordsWereDeleted || recordsWereModified) {
						completionHandler(BTFetchResultNewData, weakFetchChangesOp.moreComing);
					} else {
						completionHandler(BTFetchResultNoData, weakFetchChangesOp.moreComing);
					}
				}
				
				if (weakFetchChangesOp.moreComing) {
//NSLog(@"\n==== FETCH detected moreComing ====");
					[self _fetchRecordChangesWithServerChangeToken:newServerChangeToken
												 completionHandler:completionHandler];
				} else {
					@synchronized (self) {
						_isCurrentlyFetching = NO;
					}
				}
			}
		} else {
			if (completionHandler) {
				completionHandler(BTFetchResultNoData, weakFetchChangesOp.moreComing);
			}
		}
	};
	fetchChangesOp.recordChangedBlock = ^(CKRecord *record) {
		
		// First check to see if the local record already matches and if
		// it does, do nothing.
		NSString *identifier = record.recordID.recordName;
		NSData *systemFields = [_localDatabase systemFieldsDataForRecordWithIdentifier:identifier error:nil];
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
		
//NSLog(@"\n==== SAVING Fetched Record ====\n%@\n=========================", record);
		
		// Check for local changes. If local changes (that have not been sync'd)
		// exist and they are newer, just save the system fields so that when
		// the modify records runs, the local changes will be pushed to CloudKit.
		NSDate *now = [NSDate date];
		NSDictionary *changes = [_localDatabase recordChangeOfType:record.recordType
													withIdentifier:identifier
														beforeDate:now
															 error:nil];
		BOOL saveServerVersion = YES;
		if (changes && changes.count > 0) {
			NSDate *serverModificationDate = record.modificationDate;
			NSDate *clientModificationDate = changes[BTCloudKitSyncChangeLastModifiedKey];
			
			// There are some situations where if both the server and client
			// change nearly simultaneously, partial records may be saved, so
			// this helps to clearly have a known difference in time change for
			// records.
			NSTimeInterval difference = ABS([serverModificationDate timeIntervalSince1970] - [clientModificationDate timeIntervalSince1970]);
			if (difference > kBTCloudKitSyncPreferServerChangeIfWithinSeconds) {
				NSComparisonResult comparisonResult = [serverModificationDate compare:clientModificationDate];
				if (comparisonResult == NSOrderedAscending) {
					// The client record is newer. Save off the system fields so
					// the next time modify records operation runs, the client's
					// version will be selected.
					saveServerVersion = NO;
					[self _saveSystemFieldsForRecord:record];
					NSLog(@"Fetched server change, but keeping client record because it is newer (%@)", record.recordType);
				}
			}
		}
		
		if (saveServerVersion) {
			if ([self _saveRecordLocally:record] == YES) {
				recordsWereModified = YES;
				
				// If this device has a modified version of this record in the change
				// queue, perhaps we should discard the local changes (server wins)
				if (changes) {
//NSLog(@"\n==== DISCARDING unsynced local changes ====\n%@\n=========================", record);
					[_localDatabase purgeRecordChangeOfRecordType:record.recordType
												   withIdentifier:identifier
													   beforeDate:now
															error:nil];
				}
				NSLog(@"Saving server-fetched change (%@)", record.recordType);
			}
		}
	};
	fetchChangesOp.recordWithIDWasDeletedBlock = ^(CKRecordID *recordID) {
		NSError *error = nil;
		
		// Determine the record type by referring to the previously-saved system
		// fields. If this device has no system fields, assume that this device
		// knows nothing of this deleted record.
		NSData *systemFields = [_localDatabase systemFieldsDataForRecordWithIdentifier:recordID.recordName error:nil];
		NSString *recordType = nil;
		if (systemFields) {
			NSKeyedUnarchiver *archiver = [[NSKeyedUnarchiver alloc] initForReadingWithData:systemFields];
			archiver.requiresSecureCoding = YES;
			CKRecord *ckRecord = [[CKRecord alloc] initWithCoder:archiver];
			recordType = ckRecord.recordType;
		}
		if (recordType) {
			[_localDatabase deleteRecordWithIdentifier:recordID.recordName withRecordType:recordType error:&error];
			if (error) {
				// If there was an error deleting, not sure what to do, but it
				// may not be crucial.
				NSLog(@"Error deleting a record while fetching server changes: %@", recordID.recordName);
			} else {
				NSLog(@"Record deleted (%@) during fetch.", recordType);
				// To purge any pending changes, we need to know the record type,
				// which we should have in the system fields.
				
				[_localDatabase purgeRecordChangeOfRecordType:recordType withIdentifier:recordID.recordName beforeDate:[NSDate date] error:nil];
				[_localDatabase deleteSystemFieldsForRecordWithIdentifier:recordID.recordName error:nil];
				
				recordsWereDeleted = YES;
//NSLog(@"\n==== DELETED record during fetch ====\n%@\n=========================", recordID.recordName);
			}
		}
	};
	
	[_fetchQueue addOperation:fetchChangesOp];
}

- (void)_saveServerChangeToken:(CKServerChangeToken *)serverChangeToken
{
	NSData *encodedServerChangeToken = [NSKeyedArchiver archivedDataWithRootObject:serverChangeToken];
	[[NSUserDefaults standardUserDefaults] setObject:encodedServerChangeToken forKey:kBTCloudKitSyncServerChangeTokenKey];
	[[NSUserDefaults standardUserDefaults] synchronize];
}

- (void)_clearServerChangeToken
{
	[[NSUserDefaults standardUserDefaults] removeObjectForKey:kBTCloudKitSyncServerChangeTokenKey];
	[[NSUserDefaults standardUserDefaults] synchronize];
}

- (BOOL)_saveRecordLocally:(CKRecord *)record
{
	BOOL recordSaved = NO;
	
	BOOL addRecord = NO;
	NSMutableDictionary *mutableRecordInfo = nil;
	NSDictionary *recordInfo = [_localDatabase infoForRecordType:record.recordType withIdentifier:record.recordID.recordName error:nil];
	if (recordInfo == nil) {
		// This is a new record
		addRecord = YES;
		
		mutableRecordInfo = [NSMutableDictionary new];
	} else {
		mutableRecordInfo = [NSMutableDictionary dictionaryWithDictionary:recordInfo];
	}
	
	[[record allKeys] enumerateObjectsUsingBlock:^(NSString * _Nonnull key, NSUInteger idx, BOOL * _Nonnull stop) {
		mutableRecordInfo[key] = record[key];
	}];
	
	NSError *error = nil;
	if (addRecord) {
		if ([_localDatabase addRecordInfo:mutableRecordInfo withRecordType:record.recordType withIdentifier:record.recordID.recordName error:&error] == NO) {
			// TO-DO: Figure out how to properly handle this error
			NSLog(@"Error adding a record sent from the server: %@", error);
		} else {
			recordSaved = YES;
		}
	} else {
		if ([_localDatabase updateRecordInfo:mutableRecordInfo withRecordType:record.recordType withIdentifier:record.recordID.recordName error:&error] == NO) {
			// TO-DO: Figure out how to properly handle this error
			NSLog(@"Error updating a record sent from the server: %@", error);
		} else {
			recordSaved = YES;
		}
	}
	
	[self _saveSystemFieldsForRecord:record];
	
	return recordSaved;
}


- (void)_saveSystemFieldsForRecord:(CKRecord *)record
{
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
}

@end
