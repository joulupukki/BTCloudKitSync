//
//  BTDB.m
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

#import "BTDB.h"

#import "BTConstants.h"
#import "BTContact.h"
#import "BTContactChange.h"

#import "FMDB.h"

#define kBTDBDirectoryName @"BTCloudKitSyncDemo"
#define kBTDBFileName @"BTCloudKitSyncDemo.sqlitedb"

#define kBTDBCreateContactTableSQL @"CREATE TABLE IF NOT EXISTS contacts (identifier TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, last_modified DOUBLE)"
#define kBTDBCreateContactChangeLogTableSQL @"CREATE TABLE IF NOT EXISTS contacts_changelog (pk INTEGER PRIMARY KEY, identifier TEXT, identifierOLD TEXT, first_nameNEW TEXT, first_nameOLD TEXT, last_nameNEW TEXT, last_nameOLD TEXT, last_modifiedNEW DOUBLE, last_modifiedOLD DOUBLE, sqlAction VARCHAR(15), timestamp DOUBLE)"
#define kBTDBCreateCKRecordTableSQL @"CREATE TABLE IF NOT EXISTS ckrecords (identifier TEXT PRIMARY KEY, sync_data BLOB)"

#define kBTDBCreateInsertTriggerSQL @"CREATE TRIGGER IF NOT EXISTS insert_contact AFTER INSERT ON contacts BEGIN INSERT INTO contacts_changelog (identifier,first_nameNEW,last_nameNEW,last_modifiedNEW,sqlAction,timestamp) VALUES (new.identifier,new.first_name,new.last_name,new.last_modified,'INSERT',strftime('%s','now')); END"
#define kBTDBCreateUpdateTriggerSQL @"CREATE TRIGGER IF NOT EXISTS update_contact AFTER UPDATE ON contacts BEGIN INSERT INTO contacts_changelog  (identifier,identifierOLD,first_nameNEW,first_nameOLD,last_nameNEW,last_nameOLD,last_modifiedNEW,last_modifiedOLD,sqlAction,timestamp) VALUES (new.identifier,old.identifier,new.first_name,old.first_name,new.last_name,old.last_name,new.last_modified,old.last_modified,'UPDATE',strftime('%s','now')); END"
#define kBTDBCreateDeleteTriggerSQL @"CREATE TRIGGER IF NOT EXISTS delete_contact DELETE ON contacts BEGIN INSERT INTO contacts_changelog (identifier,sqlAction,timestamp) VALUES (old.identifier,'DELETE',strftime('%s','now')); END"

#define kBTDBDropInsertTriggerSQL @"DROP TRIGGER IF EXISTS insert_contact"
#define kBTDBDropUpdateTriggerSQL @"DROP TRIGGER IF EXISTS update_contact"
#define kBTDBDropDeleteTriggerSQL @"DROP TRIGGER IF EXISTS delete_contact"

@interface BTDB ()

@property (nonatomic, strong) FMDatabaseQueue *dbQueue;

@end

@implementation BTDB

+ (BTDB *)sharedInstance
{
	static dispatch_once_t onceToken = 0;
	static BTDB *_sharedInstance = nil;
	
	dispatch_once(&onceToken, ^{
		_sharedInstance = [[BTDB alloc] init];
	});
	
	return _sharedInstance;
}

- (instancetype)init
{
	if (self = [super init]) {
		NSFileManager *fm = [NSFileManager defaultManager];
		
		NSArray *paths = NSSearchPathForDirectoriesInDomains(NSLibraryDirectory, NSUserDomainMask, YES);
		NSString *documentsDirectory = [paths objectAtIndex:0];
		
		NSString *appDirectory = [documentsDirectory stringByAppendingPathComponent:@"kBTDBDirectoryName"];
		
		// Make sure this directory exists
		if ([fm fileExistsAtPath:appDirectory] == NO)
		{
			[fm createDirectoryAtPath:appDirectory withIntermediateDirectories:YES attributes:nil error:nil];
		}
		
		NSString *dbPath = [appDirectory stringByAppendingPathComponent:kBTDBFileName];
		
		NSLog(@"DB Path: %@", dbPath);
		
		_dbQueue = [FMDatabaseQueue databaseQueueWithPath:dbPath];
		if (_dbQueue == nil) {
			NSLog(@"[FMDatabaseQueue databaseQueueWithPath:%@] failed", dbPath);
			NSAssert(0, @"Failed to initialize database.");
		}
		
		NSError *error = nil;
		if ([self _configureDatabaseWithError:&error] == NO) {
			NSLog(@"Could not configure the database: %@", error);
			NSAssert(0, @"Failed to configure the database.");
		}
	}
	return self;
}

- (NSArray<BTContact *> *)readAllContacts
{
	NSMutableArray *contactsA = [NSMutableArray new];
	[_dbQueue inDatabase:^(FMDatabase *db) {
		//(identifier TEXT PRIMARY KEY, first_name TEXT, last_name TEXT, last_modified DATE)"

		FMResultSet *rs = [db executeQuery:@"SELECT identifier,first_name,last_name,last_modified FROM contacts ORDER BY identifier ASC"];
		if (rs) {
			while ([rs next]) {
				BTContact *contact = [self _contactFromResultSet:rs];
				if (contact) {
					[contactsA addObject:contact];
				}
			}
			[rs close];
		}
	}];
	return contactsA;
}

- (BTContact *)readContactWithIdentifier:(NSString *)identifier
{
	if (identifier == nil) {
		return nil;
	}
	
	__block BTContact *contact = nil;
	[_dbQueue inDatabase:^(FMDatabase *db) {
		FMResultSet *rs = [db executeQuery:@"SELECT identifier,first_name,last_name,last_modified FROM contacts WHERE identifier=?", identifier];
		if (rs) {
			while ([rs next]) {
				contact = [self _contactFromResultSet:rs];
				if (contact) {
					break;
				}
			}
			[rs close];
		}
	}];
	
	return contact;
}

- (BOOL)addContact:(BTContact *)contact error:(NSError **)error
{
	return [self addContact:contact shouldLogChanges:YES error:error];
}

- (BOOL)addContact:(BTContact *)contact shouldLogChanges:(BOOL)logChanges error:(NSError **)error
{
	if (contact == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-1 userInfo:@{NSLocalizedDescriptionKey:@"addContact: sent nil contact."}];
		}
		return NO;
	}
	
	// Simple validation to ensure that the contact has at least a first name or a last name
	if (contact.firstName == nil && contact.lastName == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-2 userInfo:@{NSLocalizedDescriptionKey:@"addContact: sent a contact with no first or last name set."}];
		}
		return NO;
	}
	
	if (contact.identifier == nil) {
		// Create a new identifier
		NSUUID *uuid = [NSUUID UUID];
		contact.identifier = [uuid UUIDString];
	}
	
	if (logChanges || contact.lastModified == nil) {
		contact.lastModified = [NSDate date];
	}
	
	__block BOOL success = YES;
	[_dbQueue inTransaction:^(FMDatabase *db, BOOL *rollback) {
		BOOL shouldLogChanges = logChanges;
		if (logChanges && [self _isSyncEnabled] == NO) {
			// Override to prevent change logging if sync is not enabled
			shouldLogChanges = NO;
		}
		NSError *dbError = nil;
		if ([self _enableContactChangeLogging:shouldLogChanges inDatabase:db error:&dbError] == NO) {
			if (error) {
				*error = dbError;
			}
			*rollback = YES;
			success = NO;
			return;
		}
		
		if ([db executeUpdate:@"INSERT INTO contacts (identifier,first_name,last_name,last_modified) VALUES (?,?,?,?)",
			 contact.identifier, contact.firstName, contact.lastName, contact.lastModified] == NO) {
			if (error) {
				*error = [db lastError];
			}
			*rollback = YES;
			success = NO;
			return;
		}
	}];
	
	dispatch_async(dispatch_get_main_queue(), ^{
		[[NSNotificationCenter defaultCenter] postNotificationName:kBTContactAddedNotification object:self userInfo:@{kBTContactIdentifierKey:contact.identifier}];
	});
	
	return success;
}

- (BOOL)updateContact:(BTContact *)contact error:(NSError **)error
{
	return [self updateContact:contact shouldLogChanges:YES error:error];
}

- (BOOL)updateContact:(BTContact *)contact shouldLogChanges:(BOOL)logChanges error:(NSError **)error
{
	if (contact == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-1 userInfo:@{NSLocalizedDescriptionKey:@"updateContact: sent nil contact."}];
		}
		return NO;
	}
	
	// Simple validation to ensure that the contact has at least a first name or a last name
	if (contact.firstName == nil && contact.lastName == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-2 userInfo:@{NSLocalizedDescriptionKey:@"updateContact: sent a contact with no first or last name set."}];
		}
		return NO;
	}
	
	if (contact.identifier == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-3 userInfo:@{NSLocalizedDescriptionKey:@"updateContact: sent a contact with no identifier."}];
		}
		return NO;
	}
	
	if (logChanges || contact.lastModified == nil) {
		contact.lastModified = [NSDate date];
	}
	
	__block BOOL success = YES;
	[_dbQueue inTransaction:^(FMDatabase *db, BOOL *rollback) {
		BOOL shouldLogChanges = logChanges;
		if (logChanges && [self _isSyncEnabled] == NO) {
			// Override to prevent change logging if sync is not enabled
			shouldLogChanges = NO;
		}
		NSError *dbError = nil;
		if ([self _enableContactChangeLogging:shouldLogChanges inDatabase:db error:&dbError] == NO) {
			if (error) {
				*error = dbError;
			}
			*rollback = YES;
			success = NO;
			return;
		}
		
		if ([db executeUpdate:@"UPDATE contacts SET first_name=?,last_name=?,last_modified=? WHERE identifier=?",
			 contact.firstName, contact.lastName, contact.lastModified, contact.identifier] == NO) {
			if (error) {
				*error = [db lastError];
			}
			*rollback = YES;
			success = NO;
			return;
		}
	}];
	
	dispatch_async(dispatch_get_main_queue(), ^{
		[[NSNotificationCenter defaultCenter] postNotificationName:kBTContactUpdatedNotification object:self userInfo:@{kBTContactIdentifierKey:contact.identifier}];
	});
	
	return success;
}

- (BOOL)deleteContactWithIdentifier:(NSString *)identifier error:(NSError **)error
{
	return [self deleteContactWithIdentifier:identifier shouldLogChanges:YES error:error];
}

- (BOOL)deleteContactWithIdentifier:(NSString *)identifier shouldLogChanges:(BOOL)logChanges error:(NSError **)error
{
	if (identifier == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-1 userInfo:@{NSLocalizedDescriptionKey:@"deleteContact: sent nil identifier."}];
		}
		return NO;
	}
	
	__block BOOL success = YES;
	[_dbQueue inTransaction:^(FMDatabase *db, BOOL *rollback) {
		BOOL shouldLogChanges = logChanges;
		if (logChanges && [self _isSyncEnabled] == NO) {
			// Override to prevent change logging if sync is not enabled
			shouldLogChanges = NO;
		}
		NSError *dbError = nil;
		if ([self _enableContactChangeLogging:shouldLogChanges inDatabase:db error:&dbError] == NO) {
			if (error) {
				*error = dbError;
			}
			*rollback = YES;
			success = NO;
			return;
		}
		
		if ([db executeUpdate:@"DELETE FROM contacts WHERE identifier=?", identifier] == NO) {
			if (error) {
				*error = [db lastError];
			}
			*rollback = YES;
			success = NO;
			return;
		}
	}];
	
	dispatch_async(dispatch_get_main_queue(), ^{
		[[NSNotificationCenter defaultCenter] postNotificationName:kBTContactDeletedNotification object:self userInfo:@{kBTContactIdentifierKey:identifier}];
	});
	
	return success;
}

#pragma mark - Change Log

- (NSArray<BTContactChange *> *)contactChangesBeforeDate:(NSDate *)date limit:(NSUInteger)limit
{
	NSMutableDictionary<NSString *, BTContactChange *> *changes = [NSMutableDictionary new];
	[_dbQueue inDatabase:^(FMDatabase *db) {
		//identifier,first_nameNEW,first_nameOLD,last_nameNEW,last_nameOLD,last_modifiedNEW,last_modifiedOLD,sqlAction,timestamp
		NSMutableString *sql = [[NSMutableString alloc] initWithString:@"SELECT identifier,first_nameNEW,first_nameOLD,last_nameNEW,last_nameOLD,last_modifiedNEW,last_modifiedOLD,sqlAction,timestamp FROM contacts_changelog ORDER BY timestamp ASC"];
		if (limit > 0) {
			[sql appendFormat:@" LIMIT %lu", (unsigned long)limit];
		}
		
		FMResultSet *rs = [db executeQuery:sql];
		if (rs) {
			while ([rs next]) {
				NSString *identifier = [rs stringForColumn:@"identifier"];
				if (identifier == nil) {
					continue; // should never happen
				}
				
				BTContactChange *change = changes[identifier];
				if (change == nil) {
					change = [[BTContactChange alloc] init];
					change.contactIdentifier = identifier;
				}
				
				BTContactChangeType changeType;
				NSString *value = [rs stringForColumn:@"sqlAction"];
				if ([value isEqualToString:@"INSERT"]) {
					changeType = BTContactChangeTypeInsert;
				} else if ([value isEqualToString:@"UPDATE"]) {
					changeType = BTContactChangeTypeUpdate;
				} else {
					changeType = BTContactChangeTypeDelete;
				}
				change.changeType = changeType;
				
				change.timestamp = [rs dateForColumn:@"timestamp"];
				
				NSDate *date;
				NSDate *oldDate;
				NSString *oldValue;
				
				switch (changeType) {
					case BTContactChangeTypeInsert:
					{
						value = [rs stringForColumn:@"first_nameNEW"];
						if (value) {
							change.changes[@"firstName"] = value;
						}
						value = [rs stringForColumn:@"last_nameNEW"];
						if (value) {
							change.changes[@"lastName"] = value;
						}
						date = [rs dateForColumn:@"last_modifiedNEW"];
						if (date) {
							change.changes[@"lastModified"] = date;
						}
						break;
					}
					case BTContactChangeTypeUpdate:
					{
						value = [rs stringForColumn:@"first_nameNEW"];
						oldValue = [rs stringForColumn:@"first_nameOLD"];
						if ((value && !oldValue) || (!value && oldValue) || (value && oldValue && [value isEqualToString:oldValue] == NO)) {
							if (value) {
								change.changes[@"firstName"] = value;
							} else {
								[change.changes removeObjectForKey:@"firstName"];
							}
						}
						
						value = [rs stringForColumn:@"last_nameNEW"];
						oldValue = [rs stringForColumn:@"last_nameOLD"];
						if ((value && !oldValue) || (!value && oldValue) || (value && oldValue && [value isEqualToString:oldValue] == NO)) {
							if (value) {
								change.changes[@"lastName"] = value;
							} else {
								[change.changes removeObjectForKey:@"lastName"];
							}
						}
						
						date = [rs dateForColumn:@"last_modifiedNEW"];
						oldDate = [rs dateForColumn:@"last_modifiedOLD"];
						if ((date && !oldDate) || (!date && oldDate) || (date && oldDate && [date isEqualToDate:oldDate] == NO)) {
							if (date) {
								change.changes[@"lastModified"] = date;
							} else {
								[change.changes removeObjectForKey:@"lastModified"];
							}
						}
						
						break;
					}
					case BTContactChangeTypeDelete:
					{
						change.contactIdentifier = identifier;
						[change.changes removeAllObjects];
						break;
					}
				}
				
				changes[identifier] = change;
			}
			[rs close];
		}
	}];
	return changes.allValues;
}

- (BTContactChange *)contactChangeForIdentifier:(NSString *)identifier beforeDate:(NSDate *)date
{
	__block BTContactChange *change = nil;
	
	[_dbQueue inDatabase:^(FMDatabase *db) {
		//identifier,first_nameNEW,first_nameOLD,last_nameNEW,last_nameOLD,last_modifiedNEW,last_modifiedOLD,sqlAction,timestamp
		NSMutableString *sql = [[NSMutableString alloc] initWithString:@"SELECT first_nameNEW,first_nameOLD,last_nameNEW,last_nameOLD,last_modifiedNEW,last_modifiedOLD,sqlAction,timestamp FROM contacts_changelog WHERE identifier = ? ORDER BY timestamp ASC"];
		
		FMResultSet *rs = [db executeQuery:sql, identifier];
		if (rs) {
			while ([rs next]) {
				if (change == nil) {
					change = [[BTContactChange alloc] init];
					change.contactIdentifier = identifier;
				}
				
				BTContactChangeType changeType;
				NSString *value = [rs stringForColumn:@"sqlAction"];
				if ([value isEqualToString:@"INSERT"]) {
					changeType = BTContactChangeTypeInsert;
				} else if ([value isEqualToString:@"UPDATE"]) {
					changeType = BTContactChangeTypeUpdate;
				} else {
					changeType = BTContactChangeTypeDelete;
				}
				change.changeType = changeType;
				
				change.timestamp = [rs dateForColumn:@"timestamp"];
				
				NSDate *date;
				NSDate *oldDate;
				NSString *oldValue;
				
				switch (changeType) {
					case BTContactChangeTypeInsert:
					{
						value = [rs stringForColumn:@"first_nameNEW"];
						if (value) {
							change.changes[@"firstName"] = value;
						}
						value = [rs stringForColumn:@"last_nameNEW"];
						if (value) {
							change.changes[@"lastName"] = value;
						}
						date = [rs dateForColumn:@"last_modifiedNEW"];
						if (date) {
							change.changes[@"lastModified"] = date;
						}
						break;
					}
					case BTContactChangeTypeUpdate:
					{
						value = [rs stringForColumn:@"first_nameNEW"];
						oldValue = [rs stringForColumn:@"first_nameOLD"];
						if ((value && !oldValue) || (!value && oldValue) || (value && oldValue && [value isEqualToString:oldValue] == NO)) {
							if (value) {
								change.changes[@"firstName"] = value;
							} else {
								[change.changes removeObjectForKey:@"firstName"];
							}
						}
						
						value = [rs stringForColumn:@"last_nameNEW"];
						oldValue = [rs stringForColumn:@"last_nameOLD"];
						if ((value && !oldValue) || (!value && oldValue) || (value && oldValue && [value isEqualToString:oldValue] == NO)) {
							if (value) {
								change.changes[@"lastName"] = value;
							} else {
								[change.changes removeObjectForKey:@"lastName"];
							}
						}
						
						date = [rs dateForColumn:@"last_modifiedNEW"];
						oldDate = [rs dateForColumn:@"last_modifiedOLD"];
						if ((date && !oldDate) || (!date && oldDate) || (date && oldDate && [date isEqualToDate:oldDate] == NO)) {
							if (date) {
								change.changes[@"lastModified"] = date;
							} else {
								[change.changes removeObjectForKey:@"lastModified"];
							}
						}
						
						break;
					}
					case BTContactChangeTypeDelete:
					{
						change.contactIdentifier = identifier;
						break;
					}
				}
			}
			[rs close];
		}
	}];
	
	return change;
}

- (BOOL)purgeChangesBeforeDate:(NSDate *)date error:(NSError **)error
{
	if (date == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-1 userInfo:@{NSLocalizedDescriptionKey:@"purgeChangesBeforeDate: sent nil date."}];
		}
		return NO;
	}
	
	__block BOOL success = YES;
	[_dbQueue inTransaction:^(FMDatabase *db, BOOL *rollback) {
		NSError *dbError = nil;
		if ([self _enableContactChangeLogging:NO inDatabase:db error:&dbError] == NO) {
			if (error) {
				*error = dbError;
			}
			*rollback = YES;
			success = NO;
			return;
		}
		
		if ([db executeUpdate:@"DELETE FROM contacts_changelog WHERE timestamp < ?", date] == NO) {
			if (error) {
				*error = [db lastError];
			}
			*rollback = YES;
			success = NO;
			return;
		}
	}];
	
	return success;
}


- (BOOL)populateContactChangeLogReturningError:(NSError **)error
{
	__block BOOL success = YES;
	[_dbQueue inTransaction:^(FMDatabase *db, BOOL *rollback) {
		if ([db executeUpdate:@"INSERT INTO contacts_changelog (identifier,first_nameNEW,last_nameNEW,last_modifiedNEW,sqlAction,timestamp) SELECT identifier,first_name,last_name,last_modified,'INSERT',strftime('%s','now') FROM contacts"] == NO) {
			if (error) {
				*error = [db lastError];
			}
			*rollback = YES;
			success = NO;
		}
	}];
	
	return success;
}


#pragma mark - CKRecord System Field Caching

- (NSData *)syncDataForRecordWithIdentifier:(NSString *)identifier error:(NSError **)error
{
	if (identifier == nil) {
		return nil;
	}
	
	__block NSData *syncData = nil;
	[_dbQueue inDatabase:^(FMDatabase *db) {
		FMResultSet *rs = [db executeQuery:@"SELECT sync_data FROM ckrecords WHERE identifier=?", identifier];
		if (rs) {
			while ([rs next]) {
				syncData = [rs dataForColumn:@"sync_data"];
				if (syncData) {
					break;
				}
			}
			[rs close];
		} else {
			if (error) {
				*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-1 userInfo:@{NSLocalizedDescriptionKey:[db lastError] ? db.lastError.localizedDescription : @"Error accessing the database."}];
			}
		}
	}];
	
	return syncData;
}


- (BOOL)saveSyncData:(NSData *)data withIdentifier:(NSString *)identifier error:(NSError **)error
{
	if (data == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-1 userInfo:@{NSLocalizedDescriptionKey:@"saveSyncData: called with nil data."}];
		}
		return NO;
	}
	
	if (identifier == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-1 userInfo:@{NSLocalizedDescriptionKey:@"saveSyncData: call with nil identifier."}];
		}
		return NO;
	}
	
	__block BOOL success = YES;
	[_dbQueue inTransaction:^(FMDatabase *db, BOOL *rollback) {
		if ([db executeUpdate:@"INSERT OR REPLACE INTO ckrecords (identifier,sync_data) VALUES (?,?)", identifier, data] == NO) {
			if (error) {
				*error = [db lastError];
			}
			*rollback = YES;
			success = NO;
			return;
		}
	}];
	
	return success;
}


- (BOOL)deleteSyncDataForRecordWithIdentifier:(NSString *)identifier error:(NSError **)error
{
	if (identifier == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-1 userInfo:@{NSLocalizedDescriptionKey:@"deleteContact: sent nil identifier."}];
		}
		return NO;
	}
	
	__block BOOL success = YES;
	[_dbQueue inTransaction:^(FMDatabase *db, BOOL *rollback) {

		if ([db executeUpdate:@"DELETE FROM ckrecords WHERE identifier=?", identifier] == NO) {
			if (error) {
				*error = [db lastError];
			}
			*rollback = YES;
			success = NO;
			return;
		}
	}];
	
	return success;
}


- (BOOL)purgeSyncDataWithError:(NSError **)error
{
	__block BOOL success = YES;
	[_dbQueue inTransaction:^(FMDatabase *db, BOOL *rollback) {
		if ([db executeUpdate:@"DELETE FROM ckrecords"] == NO) {
			if (error) {
				*error = [db lastError];
			}
			*rollback = YES;
			success = NO;
			return;
		}
	}];
	
	return success;
}


#pragma mark - BTCloudKitSyncDatabase (Protocol) Methods


- (NSString *)databaseDescription
{
	return @"BTDB-FMDB";
}


- (NSString *)recordZoneName
{
	return @"BTCloudKitSyncDemo";
}

- (NSArray<NSString *> *)recordTypes
{
	return @[@"Contact"];
}

- (NSArray<NSString *> *)databaseChangeNotificationNames
{
	return @[kBTContactAddedNotification, kBTContactUpdatedNotification, kBTContactDeletedNotification];
}

- (NSDictionary<NSString *, NSObject *> *)infoForRecordType:(NSString *)recordType withIdentifier:(NSString *)identifier error:(NSError **)error
{
	if ([recordType isEqualToString:@"Contact"] == NO) {
		// TO-DO: Make sure to set an NSError. In this app, this shouldn't ever happen though since the only record type is Contact.
		return nil;
	}
	
	BTContact *contact = [self readContactWithIdentifier:identifier];
	if (contact == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:1 userInfo:@{NSLocalizedDescriptionKey:@"Contact record not found in database."}];
		}
		return nil;
	}
	
	NSDictionary *contactInfo = [contact dictionaryRepresentation];
	return contactInfo;
}

- (BOOL)addRecordInfo:(NSDictionary<NSString *, NSObject *> *)recordInfo withRecordType:(NSString *)recordType withIdentifier:(NSString *)identifier error:(NSError **)error
{
	if ([recordType isEqualToString:@"Contact"] == NO) {
		// TO-DO: Make sure to set an NSError. In this app, this shouldn't ever happen though since the only record type is Contact.
		return nil;
	}
	
	BTContact *contact = [BTContact contactWithDictionary:recordInfo];
	contact.identifier = identifier;
	return [self addContact:contact shouldLogChanges:NO error:error];
}

- (BOOL)updateRecordInfo:(NSDictionary<NSString *, NSObject *> *)recordInfo withRecordType:(NSString *)recordType withIdentifier:(NSString *)identifier error:(NSError **)error
{
	if ([recordType isEqualToString:@"Contact"] == NO) {
		// TO-DO: Make sure to set an NSError. In this app, this shouldn't ever happen though since the only record type is Contact.
		return nil;
	}
	
	BTContact *contact = [self readContactWithIdentifier:identifier];
	if (contact) {
		if (recordInfo[@"firstName"]) {
			contact.firstName = (NSString *)recordInfo[@"firstName"];
		}
		if (recordInfo[@"lastName"]) {
			contact.lastName = (NSString *)recordInfo[@"lastName"];
		}
		if (recordInfo[@"lastModified"]) {
			contact.lastModified = (NSDate *)recordInfo[@"lastModified"];
		}
	}
	
	return [self updateContact:contact shouldLogChanges:NO error:error];
}

- (BOOL)deleteRecordWithIdentifier:(NSString *)identifier withRecordType:(NSString *)recordType error:(NSError **)error
{
	if ([recordType isEqualToString:@"Contact"] == NO) {
		// TO-DO: Make sure to set an NSError. In this app, this shouldn't ever happen though since the only record type is Contact.
		return nil;
	}
	
	return [self deleteContactWithIdentifier:identifier error:error];
}


- (NSArray<NSDictionary *> *)recordChangesOfType:(NSString *)recordType beforeDate:(NSDate *)date limit:(NSUInteger)limit error:(NSError *__autoreleasing *)error
{
	if ([recordType isEqualToString:@"Contact"] == NO) {
		// TO-DO: Make sure to set an NSError. In this app, this shouldn't ever happen though since the only record type is Contact.
		return nil;
	}
	
	NSMutableArray *recordChanges = [NSMutableArray new];
	
	NSArray *changes = [self contactChangesBeforeDate:date limit:limit];
	if (changes) {
		[changes enumerateObjectsUsingBlock:^(BTContactChange * _Nonnull change, NSUInteger idx, BOOL * _Nonnull stop) {
			NSMutableDictionary *changeInfo = [NSMutableDictionary new];
			changeInfo[BTCloudKitSyncChangeRecordIdentifier] = change.contactIdentifier;
			changeInfo[BTCloudKitSyncChangeLastModifiedKey] = change.changes[@"lastModified"];
			switch (change.changeType) {
				case BTContactChangeTypeInsert:
				{
					changeInfo[BTCloudKitSyncChangeTypeKey] = BTCloudKitSyncChangeTypeInsert;
					break;
				}
				case BTContactChangeTypeUpdate:
				{
					changeInfo[BTCloudKitSyncChangeTypeKey] = BTCloudKitSyncChangeTypeUpdate;
					break;
				}
				case BTContactChangeTypeDelete:
				{
					changeInfo[BTCloudKitSyncChangeTypeKey] = BTCloudKitSyncChangeTypeDelete;
					break;
				}
			}
			changeInfo[BTCloudKitSyncChangeRecordInfoKey] = change.changes;
			
			[recordChanges addObject:changeInfo];
		}];
	}
	
	return recordChanges;
}

- (BOOL)purgeRecordChangesOfType:(NSString *)recordType beforeDate:(NSDate *)date error:(NSError **)error
{
	if ([recordType isEqualToString:@"Contact"] == NO) {
		// TO-DO: Make sure to set an NSError. In this app, this shouldn't ever happen though since the only record type is Contact.
		return nil;
	}
	
	return [self purgeChangesBeforeDate:date error:error];
}

- (NSDictionary *)recordChangeOfType:(NSString *)recordType withIdentifier:(NSString *)identifier beforeDate:(NSDate *)date error:(NSError *)error
{
	if ([recordType isEqualToString:@"Contact"] == NO) {
		// TO-DO: Make sure to set an NSError. In this app, this shouldn't ever happen though since the only record type is Contact.
		return nil;
	}
	
	BTContactChange *change = [self contactChangeForIdentifier:identifier beforeDate:date];
	if (change) {
		NSMutableDictionary *changeInfo = [[NSMutableDictionary alloc] initWithDictionary:change.changes];
		changeInfo[BTCloudKitSyncChangeRecordIdentifier] = change.contactIdentifier;
		changeInfo[BTCloudKitSyncChangeLastModifiedKey] = change.changes[@"lastModified"];
		return changeInfo;
	}
	
	return nil;
}


- (BOOL)purgeRecordChangeOfRecordType:(NSString *)recordType withIdentifier:(NSString *)identifier beforeDate:(NSDate *)date error:(NSError *__autoreleasing *)error
{
	if ([recordType isEqualToString:@"Contact"] == NO) {
		// TO-DO: Make sure to set an NSError. In this app, this shouldn't ever happen though since the only record type is Contact.
		return nil;
	}
	
	if (identifier == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-1 userInfo:@{NSLocalizedDescriptionKey:@"purgeRecordChangeWithIdentifier:: sent nil identifier."}];
		}
		return NO;
	}
	
	if (date == nil) {
		if (error) {
			*error = [NSError errorWithDomain:@"com.bluegrassguru.BTDB" code:-1 userInfo:@{NSLocalizedDescriptionKey:@"purgeRecordChangeWithIdentifier:: sent nil date."}];
		}
		return NO;
	}
	
	__block BOOL success = YES;
	[_dbQueue inTransaction:^(FMDatabase *db, BOOL *rollback) {
		NSError *dbError = nil;
		if ([self _enableContactChangeLogging:NO inDatabase:db error:&dbError] == NO) {
			if (error) {
				*error = dbError;
			}
			*rollback = YES;
			success = NO;
			return;
		}
		
		NSString *sql = @"DELETE FROM contacts_changelog WHERE timestamp < ? AND identifier = ?";
		if ([db executeUpdate:sql, date, identifier] == NO) {
			if (error) {
				*error = [db lastError];
			}
			*rollback = YES;
			success = NO;
			return;
		}
	}];
	
	return success;
}

- (BOOL)configureChangeTrackingForRecordType:(NSString *)recordType error:(NSError **)error
{
	if ([recordType isEqualToString:@"Contact"] == NO) {
		// TO-DO: Make sure to set an NSError. In this app, this shouldn't ever happen though since the only record type is Contact.
		return nil;
	}
	
	return [self populateContactChangeLogReturningError:error];
}


- (NSData *)systemFieldsDataForRecordWithIdentifier:(NSString *)identifier error:(NSError **)error
{
	return [self syncDataForRecordWithIdentifier:identifier error:error];
}

- (BOOL)saveSystemFieldsData:(NSData *)data withIdentifier:(NSString *)identifier error:(NSError **)error
{
	return [self saveSyncData:data withIdentifier:identifier error:error];
}

- (BOOL)deleteSystemFieldsForRecordWithIdentifier:(NSString *)identifier error:(NSError **)error
{
	return [self deleteSyncDataForRecordWithIdentifier:identifier error:error];
}

- (BOOL)purgeAllSystemFieldsWithError:(NSError **)error
{
	return [self purgeSyncDataWithError:error];
}

#pragma mark - Private Methods


- (BOOL)_configureDatabaseWithError:(NSError **)error
{
	__block NSError *dbError = nil;
	[_dbQueue inTransaction:^(FMDatabase *db, BOOL *rollback) {
		
		// Create contacts table
		if ([db executeUpdate:kBTDBCreateContactTableSQL] == NO) {
			*rollback = YES;
			dbError = [db lastError];
		}
		
		// Create contacts change log table
		if ([db executeUpdate:kBTDBCreateContactChangeLogTableSQL] == NO) {
			*rollback = YES;
			dbError = [db lastError];
		}
		
		// Create the CKRecords table (used to re-construct CKRecords to
		// communicate with the CloudKit server on objects that have previously
		// been saved/received from the server).
		if ([db executeUpdate:kBTDBCreateCKRecordTableSQL] == NO) {
			*rollback = YES;
			dbError = [db lastError];
		}
	}];
	
	if (error && *error) {
		if (error) {
			*error = dbError;
		}
		return NO;
	}
	return YES;
}


- (BOOL)_enableContactChangeLogging:(BOOL)enable inDatabase:(FMDatabase *)db error:(NSError **)error
{
	if (enable) {
		if ([db executeUpdate:kBTDBCreateInsertTriggerSQL] == NO) {
			if (error) {
				*error = [db lastError];
				return NO;
			}
		}
		
		if ([db executeUpdate:kBTDBCreateUpdateTriggerSQL] == NO) {
			if (error) {
				*error = [db lastError];
				return NO;
			}
		}
		
		if ([db executeUpdate:kBTDBCreateDeleteTriggerSQL] == NO) {
			if (error) {
				*error = [db lastError];
				return NO;
			}
		}
	} else {
		if ([db executeUpdate:kBTDBDropInsertTriggerSQL] == NO) {
			if (error) {
				*error = [db lastError];
				return NO;
			}
		}
		
		if ([db executeUpdate:kBTDBDropUpdateTriggerSQL] == NO) {
			if (error) {
				*error = [db lastError];
				return NO;
			}
		}
		
		if ([db executeUpdate:kBTDBDropDeleteTriggerSQL] == NO) {
			if (error) {
				*error = [db lastError];
				return NO;
			}
		}
	}
	
	return YES;
}

//- (BOOL)_contactChangeLogEnabled
//{
//	__block BOOL changeLogEnabled = NO;
//	[_dbQueue inDatabase:^(FMDatabase *db) {
//		FMResultSet *rs = [db executeQuery:@"SELECT name FROM sqlite_master WHERE name='insert_contact' AND type='trigger'"];
//		if (rs) {
//			changeLogEnabled = YES;
//		}
//	}];
//	
//	return YES;
//}


- (BTContact *)_contactFromResultSet:(FMResultSet *)resultSet
{
	if (resultSet == nil) {
		return nil;
	}
	
	BTContact *contact = [[BTContact alloc] init];
	NSString *value = [resultSet stringForColumn:@"identifier"];
	if (value == nil) {
		// Can't have a contact without an identifier
		return nil;
	}
	contact.identifier = value;
	
	value = [resultSet stringForColumn:@"first_name"];
	if (value) {
		contact.firstName = value;
	}
	
	value = [resultSet stringForColumn:@"last_name"];
	if (value) {
		contact.lastName = value;
	}
	
	NSDate *lastModifiedDate = [resultSet dateForColumn:@"last_modified"];
	if (lastModifiedDate) {
		contact.lastModified = lastModifiedDate;
	} else {
		contact.lastModified = [NSDate distantPast];
	}
	
	// Make sure the contact has at least a first or last name
	if (contact.firstName == nil && contact.lastName == nil) {
		return nil;
	}
	
	return contact;
}

- (BOOL)_isSyncEnabled
{
	return [[NSUserDefaults standardUserDefaults] boolForKey:kBTCloudKitSyncSettingSyncEnabledKey];
}

@end
