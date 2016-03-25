//
//  BTContact.m
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

#import "BTContact.h"

@implementation BTContact

- (NSString *)fullName
{
	NSMutableString *name = [NSMutableString new];
	
	if (_firstName) {
		[name appendString:_firstName];
	}
	
	if (_lastName) {
		if (name.length > 0) {
			[name appendString:@" "];
		}
		[name appendString:_lastName];
	}
	return name;
}

- (NSDictionary *)dictionaryRepresentation
{
	NSMutableDictionary *contactInfo = [NSMutableDictionary new];
	
	if (_firstName) {
		contactInfo[@"firstName"] = _firstName;
	}
	if (_lastName) {
		contactInfo[@"lastName"] = _lastName;
	}
	if (_lastModified) {
		contactInfo[@"lastModified"] = _lastModified;
	}
	
	return contactInfo;
}


+ (BTContact *)contactWithDictionary:(NSDictionary *)contactInfo
{
	if (contactInfo == nil) {
		return nil;
	}
	
	BTContact *contact = [[BTContact alloc] init];
	if (contactInfo[@"firstName"]) {
		contact.firstName = contactInfo[@"firstName"];
	}
	if (contactInfo[@"lastName"]) {
		contact.lastName = contactInfo[@"lastName"];
	}
	if (contactInfo[@"lastModified"]) {
		contact.lastModified = contactInfo[@"lastModified"];
	}
	return contact;
}


@end
