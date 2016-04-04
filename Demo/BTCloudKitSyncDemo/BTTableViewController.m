//
//  BTTableViewController.m
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

#import "BTTableViewController.h"

#import "BTDB.h"
#import "BTConstants.h"
#import "BTContact.h"
#import "BTCloudKitSync.h"

#import "SVProgressHUD.h"

typedef enum : NSUInteger {
	SectionIndexSyncSettings,
	SectionIndexContacts,
} SectionIndex;

@interface BTTableViewController ()

@property (nonatomic, strong) NSMutableArray *contactsA;
@property (nonatomic, strong) NSDateFormatter *dateFormatter;

@end

@implementation BTTableViewController

- (void)viewDidLoad {
    [super viewDidLoad];
    
    // Uncomment the following line to preserve selection between presentations.
    // self.clearsSelectionOnViewWillAppear = NO;
    
    // Uncomment the following line to display an Edit button in the navigation bar for this view controller.
    // self.navigationItem.rightBarButtonItem = self.editButtonItem;
	_contactsA = [NSMutableArray new];
	[self _loadContacts];
	
	_dateFormatter = [[NSDateFormatter alloc] init];
	_dateFormatter.timeStyle = NSDateFormatterMediumStyle;
	_dateFormatter.dateStyle = NSDateFormatterShortStyle;
	_dateFormatter.locale = [NSLocale currentLocale];
	
	UIBarButtonItem *editButton = [[UIBarButtonItem alloc] initWithBarButtonSystemItem:UIBarButtonSystemItemEdit target:self action:@selector(_editButtonPressed:)];
	UIBarButtonItem *addButton = [[UIBarButtonItem alloc] initWithBarButtonSystemItem:UIBarButtonSystemItemAdd target:self action:@selector(_addButtonPressed:)];
	
	[self.navigationItem setLeftBarButtonItem:editButton animated:NO];
	[self.navigationItem setRightBarButtonItem:addButton animated:NO];
	
	[[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(_contactAdded:) name:kBTContactAddedNotification object:nil];
	[[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(_contactUpdated:) name:kBTContactUpdatedNotification object:nil];
	[[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(_contactDeleted:) name:kBTContactDeletedNotification object:nil];
	
	[[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(_syncMessageReceived:) name:kBTCloudKitSyncMessageNotification object:nil];
	[[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(_syncSucceeded:) name:kBTCloudKitSyncSuccessNotification object:nil];
	[[NSNotificationCenter defaultCenter] addObserver:self selector:@selector(_syncFailed:) name:kBTCloudKitSyncErrorNotification object:nil];
}

- (void)didReceiveMemoryWarning {
    [super didReceiveMemoryWarning];
    // Dispose of any resources that can be recreated.
}

#pragma mark - Table view data source

- (NSInteger)numberOfSectionsInTableView:(UITableView *)tableView {
    return 2;
}

- (NSInteger)tableView:(UITableView *)tableView numberOfRowsInSection:(NSInteger)section {
	switch ((SectionIndex)section) {
		case SectionIndexSyncSettings:
			return 3;
		case SectionIndexContacts:
			return _contactsA.count;
	}
}

- (UITableViewCell *)tableView:(UITableView *)tableView cellForRowAtIndexPath:(NSIndexPath *)indexPath {
    UITableViewCell *cell = [tableView dequeueReusableCellWithIdentifier:@"ContactCell" forIndexPath:indexPath];
	cell.accessoryView = nil;
	
	switch ((SectionIndex)indexPath.section) {
		case SectionIndexSyncSettings:
		{
			BTCloudKitSync *sync = [BTCloudKitSync sharedInstance];
			
			switch (indexPath.row) {
				case 0:
				{
					cell.textLabel.text = NSLocalizedString(@"CloudKit Sync", @"Cell title for enabling CloudKit sync.");
					cell.detailTextLabel.text = nil;
					
					UISwitch *syncSwitch = [[UISwitch alloc] initWithFrame:CGRectZero];
					syncSwitch.on = sync.syncEnabled;
					[syncSwitch addTarget:self action:@selector(_syncSwitchChanged:) forControlEvents:UIControlEventValueChanged];
					cell.accessoryView = syncSwitch;
					break;
				}
				case 1:
				{
					cell.textLabel.text = NSLocalizedString(@"Sync Status", @"Title of cell to describe the latest/current sync status.");
					if (sync.syncEnabled) {
						if (sync.lastSyncDate) {
							cell.detailTextLabel.text = [_dateFormatter stringFromDate:sync.lastSyncDate];
						} else {
							cell.detailTextLabel.text = NSLocalizedString(@"never synchronized", @"Cell detail text to indicate that a sync with CloudKit has never occurred.");
						}
					} else {
						cell.detailTextLabel.text = NSLocalizedString(@"sync not enabled", @"Cell detail text to indicate that CloudKit sync is not enabled.");
					}
					break;
				}
				case 2:
				{
					cell.textLabel.text = NSLocalizedString(@"Number of Contacts", @"Title of cell to indicate how many contacts are in the database.");
					cell.detailTextLabel.text = [NSString stringWithFormat:@"%lu", (unsigned long)_contactsA.count];
				}
			}
			break;
		}
		case SectionIndexContacts:
		{
			BTContact *contact = _contactsA[indexPath.row];
			cell.textLabel.text = [contact fullName];
			if (contact.lastModified) {
				cell.detailTextLabel.text = [_dateFormatter stringFromDate:contact.lastModified];
			} else {
				cell.detailTextLabel.text = nil;
			}
			break;
		}
	}
	
    return cell;
}

// Override to support conditional editing of the table view.
- (BOOL)tableView:(UITableView *)tableView canEditRowAtIndexPath:(NSIndexPath *)indexPath
{
	if (indexPath.section == SectionIndexSyncSettings) {
		return NO;
	}
    return YES;
}

// Override to support editing the table view.
- (void)tableView:(UITableView *)tableView commitEditingStyle:(UITableViewCellEditingStyle)editingStyle forRowAtIndexPath:(NSIndexPath *)indexPath {
    if (editingStyle == UITableViewCellEditingStyleDelete) {
        // Delete the row from the data source
		BTContact *contact = _contactsA[indexPath.row];
		NSError *error = nil;
		if ([[BTDB sharedInstance] deleteContactWithIdentifier:contact.identifier error:&error] == NO) {
			NSLog(@"Could not delete the contact: %@", [contact fullName]);
		} else {
			NSLog(@"Deleted the contact: %@", [contact fullName]);
		}
    }
}

/*
// Override to support rearranging the table view.
- (void)tableView:(UITableView *)tableView moveRowAtIndexPath:(NSIndexPath *)fromIndexPath toIndexPath:(NSIndexPath *)toIndexPath {
}
*/

/*
// Override to support conditional rearranging of the table view.
- (BOOL)tableView:(UITableView *)tableView canMoveRowAtIndexPath:(NSIndexPath *)indexPath {
    // Return NO if you do not want the item to be re-orderable.
    return YES;
}
*/

#pragma mark - UITableViewDelegate

- (NSIndexPath *)tableView:(UITableView *)tableView willSelectRowAtIndexPath:(NSIndexPath *)indexPath
{
	// Prevent the sync rows from being selected
	if (indexPath.section == SectionIndexSyncSettings) {
		return nil;
	}
	
	return indexPath;
}

- (void)tableView:(UITableView *)tableView didSelectRowAtIndexPath:(NSIndexPath *)indexPath
{
	[tableView deselectRowAtIndexPath:indexPath animated:YES];
	
	// Change to another random name
	BTDB *db = [BTDB sharedInstance];
	BTContact *contact = _contactsA[indexPath.row];
	
	NSString *oldFirstName = contact.firstName;
	NSString *oldLastName = contact.lastName;
	
	// Randomly only change the first or last name so that we can test the
	// partial updating of a record in CloudKit.
	NSUInteger firstOrLastName = arc4random() % 2;
	if (firstOrLastName == 0) {
		contact.firstName = [self _randomFirstName];
	} else {
		contact.lastName = [self _randomLastName];
	}
	
	NSError *error = nil;
	if ([db updateContact:contact error:&error] == NO) {
		NSLog(@"Error updating contact (%@ %@): %@", oldFirstName, oldLastName, error);
	} else {
		NSLog(@"Updated contact successfully: %@", [contact fullName]);
	}
}

/*
#pragma mark - Navigation

// In a storyboard-based application, you will often want to do a little preparation before navigation
- (void)prepareForSegue:(UIStoryboardSegue *)segue sender:(id)sender {
    // Get the new view controller using [segue destinationViewController].
    // Pass the selected object to the new view controller.
}
*/

#pragma mark - Private Methods

- (void)_loadContacts
{
	[_contactsA removeAllObjects];
	NSArray *allContacts = [[BTDB sharedInstance] readAllContacts];
	if (allContacts) {
		[_contactsA addObjectsFromArray:allContacts];
	}
}


- (void)_editButtonPressed:(id)sender
{
	self.tableView.editing = YES;
	
	UIBarButtonItem *doneButton = [[UIBarButtonItem alloc] initWithBarButtonSystemItem:UIBarButtonSystemItemDone target:self action:@selector(_doneButtonPressed:)];
	
	[self.navigationItem setLeftBarButtonItem:nil animated:YES];
	[self.navigationItem setRightBarButtonItem:doneButton animated:YES];
}

- (void)_addButtonPressed:(id)sender
{
	NSString *firstName = [self _randomFirstName];
	NSString *lastName = [self _randomLastName];
	
	BTDB *db = [BTDB sharedInstance];
	
	BTContact *contact = [[BTContact alloc] init];
	contact.firstName = firstName;
	contact.lastName = lastName;
	
	NSError *error = nil;
	if ([db addContact:contact error:&error] == NO) {
		NSLog(@"Error adding contact (%@): %@", [contact fullName], error);
	} else {
		NSLog(@"Added contact successfully: %@", [contact fullName]);
	}
}

- (void)_doneButtonPressed:(id)sender
{
	UIBarButtonItem *editButton = [[UIBarButtonItem alloc] initWithBarButtonSystemItem:UIBarButtonSystemItemEdit target:self action:@selector(_editButtonPressed:)];
	UIBarButtonItem *addButton = [[UIBarButtonItem alloc] initWithBarButtonSystemItem:UIBarButtonSystemItemAdd target:self action:@selector(_addButtonPressed:)];
	
	[self.navigationItem setLeftBarButtonItem:editButton animated:YES];
	[self.navigationItem setRightBarButtonItem:addButton animated:YES];
	
	self.tableView.editing = NO;
}


- (NSString *)_randomFirstName
{
	static NSArray *firstNames = nil;
	if (firstNames == nil) {
		firstNames = @[@"James", @"John", @"Robert", @"Michael", @"William", @"David", @"Richard", @"Joseph", @"Charles", @"Thomas",
					   @"Mary", @"Patricia", @"Jennifer", @"Elizabeth", @"Linda", @"Barbara", @"Susan", @"Margaret", @"Jessica", @"Sarah"];
	}
	
	NSUInteger idx = arc4random() % firstNames.count;
	return firstNames[idx];
}

- (NSString *)_randomLastName
{
	static NSArray *lastNames = nil;
	if (lastNames == nil) {
		lastNames = @[@"Smith", @"Johnson", @"Williams", @"Brown", @"Jones", @"Miller", @"Davis", @"Garcia", @"Rodriguez", @"Wilson",
					  @"Martinez", @"Anderson", @"Taylor", @"Thomas", @"Hernandez", @"Moore", @"Martin", @"Jackson", @"Thompson", @"White"];
	}
	
	NSUInteger idx = arc4random() % lastNames.count;
	return lastNames[idx];
}

- (void)_contactAdded:(NSNotification *)notification
{
	NSDictionary *userInfo = notification.userInfo;
	if (userInfo == nil) {
		return;
	}
	
	NSString *contactIdentifier = userInfo[kBTContactIdentifierKey];
	if (contactIdentifier == nil) {
		return;
	}
	
	BTDB *db = [BTDB sharedInstance];
	BTContact *contact = [db readContactWithIdentifier:contactIdentifier];
	if (contact == nil) {
		return;
	}
	
	[_contactsA addObject:contact];
	[_contactsA sortUsingComparator:^NSComparisonResult(__kindof BTContact * _Nonnull contactA, __kindof BTContact * _Nonnull contactB) {
		NSComparisonResult result = [contactA.identifier compare:contactB.identifier];
		if (result == NSOrderedSame) {
			result = [contactA.identifier compare:contactB.identifier];
		}
		return result;
	}];
	NSUInteger idx = [_contactsA indexOfObject:contact];
	NSIndexPath *indexPath = [NSIndexPath indexPathForRow:idx inSection:SectionIndexContacts];
	[self.tableView insertRowsAtIndexPaths:@[indexPath] withRowAnimation:UITableViewRowAnimationFade];
	
	// Update the contacts count row
	UITableViewCell *cell = [self.tableView cellForRowAtIndexPath:[NSIndexPath indexPathForRow:2 inSection:SectionIndexSyncSettings]];
	if (cell) {
		cell.detailTextLabel.text = [NSString stringWithFormat:@"%lu", (unsigned long)_contactsA.count];
	}
}

- (void)_contactUpdated:(NSNotification *)notification
{
	NSDictionary *userInfo = notification.userInfo;
	if (userInfo == nil) {
		return;
	}
	
	NSString *contactIdentifier = userInfo[kBTContactIdentifierKey];
	if (contactIdentifier == nil) {
		return;
	}
	
	BTDB *db = [BTDB sharedInstance];
	BTContact *contact = [db readContactWithIdentifier:contactIdentifier];
	if (contact == nil) {
		return;
	}
	
	NSIndexPath *indexPath = [self _indexPathForContactWithIdentifier:contactIdentifier];
	if (indexPath == nil) {
		return;
	}
	
	[_contactsA replaceObjectAtIndex:indexPath.row withObject:contact];
	[self.tableView reloadRowsAtIndexPaths:@[indexPath] withRowAnimation:UITableViewRowAnimationFade];
}

- (void)_contactDeleted:(NSNotification *)notification
{
	NSDictionary *userInfo = notification.userInfo;
	if (userInfo == nil) {
		return;
	}
	
	NSString *contactIdentifier = userInfo[kBTContactIdentifierKey];
	if (contactIdentifier == nil) {
		return;
	}
	
	NSIndexPath *indexPath = [self _indexPathForContactWithIdentifier:contactIdentifier];
	if (indexPath == nil) {
		return;
	}
	
	[_contactsA removeObjectAtIndex:indexPath.row];
	[self.tableView deleteRowsAtIndexPaths:@[indexPath] withRowAnimation:UITableViewRowAnimationFade];
	
	// Update the contacts count row
	UITableViewCell *cell = [self.tableView cellForRowAtIndexPath:[NSIndexPath indexPathForRow:2 inSection:SectionIndexSyncSettings]];
	if (cell) {
		cell.detailTextLabel.text = [NSString stringWithFormat:@"%lu", (unsigned long)_contactsA.count];
	}
}


- (void)_syncMessageReceived:(NSNotification *)notification
{
	if (notification.userInfo == nil) {
		return;
	}
	NSString *message = notification.userInfo[kBTCloudKitSyncMessageKey];
	if (message) {
		[self _updateSyncMessage:message];
	}
}

- (void)_syncSucceeded:(NSNotification *)notification
{
	NSIndexPath *indexPath = [NSIndexPath indexPathForRow:1 inSection:SectionIndexSyncSettings];
	[self.tableView reloadRowsAtIndexPaths:@[indexPath] withRowAnimation:UITableViewRowAnimationFade];
}

- (void)_syncFailed:(NSNotification *)notification
{
	if (notification.userInfo == nil) {
		return;
	}
	NSError *error = notification.userInfo[kBTCloudKitSyncErrorKey];
	if (error) {
		[self _updateSyncMessage:error.localizedDescription];
	}
}


- (void)_updateSyncMessage:(NSString *)message
{
	if (message == nil) {
		return;
	}
	
	NSIndexPath *indexPath = [NSIndexPath indexPathForRow:1 inSection:SectionIndexSyncSettings];
	UITableViewCell *cell = [self.tableView cellForRowAtIndexPath:indexPath];
	cell.detailTextLabel.text = message;
}


- (NSIndexPath *)_indexPathForContactWithIdentifier:(NSString *)identifier
{
	NSUInteger contactIndex = [_contactsA indexOfObjectPassingTest:^BOOL(__kindof BTContact * _Nonnull contact, NSUInteger idx, BOOL * _Nonnull stop) {
		if ([contact.identifier isEqualToString:identifier]) {
			*stop = YES;
			return YES;
		}
		return NO;
	}];
	
	if (contactIndex == NSNotFound) {
		return nil;
	}
	
	return [NSIndexPath indexPathForRow:contactIndex inSection:SectionIndexContacts];
}


- (void)_syncSwitchChanged:(UISwitch *)syncSwitch
{
	[self _showHUDWithMessage:NSLocalizedString(@"Please wait...", @"Please wait message when enabling/disabling sync")];
	
	BTCloudKitSync *sync = [BTCloudKitSync sharedInstance];
	[sync enableSync:syncSwitch.on withCompletionBlock:^(BOOL success, NSError *error) {
		[self _hideHUDMessage];
		
		if (!success) {
			NSString *errorMessage;
			if (error) {
				errorMessage = [error localizedDescription];
			} else {
				errorMessage = NSLocalizedString(@"Could not change the sync status. Please try again later.", @"Error message shown to a user if we weren't able to enable/disable CloudKit sync.");
			}
			
			NSString *errorTitle;
			if (syncSwitch.on) {
				errorTitle = NSLocalizedString(@"Could not enable sync", @"Title of error message if CloudKit sync could not be enabled.");
			} else {
				errorTitle = NSLocalizedString(@"Could not disable sync", @"Title of error message if CloudKit sync could not be disabled.");
			}
			
			// Reset the switch to its original state
			syncSwitch.on = !syncSwitch.on;
			
			UIAlertController *alert = [UIAlertController alertControllerWithTitle:errorTitle
																		   message:errorMessage
																	preferredStyle:UIAlertControllerStyleAlert];
			UIAlertAction *okAction = [UIAlertAction actionWithTitle:NSLocalizedString(@"OK", @"OK button title")
															   style:UIAlertActionStyleDefault
															 handler:nil];
			[alert addAction:okAction];
			[self presentViewController:alert animated:YES completion:nil];
		}
	}];
}

- (void)_showHUDWithMessage:(NSString *)message
{
	UIInterfaceOrientation orientation = [[UIApplication sharedApplication] statusBarOrientation];
	if (UIInterfaceOrientationIsLandscape(orientation) == YES)
		[SVProgressHUD setOffsetFromCenter:UIOffsetMake(0, 0)];
	else
		[SVProgressHUD setOffsetFromCenter:UIOffsetMake(0, -100)];
	
	[SVProgressHUD setDefaultStyle:SVProgressHUDStyleDark];
	[SVProgressHUD setOffsetFromCenter:UIOffsetMake(0, 0)];
	[SVProgressHUD setDefaultMaskType:SVProgressHUDMaskTypeClear];
	[SVProgressHUD showWithStatus:message];
}

- (void)_hideHUDMessage
{
	[SVProgressHUD dismiss];
}

@end
