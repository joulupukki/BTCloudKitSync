//
//  AppDelegate.m
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

#import "AppDelegate.h"

#import "BTDB.h"
#import "BTCloudKitSync.h"

@interface AppDelegate ()

@end

@implementation AppDelegate


- (BOOL)application:(UIApplication *)application didFinishLaunchingWithOptions:(NSDictionary *)launchOptions
{
	// Override point for customization after application launch.
	BTDB *db = [BTDB sharedInstance];
	BTCloudKitSync *sync = [BTCloudKitSync sharedInstance];
	[sync configureWithDatabase:db];
	
//	UIUserNotificationSettings *notificationSettings = [UIUserNotificationSettings settingsForTypes:UIUserNotificationTypeNone categories:nil];
	[application registerForRemoteNotifications];
//	[application registerUserNotificationSettings:notificationSettings];
	
	return YES;
}

- (void)applicationWillResignActive:(UIApplication *)application
{
	// Sent when the application is about to move from active to inactive state. This can occur for certain types of temporary interruptions (such as an incoming phone call or SMS message) or when the user quits the application and it begins the transition to the background state.
	// Use this method to pause ongoing tasks, disable timers, and throttle down OpenGL ES frame rates. Games should use this method to pause the game.
}

- (void)applicationDidEnterBackground:(UIApplication *)application
{
	// Use this method to release shared resources, save user data, invalidate timers, and store enough application state information to restore your application to its current state in case it is terminated later.
	// If your application supports background execution, this method is called instead of applicationWillTerminate: when the user quits.
}

- (void)applicationWillEnterForeground:(UIApplication *)application
{
	// Called as part of the transition from the background to the inactive state; here you can undo many of the changes made on entering the background.
}

- (void)applicationDidBecomeActive:(UIApplication *)application
{
	// Restart any tasks that were paused (or not yet started) while the application was inactive. If the application was previously in the background, optionally refresh the user interface.
	
	// This is a test to see how the databse code behaves when making
	// simultaneous changes from both the server and the local client.
//	BTDB *db = [BTDB sharedInstance];
//	
//	dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)(5 * NSEC_PER_SEC)), dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
//		for (int i = 0; i < 100; i++) {
//			BTContact *contact = [[BTContact alloc] init];
//			contact.firstName = [NSString stringWithFormat:@"%d", i];
//			contact.lastName = @"Server";
//			NSError *error = nil;
//			if ([db addContact:contact shouldLogChanges:NO error:&error] == NO) {
//				NSLog(@"Error adding server contact (%@): %@", [contact fullName], error);
//			}
//		}
//	});
//	
//	dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)(5 * NSEC_PER_SEC)), dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
//		for (int i = 0; i < 100; i++) {
//			BTContact *contact = [[BTContact alloc] init];
//			contact.firstName = [NSString stringWithFormat:@"%d", i];
//			contact.lastName = @"Client";
//			NSError *error = nil;
//			if ([db addContact:contact shouldLogChanges:YES error:&error] == NO) {
//				NSLog(@"Error adding client contact (%@): %@", [contact fullName], error);
//			}
//		}
//	});
}

- (void)applicationWillTerminate:(UIApplication *)application
{
	// Called when the application is about to terminate. Save data if appropriate. See also applicationDidEnterBackground:.
}

- (void)application:(UIApplication *)application didReceiveRemoteNotification:(NSDictionary *)userInfo fetchCompletionHandler:(void (^)(UIBackgroundFetchResult))completionHandler
{
	BTCloudKitSync *sync = [BTCloudKitSync sharedInstance];
	if (sync.syncEnabled == NO) {
		completionHandler(UIBackgroundFetchResultNoData);
		return;
	}
	
	[sync fetchRecordChangesWithCompletionHandler:^(BTFetchResult result, BOOL moreComing) {
		if (moreComing == NO) {
			switch (result) {
				case BTFetchResultNewData:
					completionHandler(UIBackgroundFetchResultNewData);
					break;
				case BTFetchResultNoData:
					completionHandler(UIBackgroundFetchResultNoData);
					break;
				case BTFetchResultFailed:
					completionHandler(UIBackgroundFetchResultFailed);
					break;
			}
		}
	}];
}

@end
