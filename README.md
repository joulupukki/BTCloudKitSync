# BTCloudKitSync

A class that provides simple [CloudKit](https://developer.apple.com/icloud/) sync for iOS apps with a local cache/database.

The purpose of BTCloudKitSync is to add sync a local cache of data through a private CloudKit database ([CKDatabase](https://developer.apple.com/library/ios/documentation/CloudKit/Reference/CloudKit_Framework_Reference/index.html)).

## Background

I started this project to learn how CloudKit works before adding sync it to my own [Bluegrass Guru](http://bluegrassguru.com/) app. After spending the time getting a sample app working with Cloud Kit (the BTCloudKitSyncDemo app), I realized I would have to add in a *lot* of code to make Bluegrass Guru sync its records.

I wondered if it was possible to abstract the sync code to make it easy to add sync to Bluegrass Guru and that's where BTCloudKitSync popped into my head. So far, it seems like it will work for what I need.

In Bluegrass Guru, I sync songs, tags, and set lists between a user's iOS devices. Other apps may have more complicated database and sync requirements, so BTCloudKitSync may not be the answer, but hopefully something in this project helps you answer some questions about how CloudKit works.

## Contributing to BTCloudKitSync

There are still a number of things to solve in BTCloudKitSync. If you'd like to help make BTCloudKitSync more robust, please submit pull requests.

If you'd like to get involved or have general questions, please feel free to contact me:

Twitter: [@bhtimothy](https://twitter.com/bhtimothy)


## THIS IS A WORK IN PROGRESS (ITEMS TO-DO)

There are a number of things that are not yet implemented in BTCloudKitSync. Check the **[Issues](https://github.com/joulupukki/BTCloudKitSync/issues)** tab to see the list of remaining items.

## Overview

BTCloudKitSync works through a **BTCloudKitSyncDatabase** protocol, which your app should implement. BTCloudKitSync doesn't care what technology you use for your local database. The demo uses SQLite, but you could use CoreData or something else.

In your BTCloudKitSyncDatabase implementation, you provide the types of records that should be synchronized. You also provide the changes as an array of NSDictionary objects, which are generally *insert*, *update*, or *delete* changes. In order to keep sync as efficient as possible, your change objects should only include the fields that actually changed.

Your BTCloudKitSyncDatabase also provides the names of notifications it posts via NSNotificationCenter. BTCloudKitSync observes these notifications and uses them to sync automatically if there has been no further changes after three seconds.

## Concepts used from CloudKit

1. CKModifyRecordsOperation to upload records to CloudKit
2. CKFetchChangesOperation to download record changes from CloudKit
3. CKSubscription for receiving changes from other devices
	* Sends silent Apple Push Notifications (APS) to the App Delegate when changes happen in the private CloudKit database from other devices

## The Demo App (BTCloudKitSyncDemo)

This is a simplistic address book app with a single [UITableViewController](https://developer.apple.com/library/ios/documentation/UIKit/Reference/UITableViewController_Class/) that displays random first and last names.

* Tap on the **+** button to create a new record. The app will randomly generate a first and last name
* Tap on a contact row to randomly change the first/last name
* Swipe to delete a contact row
* The iOS Simulator cannot receive silent push notifications, but if you install the app on an iOS device and make changes in the simulator, you should see the iOS device update with the changes

The provided demo app uses SQLite for the local database. An important step in synchronizing data with CloudKit is tracking local changes. The demo project uses [SQLite Triggers](https://sqlite.org/lang_createtrigger.html) to track local changes, which makes tracking changes extremely simple.

When adding changes from the server, SQLite Triggers are temporarily disabled (no need to track server changes). [FMDatabaseQueue](https://ccgus.github.io/fmdb/html/Classes/FMDatabaseQueue.html) is used to perform queries and updates in the database.


## Acknowledgements
* CloudKit team at Apple - Thanks for the cool tech and also answering questions!

The demo app uses the following:

* FBDB - an Objective-C wrapper around SQLite: <https://github.com/ccgus/fmdb>
* SVProgressHUD - A lightweight progress HUD: <https://github.com/SVProgressHUD/SVProgressHUD>
