//
//  BYKVStorage.swift
//  BYCache
//
//  Created by Beryter on 2019/7/31.
//  Copyright © 2019 Beryter. All rights reserved.
//

import Foundation
import QuartzCore
import SQLite3
import UIKit

let maxErrorRetryCount = 8
let minRetryTimeInterval: TimeInterval = 2
let pathLengthMax = PATH_MAX - 64
let DBFileName = "manifest.sqlite"
let DBShmFileName = "manifest.sqlite-shm"
let DBWalFileName = "manifest.sqlite-wal"
let DataDirectoryName = "data"
let TrashDirectoryName = "trash"

class BYKVStorageItem {
    var key: String
    var value: Data?
    var fileName: String?
    var size: Int = 0
    var modTime: Int = 0
    var accessTime: Int = 0
    var extendedData: Data?
    init(key: String) {
        self.key = key
    }
}

enum BYKVStorageType {
    case File
    case SQL
    case Mix
}

class BYKVStorage {
    private(set) var path: String
    private(set) var type: BYKVStorageType
    var errorLogsEnabled = true

    private var trashQueue = DispatchQueue(label: "com.beryter.cache.disk.trash")
    private var dbPath: String
    private var dataPath: String
    private var trashPath: String
    private var dbLastOpenErrorTime: TimeInterval = 0
    private var dbOpenErrorCount = 0
    private var db: OpaquePointer?
    private var dbStmtCache = [String: OpaquePointer]()

    init?(path: String, type: BYKVStorageType) {
        if path.isEmpty || path.count > pathLengthMax {
            return nil
        }
        self.path = path
        self.type = type
        dataPath = (path as NSString).appendingPathComponent(DataDirectoryName)
        trashPath = (path as NSString).appendingPathComponent(TrashDirectoryName)
        dbPath = (path as NSString).appendingPathComponent(DBFileName)
        do {
            try FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: true)
            try FileManager.default.createDirectory(atPath: dataPath, withIntermediateDirectories: true)
            try FileManager.default.createDirectory(atPath: trashPath, withIntermediateDirectories: true)
        } catch {
            return nil
        }
        if !dbOpen() || !dbInitialize() {
            _ = dbClose()
            reset()
            if !dbOpen() || !dbInitialize() {
                _ = dbClose()
                return nil
            }
        }
        fileEmptyTrashInBackground()
    }

    deinit {
        let taskID = UIApplication.shared.beginBackgroundTask {
        }
        _ = dbClose()
        if taskID != UIBackgroundTaskIdentifier.invalid {
            UIApplication.shared.endBackgroundTask(taskID)
        }
    }

    private func reset() {
        try? FileManager.default.removeItem(atPath: (path as NSString).appendingPathComponent(DBFileName))
        try? FileManager.default.removeItem(atPath: (path as NSString).appendingPathComponent(DBShmFileName))
        try? FileManager.default.removeItem(atPath: (path as NSString).appendingPathComponent(DBWalFileName))
        _ = moveAllFileToTrash()
        fileEmptyTrashInBackground()
    }

    private func dbOpen() -> Bool {
        if let _ = db { return true }
        let res = sqlite3_open(dbPath.cString(using: .utf8), &db)
        if res == SQLITE_OK {
            dbStmtCache.removeAll()
            dbLastOpenErrorTime = 0
            dbOpenErrorCount = 0
            return true
        }
        db = nil
        dbStmtCache.removeAll()
        dbLastOpenErrorTime = CACurrentMediaTime()
        dbOpenErrorCount += 1
        if errorLogsEnabled {
            print("file \(#file) line \(#line) function \(#function) sqlite open failed \(res).")
        }
        return false
    }

    private func dbClose() -> Bool {
        guard let _ = db else { return true }
        var res: Int32 = 0
        var retry = false
        var stmtFinalized = false
        dbStmtCache.removeAll()
        repeat {
            retry = false
            res = sqlite3_close(db)
            if res == SQLITE_BUSY || res == SQLITE_LOCKED {
                if !stmtFinalized {
                    stmtFinalized = true
                    while let stmt = sqlite3_next_stmt(db, nil), Int(bitPattern: stmt) != 0 {
                        sqlite3_finalize(stmt)
                        retry = true
                    }
                }
            } else if res != SQLITE_OK {
                if errorLogsEnabled {
                    print("file \(#file) line \(#line) function \(#function) sqlite close failed \(res).")
                }
            }
        } while retry
        db = nil
        return true
    }

    private func dbInitialize() -> Bool {
        let sql = """
        pragma journal_mode = wal; pragma synchronous = normal; create table if not exists manifest (key text, filename text, size integer, inline_data blob, modification_time integer, last_access_time integer, extended_data blob, primary key(key)); create index if not exists last_access_time_idx on manifest(last_access_time);
        """
        return dbExecute(sql)
    }

    private func dbCheck() -> Bool {
        if let _ = db { return true }
        if dbOpenErrorCount < maxErrorRetryCount, CACurrentMediaTime() - dbLastOpenErrorTime > minRetryTimeInterval {
            return dbOpen() && dbInitialize()
        }
        return false
    }

    private func dbCheckPoint() {
        if !dbCheck() { return }
        sqlite3_wal_checkpoint(db, nil)
    }

    private func dbExecute(_ sql: String) -> Bool {
        guard !sql.isEmpty else { return false }
        guard dbCheck() else { return false }
        var error: UnsafeMutablePointer<Int8>?
        let res = sqlite3_exec(db, sql.cString(using: .utf8), nil, nil, &error)
        if error != nil {
            if errorLogsEnabled {
                print("file \(#file) line \(#line) function \(#function) sqlite close failed \(String(describing: error)).")
            }
            sqlite3_free(error)
        }
        return res == SQLITE_OK
    }

    private func dbPrepareStmt(_ sql: String) -> OpaquePointer? {
        guard dbCheck() else { return nil }
        guard !sql.isEmpty else { return nil }
        var stmt = dbStmtCache[sql]

        if let st = stmt {
            sqlite3_reset(st)
            return st
        }
        let res = sqlite3_prepare_v2(db, sql.cString(using: .utf8), -1, &stmt, nil)
        if res != SQLITE_OK {
            if errorLogsEnabled {
                print("file \(#file) line \(#line) function \(#function) sqlite close failed \(res) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
            }
            return nil
        }
        dbStmtCache[sql] = stmt!
        return stmt
    }

    private func dbJoinedKeys(_ keys: [String]) -> String {
        var str = ""
        for i in 0 ..< keys.count {
            str += "?"
            if i + 1 != keys.count {
                str += ","
            }
        }
        return str
    }

    private func dbBindJoinedKeys(_ keys: [String], stmt: OpaquePointer, fromIndex index: Int) {
        for (i, key) in keys.enumerated() {
            sqlite3_bind_text(stmt, Int32(index + i), key.cString(using: .utf8), -1, nil)
        }
    }

    private func dbSave(withKey key: String, value: Data, fileName: String, extendedData: Data? = nil) -> Bool {
        let sql = "insert or replace into manifest (key, filename, size, inline_data, modification_time, last_access_time, extended_data) values (?1, ?2, ?3, ?4, ?5, ?6, ?7);"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return false }
        let timestamp = Int(time(nil))
        
        sqlite3_bind_int(stmt, 3, Int32(value.count))
        sqlite3_bind_text(stmt, 1, key.cString(using: .utf8), -1, nil)
        sqlite3_bind_text(stmt, 2, fileName.cString(using: .utf8), -1, nil)
        
        let bytes = [UInt8](value)
        if fileName.isEmpty {
            sqlite3_bind_blob(stmt, 4, UnsafeRawPointer(bytes), Int32(value.count), nil)
        } else {
            sqlite3_bind_blob(stmt, 4, nil, 0, nil)
        }
        sqlite3_bind_int(stmt, 5, Int32(timestamp))
        sqlite3_bind_int(stmt, 6, Int32(timestamp))
        if let ext = extendedData {
            let extendBytes = [UInt8](ext)
            sqlite3_bind_blob(stmt, 7, UnsafeRawPointer(extendBytes), Int32(ext.count), nil)
        } else {
            sqlite3_bind_blob(stmt, 7, nil, 0, nil)
        }
        let result = sqlite3_step(st)
        if result != SQLITE_DONE {
            if errorLogsEnabled {
                print("file \(#file) line \(#line) function \(#function) sqlite save failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
            }
            return false
        }
        return true
    }

    private func dbUpdateAccessTime(withKey key: String) -> Bool {
        let sql = "update manifest set last_access_time = ?1 where key = ?2;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return false }
        sqlite3_bind_int(st, 1, Int32(time(nil)))
        sqlite3_bind_text(st, 2, key.cString(using: .utf8), -1, nil)
        let result = sqlite3_step(st)
        if result == SQLITE_DONE { return true }
        if !errorLogsEnabled { return false }
        print("file \(#file) line \(#line) function \(#function) sqlite update failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
        return false
    }

    private func dbUpdateAccessTime(withKeys keys: [String]) -> Bool {
        guard dbCheck() else { return false }
        let t = time(nil)
        let sql = String(format: "update manifest set last_access_time = %d where key in (%@);", t, dbJoinedKeys(keys))
//        "update manifest set last_access_time = \(t) where key in (\(dbJoinedKeys(keys)));"
        var stmt: OpaquePointer?
        var result = sqlite3_prepare_v2(db, sql.cString(using: .utf8), -1, &stmt, nil)
        if result != SQLITE_OK || stmt == nil {
            if errorLogsEnabled {
                print("file \(#file) line \(#line) function \(#function) sqlite update failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
            }
            return false
        }
        dbBindJoinedKeys(keys, stmt: stmt!, fromIndex: 1)
        result = sqlite3_step(stmt!)
        sqlite3_finalize(stmt!)
        if result == SQLITE_DONE { return true }
        if !errorLogsEnabled { return false }
        print("file \(#file) line \(#line) function \(#function) sqlite update failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
        return false
    }

    private func dbDeleteItem(withKey key: String) -> Bool {
        let sql = "delete from manifest where key = ?1;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return false }
        sqlite3_bind_text(st, 1, key.cString(using: .utf8), -1, nil)
        let result = sqlite3_step(stmt)
        if result == SQLITE_DONE { return true }
        if !errorLogsEnabled { return false }
        print("file \(#file) line \(#line) function \(#function) sqlite delete failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
        return false
    }

    private func dbDeleteItems(withKeys keys: [String]) -> Bool {
        guard dbCheck() else { return false }
        let sql = "delete from manifest where key in (\(dbJoinedKeys(keys)));"
        var stmt: OpaquePointer?
        var result = sqlite3_prepare_v2(db, sql.cString(using: .utf8), -1, &stmt, nil)
        if result != SQLITE_OK || stmt == nil {
            if errorLogsEnabled {
                print("file \(#file) line \(#line) function \(#function) sqlite delete failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
            }
            return false
        }
        dbBindJoinedKeys(keys, stmt: stmt!, fromIndex: 1)
        result = sqlite3_step(stmt)
        sqlite3_finalize(stmt)
        if result == SQLITE_DONE { return true }
        if !errorLogsEnabled { return false }
        print("file \(#file) line \(#line) function \(#function) sqlite delete failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
        return false
    }

    private func dbDeleteItemsLargerThan(size: Int) -> Bool {
        let sql = "delete from manifest where size > ?1;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return false }
        sqlite3_bind_int(st, 1, Int32(size))
        let result = sqlite3_step(st)
        if result == SQLITE_DONE { return true }
        if !errorLogsEnabled { return false }
        print("file \(#file) line \(#line) function \(#function) sqlite delete failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
        return false
    }

    private func dbDeleteItemsEarlierThan(time: Int) -> Bool {
        let sql = "delete from manifest where last_access_time < ?1;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return false }
        sqlite3_bind_int(st, 1, Int32(time))
        let result = sqlite3_step(st)
        if result == SQLITE_DONE { return true }
        if !errorLogsEnabled { return false }
        print("file \(#file) line \(#line) function \(#function) sqlite delete failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
        return false
    }

    private func dbGetItem(fromStmt stmt: OpaquePointer, excludeInlineData: Bool) -> BYKVStorageItem {
        var i: Int32 = 0
        let key = sqlite3_column_text(stmt, i)
        i += 1
        let filename = sqlite3_column_text(stmt, i)
        i += 1
        let size = sqlite3_column_int(stmt, i)
        i += 1

        let inline_data = excludeInlineData ? nil : sqlite3_column_blob(stmt, i)
        let inline_data_bytes = excludeInlineData ? 0 : sqlite3_column_bytes(stmt, i)
        i += 1

        let modification_time = sqlite3_column_int(stmt, i)
        i += 1
        let last_access_time = sqlite3_column_int(stmt, i)
        i += 1
        let extended_data = sqlite3_column_blob(stmt, i)
        let extended_data_bytes = sqlite3_column_bytes(stmt, i)
        i += 1

        var keyStr = ""
        if let ky = key {
            keyStr = String(cString: ky)
        }

        let item = BYKVStorageItem(key: keyStr)
        if let fn = filename, Int(bitPattern: fn) != 0 {
            item.fileName = String(cString: fn)
        }

        item.size = Int(size)

        if let indata = inline_data, inline_data_bytes > 0 {
            item.value = Data(bytes: indata, count: Int(inline_data_bytes))
        }
        item.modTime = Int(modification_time)
        item.accessTime = Int(last_access_time)
        if let exdata = extended_data, extended_data_bytes > 0 {
            item.extendedData = Data(bytes: exdata, count: Int(extended_data_bytes))
        }
        return item
    }

    private func dbGetItem(withKey key: String, excludeInlineData: Bool) -> BYKVStorageItem? {
        let sql = excludeInlineData ? "select key, filename, size, modification_time, last_access_time, extended_data from manifest where key = ?1;" : "select key, filename, size, inline_data, modification_time, last_access_time, extended_data from manifest where key = ?1;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return nil }
        sqlite3_bind_text(st, 1, key.cString(using: .utf8), -1, nil)
        let result = sqlite3_step(st)
        if result == SQLITE_ROW {
            let item = dbGetItem(fromStmt: st, excludeInlineData: excludeInlineData)
            return item
        }
        if let err = sqlite3_errmsg(db) {
            let ss = String(cString: err)
            print("2222222->\(ss)")
        }
        print("file \(#file) line \(#line) function \(#function) sqlite query failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
        if result != SQLITE_DONE, errorLogsEnabled {
            
            print("file \(#file) line \(#line) function \(#function) sqlite query failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
        }
        return nil
    }

    private func dbGetItems(withKeys keys: [String], excludeInlineData: Bool) -> [BYKVStorageItem]? {
        guard dbCheck() else { return nil }
        var sql = ""
        if excludeInlineData {
            sql = "select key, filename, size, modification_time, last_access_time, extended_data from manifest where key in (\(dbJoinedKeys(keys)));"
        } else {
            sql = "select key, filename, size, inline_data, modification_time, last_access_time, extended_data from manifest where key in (\(dbJoinedKeys(keys)))"
        }
        var stmt: OpaquePointer?
        var result = sqlite3_prepare_v2(db, sql.cString(using: .utf8), -1, &stmt, nil)
        if result != SQLITE_OK {
            if errorLogsEnabled {
                print("file \(#file) line \(#line) function \(#function) sqlite stmt prepare failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
            }
            return nil
        }
        guard let st = stmt else {
            print("file \(#file) line \(#line) function \(#function) sqlite stmt prepare failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
            return nil
        }
        dbBindJoinedKeys(keys, stmt: st, fromIndex: 1)
        var items: [BYKVStorageItem]? = [BYKVStorageItem]()
        repeat {
            result = sqlite3_step(st)
            if result == SQLITE_ROW {
                let item = dbGetItem(fromStmt: st, excludeInlineData: excludeInlineData)
                items!.append(item)
            } else if result == SQLITE_DONE {
                break
            } else {
                if errorLogsEnabled {
                    print("file \(#file) line \(#line) function \(#function) sqlite query failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
                }
                items = nil
                break
            }
        } while true
        sqlite3_finalize(st)
        return items
    }

    private func dbGetValue(withKey key: String) -> Data? {
        let sql = "select inline_data from manifest where key = ?1;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return nil }
        sqlite3_bind_text(stmt, 1, key.cString(using: .utf8), -1, nil)
        let result = sqlite3_step(st)
        if result == SQLITE_ROW {
            let inline_data = sqlite3_column_blob(st, 0)
            let inline_data_bytes = sqlite3_column_bytes(st, 0)
            guard let data = inline_data, inline_data_bytes > 0 else { return nil }
            return Data(bytes: data, count: Int(inline_data_bytes))
        }
        if result != SQLITE_DONE, errorLogsEnabled {
            print("file \(#file) line \(#line) function \(#function) sqlite query failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
        }
        return nil
    }

    private func dbGetFileName(withKey key: String) -> String? {
        let sql = "select filename from manifest where key = ?1;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return nil }
        sqlite3_bind_text(st, 1, key.cString(using: .utf8), -1, nil)
        let result = sqlite3_step(st)
        if result == SQLITE_ROW {
            let filename = sqlite3_column_text(st, 0)
            guard let name = filename, Int(bitPattern: filename) != 0 else { return nil }
            return String(cString: name)
        }
        if result != SQLITE_DONE, errorLogsEnabled {
            print("file \(#file) line \(#line) function \(#function) sqlite query failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
        }
        return nil
    }

    private func dbGetFileNames(withKeys keys: [String]) -> [String] {
        guard dbCheck() else { return [] }
        let sql = "select filename from manifest where key in (\(dbJoinedKeys(keys)));"
        var stmt: OpaquePointer?
        var result = sqlite3_prepare_v2(db, sql.cString(using: .utf8), -1, &stmt, nil)
        if result != SQLITE_OK || stmt == nil {
            if errorLogsEnabled {
                print("file \(#file) line \(#line) function \(#function) sqlite stmt prepare failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
            }
            return []
        }
        dbBindJoinedKeys(keys, stmt: stmt!, fromIndex: 1)
        var fileNames = [String]()
        repeat {
            result = sqlite3_step(stmt)
            if result == SQLITE_ROW {
                let filename = sqlite3_column_text(stmt, 0)
                if let name = filename, Int(bitPattern: name) != 0 {
                    let nameStr = String(cString: name)
                    fileNames.append(nameStr)
                }
            } else if result == SQLITE_DONE {
                break
            } else {
                if errorLogsEnabled {
                    print("file \(#file) line \(#line) function \(#function) sqlite query failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
                }
                break
            }
        } while true
        sqlite3_finalize(stmt)
        return fileNames
    }

    private func dbGetFileNames(largerThanSize size: Int) -> [String] {
        let sql = "select filename from manifest where size > ?1 and filename is not null;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return [] }
        sqlite3_bind_int(st, 1, Int32(size))
        var fileNames = [String]()
        repeat {
            let result = sqlite3_step(st)
            if result == SQLITE_ROW {
                let fileName = sqlite3_column_text(st, 0)
                if let name = fileName, Int(bitPattern: fileName) != 0 {
                    let nameStr = String(cString: name)
                    fileNames.append(nameStr)
                }
            } else if result == SQLITE_DONE {
                break
            } else {
                if errorLogsEnabled {
                    print("file \(#file) line \(#line) function \(#function) sqlite query failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
                }
            }

        } while true
        return fileNames
    }

    private func dbGetFileNamesEarlierThan(time: Int) -> [String] {
        let sql = "select filename from manifest where last_access_time < ?1 and filename is not null;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return [] }
        sqlite3_bind_int(st, 1, Int32(time))
        var list = [String]()
        repeat {
            let result = sqlite3_step(st)
            if result == SQLITE_ROW {
                let filename = sqlite3_column_text(st, 0)
                if let name = filename, Int(bitPattern: name) != 0 {
                    let nameStr = String(cString: name)
                    list.append(nameStr)
                }
            } else if result == SQLITE_DONE {
                break
            } else {
                if errorLogsEnabled {
                    print("file \(#file) line \(#line) function \(#function) sqlite query failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
                }
                break
            }
        } while true
        return list
    }

    private func dbGetItemSizeInfoOrderByTimeAsc(withLimit limit: Int) -> [BYKVStorageItem]? {
        let sql = "select key, filename, size from manifest order by last_access_time asc limit ?1;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return nil }
        sqlite3_bind_int(st, 1, Int32(limit))
        var list = [BYKVStorageItem]()
        repeat {
            let result = sqlite3_step(st)
            if result == SQLITE_ROW {
                let key = sqlite3_column_text(st, 0)
                let filename = sqlite3_column_text(st, 1)
                let size = sqlite3_column_int(st, 2)
                let keyStr = (key != nil) ? String(cString: key!) : nil
                if let tmp = keyStr {
                    let item = BYKVStorageItem(key: tmp)
                    if let name = filename {
                        item.fileName = String(cString: name)
                    }
                    item.size = Int(size)
                    list.append(item)
                }
            } else if result == SQLITE_DONE {
                break
            } else {
                if errorLogsEnabled {
                    print("file \(#file) line \(#line) function \(#function) sqlite query failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
                }
                return nil
            }
        } while true
        return list
    }

    private func dbGetItemCount(withKey key: String) -> Int {
        let sql = "select count(key) from manifest where key = ?1;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return -1 }
        sqlite3_bind_text(st, 1, key.cString(using: .utf8), -1, nil)
        let result = sqlite3_step(st)
        if result != SQLITE_ROW {
            if errorLogsEnabled {
                print("file \(#file) line \(#line) function \(#function) sqlite query failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
            }
            return -1
        }
        return Int(sqlite3_column_int(st, 0))
    }

    private func dbGetTotalItemSize() -> Int {
        let sql = "select sum(size) from manifest;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return -1 }
        let result = sqlite3_step(st)
        if result != SQLITE_ROW {
            if errorLogsEnabled {
                print("file \(#file) line \(#line) function \(#function) sqlite query failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
            }
            return -1
        }
        return Int(sqlite3_column_int(st, 0))
    }

    private func dbGetTotalItemCount() -> Int {
        let sql = "select count(*) from manifest;"
        let stmt = dbPrepareStmt(sql)
        guard let st = stmt else { return -1 }
        let result = sqlite3_step(st)
        if result != SQLITE_ROW {
            if errorLogsEnabled {
                print("file \(#file) line \(#line) function \(#function) sqlite query failed \(result) \(String(cString: (sqlite3_errmsg(db) ?? UnsafePointer<Int8>(bitPattern: 0)!))).")
            }
            return -1
        }
        return Int(sqlite3_column_int(st, 0))
    }

    // MARK: - File

    private func writeData(_ data: Data, fileName: String) -> Bool {
        let path = (dataPath as NSString).appendingPathComponent(fileName)
        let suc = (data as NSData).write(to: URL(fileURLWithPath: path), atomically: false)
        return suc
    }

    private func readData(fromFileName name: String) -> Data? {
        let path = (dataPath as NSString).appendingPathComponent(name)
        let data = try? Data(contentsOf: URL(fileURLWithPath: path), options: .mappedIfSafe)
        return data
    }

    private func deleteFile(withName name: String) -> Bool {
        let path = (dataPath as NSString).appendingPathComponent(name)
        do {
            try FileManager.default.removeItem(atPath: path)
        } catch {
            return false
        }
        return true
    }

    private func moveAllFileToTrash() -> Bool {
        let uuidref = CFUUIDCreate(kCFAllocatorDefault)
        let uuid = CFUUIDCreateString(kCFAllocatorDefault, uuidref)
        let tmpPath = (trashPath as NSString).appendingPathComponent(uuid! as String)
        do {
            try FileManager.default.moveItem(atPath: dataPath, toPath: tmpPath)
            try FileManager.default.createDirectory(atPath: dataPath, withIntermediateDirectories: true, attributes: nil)
        } catch {
            return false
        }
        return true
    }

    private func fileEmptyTrashInBackground() {
        let path = trashPath
        trashQueue.async {
            let manager = FileManager()
            let directoryContents = try? manager.contentsOfDirectory(atPath: path)
            guard let list = directoryContents else { return }
            for subPath in list {
                let fullPath = (path as NSString).appendingPathComponent(subPath)
                try? manager.removeItem(atPath: fullPath)
            }
        }
    }

    // MARK: - public operation functions

    func saveItem(_ item: BYKVStorageItem) -> Bool {
        guard let value = item.value else { return false }
        return saveItem(withKey: item.key, value: value, fileName: item.fileName, extendedData: item.extendedData)
    }

    func saveItem(withKey key: String, value: Data, fileName: String? = nil, extendedData: Data? = nil) -> Bool {
        guard !key.isEmpty && !value.isEmpty else { return false }
        if type == .File, fileName == nil || fileName!.isEmpty { return false }

        if fileName != nil && !fileName!.isEmpty {
            if !writeData(value, fileName: fileName!) { return false }
            if !dbSave(withKey: key, value: value, fileName: fileName!, extendedData: extendedData) {
                _ = deleteFile(withName: fileName!)
                return false
            }
            return true
        }
        if type != .SQL {
            let filename = dbGetFileName(withKey: key)
            if filename != nil {
                _ = deleteFile(withName: filename!)
            }
        }

        return dbSave(withKey: key, value: value, fileName: "", extendedData: extendedData)
    }

    func removeItem(withKey key: String) -> Bool {
        if key.isEmpty { return false }
        switch type {
        case .SQL:
            return dbDeleteItem(withKey: key)
        case .File, .Mix:
            let fileName = dbGetFileName(withKey: key)
            if fileName != nil {
                _ = deleteFile(withName: fileName!)
            }
            return dbDeleteItem(withKey: key)
        }
    }

    func removeItems(withKeys keys: [String]) -> Bool {
        if keys.isEmpty { return false }
        switch type {
        case .SQL:
            return dbDeleteItems(withKeys: keys)
        case .File, .Mix:
            let filenames = dbGetFileNames(withKeys: keys)
            for name in filenames {
                _ = deleteFile(withName: name)
            }
            return dbDeleteItems(withKeys: keys)
        }
    }

    func removeItemsLargerThanSize(_ size: Int) -> Bool {
        if size == Int.max { return true }
        if size <= 0 { return removeAllItems() }
        switch type {
        case .SQL:
            if dbDeleteItemsLargerThan(size: size) {
                dbCheckPoint()
                return true
            }
        case .File, .Mix:
            let filenames = dbGetFileNames(largerThanSize: size)
            for name in filenames {
                _ = deleteFile(withName: name)
            }
            if dbDeleteItemsLargerThan(size: size) {
                dbCheckPoint()
                return true
            }
        }
        return false
    }

    func removeItemsEarlier(_ time: Int) -> Bool {
        if time <= 0 { return true }
        if time == Int.max { return removeAllItems() }
        switch type {
        case .SQL:
            if dbDeleteItemsEarlierThan(time: time) {
                dbCheckPoint()
                return true
            }
        case .File, .Mix:
            let filenames = dbGetFileNamesEarlierThan(time: time)
            for name in filenames {
                _ = deleteFile(withName: name)
            }
            if dbDeleteItemsEarlierThan(time: time) {
                dbCheckPoint()
                return true
            }
        }
        return false
    }

    func removeItemsToFitSize(_ size: Int) -> Bool {
        if size == Int.max { return true }
        if size <= 0 { return removeAllItems() }
        var total = dbGetTotalItemSize()
        if total < 0 { return false }
        if total <= size { return true }

        var items: [BYKVStorageItem]?
        var suc = false
        repeat {
            let perCount = 16
            items = dbGetItemSizeInfoOrderByTimeAsc(withLimit: perCount)
            guard let list = items else { break }
            for item in list {
                if total <= size { break }
                if let filename = item.fileName {
                    _ = deleteFile(withName: filename)
                }
                suc = dbDeleteItem(withKey: item.key)
                total -= item.size
                if !suc { break }
            }
        } while total > size && items != nil && items!.count > 0 && suc
        if suc { dbCheckPoint() }
        return suc
    }

    func removeItemsToFitCount(_ count: Int) -> Bool {
        if count == Int.max { return true }
        if count <= 0 { return removeAllItems() }
        var total = dbGetTotalItemCount()
        if total < 0 { return false }
        if total <= count { return true }

        var items: [BYKVStorageItem]?
        var suc = false
        repeat {
            let perCount = 16
            items = dbGetItemSizeInfoOrderByTimeAsc(withLimit: perCount)
            guard let list = items else { break }
            for item in list {
                if total <= count { break }
                if let filename = item.fileName {
                    _ = deleteFile(withName: filename)
                }
                suc = dbDeleteItem(withKey: item.key)
                total -= 1
                if !suc { break }
            }
        } while total > count && items != nil && items!.count > 0 && suc
        if suc { dbCheckPoint() }
        return suc
    }

    func removeAllItems() -> Bool {
        if !dbClose() { return false }
        reset()
        if !dbOpen() { return false }
        if !dbInitialize() { return false }
        return true
    }

    func removeAllItems(withProgress progress: (_ removedCount: Int, _ totalCount: Int) -> Void, completion: (_ success: Bool) -> Void) {
        let total = dbGetTotalItemCount()
        if total <= 0 {
            completion(total < 0)
            return
        }

        var left = total
        let percount = 32
        var items: [BYKVStorageItem]?
        var suc = false
        repeat {
            items = dbGetItemSizeInfoOrderByTimeAsc(withLimit: percount)
            guard let list = items else { break }
            for item in list {
                if left <= 0 { break }
                if let filename = item.fileName {
                    _ = deleteFile(withName: filename)
                }
                suc = dbDeleteItem(withKey: item.key)
                left -= 1
                if !suc { break }
            }
            progress(total - left, total)

        } while left > 0 && items != nil && items!.count > 0 && suc
        if suc { _ = dbCheckPoint() }
        completion(!suc)
    }

    func item(forKey key: String) -> BYKVStorageItem? {
        guard !key.isEmpty else { return nil }
        let item = dbGetItem(withKey: key, excludeInlineData: false)
        guard let it = item else { return nil }
        
        _ = dbUpdateAccessTime(withKey: key)
        if let filename = it.fileName, !filename.isEmpty {
            it.value = readData(fromFileName: filename)
            if it.value == nil {
                _ = dbDeleteItem(withKey: key)
                return nil
            }
        }
        return it
    }

    func itemInfo(forKey key: String) -> BYKVStorageItem? {
        if key.isEmpty { return nil }
        let item = dbGetItem(withKey: key, excludeInlineData: true)
        return item
    }

    func itemValue(forKey key: String) -> Data? {
        if key.isEmpty { return nil }
        var value: Data?
        switch type {
        case .File:
            let filename = dbGetFileName(withKey: key)
            if let fn = filename, !fn.isEmpty {
                value = readData(fromFileName: fn)
                if value == nil {
                    _ = dbDeleteItem(withKey: key)
                }
            }
        case .SQL:
            value = dbGetValue(withKey: key)
        case .Mix:
            let filename = dbGetFileName(withKey: key)
            if let fn = filename, !fn.isEmpty {
                value = readData(fromFileName: fn)
                if value == nil {
                    _ = dbDeleteItem(withKey: key)
                }
            } else {
                value = dbGetValue(withKey: key)
            }
        }

        if let _ = value {
            _ = dbUpdateAccessTime(withKey: key)
        }

        return value
    }

    func items(forKeys keys: [String]) -> [BYKVStorageItem]? {
        if keys.isEmpty { return nil }
        var items = dbGetItems(withKeys: keys, excludeInlineData: false)
        var contains = [Int]()
        if type != .SQL, let list = items {
            for i in 0 ..< list.count {
                let item = list[i]
                guard let fileName = item.fileName, !fileName.isEmpty else { continue }
                item.value = readData(fromFileName: fileName)
                if item.value == nil {
                    _ = dbDeleteItem(withKey: item.key)
                } else {
                    contains.append(i)
                }
            }
            var tmp = [BYKVStorageItem]()
            for i in contains {
                tmp.append(list[i])
            }
            items = tmp
        }
        if items != nil, items!.count > 0 {
            _ = dbUpdateAccessTime(withKeys: keys)
        }
        return items
    }

    func itemInfos(forKeys keys: [String]) -> [BYKVStorageItem]? {
        if keys.isEmpty { return nil }
        return dbGetItems(withKeys: keys, excludeInlineData: true)
    }

    func itemValues(forKeys keys: [String]) -> [String: Data]? {
        let items = self.items(forKeys: keys)
        guard let list = items else { return nil }
        var res = [String: Data]()
        for item in list {
            if let value = item.value {
                res[item.key] = value
            }
        }
        return res.isEmpty ? nil : res
    }

    func itemExist(forKey key: String) -> Bool {
        if key.isEmpty { return false }
        return dbGetItemCount(withKey: key) > 0
    }

    func itemsCount() -> Int {
        return dbGetTotalItemCount()
    }

    func itemsSize() -> Int {
        return dbGetTotalItemSize()
    }
}
