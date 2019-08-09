//
//  BYDiskCache.swift
//  BYCache
//
//  Created by Beryter on 2019/7/31.
//  Copyright © 2019 Beryter. All rights reserved.
//

import CommonCrypto
import Foundation
import UIKit

class BYDiskCacheKey {
    let value: String
    init(_ value: String) {
        self.value = value
    }
}

class BYDiskCache {
    static var globalInstances = NSMapTable<BYDiskCacheKey, BYDiskCache>(keyOptions: .strongMemory, valueOptions: .weakMemory)
    static var globalInstancesLock = DispatchSemaphore(value: 1)

    /// 缓存的名称，默认为nil
    var name: String?
    /// 缓存的路径，只读
    private(set) var path: String = ""
    /// 设定缓存方式界定值，操过这个值将采用文件存储，否则会采用数据库存储。单位为bytes。
    private(set) var inlineThreshold: Int = 20480

    var customArchiveBlock: ((Encodable) -> Data)?
    var customUnarchiveBlock: ((Data) -> Decodable)?
    var customFileNameBlock: ((String) -> String)?

    /// 缓存对象个数的最大值，默认为Int.max
    var countLimit = Int.max
    /// 缓存容量的最大值，默认为Int.max
    var costLimit = Int.max
    /// 设置过期时间，
    var ageLimit = TimeInterval.greatestFiniteMagnitude
    /// 需要的最小磁盘剩余空间
    var freeDiskSpaceLimit: Int = 0
    /// 自动检查缓存状态频率，默认60s，1分钟。不符合缓存规则的对象将被移除
    var autoTrimInterval: TimeInterval = 60
    /// 开启调试日志
    var errorLogsEnabled = true

    private var kv: BYKVStorage?
    private var lock = DispatchSemaphore(value: 1)
    private var queue = DispatchQueue(label: "com.beryter.cache.disk", attributes: [.concurrent])

    class func cache(path: String, inlineThreshold: Int? = nil) -> BYDiskCache? {
        let cc = globalCache(withPath: path)
        if cc != nil { return cc! }
        let res = BYDiskCache(path: path, inlineThreshold: inlineThreshold)
        res?.trimRecursively()
        return res
    }

    private init?(path: String, inlineThreshold: Int?) {
        self.path = path
        self.inlineThreshold = inlineThreshold ?? 1024 * 20
        var type = BYKVStorageType.File
        if 0 == self.inlineThreshold {
            type = .File
        } else if Int.max == self.inlineThreshold {
            type = .SQL
        } else {
            type = .Mix
        }
        let kv = BYKVStorage(path: path, type: type)
        if kv == nil { return nil }
        self.kv = kv!
        self.path = path
        NotificationCenter.default.addObserver(self, selector: #selector(appWillBeTerminated), name: UIApplication.willTerminateNotification, object: nil)
    }

    func containsObject(forKey key: String) -> Bool {
        if key.isEmpty { return false }
        guard let kv = kv else { return false }
        _ = lock.wait(timeout: DispatchTime.distantFuture)
        let con = kv.itemExist(forKey: key)
        _ = lock.signal()
        return con
    }

    func containsObject(forKey key: String, completion: @escaping (_ key: String, _ constains: Bool) -> Void) {
        queue.async { [weak self] in
            guard let self = self else { return }
            let con = self.containsObject(forKey: key)
            completion(key, con)
        }
    }

    func object<T: Decodable>(forKey key: String) -> (object: T?, extendedData: Data?) {
        if key.isEmpty { return (nil, nil) }
        _ = lock.wait(timeout: DispatchTime.distantFuture)
        let item = kv?.item(forKey: key)
        _ = lock.signal()
        guard let it = item else { return (nil, nil) }
        guard let value = it.value else { return (nil, nil) }
        var object: T?
        if let bl = customUnarchiveBlock {
            object = bl(value) as? T
        } else {
            object = try? JSONDecoder().decode(T.self, from: value)
        }
        guard let ob = object else { return (nil, nil) }
        return (ob, it.extendedData)
    }

    func object<T: Decodable>(forKey key: String, completion: @escaping (_ key: String, _ object: T?, _ extendedData: Data?) -> Void) {
        queue.async { [weak self] in
            guard let self = self else { return }
            let res: (T?, Data?) = self.object(forKey: key)
            completion(key, res.0, res.1)
        }
    }

    func setObject<T: Encodable>(_ object: T?, extendedData: Data?, forKey key: String) {
        if key.isEmpty { return }
        guard let ob = object else {
            removeObject(forKey: key)
            return
        }
        var value: Data?
        if let bl = customArchiveBlock {
            value = bl(ob)
        } else {
            value = try? JSONEncoder().encode(ob)
        }

        guard let vl = value else { return }

        var filename: String?
        if kv!.type != .SQL, vl.count > inlineThreshold {
            filename = fileNameForKey(key)
        }
        _ = lock.wait(timeout: DispatchTime.distantFuture)
        _ = kv?.saveItem(withKey: key, value: vl, fileName: filename, extendedData: extendedData)
        _ = lock.signal()
    }

    func setObject<T: Encodable>(_ object: T?, extendedData: Data?, forKey key: String, completion: @escaping () -> Void) {
        queue.async { [weak self] in
            guard let self = self else { return }
            self.setObject(object, extendedData: extendedData, forKey: key)
            completion()
        }
    }

    func removeObject(forKey key: String) {
        if key.isEmpty { return }
        _ = lock.wait(timeout: DispatchTime.distantFuture)
        _ = kv?.removeItem(withKey: key)
        _ = lock.signal()
    }

    func removeObject(forKey key: String, completion: @escaping (_ key: String) -> Void) {
        queue.async { [weak self] in
            guard let self = self else { return }
            self.removeObject(forKey: key)
            completion(key)
        }
    }

    func removeAllObjects() {
        _ = lock.wait(timeout: DispatchTime.distantFuture)
        _ = kv?.removeAllItems()
        _ = lock.signal()
    }

    func removeAllObjects(_ completion: @escaping () -> Void) {
        queue.async { [weak self] in
            guard let self = self else { return }
            self.removeAllObjects()
            completion()
        }
    }

    func totalCount() -> Int {
        _ = lock.wait(timeout: DispatchTime.distantFuture)
        let count = kv!.itemsCount()
        _ = lock.signal()
        return count
    }

    func totalCount(_ completion: @escaping (Int) -> Void) {
        queue.async { [weak self] in
            guard let self = self else { return }
            let count = self.totalCount()
            completion(count)
        }
    }

    func totalCost() -> Int {
        _ = lock.wait(timeout: DispatchTime.distantFuture)
        let cost = kv!.itemsSize()
        _ = lock.signal()
        return cost
    }

    func totalCost(_ completion: @escaping (Int) -> Void) {
        queue.async { [weak self] in
            guard let self = self else { return }
            let count = self.totalCost()
            completion(count)
        }
    }

    func trimToCount(_ count: Int) {
        _ = lock.wait(timeout: DispatchTime.distantFuture)
        p_trimToCount(count)
        _ = lock.signal()
    }

    func trimToCount(_ count: Int, completion: @escaping () -> Void) {
        queue.async { [weak self] in
            guard let self = self else { return }
            self.trimToCount(count)
            completion()
        }
    }

    func trimToCost(_ cost: Int) {
        _ = lock.wait(timeout: DispatchTime.distantFuture)
        p_trimToCost(cost)
        _ = lock.signal()
    }

    func trimToCost(_ cost: Int, completion: @escaping () -> Void) {
        queue.async { [weak self] in
            guard let self = self else { return }
            self.trimToCost(cost)
            completion()
        }
    }

    func trimToAge(_ age: TimeInterval) {
        _ = lock.wait(timeout: DispatchTime.distantFuture)
        p_trimToAge(age)
        _ = lock.signal()
    }

    func trimToAge(_ age: TimeInterval, completion: @escaping () -> Void) {
        queue.async { [weak self] in
            guard let self = self else { return }
            self.trimToAge(age)
            completion()
        }
    }

    /// 磁盘剩余空间
    class func freeDiskSize() -> Int {
        let attrs = try? FileManager.default.attributesOfFileSystem(forPath: NSHomeDirectory())
        guard let ats = attrs else { return -1 }
        let space = ats[.systemFreeSize] as? Int
        guard let free = space else { return -1 }
        return max(-1, free)
    }

    class func globalCache(withPath path: String) -> BYDiskCache? {
        if path.isEmpty { return nil }
        _ = BYDiskCache.globalInstancesLock.wait(timeout: DispatchTime.distantFuture)
        let cache = BYDiskCache.globalInstances.object(forKey: BYDiskCacheKey(path))
        _ = BYDiskCache.globalInstancesLock.signal()
        return cache
    }

    class func addGlobalCache(_ cache: BYDiskCache) {
        if cache.path.isEmpty { return }
        _ = BYDiskCache.globalInstancesLock.wait(timeout: DispatchTime.distantFuture)
        BYDiskCache.globalInstances.setObject(cache, forKey: BYDiskCacheKey(cache.path))
        _ = BYDiskCache.globalInstancesLock.signal()
    }

    @objc private func appWillBeTerminated() {
        _ = lock.wait(timeout: DispatchTime.distantFuture)
        kv = nil
        _ = lock.signal()
    }

    private func trimRecursively() {
        DispatchQueue.main.asyncAfter(deadline: .now() + autoTrimInterval) {
            DispatchQueue.global().async(group: nil, qos: .utility, flags: .noQoS) {
                self.trimInBackground()
                self.trimRecursively()
            }
        }
    }

    private func trimInBackground() {
        queue.async { [weak self] in
            guard let self = self else { return }
            _ = self.lock.wait(timeout: DispatchTime.distantFuture)
            self.p_trimToCost(self.costLimit)
            self.p_trimToCount(self.countLimit)
            self.p_trimToAge(self.ageLimit)
            self.trimToFreeDiskSpace(self.freeDiskSpaceLimit)
            _ = self.lock.signal()
        }
    }

    private func p_trimToCost(_ limit: Int) {
        if limit >= Int.max { return }
        _ = kv?.removeItemsToFitSize(limit)
    }

    private func p_trimToCount(_ limit: Int) {
        if limit >= Int.max { return }
        _ = kv?.removeItemsToFitCount(limit)
    }

    private func p_trimToAge(_ limit: TimeInterval) {
        if limit <= 0 {
            _ = kv?.removeAllItems()
            return
        }
        let timestamp = Double(time(nil))
        if timestamp <= limit { return }
        let age = timestamp - limit
        if age >= Double.greatestFiniteMagnitude { return }
        _ = kv?.removeItemsEarlier(Int(age))
    }

    private func trimToFreeDiskSpace(_ limit: Int) {
        if limit == 0 { return }
        let totalBytes = kv?.itemsSize() ?? 0
        if totalBytes <= 0 { return }
        let diskFreeBytes = BYDiskCache.freeDiskSize()
        if diskFreeBytes < 0 { return }
        let needTrimBytes = limit - diskFreeBytes
        if needTrimBytes <= 0 { return }
        var costLimit = totalBytes - needTrimBytes
        costLimit = max(0, costLimit)
        p_trimToCost(costLimit)
    }

    /// 字符串md5
    private func stringMD5(_ str: String) -> String {
        let tmp = str.cString(using: .utf8)
        let strlen = CC_LONG(str.lengthOfBytes(using: .utf8))
        let digestLen = Int(CC_MD5_DIGEST_LENGTH)
        let result = UnsafeMutablePointer<CUnsignedChar>.allocate(capacity: digestLen)
        CC_MD5(tmp, strlen, result)
        var hash = ""
        for i in 0 ..< digestLen {
            hash = hash.appendingFormat("%02x", result[i])
        }
        free(result)
        return hash
    }

    private func fileNameForKey(_ key: String) -> String {
        var fileName: String?
        if let block = customFileNameBlock {
            fileName = block(key)
        }
        if fileName == nil {
            fileName = stringMD5(key)
        }
        return fileName!
    }
}

extension BYDiskCache: CustomDebugStringConvertible {
    var debugDescription: String {
        let cn = String(describing: BYDiskCache.self)
        if let n = name {
            return "\(cn): (\(n), \(path))"
        } else {
            return "\(cn): (\(path))"
        }
    }
}
