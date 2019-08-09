//
//  Model.swift
//  BYCache
//
//  Created by Beryter on 2019/8/8.
//  Copyright © 2019 Beryter. All rights reserved.
//

import Foundation

enum Sex: String, Codable {
    case unknow
    case female
    case male
}

class Model: Codable  {
    var name: String?
    var age = 0
    var addr: String?
    var sex = Sex.male
}
extension Model: CustomDebugStringConvertible {
    var debugDescription: String {
        return """
        "name": \(name ?? ""),
        "age": \(age),
        "addr": \(addr ?? ""),
        "sex": \(sex.rawValue)
        """
    }
}
