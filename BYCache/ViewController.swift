//
//  ViewController.swift
//  BYCache
//
//  Created by Beryter on 2019/7/31.
//  Copyright © 2019 Beryter. All rights reserved.
//

import UIKit

class ViewController: UIViewController {
  
    override func viewDidLoad() {
        super.viewDidLoad()
        view.backgroundColor = .white
        test()
    }
    
    func test() {
        let paths = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)
        let path = paths[0] + "/" + "test"
        let cache = BYDiskCache.cache(path: path)
        guard let ch = cache else {
            print("缓存创建失败")
            return
        }
        
        let model = Model()
        model.name = "张三"
        model.age = 18
        model.addr = "北京市朝阳区望京街10号"
        model.sex = .female
        let key = "\(NSDate().timeIntervalSince1970)"
        ch.setObject(model, extendedData: nil, forKey: key)
        print("设置缓存")
        let ob: (Model?, Data?) = ch.object(forKey: key)
        print("获取缓存")
        if let m = ob.0 {
            print("数据取出来了\(m.debugDescription)")
        }
    }
   
}


