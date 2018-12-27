package com.entry;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: lsp
 * @Date: 2018/12/25 19:35
 * @Description:
 */
@Data
@AllArgsConstructor //生成全参的构造方法
@NoArgsConstructor  //生成无参的构造方法
public class Product {
    private String name;
    private String author;
    private String version;

    public Product(String name) {
        this.name = name;
    }

    public Product(String author, String version) {
        this.author = author;
        this.version = version;
    }
}
