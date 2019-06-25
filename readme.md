# es sdk

基于 gopkg.in/olivere/elastic.v6 ，在此之上封装了一些快捷功能。

## 1. 创建客户端

引入es sdk
```go
import "github.com/zjlletian/es"
```

如果有多个节点使用`,`分隔
```go
host := "http://node-1:9200,http://node-2:9200"
esClient, err := es.NewEsClient(host)
if err != nil {
    handel error ....
}
```

## 2. index相关

#### 2.1 判断index是否存在
判断名字为 pay_order 的index 是否存在。
```go
exist, err := esClient.IsIndexExist("pay_order")
```

#### 2.2 创建index
创建一个名为 pay_order 的index。
```go
err := esClient.CreateIndex("pay_order") 
```

创建index时可以指定mapping。
```go
mapping := `{
  "settings":{
  "number_of_shards":1,
    "number_of_replicas":0
  },
  "mappings":{
    "tweet":{
      "properties":{
        "tags":{
          "type":"string"
        },
        "location":{
          "type":"geo_point"
        }
      }
    }
  }
}`
err := esClient.CreateIndex("pay_order", es.Mapping(mapping))
```

#### 2.3 删除index
删除一个index
```go
err := esClient.DeleteIndex("pay_order") 
```
如果删除多个index，用逗号隔开
```go
err := esClient.DeleteIndex("pay_order_1, pay_order_2") 
```

#### 2.4 获取index
使用json tag指定要返回的_source字段。如果需要返回_source外的参数，可以使用es tag可以指定。如果不使用可以不指定。
* _id : 文档id, 对应字段类型必须为 string
* _index : 文档index, 对应字段类型必须为 string
* _type : 文档type, 对应字段类型必须为 string
* _version : 查询结果的版本, 对应字段类型必须为 int64, 如果es返回的_version为null, 值则为0
* _score : 查询结果的分数, 对应字段类型必须为 float64, 如果es返回的_score为null, 值则为0

```go
// 自定义pay_order的数据结构
type Order struct {
	Userid    int64   `json:"userid"`
	Status    string  `json:"status"`
	PayTime   string  `json:"pay_time"`
	Subject   string  `json:"subject"`
	Count     int64   `json:"count"`
	OrderId   string  `json:"order_id"`
	EsId      string  `es:"_id" json:"-"`
	EsIndex   string  `es:"_index" json:"-"`
	EsType    string  `es:"_type" json:"-"`
	EsScore   float64 `es:"_score" json:"-"`
	EsVersion int64   `es:"_version" json:"-"`
}

// 这里需要指定index名称和index对应的类型，如果类型不是struct或struct的指针, 将会panic
orderIndex := esClient.Index("pay_order", Order{}) 
```

如果指定要查询多个index, 可以用逗号分隔或 *匹配, 但是要注意：多index查询只对index.Query()方法返回的结果生效, 如果用于直接操作文档则只对index列表中的第一个index生效。
```go
orderIndex := esClient.Index("pay_order1,pay_order2", Order{}) 
```

## 3. 文档操作

#### 3.1 关于type
es6 开始逐步移除type, 为了兼容es6.xx 和以后版本, sdk中type默认为_doc, 如果自定义成其他的type可能在以后版本中有兼容性问题。
```go
// 使用 SetDocType 方法定义为其他type
orderIndex.SetDocType("xxx")
```

#### 3.2 插入文档
```go
newOrder := Order{
    Userid:  1231231,
    Status:  "finish",
    PayTime: "2019-01-01T00:00:00+0800",
    Subject: "超级会员",
    Count:   31,
    OrderId: "1",
}
orderIndex.Save(newOrder.OrderId, newOrder)
```

#### 3.3 获取文档
```go
res, err := orderIndex.Find("1")
fmt.Println(res, err)

order := res.(Order)
fmt.Println(order.Userid)
fmt.Println(order.Status)
fmt.Println(order.PayTime)
```

#### 3.4 更新文档
```go
err = orderIndex.Update("1", map[string]interface{}{
    "count":  365,
})
```

#### 3.5 删除文档
```go
orderIndex.Delete("1")
```

## 4. 数据查询
#### 4.1 分页查询
根据分页查询结果，默认的分页从1开始，每页100项, 返回对应页的数据和所有数据的总数。
```go
query := orderIndex.Query().
    Term("userid", 238766003).   // userid = 238766003
    Term("status", "finish").    // status = finish
    Range("count", es.Gt(30)).   // count > 30
    Range("pay_time",
        es.Gte("2019-01-01T00:00:00+0800"), // pay_time >= "2019-01-01T00:00:00+0800" 
        es.Lte("2019-12-30T00:00:00+0800"), // pay_time <= "2019-12-30T00:00:00+0800"
    ).
    ShouldMatchPhrase("subject", "超级会员"). // 包含中文，所以需要 MatchPhrase
    ShouldMatchPhrase("subject", "普通会员").
    MinimumShouldMatch(1).                  // should条件最小满足数量，默认1
    OrderBy("finish_time", -1).             // 根据 finish_time 降序排序
    Page(1, 10)                             // 分页，获取第一页，每页10项

// 获取分页数据
if res, total, err:= query.GetList(); err!= nil {
    handel error ....
} else {
    fmt.Printf("total count: %d\n", total)  // 所有满足条件的数据总数
    for _, item := range res {
      fmt.Println(item.(Order).OrderId)    // 请求到的指定分页的数据
    }      
}
```

Search()方法可以获取原生的 *elastic.SearchResult, 以获取其他详细结果，如高亮，分数。
ps: 指定 size 和 from，会忽略 query.Page() 指定的页码和页面大小。
```go
// Search(size int, from int)
r, err := query.Search(10, 0)
```

以上查询转换成http请求格式如下
```
POST http://xxxxxx:xxx/pay_order/_search
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "userid":238766003}},
        { "term": { "status": "finish" }},
        { "range": { "count": { "gt": 30 }}},
        { "range": { "pay_time": { 
            "gte": "2019-01-01T00:00:00+0800", 
            "lte": "2019-12-30T00:00:00+0800"
          }
        }}
      ],
      "should":[
        { "match_phrase": { "subject": "超级会员" }},
        { "match_phrase": { "subject": "普通会员" }}
      ],
      "minimum_should_match":1
    }
  },
  "size": 10,
  "from": 0,
  "sort": [
    {
      "finish_time": {
        "order": "desc"
      }
    }
  ]
}
```

#### 4.2 根据scroll查询结果
如果需要查询的结果数量很多，如导入导出操作，推荐使用scroll来提升性能。
ps: scroll 查询会忽略 query.Page() 指定的页码和页面大小，请指定ScrollSize。
```go
/*
  ScrollSize(scrollSize int), 设置每次获取的batch数量，默认为1000
  ScrollAlive(alive string), 设置scroll生存时间, 默认5分钟. 每次请求会重新刷新
*/

scroll, err := query.ScrollSize(1000).ScrollAlive("1m").GetScroll()
if err != nil {
    handrl error // 处理错误
}

fmt.Printf("total count: %d\n", scroll.Total) // 获取所有数据总数

for {
    item, err := scroll.Next()
    if err != nil {
        handrl error // 处理错误
    }
    if item == nil {
        fmt.Println("done scan") // 返回nil, 说明遍历结束
        break
    }
    fmt.Println(item.(Order).OrderId) // 处理结果
}
```
