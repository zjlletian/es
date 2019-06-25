package es

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	"gopkg.in/olivere/elastic.v6"
)

// es index
type Index struct {
	indexName  []string
	structType reflect.Type
	client     *EsClient
	docType    string
	fields     *elastic.FetchSourceContext
	esFields   map[string]string
}

// 不允许外部调用，仅允许 client.Index(xxx, xxx) 返回
func newIndex(indexName string, typ interface{}, client *EsClient) *Index {
	// typ 类型必须是 struct或struct的指针
	t := reflect.TypeOf(typ)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		panic("index type must be GO struct")
	}
	// 设置es的要返回的字段，减少数据传输开销
	numField := t.NumField()
	sc := elastic.NewFetchSourceContext(false)
	esFields := map[string]string{}
	if t.NumField() > 0 {
		fields := []string{}
		for i := 0; i < numField; i++ {
			f := t.Field(i)
			if f.Anonymous {
				continue
			}
			// 解析要返回的_source字段
			jsonTag := f.Tag.Get("json")
			if jsonTag == "" {
				jsonTag = f.Name
			} else {
				tag := strings.Split(jsonTag, ",")[0]
				if tag != "-" {
					fields = append(fields, strings.Trim(tag, " "))
				}
			}
			// es内置字段
			switch f.Tag.Get("es") {
			case "_index":
				if f.Type.Kind() == reflect.String {
					esFields["_index"] = f.Name
				}
			case "_id":
				if f.Type.Kind() == reflect.String {
					esFields["_id"] = f.Name
				}
			case "_score":
				if f.Type.Kind() == reflect.Float64 {
					esFields["_score"] = f.Name
				}
			case "_type":
				if f.Type.Kind() == reflect.String {
					esFields["_type"] = f.Name
				}
			case "_version":
				if f.Type.Kind() == reflect.Int64 {
					esFields["_version"] = f.Name
				}
			}
		}
		if len(fields) > 0 {
			sc.SetFetchSource(true)
			sc.Include(fields...)
		}
	}
	indexNames := []string{}
	for _, n := range strings.Split(indexName, ",") {
		indexNames = append(indexNames, strings.Trim(n, " "))
	}
	// 返回index
	return &Index{
		indexName:  indexNames,
		client:     client,
		structType: t,
		docType:    "_doc",
		fields:     sc,
		esFields:   esFields,
	}
}

// 设置type类型，默认为 _doc, 以兼容es6.xx 和以后版本，如果自定义成其他的 type可能在以后版本中有兼容性问题。
func (this *Index) SetDocType(docType string) {
	this.docType = docType
}

// 获取对应index的query
func (this *Index) Query() *Query {
	return newQuery(this)
}

// 保存文档
func (this *Index) Save(id string, doc interface{}) (err error) {
	client := this.client.rawClient
	ctx := context.Background()
	_, err = client.Index().Index(this.indexName[0]).Type(this.docType).Id(id).BodyJson(doc).Do(ctx)
	return
}

// 获取文档
func (this *Index) Find(id string) (doc interface{}, err error) {
	client := this.client.rawClient
	ctx := context.Background()
	res, e := client.Get().Index(this.indexName[0]).Type(this.docType).Id(id).Do(ctx)
	if e != nil && e.Error() == "elastic: Error 404 (Not Found)" {
		return nil, nil
	} else if e != nil {
		return nil, err
	}
	return this.source2Struct(res.Source, res.Id, this.indexName[0], res.Type, res.Version, nil)
}

// 更新文档
func (this *Index) Update(id string, updates map[string]interface{}) (err error) {
	client := this.client.rawClient
	ctx := context.Background()
	_, err = client.Update().Index(this.indexName[0]).Type(this.docType).Id(id).Doc(updates).Do(ctx)
	return
}

// 删除文档
func (this *Index) Delete(id string) (err error) {
	client := this.client.rawClient
	ctx := context.Background()
	_, err = client.Delete().Index(this.indexName[0]).Type(this.docType).Id(id).Do(ctx)
	return
}

// 将二进制解析成对应结构
func (this *Index) source2Struct(source *json.RawMessage, id string, indexName string, typ string, version *int64, score *float64) (interface{}, error) {
	v := reflect.New(this.structType).Elem()
	if source != nil {
		if err := json.Unmarshal(*source, v.Addr().Interface()); err != nil {
			return nil, err
		}
	}
	if this.esFields["_id"] != "" {
		v.FieldByName(this.esFields["_id"]).SetString(id)
	}
	if this.esFields["_index"] != "" {
		v.FieldByName(this.esFields["_index"]).SetString(indexName)
	}
	if this.esFields["_type"] != "" {
		v.FieldByName(this.esFields["_type"]).SetString(typ)
	}
	if this.esFields["_version"] != "" && version != nil {
		v.FieldByName(this.esFields["_version"]).SetInt(*version)
	}
	if this.esFields["_score"] != "" && score != nil {
		v.FieldByName(this.esFields["_score"]).SetFloat(*score)
	}
	return v.Interface(), nil
}
