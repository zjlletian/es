package es

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/olivere/elastic.v6"
)

// es客户端，在原始客户端上封装一些其他功能
type EsClient struct {
	rawClient *elastic.Client
}

// 创建es客户端
func NewEsClient(host string) (client *EsClient, err error) {
	hosts := []string{}
	for _, h := range strings.Split(host, ",") {
		hosts = append(hosts, strings.Trim(h, " "))
	}
	rawClient, err := elastic.NewClient(
		elastic.SetURL(hosts...),
		elastic.SetSniff(false),
		elastic.SetHealthcheckInterval(10*time.Second),
		elastic.SetGzip(true),
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
		elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)))
	if err != nil {
		return
	}
	client = &EsClient{
		rawClient: rawClient,
	}
	return
}

// 获取原始客户端连接
func (this *EsClient) GetRawClient() *elastic.Client {
	return this.rawClient
}

// 创建index的选项
type CreateIndexOption func(*elastic.IndicesCreateService)

// 设置index的mapping
func Mapping(mapping string) CreateIndexOption {
	return func(cs *elastic.IndicesCreateService) {
		cs.BodyString(mapping)
	}
}

// 创建index
func (this *EsClient) CreateIndex(indexName string, options ...CreateIndexOption) (err error) {
	ctx := context.Background()
	cs := this.rawClient.CreateIndex(indexName)
	for _, option := range options {
		option(cs)
	}
	_, e := cs.Do(ctx)
	if e != nil {
		return e
	}
	return
}

// 删除index
func (this *EsClient) DeleteIndex(indexName string) (err error) {
	ctx := context.Background()
	_, err = this.rawClient.DeleteIndex(strings.Split(indexName, ",")...).Do(ctx)
	return
}

// 判断index是否存在
func (this *EsClient) IsIndexExist(indexName string) (exist bool, err error) {
	ctx := context.Background()
	return this.rawClient.IndexExists(indexName).Do(ctx)
}

// 获取index
func (this *EsClient) Index(indexName string, typ interface{}) *Index {
	return newIndex(indexName, typ, this)
}
