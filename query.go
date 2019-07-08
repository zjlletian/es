package es

import (
	"context"
	"encoding/json"
	"errors"

	"gopkg.in/olivere/elastic.v6"
)

// range 选项
type RangeOption func(r *elastic.RangeQuery)

//range gt
func Gt(value interface{}) RangeOption {
	return func(r *elastic.RangeQuery) {
		r.Gt(value)
	}
}

//range gte
func Gte(value interface{}) RangeOption {
	return func(r *elastic.RangeQuery) {
		r.Gte(value)
	}
}

//range lt
func Lt(value interface{}) RangeOption {
	return func(r *elastic.RangeQuery) {
		r.Lt(value)
	}
}

//range lte
func Lte(value interface{}) RangeOption {
	return func(r *elastic.RangeQuery) {
		r.Lte(value)
	}
}

// Scroll
type Scroll struct {
	scrollId string
	item     chan interface{}
	Total    int64
	err      error
}

// Get next item from scroll
func (s *Scroll) Next() (item interface{}, err error) {
	return <-s.item, s.err
}

// 请求query
type Query struct {
	index          *Index
	filter         []elastic.Query
	must           []elastic.Query
	mustNot        []elastic.Query
	should         []elastic.Query
	minShouldMatch int
	size           int
	from           int
	sorters        []elastic.Sorter
	scrollAlive    string
	scrollSize     int
}

// 不允许外部调用，仅允许 index.Query(xxx) 返回
func newQuery(index *Index) *Query {
	q := &Query{
		index:          index,
		filter:         []elastic.Query{},
		must:           []elastic.Query{},
		mustNot:        []elastic.Query{},
		should:         []elastic.Query{},
		minShouldMatch: 1,
		size:           100,
		from:           0,
		sorters:        []elastic.Sorter{},
		scrollAlive:    "5m",
		scrollSize:     1000,
	}
	return q
}

// 添加Filter条件
func (q *Query) AddFilter(elasticQuery elastic.Query) *Query {
	q.filter = append(q.filter, elasticQuery)
	return q
}

// 添加Must条件
func (q *Query) AddMust(elasticQuery elastic.Query) *Query {
	q.filter = append(q.must, elasticQuery)
	return q
}

// 添加MustNot条件
func (q *Query) AddMustNot(elasticQuery elastic.Query) *Query {
	q.filter = append(q.mustNot, elasticQuery)
	return q
}

// 添加Should条件
func (q *Query) AddShould(elasticQuery elastic.Query) *Query {
	q.should = append(q.should, elasticQuery)
	return q
}

// MinimumShouldMatch 默认是1
func (q *Query) MinimumShouldMatch(minShouldMatch int) *Query {
	q.minShouldMatch = minShouldMatch
	return q
}

//  -------------------------------------------- Filter -------------------------------------------
// Term in Filter
func (q *Query) Term(fieldName string, value interface{}) *Query {
	return q.AddFilter(elastic.NewTermQuery(fieldName, value))
}

// Terms in Filter
func (q *Query) Terms(fieldName string, values ...interface{}) *Query {
	return q.AddFilter(elastic.NewTermsQuery(fieldName, values...))
}

// Match in Filter
func (q *Query) Match(fieldName string, value interface{}) *Query {
	return q.AddFilter(elastic.NewMatchQuery(fieldName, value))
}

// MatchPhrase in Filter
func (q *Query) MatchPhrase(fieldName string, value interface{}) *Query {
	return q.AddFilter(elastic.NewMatchPhraseQuery(fieldName, value))
}

// Range in Filter
func (q *Query) Range(fieldName string, rangeOptions ...RangeOption) *Query {
	r := elastic.NewRangeQuery(fieldName)
	for _, rangeOption := range rangeOptions {
		rangeOption(r)
	}
	return q.AddFilter(r)
}

// Exist in Filter
func (q *Query) Exist(fieldName string) *Query {
	return q.AddFilter(elastic.NewExistsQuery(fieldName))
}

//  -------------------------------------------- Should -------------------------------------------
// Term in Should
func (q *Query) ShouldTerm(fieldName string, value interface{}) *Query {
	return q.AddShould(elastic.NewTermQuery(fieldName, value))
}

// Terms in Should
func (q *Query) ShouldTerms(fieldName string, values ...interface{}) *Query {
	return q.AddShould(elastic.NewTermsQuery(fieldName, values...))
}

// Match in Should
func (q *Query) ShouldMatch(fieldName string, value interface{}) *Query {
	return q.AddShould(elastic.NewMatchQuery(fieldName, value))
}

// MatchPhrase in Should
func (q *Query) ShouldMatchPhrase(fieldName string, value interface{}) *Query {
	return q.AddShould(elastic.NewMatchPhraseQuery(fieldName, value))
}

// Range in Should
func (q *Query) ShouldRange(fieldName string, rangeOptions ...RangeOption) *Query {
	r := elastic.NewRangeQuery(fieldName)
	for _, rangeOption := range rangeOptions {
		rangeOption(r)
	}
	return q.AddShould(r)
}

// Exist in Should
func (q *Query) ShouldExist(fieldName string) *Query {
	return q.AddShould(elastic.NewExistsQuery(fieldName))
}

//  -------------------------------------------- Must -------------------------------------------
// Term in Must
func (q *Query) MustTerm(fieldName string, value interface{}) *Query {
	return q.AddMust(elastic.NewTermQuery(fieldName, value))
}

// Term in Must
func (q *Query) MustTerms(fieldName string, values ...interface{}) *Query {
	return q.AddMust(elastic.NewTermsQuery(fieldName, values...))
}

// Match in Must
func (q *Query) MustMatch(fieldName string, value interface{}) *Query {
	return q.AddMust(elastic.NewMatchQuery(fieldName, value))
}

// MatchPhrase in Must
func (q *Query) MustMatchPhrase(fieldName string, value interface{}) *Query {
	return q.AddMust(elastic.NewMatchPhraseQuery(fieldName, value))
}

// Range in Must
func (q *Query) MustRange(fieldName string, rangeOptions ...RangeOption) *Query {
	r := elastic.NewRangeQuery(fieldName)
	for _, rangeOption := range rangeOptions {
		rangeOption(r)
	}
	return q.AddMust(r)
}

// Exist in Must
func (q *Query) MustExist(fieldName string) *Query {
	return q.AddMust(elastic.NewExistsQuery(fieldName))
}

//  -------------------------------------------- MustNot -------------------------------------------
// Term in MustNot
func (q *Query) MustNotTerm(fieldName string, value interface{}) *Query {
	return q.AddMustNot(elastic.NewTermQuery(fieldName, value))
}

// Terms in MustNot
func (q *Query) MustNotTerms(fieldName string, values ...interface{}) *Query {
	return q.AddMustNot(elastic.NewTermsQuery(fieldName, values...))
}

// Match in MustNot
func (q *Query) MustNotMatch(fieldName string, value interface{}) *Query {
	return q.AddMustNot(elastic.NewMatchQuery(fieldName, value))
}

// MatchPhrase in MustNot
func (q *Query) MustNotMatchPhrase(fieldName string, value interface{}) *Query {
	return q.AddMustNot(elastic.NewMatchPhraseQuery(fieldName, value))
}

// Range in MustNot
func (q *Query) MustNotRange(fieldName string, rangeOptions ...RangeOption) *Query {
	r := elastic.NewRangeQuery(fieldName)
	for _, rangeOption := range rangeOptions {
		rangeOption(r)
	}
	return q.AddMustNot(r)
}

// Exist in Must
func (q *Query) MustNotExist(fieldName string) *Query {
	return q.AddMustNot(elastic.NewExistsQuery(fieldName))
}

// 依据字段将序排列。 sortType: 1 升序排列， -1 降序排列
func (q *Query) OrderBy(fieldName string, sortType int) *Query {
	s := elastic.NewFieldSort(fieldName)
	if sortType == -1 {
		s.Desc()
	}
	q.sorters = append(q.sorters, s)
	return q
}

// 设置分页 pageSize:每页数量，pageNum:页码,从1开始
func (q *Query) Page(pageNum int64, pageSize int64) *Query {
	if pageNum < 0 {
		pageNum = 0
	}
	if pageSize < 0 {
		pageSize = 0
	}
	q.size = int(pageSize)
	q.from = int((pageNum - 1) * pageSize)
	return q
}

// 设置scroll存在时间, 默认5m (五分钟)
func (q *Query) ScrollAlive(alive string) *Query {
	q.scrollAlive = alive
	return q
}

// 设置scroll每次返回的结果数量, 默认1000
func (q *Query) ScrollSize(scrollSize int) *Query {
	q.scrollSize = scrollSize
	return q
}

// 获取分页列表
func (q *Query) GetList() (result []interface{}, total int64, err error) {
	r, e := q.Search(q.size, q.from)
	if e != nil {
		return nil, 0, e
	}
	return q.pargeSearchResult(r)
}

// 请求查询, 指定size与from, 返回原生查询结果
func (q *Query) Search(size int, from int) (*elastic.SearchResult, error) {
	index := q.index
	rawClient := index.client.rawClient
	search := rawClient.Search(index.indexName...).Query(q.getBoolQuery()).FetchSourceContext(index.fields)
	//排序
	if q.sorters != nil && len(q.sorters) > 0 {
		search.SortBy(q.sorters...)
	}
	ctx := context.Background()
	return search.Size(size).From(from).Do(ctx)
}

// 获取scroll
func (q *Query) GetScroll() (*Scroll, error) {
	index := q.index
	rawClient := index.client.rawClient
	scrollService := rawClient.Scroll(index.indexName...).Query(q.getBoolQuery()).Scroll(q.scrollAlive)
	//排序
	if q.sorters != nil && len(q.sorters) > 0 {
		scrollService.SortBy(q.sorters...)
	}
	ctx := context.Background()
	size := q.scrollSize
	r, err := scrollService.Size(size).Do(ctx)
	if err != nil {
		return nil, err
	}
	result, total, err := q.pargeSearchResult(r)
	if err != nil {
		return nil, err
	}
	scroll := &Scroll{
		scrollId: r.ScrollId,
		Total:    total,
		item:     make(chan interface{}, 0),
		err:      nil,
	}
	go q.scanScroll(scroll, result, size)
	return scroll, nil
}

// 循环获取scroll数据
func (q *Query) scanScroll(s *Scroll, result []interface{}, size int) {
	index := q.index
	rawClient := index.client.rawClient
	for {
		if result == nil || len(result) == 0 {
			break
		}
		for _, item := range result {
			s.item <- item
		}
		ctx := context.Background()
		r, err := rawClient.Scroll().ScrollId(s.scrollId).Do(ctx)
		if err != nil {
			if err.Error() != "EOF" {
				s.err = err
			}
			break
		}
		result, s.Total, err = q.pargeSearchResult(r)
		if err != nil {
			s.err = err
			break
		}
	}
	ctx := context.Background()
	rawClient.Scroll().ScrollId(s.scrollId).Clear(ctx)
	close(s.item)
}

// 获取boolquery
func (q *Query) getBoolQuery() *elastic.BoolQuery {
	query := elastic.NewBoolQuery()
	//filter
	if q.filter != nil && len(q.filter) > 0 {
		query.Filter(q.filter...)
	}
	//must
	if q.must != nil && len(q.must) > 0 {
		query.Must(q.must...)
	}
	//must not
	if q.mustNot != nil && len(q.mustNot) > 0 {
		query.MustNot(q.mustNot...)
	}
	//should
	if q.should != nil && len(q.should) > 0 {
		query.Should(q.should...)
		query.MinimumNumberShouldMatch(q.minShouldMatch)
	}
	return query
}

// 获取query部分的string
func (q *Query) String() string {
	s, _ := q.getBoolQuery().Source()
	b, _ := json.Marshal(s)
	return string(b)
}

// 解析搜索结果
func (q *Query) pargeSearchResult(r *elastic.SearchResult) (result []interface{}, total int64, err error) {
	if r.Hits == nil || r.Hits.Hits == nil {
		return nil, 0, errors.New("Hits result not found")
	}
	var slice []interface{}
	for _, hit := range r.Hits.Hits {
		doc, err := q.index.source2Struct(hit.Source, hit.Id, hit.Index, hit.Type, hit.Version, hit.Score)
		if err != nil {
			return nil, 0, err
		}
		slice = append(slice, doc)
	}
	return slice, r.TotalHits(), nil
}
