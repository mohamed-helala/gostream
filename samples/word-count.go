package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"loopy"
	"math/rand"
	"strings"
	"time"
)

var sents []string = []string{"thanks, nancy",
	"hello, everyone, and thank you for joining us",
	"we have a lot of news to share with you today about the details of our march quarter, as well as a significant increase to our capital return program",
	"first, i'd like to talk about our business and the road ahead",
	"we're now half way through our fiscal 2013, and we've accomplished a tremendous amount",
	"we've introduced and ramped production of an unprecedented number of new products, and we've set many new sales records",
	"our revenue for the first half was over $98 billion, and our net income was over $22 billion",
	"during that time we sold 85 million iphones and 42 million ipads",
	"these are very, very large numbers, unimaginable even to us just a few years ago",
	"despite producing results that met or beat our guidance as we have done consistently, we know they didn't meet everyone's expectations",
	"and though we've achieved incredible scale and financial success, we acknowledge that our growth rate has slowed and our margins have decreased from the exceptionally high level we experienced in 2012",
	"our revenues grew about $13 billion in the first half of the fiscal year",
	"even though that's like adding the total first half revenue of five fortune 500 companies, our average weekly growth slowed to 19% and our gross margins are closer to the levels of a few years ago",
	"our fiscal 2012 results were incredibly strong and that's making comparisons very difficult this year",
	"last year our business benefited from both high growth and demand for our products and a corresponding growth in channel inventories along with a richer mix of higher gross margin products, a more favorable foreign currency environment and historically low costs",
	"these compares are made further challenging until we anniversary the launch of the ipad mini, which as you know we strategically priced at a lower margin",
	"as peter will discuss, we are guiding to flat revenues year-over-year for the june quarter along with a slight sequential decline in gross margins.",
	"the decline in apple's stock price over the last couple of quarters has been very frustrating to all of us",
	"but apple remains very strong and will continue to do what we do best",
	"we can't control items such as exchange rates and world economies and even certain cost pressures",
	"but the most important objective for apple will always be creating innovative products and that is directly within our control",
	"we will continue to focus on the long-term and we remain very optimistic about our future",
	"we are participating on large and growing markets",
	"we see great opportunities in front of us, particularly given the long-term prospects of the smartphone and tablet market, the strength of our incredible ecosystem which we plan to continue to augment with new services, our plans for expanded distribution and the potential of exciting new product categories",
	"take the smartphone market, for example",
	"idc estimates that this market will double between 2012 and 2016 to an incredible 1.4 billion units annually and gartner estimates that the tablet market is growing at an even faster rate from 125 million units in 2012 to a projected 375 million by 2016",
	"our teams are hard at work on some amazing new hardware, software and services that we can't wait to introduce this fall and throughout 2014",
	"we continue to be very confident in our future product plans",
	"apple has many distinct and unique advantages as the only company in the industry with world-class skills in hardware, software and services",
	"we have the strongest ecosystem in the industry with app stores in 155 countries, itunes music stores in 119 countries, hundreds of millions of icloud users around the world; and most importantly, the highest loyalty and customer satisfaction rates in the business",
	"and of course, we have a tremendous culture of innovation with a relentless focus on making the world's best products that change people's lives",
	"this is the same culture and company that brought the world the iphone and ipad and we've got a lot more surprises in the works",
	"a little over a year ago, we announced a plan to return $45 billion to shareholders over three years",
	"since we began paying dividends last august and began share buybacks last october, we've already returned $10 billion under that program",
	"while we continue to generate cash in excess of our needs to operate the business, invest in our future and maintain flexibility to take advantage of strategic opportunities, we remain firmly committed to our objective of delivering attractive returns to shareholders through both our business performance and the return of capital",
	"so, today, we are announcing an aggressive plan that more than doubles the size of the capital return program we announced last year to a total of $100 billion by the end of calendar year 2015",
	"the vast majority of our incremental cash return will be in the form of share repurchases",
	"as the board and management team deliberated among the various alternatives to returning cash, we concluded that investing in apple was the best",
	"in addition to share repurchases, we are increasing our current dividend by 15% to further appeal to investors seeking yield",
	"and as part of our updated program, we will access the debt market.",
	"peter will provide more details about all of this in a moment",
	"we appreciate the input that so many of our shareholders have provided us on how best to deploy our cash",
	"we will review our cash allocation strategy each year, and we will continue to invest confidently in the business to bring great new products to market, strategically deploy capital in our supply chain, our retail stores and our infrastructure, and make acquisitions to expand our capabilities",
	"we will be disciplined in what we do, but we will not underinvest",
	"i'd now like to turn the call over to peter to discuss the details of the march quarter."}

//##############################
// Random sentence spout
//##############################
type Tuple map[string]interface{}

type State map[string]int

func (t Tuple) Clone() loopy.T {
	tc := make(Tuple)
	for k, v := range t {
		tc[k] = v
	}
	return tc
}

func (t Tuple) Dispose() {

}

func (t State) Dispose() {

}

type RandSentsSpout struct {
	sents []string
	r     *rand.Rand
}

func (sp *RandSentsSpout) Read() loopy.T {
	tuple := make(Tuple)
	tuple["sentence"] = sp.sents[sp.r.Intn(len(sp.sents))]
	return loopy.NewMessage(tuple)
}

func NewRandSenSpout(sents []string) loopy.Spout {
	return &RandSentsSpout{sents, rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func main() {
	g := CreateGraph()
	g.Execute()
	g.Wait()
}

func CreateGraph() *loopy.OGraph {

	counter := &loopy.Function{FuncName: "counter", Reducer: func(u, x loopy.T, params loopy.Params) (u1, y loopy.T) {
		collector := u.(State)
		tuple := loopy.MessageV(x).(Tuple)
		word := tuple["word"].(string)
		collector[word] += 1
		tuple["count"] = collector[word]
		fmt.Println(word, collector[word])
		return u, x
	}}

	f := func(x loopy.T) []loopy.T {
		if x == nil {
			return nil
		}
		ituple := loopy.MessageV(x).(Tuple)
		words := strings.Fields(ituple["sentence"].(string))
		ret := make([]loopy.T, len(words))
		for i := 0; i < len(words); i++ {
			otuple := make(Tuple)
			otuple["word"] = words[i]
			ret[i] = loopy.NewMessage(otuple)
		}
		return ret
	}

	p := func(x loopy.T, i int, n int) int {
		tuple := loopy.MessageV(x).(Tuple)
		v, err := Hash(tuple["word"].(string))
		if err != nil {
			panic(fmt.Sprintf("Couldn't hash element %v", x))
		}
		k := v % uint32(n-1)
		return int(k)
	}

	h1 := func(g *loopy.OGraph, i int) (*loopy.Processor, *loopy.Processor) {
		a := g.Source(NewRandSenSpout(sents))
		return a.Proc, a.Proc
	}

	h2 := func(g *loopy.OGraph, i int) (*loopy.Processor, *loopy.Processor) {
		s := g.Reduce(make(State), loopy.Functions{counter})
		e := s.Ground()
		return s.Proc, e.Proc
	}

	g := loopy.NewOGraph()

	g.List(5, h1).Group(5, 7, f, p).List(7, h2)
}

func GetBytes(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	} else {
		return buf.Bytes(), nil
	}
}

func Hash(key interface{}) (uint32, error) {
	var (
		data []byte
		err  error
	)
	h := fnv.New32a()
	if data, err = GetBytes(key); err != nil {
		return 0, err
	}
	h.Write(data)
	return h.Sum32(), nil
}
