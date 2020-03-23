package tools


import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type (
	Config struct {
		SwitchOff	bool

		Ratio		float32
		Sleep		time.Duration
		K			float64

		Window		time.Duration
		Bucket		int
		Request     int64
	}

	Breaker interface {
		Allow() error
		MarkSuccess()
		MarkFailed()
	}

	Group struct {
		mu   sync.RWMutex
		brks map[string]Breaker
		conf *Config
	}

	summary struct {
		mu         sync.RWMutex
		buckets    []bucket
		bucketTime int64
		lastAccess int64
		cur        *bucket
	}

	reBreaker struct {
		stat Summary
		k	float64
		request  int64
		state   int32
		r *rand.Rand
	}

)


const (
	StateOpen		int32 = iota
	StateClosed
	StateHalfopen
)

var (
	_mu		sync.RWMutex
	_conf =	&Config{
		Window:  time.Duration(3 * time.Second),
		Bucket:  10,
		Request: 100,

		Sleep: time.Duration(500 * time.Millisecond),
		Ratio: 0.5,
		// Percentage of failures must be lower than 33.33%
		K: 1.5,
	}
	_group	= NewGroup(_conf)

)

func (conf *Config) fix(){
	if conf.K == 0 {
		conf.K = 1.5
	}
	if conf.Request == 0 {
		conf.Request = 100
	}
	if conf.Ratio == 0 {
		conf.Ratio = 0.5
	}
	if conf.Sleep == 0 {
		conf.Sleep = time.Duration(500 * time.Millisecond)
	}
	if conf.Bucket == 0 {
		conf.Bucket = 10
	}
	if conf.Window == 0 {
		conf.Window = time.Duration(3 * time.Second)
	}
}

func Init(conf *Config) {
	if conf == nil {
		return
	}

	_mu.Lock()
	_conf = conf
	_mu.Unlock()
}

func Go(name string,run,fallback func() error) error {
	breaker := _group.Get(name)
	if err := breaker.Allow();err != nil {
		return fallback()
	}
	return run()
}

func newBreaker(c *Config)(b Breaker) {
	return newSRE(c)
}

func NewGroup(conf *Config) *Group {
	if conf == nil {
		_mu.RLock()
		conf = _conf
		_mu.RUnlock()
	}else {
		conf.fix()
	}
	return &Group{
		conf:conf,
		brks:make(map[string]Breaker),
	}
}

func (g *Group) Get(key string) Breaker{
	g.mu.RLock()
	brk,ok := g.brks[key]
	conf := g.conf
	g.mu.RUnlock()
	if ok {
		return brk
	}
	brk = newBreaker(conf)
	g.mu.Lock()
	if _,ok = g.brks[key];!ok {
		g.brks[key] = brk
	}
	g.mu.Unlock()
	return brk
}

func (g *Group)Reload(conf *Config) {
	if conf == nil {
		return
	}

	conf.fix()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.conf = conf
	g.brks = make(map[string]Breaker,len(g.brks))
}

func (g *Group) Go(name string,run,fallback func()error) error{
	breaker := g.Get(name)
	if err := breaker.Allow();err != nil {
		return fallback()
	}
	return run()
}




func newSRE(c *Config) Breaker {
	return &reBreaker{
		stat:New(time.Duration(c.Window),c.Bucket),
		r:	rand.New(rand.NewSource(time.Now().UnixNano())),

		request:c.Request,
		k:		c.K,
		state:	StateClosed,
	}
}

func (b *reBreaker) Allow() error {
	success, total := b.stat.Value()
	k := b.k * float64(success)

	// check overflow requests = K * success
	if total < b.request || float64(total) < k {
		if atomic.LoadInt32(&b.state) == StateOpen {
			atomic.CompareAndSwapInt32(&b.state, StateOpen, StateClosed)
		}
		return nil
	}
	if atomic.LoadInt32(&b.state) == StateClosed {
		atomic.CompareAndSwapInt32(&b.state, StateClosed, StateOpen)
	}
	dr := math.Max(0, (float64(total)-k)/float64(total+1))
	rr := b.r.Float64()
	if dr <= rr {
		return nil
	}
	return errors.New("500")
}

func (b *reBreaker) MarkSuccess() {
	b.stat.Add(1)
}

func (b *reBreaker) MarkFailed() {
	b.stat.Add(0)
}



type bucket struct {
	val   int64
	count int64
	next  *bucket
}

func (b *bucket) Add(val int64) {
	b.val += val
	b.count++
}

func (b *bucket) Value() (int64, int64) {
	return b.val, b.count
}

func (b *bucket) Reset() {
	b.val = 0
	b.count = 0
}

// Summary is a summary interface.
type Summary interface {
	Add(int64)
	Reset()
	Value() (val int64, cnt int64)
}



func New(window time.Duration, winBucket int) Summary {
	buckets := make([]bucket, winBucket)
	bucket := &buckets[0]
	for i := 1; i < winBucket; i++ {
		bucket.next = &buckets[i]
		bucket = bucket.next
	}
	bucket.next = &buckets[0]
	bucketTime := time.Duration(window.Nanoseconds() / int64(winBucket))
	return &summary{
		cur:        &buckets[0],
		buckets:    buckets,
		bucketTime: int64(bucketTime),
		lastAccess: time.Now().UnixNano(),
	}
}

// Add increments the summary by value.
func (s *summary) Add(val int64) {
	s.mu.Lock()
	s.lastBucket().Add(val)
	s.mu.Unlock()
}

// Value get the summary value and count.
func (s *summary) Value() (val int64, cnt int64) {
	now := time.Now().UnixNano()
	s.mu.RLock()
	b := s.cur
	i := s.elapsed(now)
	for j := 0; j < len(s.buckets); j++ {
		// skip all future reset bucket.
		if i > 0 {
			i--
		} else {
			v, c := b.Value()
			val += v
			cnt += c
		}
		b = b.next
	}
	s.mu.RUnlock()
	return
}

//  Reset reset the counter.
func (s *summary) Reset() {
	s.mu.Lock()
	for i := range s.buckets {
		s.buckets[i].Reset()
	}
	s.mu.Unlock()
}

func (s *summary) elapsed(now int64) (i int) {
	var e int64
	if e = now - s.lastAccess; e <= s.bucketTime {
		return
	}
	if i = int(e / s.bucketTime); i > len(s.buckets) {
		i = len(s.buckets)
	}
	return
}

func (s *summary) lastBucket() (b *bucket) {
	now := time.Now().UnixNano()
	b = s.cur
	// reset the buckets between now and number of buckets ago. If
	// that is more that the existing buckets, reset all.
	if i := s.elapsed(now); i > 0 {
		s.lastAccess = now
		for ; i > 0; i-- {
			// replace the next used bucket.
			b = b.next
			b.Reset()
		}
	}
	s.cur = b
	return
}
