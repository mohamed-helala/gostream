package loopy

type Sequence struct {
	c       uint64
	ch      chan uint64
	started bool
	closed  bool
}

func NewSequence(offset uint64) *Sequence{
	seq := &Sequence{0, make(chan uint64), false, false}
	seq.Start(offset)
	return seq
}

func (s *Sequence) Start(offset uint64) {
	if s.started {
		return
	}
	if s.closed {
		s.ch = make(chan uint64)
		s.closed = false
	}
	s.c = offset
	go func() {
		defer close(s.ch)
		for !s.closed {
			s.c++
			s.ch <- s.c
		}
	}()
	s.started = true
}

func (s *Sequence) Read() uint64 {
	if !s.closed {
		return <-s.ch
	}
	panic("Reading from a closed sequence")
}

func (s *Sequence) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.started = false
	<-s.ch
}
