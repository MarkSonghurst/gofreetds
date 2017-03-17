package freetds

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	p, err := NewConnPool(testDbConnStr(2))
	defer p.Close()
	assert.Nil(t, err)
	assert.NotNil(t, p)
	assert.Equal(t, len(p.pool), 1)
	c1, err := p.Get()
	assert.Nil(t, err)
	assert.NotNil(t, c1)
	assert.Equal(t, len(p.pool), 0)
	c2, err := p.Get()
	assert.Nil(t, err)
	assert.NotNil(t, c2)
	assert.Equal(t, len(p.pool), 0)
	p.Release(c1)
	assert.Equal(t, len(p.pool), 1)
	p.Release(c2)
	assert.Equal(t, len(p.pool), 2)
}

func TestPoolRelease(t *testing.T) {
	p, _ := NewConnPool(testDbConnStr(2))
	assert.Equal(t, p.connCount, 1)
	c1, _ := p.Get()
	c2, _ := p.Get()
	assert.Equal(t, p.connCount, 2)
	assert.Equal(t, len(p.pool), 0)
	//conn can be released to the pool by calling Close on conn
	c1.Close()
	assert.Equal(t, p.connCount, 2)
	assert.Equal(t, len(p.pool), 1)
	//or by calling pool Release
	p.Release(c2)
	assert.Equal(t, p.connCount, 2)
	assert.Equal(t, len(p.pool), 2)
}

func TestPoolBlock(t *testing.T) {
	p, _ := NewConnPool(testDbConnStr(2))
	c1, _ := p.Get()
	c2, _ := p.Get()

	//check that poolGuard channel is full
	full := false
	select {
	case p.poolGuard <- true:
	default:
		full = true
	}
	assert.True(t, full)

	go func() {
		c3, _ := p.Get()
		assert.Equal(t, c2, c3)
		c4, _ := p.Get()
		assert.Equal(t, c1, c4)
		p.Release(c3)
		p.Release(c4)
		p.Close()
	}()
	p.Release(c1)
	p.Release(c2)
}

func TestPoolCleanup(t *testing.T) {
	p, _ := NewConnPool(testDbConnStr(5))
	conns := make([]*Conn, 5)
	for i := 0; i < 5; i++ {
		c, _ := p.Get()
		conns[i] = c
	}
	for i := 0; i < 5; i++ {
		c := conns[i]
		p.Release(c)
		c.expiresFromPool = time.Now().Add(-poolExpiresInterval - time.Second)
	}
	assert.Equal(t, len(p.pool), 5)
	p.cleanup()
	assert.Equal(t, len(p.pool), 1)
}

func TestPoolReturnsLastUsedConnection(t *testing.T) {
	p, _ := NewConnPool(testDbConnStr(5))
	c1, _ := p.Get()
	c2, _ := p.Get()
	assert.Equal(t, 0, len(p.pool))
	c1.Close()
	assert.Equal(t, 1, len(p.pool))
	c2.Close()
	assert.Equal(t, 2, len(p.pool))
	assert.Equal(t, c2, p.pool[0])
	assert.Equal(t, c1, p.pool[1])
	c3, _ := p.Get()
	assert.Equal(t, c2, c3)
}

func BenchmarkConnPool(b *testing.B) {
	p, _ := NewConnPool(testDbConnStr(4))
	defer p.Close()
	done := make(chan bool)
	repeat := 20
	fmt.Printf("\n")
	for i := 0; i < repeat; i++ {
		go func(j int) {
			conn, _ := p.Get()
			defer p.Release(conn)
			fmt.Printf("running: %d pool len: %d, connCount %d\n", j, len(p.pool), p.connCount)
			conn.Exec("WAITFOR DELAY '00:00:01'")
			done <- true
		}(i)
	}
	for i := 0; i < repeat; i++ {
		<-done
	}
}

func TestMirroringConnPool(t *testing.T) {
	if !IsMirrorHostDefined() {
		t.Skip("mirror host is not defined")
	}
	p, err := NewConnPool(testDbConnStr(2))
	defer p.Close()
	assert.Nil(t, err)
	c1, err := p.Get()
	assert.Nil(t, err)
	rst, err := c1.Exec("select * from authors")
	assert.Nil(t, err)
	assert.Equal(t, 23, len(rst[0].Rows))
	failover(c1)
	//newly created connection
	c2, err := p.Get()
	assert.Nil(t, err)
	rst, err = c2.Exec("select * from authors")
	assert.Nil(t, err)
	assert.Equal(t, 23, len(rst[0].Rows))

	//reuse c1 connection
	c1.Close()
	c3, err := p.Get()
	assert.Nil(t, err)
	//c3.DbUse()
	rst, err = c3.Exec("select * from authors")
	assert.Nil(t, err)
	assert.Equal(t, 23, len(rst[0].Rows))

	c2.Close()
	c3.Close()
}

func TestConnPoolDo(t *testing.T) {
	p, _ := NewConnPool(testDbConnStr(2))
	defer p.Close()
	err := p.Do(func(conn *Conn) error {
		assert.Equal(t, 0, len(p.pool))
		assert.Equal(t, 1, p.connCount)
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(p.pool))
	assert.Equal(t, 1, p.connCount)
}

func TestPoolRemove_TwoSizedPool(t *testing.T) {
	p, _ := NewConnPool(testDbConnStr(2))
	assert.Equal(t, p.connCount, 1)
	c1, _ := p.Get()
	c2, _ := p.Get()
	// The pool has used 2 connections (c1, c2)
	assert.Equal(t, p.connCount, 2)
	// ...and has no unused connections in it.
	assert.Equal(t, len(p.pool), 0)

	// Remove c1 from the pool
	p.Remove(c1)
	assert.Nil(t, c1.belongsToPool)
	// The pool has 1 used connection (c2)
	assert.Equal(t, p.connCount, 1)
	// ...and still has no unused connections in it.
	assert.Equal(t, len(p.pool), 0)

	// Trying to release the removed conn is a safe noop
	p.Release(c1)
	c1.Close()

	// Get another connection from the pool.
	// The pool will need to create this connection
	// as it has no unused connections.
	c3, _ := p.Get()

	// The pool has used 2 connections (c2, c3)
	assert.Equal(t, p.connCount, 2)
	// ...and has no unused connections in it.
	assert.Equal(t, len(p.pool), 0)

	p.Release(c2)
	p.Release(c3)
}

func TestPoolRemove_OneSizedPool(t *testing.T) {
	p, _ := NewConnPool(testDbConnStr(1))
	assert.Equal(t, p.connCount, 1)
	c1, _ := p.Get()
	// The pool has used 1 connection (c1)
	assert.Equal(t, p.connCount, 1)
	// ...and has no unused connections in it.
	assert.Equal(t, len(p.pool), 0)

	var wg sync.WaitGroup
	chGotConn := make(chan *Conn)

	// This goroutine attemps to Get another connection from the pool.
	wg.Add(1)
	go func() {
		// The call to p.Get() will block until c1 is
		// removed from the pool by the other goroutine.
		c2, _ := p.Get()
		assert.NotNil(t, c2)
		chGotConn <- c2
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		// Sleep the goroutine for 2 seconds,
		// keeping c1 in the pool.
		time.Sleep(2 * time.Second)
		// Now remove c1 from the pool.
		// This will allow the call to p.Get()
		// in the other goroutine to unblock.
		// Note the use of the RemoveFromPool() func on the conn itself.
		c1.RemoveFromPool().Close()
		wg.Done()
	}()

	// Wait to receive c2, or timeout.
	select {
	case c2 := <-chGotConn:
		// The pool has used 1 connection (c2)
		assert.Equal(t, p.connCount, 1)
		// ...and has no unused connections in it.
		assert.Equal(t, len(p.pool), 0)
		p.Release(c2)

	case <-time.After(5 * time.Second):
		assert.Fail(t, "timed out waiting for a pooled connection")
	}

	wg.Wait()
	close(chGotConn)
}

func TestPoolRemove_OnConn(t *testing.T) {
	p, _ := NewConnPool(testDbConnStr(2))
	assert.Equal(t, p.connCount, 1)
	c1, _ := p.Get()

	c1.RemoveFromPool()
	assert.Nil(t, c1.belongsToPool)
	// Calling RemoveFromPool again is a safe noop
	c1.RemoveFromPool()
	// Trying to remove the already removed conn is a safe noop
	p.Remove(c1)
	c1.Close()
}
