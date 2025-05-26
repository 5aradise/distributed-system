package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type BalancerTestSuite struct {
	suite.Suite
}

func pointer[T any](v T) *T {
	return &v
}

func (suite *BalancerTestSuite) SetupTest() {
	port = pointer(42069)
	https = pointer(false)
	traceEnabled = pointer(false)
	timeout = 3 * time.Second

	serversPool = []*server{
		{url: "server1"},
		{url: "server2"},
		{url: "server3"},
		{url: "server4"},
		{url: "server5"},
	}
}

func (suite *BalancerTestSuite) TestChooseHealthy_OneHealthy() {
	serversPool[2].healthy.Store(true)

	url, err := chooseHealthy(2)
	suite.NoError(err)
	suite.Equal(serversPool[2].url, url)
}

func (suite *BalancerTestSuite) TestChooseHealthy_WrapAround() {
	serversPool[0].healthy.Store(true)

	url, err := chooseHealthy(2)
	suite.NoError(err)
	suite.Equal(serversPool[0].url, url)
}

func (suite *BalancerTestSuite) TestChooseHealthy_AllUnhealthy() {
	_, err := chooseHealthy(2)
	if suite.Error(err) {
		suite.ErrorIs(err, errAllServersUnhealthy)
	}
}

func (suite *BalancerTestSuite) TestForward_Success() {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Test", "hell yeah")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hello Mazur from backend"))
	}))
	defer target.Close()

	dst := target.Listener.Addr().String()
	req := httptest.NewRequest("GET", "http://lb", nil)
	rw := httptest.NewRecorder()

	err := forward(dst, rw, req)
	if suite.NoError(err) {
		suite.Equal(http.StatusOK, rw.Code)
		suite.Equal("hell yeah", rw.Header().Get("X-Test"))
		suite.Equal("Hello Mazur from backend", rw.Body.String())
	}
}

func (suite *BalancerTestSuite) TestForward_Unavailable() {
	dst := "server"
	req := httptest.NewRequest("GET", "http://lb", nil)
	rw := httptest.NewRecorder()

	err := forward(dst, rw, req)
	suite.Error(err)
	suite.Equal(http.StatusServiceUnavailable, rw.Code)
}

func TestBalancer(t *testing.T) {
	suite.Run(t, new(BalancerTestSuite))
}

func TestShouldFail(t *testing.T) {
	t.Fatal("This test should fail")
}
