// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import (
	types "github.com/SmartBFT-Go/consensus/pkg/types"
	mock "github.com/stretchr/testify/mock"
)

// RequestPool is an autogenerated mock type for the RequestPool type
type RequestPool struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *RequestPool) Close() {
	_m.Called()
}

// NextRequests provides a mock function with given fields: maxCount, maxSizeBytes
func (_m *RequestPool) NextRequests(maxCount int, maxSizeBytes uint64) ([][]byte, bool) {
	ret := _m.Called(maxCount, maxSizeBytes)

	var r0 [][]byte
	if rf, ok := ret.Get(0).(func(int, uint64) [][]byte); ok {
		r0 = rf(maxCount, maxSizeBytes)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([][]byte)
		}
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(int, uint64) bool); ok {
		r1 = rf(maxCount, maxSizeBytes)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

// Prune provides a mock function with given fields: predicate
func (_m *RequestPool) Prune(predicate func([]byte) error) {
	_m.Called(predicate)
}

// RemoveRequest provides a mock function with given fields: request
func (_m *RequestPool) RemoveRequest(request types.RequestInfo) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(types.RequestInfo) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RestartTimers provides a mock function with given fields:
func (_m *RequestPool) RestartTimers() {
	_m.Called()
}

// Size provides a mock function with given fields:
func (_m *RequestPool) Size() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// StopTimers provides a mock function with given fields:
func (_m *RequestPool) StopTimers() {
	_m.Called()
}

// Submit provides a mock function with given fields: request
func (_m *RequestPool) Submit(request []byte) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func([]byte) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
