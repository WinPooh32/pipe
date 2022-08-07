package pipe

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
)

type HandlerFunc[T any] func(ctx context.Context, in T) (out T, err error)

type Pipeline[T any] []HandlerFunc[T]

// Execute starts pipeline processing.
func Execute[T any](ctx context.Context, pipeline Pipeline[T], in T) (out T, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("pipeline: recovered panic:\n%s", debug.Stack())
		}
	}()

	for _, handler := range pipeline {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}

		out, err = handler(ctx, in)
		if err != nil {
			return out, err
		}

		in = out
	}

	out = in

	return out, nil
}

// Parallel distributes 'in' batch between jobs and executes piplene inside of separated routines.
// Order of results will be same as input.
func Parallel[T any](ctx context.Context, pipeline Pipeline[[]T], in []T, jobs int) (out []T, err error) {
	if jobs <= 0 {
		panic("jobs value must be greater than zero!")
	}

	var wg sync.WaitGroup

	batchSize := len(in) / jobs

	if len(in)%jobs > 0 {
		batchSize += 1
	}

	outputData := make([][]T, jobs)
	outputErr := make([]error, jobs)

	max := jobs * batchSize

	if max > len(in) {
		max = len(in)
	}

	var beg, end int

	for i := 0; i < jobs-1 && end < max; i++ {
		beg = i * batchSize
		end = (i + 1) * batchSize

		var batch []T

		if end <= max {
			batch = in[beg:end]
		} else {
			batch = in[beg:]
		}

		wg.Add(1)
		go func(job int) {
			defer wg.Done()
			outputData[job], outputErr[job] = Execute(ctx, pipeline, batch)
		}(i)
	}

	wg.Wait()

	if len(outputData) > len(outputErr) {
		panic("lenghts of outputErr and outputData must be equal!")
	}

	for i, v := range outputData {
		err := outputErr[i]
		if err != nil {
			return nil, err
		}

		out = append(out, v...)
	}

	return out, nil
}

// ForEach returns new handler over []T with applied handle function to every element.
func ForEach[T any](handle HandlerFunc[T]) HandlerFunc[[]T] {
	fn := func(ctx context.Context, in []T) (out []T, err error) {
		for i, v := range in {
			in[i], err = handle(ctx, v)
			if err != nil {
				return out, err
			}
		}

		out = in

		return out, nil
	}

	return fn
}
