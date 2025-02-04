package commands

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tevino/abool"
	"github.com/urfave/cli"

	"gitlab.com/gitlab-org/gitlab-runner/common"
	"gitlab.com/gitlab-org/gitlab-runner/network"
)

type RunSingleCommand struct {
	common.RunnerConfig
	network          common.Network
	WaitTimeout      int `long:"wait-timeout" description:"How long to wait in seconds before receiving the first job"`
	lastBuild        time.Time
	runForever       bool
	MaxBuilds        int `long:"max-builds" description:"How many builds to process before exiting"`
	finished         *abool.AtomicBool
	interruptSignals chan os.Signal
}

func waitForInterrupts(finished *abool.AtomicBool, abortSignal chan os.Signal, doneSignal chan int, interruptSignals chan os.Signal) {
	if interruptSignals == nil {
		interruptSignals = make(chan os.Signal)
	}
	signal.Notify(interruptSignals, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	interrupt := <-interruptSignals
	if finished != nil {
		finished.Set()
	}

	// request stop, but wait for force exit
	for interrupt == syscall.SIGQUIT {
		log.Warningln("Requested quit, waiting for builds to finish")
		interrupt = <-interruptSignals
	}

	log.Warningln("Requested exit:", interrupt)

	go func() {
		for {
			abortSignal <- interrupt
		}
	}()

	select {
	case newSignal := <-interruptSignals:
		log.Fatalln("forced exit:", newSignal)
	case <-time.After(common.ShutdownTimeout * time.Second):
		log.Fatalln("shutdown timed out")
	case <-doneSignal:
	}
}

// Things to do after a build
func (r *RunSingleCommand) postBuild() {
	if r.MaxBuilds > 0 {
		r.MaxBuilds--
	}
	r.lastBuild = time.Now()
}

func (r *RunSingleCommand) processBuild(data common.ExecutorData, abortSignal chan os.Signal) (err error) {
	jobData, healthy := r.network.RequestJob(r.RunnerConfig, nil)
	if !healthy {
		log.Println("Runner is not healthy!")
		select {
		case <-time.After(common.NotHealthyCheckInterval * time.Second):
		case <-abortSignal:
		}
		return
	}

	if jobData == nil {
		select {
		case <-time.After(common.CheckInterval):
		case <-abortSignal:
		}
		return
	}

	config := common.NewConfig()
	newBuild, err := common.NewBuild(*jobData, &r.RunnerConfig, abortSignal, data)
	if err != nil {
		return
	}

	jobCredentials := &common.JobCredentials{
		ID:    jobData.ID,
		Token: jobData.Token,
	}
	trace := r.network.ProcessJob(r.RunnerConfig, jobCredentials)
	defer trace.Fail(err, common.NoneFailure)

	err = newBuild.Run(config, trace)

	r.postBuild()

	return
}

func (r *RunSingleCommand) checkFinishedConditions() {
	if r.MaxBuilds < 1 && !r.runForever {
		log.Println("This runner has processed its build limit, so now exiting")
		r.finished.Set()
	}
	if r.WaitTimeout > 0 && int(time.Since(r.lastBuild).Seconds()) > r.WaitTimeout {
		log.Println("This runner has not received a job in", r.WaitTimeout, "seconds, so now exiting")
		r.finished.Set()
	}
	return
}

func (r *RunSingleCommand) Execute(c *cli.Context) {
	if len(r.URL) == 0 {
		log.Fatalln("Missing URL")
	}
	if len(r.Token) == 0 {
		log.Fatalln("Missing Token")
	}
	if len(r.Executor) == 0 {
		log.Fatalln("Missing Executor")
	}

	executorProvider := common.GetExecutor(r.Executor)
	if executorProvider == nil {
		log.Fatalln("Unknown executor:", r.Executor)
	}

	log.Println("Starting runner for", r.URL, "with token", r.ShortDescription(), "...")

	r.finished = abool.New()
	abortSignal := make(chan os.Signal)
	doneSignal := make(chan int, 1)
	r.runForever = r.MaxBuilds == 0

	go waitForInterrupts(r.finished, abortSignal, doneSignal, r.interruptSignals)

	r.lastBuild = time.Now()

	for !r.finished.IsSet() {
		data, err := executorProvider.Acquire(&r.RunnerConfig)
		if err != nil {
			log.Warningln("Executor update:", err)
		}

		pErr := r.processBuild(data, abortSignal)
		if pErr != nil {
			log.WithError(pErr).Error("Failed to process build")
		}

		r.checkFinishedConditions()
		executorProvider.Release(&r.RunnerConfig, data)
	}

	doneSignal <- 0
}

func init() {
	common.RegisterCommand2("run-single", "start single runner", &RunSingleCommand{
		network: network.NewGitLabClient(),
	})
}
