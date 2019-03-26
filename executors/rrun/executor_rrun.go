package rrun

import (
	"context"
	"fmt"
	"os"

	"runtime"
	"time"

	"github.com/kardianos/osext"
	log "github.com/sirupsen/logrus"

	"gitlab.com/gitlab-org/gitlab-runner/common"
	"gitlab.com/gitlab-org/gitlab-runner/executors"
	"go.megvii-inc.com/brain/platform/service/scheduler/workspace"
	"go.megvii-inc.com/brain/platform/service/scheduler/workspaceproto"

	"google.golang.org/grpc"
)

type executor struct {
	executors.AbstractExecutor
	client workspaceproto.RRunClient
	ctx    context.Context
}

func (s *executor) Prepare(options common.ExecutorPrepareOptions) error {

	// create context
	conn, err := grpc.Dial("scheduler.i.brainpp.cn:8081", grpc.WithInsecure())
	if err != nil {
		log.WithError(err).Fatal("dial sheduler")
	}
	s.client = workspaceproto.NewRRunClient(conn)
	s.ctx = context.Background()

	if options.User != "" {
		s.Shell().User = options.User
	}

	// expand environment variables to have current directory
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("Getwd: %v", err)
	}

	mapping := func(key string) string {
		switch key {
		case "PWD":
			return wd
		default:
			return ""
		}
	}

	s.DefaultBuildsDir = os.Expand(s.DefaultBuildsDir, mapping)
	s.DefaultCacheDir = os.Expand(s.DefaultCacheDir, mapping)

	// Pass control to executor
	err = s.AbstractExecutor.Prepare(options)
	if err != nil {
		return err
	}

	s.Println("Using RRun executor...")
	return nil
}

/*func (s *executor) killAndWait(cmd *exec.Cmd, waitCh chan error) error {
	for {
		s.Debugln("Aborting command...")
		helpers.KillProcessGroup(cmd)
		select {
		case <-time.After(time.Second):
		case err := <-waitCh:
			return err
		}
	}
}*/

func (e *executor) Finish(err error) {
	e.currentStage = common.ExecutorStageFinish
}

func (s *executor) Run(cmd common.ExecutorCommand) error {

	spec := &workspaceproto.RunnerSpec{
		Name:           "gitlab-runner",
		Rootfs:         "", // Inherit from master
		Commands:       []string{"bash", "-c", "while true; do echo hello; done"},
		WorkDir:        "/home/me",
		Uid:            10010,                   // Get it with the "id" command
		Gid:            10010,                   // Get it with the "id" command
		AdditionalGids: []uint32{10010, 27, 44}, // Get it with the "id" command
		Environments: map[string]string{
			"PATH": "/bin:/usr/bin:/usr/local/bin",
		},
		ShareDirs: []*workspace.ShareDirSpec{
			&workspace.ShareDirSpec{Namespace: "me", Dir: "@nfshome", Writable: true},
		},
		LogDir:    "/home/me/logs",
		EnableSsh: false,
		Labels: map[string]string{
			"whatever":  "you like",
			"arbitrary": "strings",
		},
		SchedulingHint: &workspace.SchedulingHint{
			Group:        "", // Inherit from master
			PositiveTags: []string{},
			NegativeTags: []string{},
		},
		Resources: &workspace.Resources{
			Cpu:        1,
			Gpu:        0,
			MemoryInMb: 1024,
			Ssd:        0,
		},
		ChargedGroup:    "", // Inherit from master
		Preemptible:     true,
		Priority:        "Low", // Default: Medium. Change in need
		MaxWaitTime:     int64(time.Hour),
		MinimumLifetime: int64(24 * time.Hour),
	}
	resp, err := s.client.StartRunner(s.ctx, &workspaceproto.StartRunnerRequest{Spec: spec})
	if err != nil {
		log.WithError(err).Fatal("start runner failed")
	}
	fmt.Println(resp.Id)

	/*
		// Create execution command
		c := exec.Command(s.BuildShell.Command, s.BuildShell.Arguments...)
		if c == nil {
			return errors.New("Failed to generate execution command")
		}

		helpers.SetProcessGroup(c)
		defer helpers.KillProcessGroup(c)

		// Fill process environment variables
		c.Env = append(os.Environ(), s.BuildShell.Environment...)
		c.Stdout = s.Trace
		c.Stderr = s.Trace

		if s.BuildShell.PassFile {
			scriptDir, err := ioutil.TempDir("", "build_script")
			if err != nil {
				return err
			}
			defer os.RemoveAll(scriptDir)

			scriptFile := filepath.Join(scriptDir, "script."+s.BuildShell.Extension)
			err = ioutil.WriteFile(scriptFile, []byte(cmd.Script), 0700)
			if err != nil {
				return err
			}

			c.Args = append(c.Args, scriptFile)
		} else {
			c.Stdin = bytes.NewBufferString(cmd.Script)
		}

		// Start a process
		err := c.Start()
		if err != nil {
			return fmt.Errorf("Failed to start process: %s", err)
		}

		// Wait for process to finish
		waitCh := make(chan error)
		go func() {
			err := c.Wait()
			if _, ok := err.(*exec.ExitError); ok {
				err = &common.BuildError{Inner: err}
			}
			waitCh <- err
		}()

		// Support process abort
		select {
		case err = <-waitCh:
			return err

		case <-cmd.Context.Done():
			return s.killAndWait(c, waitCh)
		}
	*/
}

func init() {
	// Look for self
	runnerCommand, err := osext.Executable()
	if err != nil {
		logrus.Warningln(err)
	}

	options := executors.ExecutorOptions{
		DefaultBuildsDir: "$PWD/builds",
		DefaultCacheDir:  "$PWD/cache",
		SharedBuildsDir:  true,
		Shell: common.ShellScriptInfo{
			Shell:         common.GetDefaultShell(),
			Type:          common.LoginShell,
			RunnerCommand: runnerCommand,
		},
		ShowHostname: false,
	}

	creator := func() common.Executor {
		return &executor{
			AbstractExecutor: executors.AbstractExecutor{
				ExecutorOptions: options,
			},
		}
	}

	featuresUpdater := func(features *common.FeaturesInfo) {
		features.Variables = true
		features.Shared = true

		if runtime.GOOS != "windows" {
			features.Session = true
			features.Terminal = true
		}
	}

	common.RegisterExecutor("rrun", executors.DefaultExecutorProvider{
		Creator:          creator,
		FeaturesUpdater:  featuresUpdater,
		DefaultShellName: options.Shell.Shell,
	})
}
