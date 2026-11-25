package datapipe

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"regexp"

	"github.com/google/uuid"
)

const maxManifestBytes = 1024 * 1024

var gitCommitPattern = regexp.MustCompile("^[0-9a-f]{40}([0-9a-f]{24})?$")

type ioField = map[string]any

type ioDeclarations map[string]ioField

type manifestIO struct {
	Input  ioDeclarations `json:"input"`
	Output ioDeclarations `json:"output"`
}

type manifestSource struct {
	RepoUUID uuid.UUID `json:"repo_uuid"`
	Commit   string    `json:"commit"`
	Content  []byte    `json:"content"`
}

func readGitManifest(ctx context.Context, repoCache string, version manifestVersion) ([]byte, error) {
	repoPath := filepath.Join(repoCache, version.repoUUID.String())
	cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "cat-file", "blob", version.commit+":unit.yml")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	content, readErr := io.ReadAll(io.LimitReader(stdout, maxManifestBytes+1))
	if readErr != nil || len(content) > maxManifestBytes {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if readErr != nil {
			return nil, readErr
		}
		return nil, fmt.Errorf("unit.yml exceeds %d bytes", maxManifestBytes)
	}
	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("read unit.yml from local Git: %w: %s", err, bytes.TrimSpace(stderr.Bytes()))
	}
	return content, nil
}
