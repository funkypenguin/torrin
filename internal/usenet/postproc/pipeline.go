package postproc

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type OutputFile struct {
	Name string
	Path string
	Size int64
}

func Process(dir string, jobName ...string) ([]OutputFile, error) {
	// Step 1: PAR2 repair if par2 files exist.
	if par2File := findFile(dir, ".par2", "vol"); par2File != "" {
		slog.Info("running par2 repair", "dir", dir)
		if err := runPar2(par2File); err != nil {
			slog.Warn("par2 repair failed", "err", err)
			// Non-fatal: files might still be ok without repair.
		}
	}

	// Step 2: Rename obfuscated RAR parts to consistent base name.
	renameObfuscatedRars(dir)

	// Step 3: RAR extraction if rar files exist.
	if rarFile := findFirstRar(dir); rarFile != "" {
		slog.Info("extracting rar", "file", rarFile)
		if err := runUnrar(rarFile, dir); err != nil {
			return nil, fmt.Errorf("unrar: %w", err)
		}
		// Clean up archive files after successful extraction.
		cleanArchives(dir)
	}

	// Step 4: Collect output files.
	files, err := collectFiles(dir)
	if err != nil {
		return nil, err
	}

	// Step 5: Rename obfuscated output files to job name.
	name := ""
	if len(jobName) > 0 {
		name = jobName[0]
	}
	if name != "" {
		for i, f := range files {
			if isObfuscatedName(f.Name) {
				ext := filepath.Ext(f.Name)
				newName := sanitizeFilename(name) + ext
				newPath := filepath.Join(filepath.Dir(f.Path), newName)
				if err := os.Rename(f.Path, newPath); err == nil {
					slog.Info("renamed obfuscated file", "from", f.Name, "to", newName)
					files[i].Name = newName
					files[i].Path = newPath
				}
			}
		}
	}

	return files, nil
}

// renameObfuscatedRars detects RAR parts with different obfuscated base names
// and renames them to a consistent name so unrar can find all volumes.
func renameObfuscatedRars(dir string) {
	entries, _ := os.ReadDir(dir)

	// Collect all .partNN.rar files.
	type rarPart struct {
		name    string
		partNum string
	}
	var parts []rarPart

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		lower := strings.ToLower(name)
		// Match .part01.rar, .part02.rar, etc.
		idx := strings.Index(lower, ".part")
		if idx < 0 || !strings.HasSuffix(lower, ".rar") {
			continue
		}
		partNum := lower[idx:] // e.g. ".part01.rar"
		parts = append(parts, rarPart{name: name, partNum: partNum})
	}

	if len(parts) < 2 {
		return // single file or no parts, nothing to rename
	}

	// Check if all parts share the same base name.
	baseName := strings.ToLower(parts[0].name[:strings.Index(strings.ToLower(parts[0].name), ".part")])
	allSame := true
	for _, p := range parts[1:] {
		b := strings.ToLower(p.name[:strings.Index(strings.ToLower(p.name), ".part")])
		if b != baseName {
			allSame = false
			break
		}
	}

	if allSame {
		return // already consistent
	}

	// Rename all parts to "archive.partNN.rar".
	slog.Info("renaming obfuscated RAR parts", "count", len(parts))
	for _, p := range parts {
		newName := "archive" + p.partNum
		oldPath := filepath.Join(dir, p.name)
		newPath := filepath.Join(dir, newName)
		if err := os.Rename(oldPath, newPath); err != nil {
			slog.Warn("rename failed", "from", p.name, "to", newName, "err", err)
		}
	}
}

// isObfuscatedName returns true if a filename looks like a random hash.
func isObfuscatedName(name string) bool {
	base := strings.TrimSuffix(name, filepath.Ext(name))
	if len(base) < 16 {
		return false
	}
	hexCount := 0
	for _, c := range base {
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
			hexCount++
		}
	}
	return float64(hexCount)/float64(len(base)) > 0.8
}

// sanitizeFilename replaces characters that aren't safe for filenames.
func sanitizeFilename(name string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", "\"", "", "<", "", ">", "", "|", "", "?", "", "*", "")
	return replacer.Replace(name)
}

func findFile(dir, ext, excludeSubstr string) string {
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := strings.ToLower(e.Name())
		if strings.HasSuffix(name, ext) {
			if excludeSubstr != "" && strings.Contains(name, excludeSubstr) {
				continue
			}
			return filepath.Join(dir, e.Name())
		}
	}
	return ""
}

// findFirstRar finds the first .rar file (or .r00/.part01.rar for split archives).
func findFirstRar(dir string) string {
	entries, _ := os.ReadDir(dir)

	// Prefer .part01.rar or .part001.rar.
	for _, e := range entries {
		name := strings.ToLower(e.Name())
		if strings.Contains(name, ".part01.rar") || strings.Contains(name, ".part001.rar") {
			return filepath.Join(dir, e.Name())
		}
	}

	// Fall back to first .rar file.
	for _, e := range entries {
		if strings.HasSuffix(strings.ToLower(e.Name()), ".rar") {
			return filepath.Join(dir, e.Name())
		}
	}
	return ""
}

func runPar2(par2File string) error {
	cmd := exec.Command("par2", "repair", par2File)
	cmd.Dir = filepath.Dir(par2File)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %w", strings.TrimSpace(string(output)), err)
	}
	return nil
}

func runUnrar(rarFile, outputDir string) error {
	cmd := exec.Command("unrar", "x", "-o+", "-y", rarFile, outputDir+"/")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %w", strings.TrimSpace(string(output)), err)
	}
	return nil
}

func cleanArchives(dir string) {
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		name := strings.ToLower(e.Name())
		isArchive := strings.HasSuffix(name, ".rar") ||
			strings.HasSuffix(name, ".par2") ||
			strings.HasSuffix(name, ".nzb") ||
			isRarPart(name)
		if isArchive {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

// isRarPart matches .r00, .r01, etc.
func isRarPart(name string) bool {
	if len(name) < 4 {
		return false
	}
	ext := name[len(name)-4:]
	if ext[0] != '.' || ext[1] != 'r' {
		return false
	}
	return ext[2] >= '0' && ext[2] <= '9' && ext[3] >= '0' && ext[3] <= '9'
}

func collectFiles(dir string) ([]OutputFile, error) {
	var files []OutputFile
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		name := strings.ToLower(info.Name())
		if strings.HasSuffix(name, ".par2") || strings.HasSuffix(name, ".nzb") {
			return nil
		}
		if strings.HasSuffix(name, ".rar") || isRarPart(name) {
			return nil
		}
		files = append(files, OutputFile{
			Name: info.Name(),
			Path: path,
			Size: info.Size(),
		})
		return nil
	})
	return files, err
}
