package postproc

import (
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type OutputFile struct {
	Name string
	Path string
	Size int64
}

// Process repairs (par2) and extracts (unrar) a downloaded usenet payload.
// passwords is the ordered list of candidate archive passwords to try (see
// PasswordCandidates); empty/nil means the release isn't encrypted.
func Process(dir string, passwords []string, jobName ...string) ([]OutputFile, error) {
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

	// Step 2b: Detect obfuscated RARs by magic bytes (e.g. hash.01, hash.02).
	renameObfuscatedRarsByMagic(dir)

	// Step 3: RAR extraction if rar files exist.
	if rarFile := findFirstRar(dir); rarFile != "" {
		inputSize := dirSize(dir)

		slog.Info("extracting rar", "file", rarFile, "password_candidates", len(passwords))
		if err := runUnrar(rarFile, dir, passwords); err != nil {
			return nil, fmt.Errorf("unrar: %w", err)
		}

		outputSize := dirSize(dir)
		if inputSize > 0 && outputSize > inputSize*20 {
			return nil, fmt.Errorf("decompression bomb detected: %dMB -> %dMB", inputSize/1e6, outputSize/1e6)
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

func isObfuscatedName(name string) bool {
	base := strings.TrimSuffix(name, filepath.Ext(name))
	if len(base) < 12 {
		return false
	}

	// Check 1: no word separators (spaces, dots, hyphens, underscores) in a long string.
	// Real media names like "Avengers.Infinity.War.2018.1080p" have dots/spaces.
	// Obfuscated names like "dQQ2uEXRNkeSPdX23RJ12y" don't.
	if len(base) >= 16 && !strings.ContainsAny(base, " .-_") {
		return true
	}

	// Check 2: hex hash (>80% hex chars).
	hexCount := 0
	for _, c := range base {
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
			hexCount++
		}
	}
	if len(base) >= 16 && float64(hexCount)/float64(len(base)) > 0.8 {
		return true
	}

	// Check 3: low vowel ratio (random strings ~27% vowels, English ~38%).
	vowels := 0
	letters := 0
	for _, c := range strings.ToLower(base) {
		if c >= 'a' && c <= 'z' {
			letters++
			if c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u' {
				vowels++
			}
		}
	}
	if letters > 10 && !strings.ContainsAny(base, " ") && float64(vowels)/float64(letters) < 0.15 {
		return true
	}

	return false
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

// runUnrar extracts rarFile, trying no password first (handles unencrypted
// archives) then each candidate in order. A wrong password fails fast — unrar
// rejects it at the archive header before writing files — so trying a few is
// cheap and safe.
func runUnrar(rarFile, outputDir string, passwords []string) error {
	var lastErr error
	for _, pw := range append([]string{""}, passwords...) {
		if err := unrarOnce(rarFile, outputDir, pw); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

func unrarOnce(rarFile, outputDir, password string) error {
	// -p<pw> supplies the archive password; -p- disables the interactive prompt
	// so an encrypted archive with a wrong/absent password fails fast instead of
	// hanging/aborting on the prompt (the old behavior, exit 255).
	args := []string{"e", "-o+", "-y"}
	if password != "" {
		args = append(args, "-p"+password)
	} else {
		args = append(args, "-p-")
	}
	args = append(args, rarFile, outputDir+"/")
	cmd := exec.Command("unrar", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %w", strings.TrimSpace(string(output)), err)
	}
	return nil
}

// PasswordCandidates builds the ordered, deduped list of passwords to try for a
// usenet archive: the NZB <meta type="password"> (the standard, most reliable
// source) first, then any password embedded in the names ({{pw}} or
// password=pw, which some indexers use). Empties and dupes are dropped.
func PasswordCandidates(metaPassword string, names ...string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(p string) {
		p = strings.TrimSpace(p)
		if p == "" || seen[p] {
			return
		}
		seen[p] = true
		out = append(out, p)
	}
	add(metaPassword)
	for _, n := range names {
		add(passwordFromName(n))
	}
	return out
}

var pwBraceRe = regexp.MustCompile(`\{\{([^}]+)\}\}`)
var pwEqRe = regexp.MustCompile(`(?i)password=([^\s&}]+)`)

// passwordFromName pulls a password embedded in an NZB/job name, e.g.
// "My.Movie {{secret}}.nzb" or "My.Movie password=secret.nzb".
func passwordFromName(name string) string {
	if m := pwBraceRe.FindStringSubmatch(name); m != nil {
		return strings.TrimSpace(m[1])
	}
	if m := pwEqRe.FindStringSubmatch(name); m != nil {
		return strings.TrimSpace(m[1])
	}
	return ""
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

// renameObfuscatedRarsByMagic detects RAR files that have no .rar extension
// (e.g. hash.01, hash.02) by reading file headers and volume numbers, then
// renames them to archive.partNNN.rar so unrar can chain them correctly.
func renameObfuscatedRarsByMagic(dir string) {
	entries, _ := os.ReadDir(dir)

	for _, e := range entries {
		name := strings.ToLower(e.Name())
		if strings.HasSuffix(name, ".rar") {
			return
		}
	}

	// Collect non-par2 files that have RAR magic bytes, reading volume number from header.
	type rarFile struct {
		name   string
		path   string
		volNum int
	}
	var rars []rarFile
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := strings.ToLower(e.Name())
		if strings.HasSuffix(name, ".par2") || strings.HasSuffix(name, ".nzb") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		volNum := rarVolumeNumber(path)
		if volNum >= 0 {
			rars = append(rars, rarFile{name: e.Name(), path: path, volNum: volNum})
		}
	}

	if len(rars) == 0 {
		return
	}

	slog.Info("detected obfuscated RAR volumes by header", "count", len(rars))

	sort.Slice(rars, func(i, j int) bool {
		vi, vj := rars[i].volNum, rars[j].volNum
		if vi >= 0 && vj >= 0 && vi != vj {
			return vi < vj
		}
		return extractTrailingNumber(rars[i].name) < extractTrailingNumber(rars[j].name)
	})

	for i, r := range rars {
		newName := fmt.Sprintf("archive.part%03d.rar", i+1)
		newPath := filepath.Join(dir, newName)
		if err := os.Rename(r.path, newPath); err != nil {
			slog.Warn("rename obfuscated rar failed", "from", r.name, "to", newName, "err", err)
		}
	}
}

func dirSize(dir string) int64 {
	var total int64
	filepath.Walk(dir, func(_ string, info os.FileInfo, _ error) error {
		if info != nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

func extractTrailingNumber(name string) int {
	ext := filepath.Ext(name)
	if ext == "" {
		return 0
	}
	ext = ext[1:]
	n := 0
	for _, c := range ext {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		} else {
			return 0
		}
	}
	return n
}

func rarVolumeNumber(path string) int {
	f, err := os.Open(path)
	if err != nil {
		return -1
	}
	defer f.Close()

	buf := make([]byte, 64)
	n, _ := f.Read(buf)
	if n < 12 {
		return -1
	}

	// Check RAR5 signature: Rar!\x1a\x07\x01\x00
	if buf[0] == 'R' && buf[1] == 'a' && buf[2] == 'r' && buf[3] == '!' &&
		buf[4] == 0x1a && buf[5] == 0x07 && buf[6] == 0x01 && buf[7] == 0x00 {
		return rar5VolumeNumber(buf[8:n])
	}

	// Check RAR4 signature: Rar!\x1a\x07\x00
	if buf[0] == 'R' && buf[1] == 'a' && buf[2] == 'r' && buf[3] == '!' &&
		buf[4] == 0x1a && buf[5] == 0x07 && buf[6] == 0x00 {
		return rar4VolumeNumber(buf[7:n])
	}

	return -1
}

// rar5VolumeNumber parses RAR5 main archive header to extract volume number.
func rar5VolumeNumber(data []byte) int {
	if len(data) < 4 {
		return 0
	}
	off := 4
	headerSize, n := readVint(data[off:])
	if n == 0 || headerSize == 0 {
		return 0
	}
	off += n
	headerType, n := readVint(data[off:])
	if n == 0 {
		return 0
	}
	off += n
	if headerType != 1 {
		return 0
	}
	headerFlags, n := readVint(data[off:])
	if n == 0 {
		return 0
	}
	off += n
	if headerFlags&0x0001 != 0 {
		_, n = readVint(data[off:])
		off += n
	}
	if headerFlags&0x0002 != 0 {
		_, n = readVint(data[off:])
		off += n
	}
	if off >= len(data) {
		return 0
	}
	archiveFlags, n := readVint(data[off:])
	if n == 0 {
		return 0
	}
	off += n
	if archiveFlags&0x0002 != 0 && off < len(data) {
		volNum, _ := readVint(data[off:])
		return int(volNum)
	}
	return 0
}

func rar4VolumeNumber(data []byte) int {
	if len(data) < 14 {
		return 0
	}
	mainHead := data[7:]
	if len(mainHead) < 7 {
		return 0
	}
	flags := uint16(mainHead[3]) | uint16(mainHead[4])<<8
	isVolume := flags&0x0001 != 0
	isFirstVolume := flags&0x0100 != 0
	if !isVolume {
		return 0
	}
	if isFirstVolume {
		return 0
	}
	return 1
}

func readVint(data []byte) (uint64, int) {
	var val uint64
	for i, b := range data {
		if i >= 10 {
			return 0, 0
		}
		val |= uint64(b&0x7f) << (7 * uint(i))
		if b&0x80 == 0 {
			return val, i + 1
		}
	}
	return 0, 0
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
	cleanDir := filepath.Clean(dir)
	var files []OutputFile
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if !strings.HasPrefix(filepath.Clean(path), cleanDir+string(os.PathSeparator)) {
			slog.Warn("skipping file outside output dir", "path", path)
			return nil
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
